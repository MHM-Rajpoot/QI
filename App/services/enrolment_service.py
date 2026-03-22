"""
Enrolment Service
Handles all enrolment data queries and aggregations
"""

import pandas as pd
from db.snowflake import get_db
from utils.filtering import (
    academic_year_start,
    academic_year_start_sql,
    build_shared_sql_conditions,
    location_sql,
    normalize_provider_id,
    normalize_shared_filters,
)


class EnrolmentService:
    """Service for enrolment data operations"""

    def __init__(self, config_file):
        self.db = get_db(config_file)

    @staticmethod
    def _escape_sql_literal(value):
        """Escape single quotes for SQL literal usage."""
        return str(value).replace("'", "''")

    @staticmethod
    def _normalize_year(value):
        """Normalize a year-like value to int or None."""
        normalized = normalize_shared_filters(start_year=value)
        return normalized["start_year"]

    @staticmethod
    def _normalize_location(location):
        """Normalize a location filter value."""
        normalized = normalize_shared_filters(location=location)
        return normalized["location"]

    @classmethod
    def _normalize_filters(cls, start_year=None, end_year=None, location=None):
        """Normalize shared dashboard filters."""
        normalized = normalize_shared_filters(
            start_year=start_year,
            end_year=end_year,
            location=location,
        )
        return normalized["start_year"], normalized["end_year"], normalized["location"]

    @staticmethod
    def _academic_year_start_sql(year_column='ay.ACADEMIC_YEAR'):
        """SQL expression to convert academic year labels to start year integers."""
        return academic_year_start_sql(year_column)

    @staticmethod
    def _location_sql(location_column='p.CONTACT_TOWN'):
        """SQL expression for normalized provider town/city."""
        return location_sql(location_column)

    @classmethod
    def _academic_year_start(cls, academic_year):
        """Convert academic year strings like 23/24 or 2023/24 to start year int."""
        return academic_year_start(academic_year)

    def _build_common_conditions(
        self,
        start_year=None,
        end_year=None,
        location=None,
        year_column='ay.ACADEMIC_YEAR',
        location_column='p.CONTACT_TOWN'
    ):
        """Build shared SQL conditions for year range and location filters."""
        conditions, _ = build_shared_sql_conditions(
            start_year=start_year,
            end_year=end_year,
            location=location,
            year_column=year_column,
            location_column=location_column,
        )
        return conditions

    def _normalize_provider_id(self, provider_id):
        """Validate and normalize provider id from request/query params."""
        return normalize_provider_id(provider_id)

    def get_available_years(self, location=None):
        """Get distinct academic year options for shared filters."""
        conditions = self._build_common_conditions(location=location)

        query = """
        SELECT DISTINCT
            ay.ACADEMIC_YEAR
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += " ORDER BY ay.ACADEMIC_YEAR"

        df = self.db.execute_query(query)
        options = []
        seen = set()

        for academic_year in df.get('ACADEMIC_YEAR', pd.Series(dtype=object)).tolist():
            start = self._academic_year_start(academic_year)
            label = str(academic_year).strip()
            if start is None or not label or start in seen:
                continue
            seen.add(start)
            options.append({'value': start, 'label': label})

        options.sort(key=lambda item: item['value'])
        return options

    def get_available_locations(self, start_year=None, end_year=None):
        """Get distinct town/city options for shared filters."""
        conditions = self._build_common_conditions(start_year=start_year, end_year=end_year)

        query = f"""
        SELECT DISTINCT
            {self._location_sql('p.CONTACT_TOWN')} AS LOCATION
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += " ORDER BY LOCATION"

        df = self.db.execute_query(query)
        return [str(value).strip() for value in df.get('LOCATION', pd.Series(dtype=object)).tolist() if str(value).strip()]

    def get_enrolment_trends(self, start_year=None, end_year=None, location=None):
        """Get historical enrolment trends by academic year."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS TOTAL_LEARNERS,
            SUM(fe.ENROLMENT_COUNT) AS TOTAL_ENROLMENTS
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR
        ORDER BY ay.ACADEMIC_YEAR
        """
        return self.db.execute_query(query)

    def get_enrolment_by_provider(self, provider_id=None, start_year=None, end_year=None, location=None):
        """Get enrolment by provider/college."""
        provider_key = self._normalize_provider_id(provider_id)
        conditions = self._build_common_conditions(start_year, end_year, location)
        if provider_key is not None:
            conditions.append(f"p.PROVIDER_SKEY = {provider_key}")

        query = f"""
        SELECT
            ay.ACADEMIC_YEAR,
            p.PROVIDER_SKEY,
            p.PROVIDER_NAME,
            {self._location_sql('p.CONTACT_TOWN')} AS LOCATION,
            p.COLLEGE_TYPE,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT,
            SUM(fe.ENROLMENT_COUNT) AS ENROLMENT_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR, p.PROVIDER_SKEY, p.PROVIDER_NAME, LOCATION, p.COLLEGE_TYPE
        ORDER BY ay.ACADEMIC_YEAR, p.PROVIDER_NAME
        """
        return self.db.execute_query(query)

    def get_enrolment_by_ssa(self, start_year=None, end_year=None, location=None):
        """Get enrolment by Sector Subject Area (Tier 1 only)."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            ssa.SSA_TIER_1_DESCRIPTION AS SSA_TIER1,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT,
            SUM(fe.ENROLMENT_COUNT) AS ENROLMENT_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR, ssa.SSA_TIER_1_DESCRIPTION
        ORDER BY ay.ACADEMIC_YEAR, ssa.SSA_TIER_1_DESCRIPTION
        """
        return self.db.execute_query(query)

    def get_enrolment_by_age(self, start_year=None, end_year=None, location=None):
        """Get enrolment by age group."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            a.AGE_GROUP_FES AS AGE_GROUP,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        JOIN PRESENTATION.DIM_AGE a ON fe.AGE_SKEY = a.AGE_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR, a.AGE_GROUP_FES
        ORDER BY ay.ACADEMIC_YEAR, a.AGE_GROUP_FES
        """
        return self.db.execute_query(query)

    def get_enrolment_by_level(self, start_year=None, end_year=None, location=None):
        """Get enrolment by qualification level."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            l.LEVEL_DESCRIPTION AS LEVEL_DESC,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        JOIN PRESENTATION.DIM_LEVEL l ON fe.LEVEL_SKEY = l.LEVEL_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR, l.LEVEL_DESCRIPTION
        ORDER BY ay.ACADEMIC_YEAR, l.LEVEL_DESCRIPTION
        """
        return self.db.execute_query(query)

    def get_providers_list(self, start_year=None, end_year=None, location=None):
        """Get list of providers with data in the selected filter range."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        query = f"""
        SELECT
            p.PROVIDER_SKEY,
            p.PROVIDER_NAME,
            p.COLLEGE_TYPE,
            {self._location_sql('p.CONTACT_TOWN')} AS LOCATION
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY p.PROVIDER_SKEY, p.PROVIDER_NAME, p.COLLEGE_TYPE, LOCATION
        ORDER BY p.PROVIDER_NAME
        """
        return self.db.execute_query(query)

    def get_ssa_list(self):
        """Get list of all SSA categories."""
        query = """
        SELECT DISTINCT
            ssa.SSA_TIER_1_DESCRIPTION AS SSA_TIER1,
            ssa.SSA_TIER_2_DESCRIPTION AS SSA_TIER2
        FROM PRESENTATION.DIM_SSA ssa
        ORDER BY ssa.SSA_TIER_1_DESCRIPTION, ssa.SSA_TIER_2_DESCRIPTION
        """
        return self.db.execute_query(query)

    def get_funding_schemes(self, provider_id=None, start_year=None, end_year=None, location=None):
        """Get funding schemes optionally filtered by provider, year range, or location."""
        provider_key = self._normalize_provider_id(provider_id)
        conditions = self._build_common_conditions(start_year, end_year, location)
        if provider_key is not None:
            conditions.append(f"fe.PROVIDER_SKEY = {provider_key}")

        query = """
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(ld.FUNDING_MODEL), ''), 'Unknown') AS FUNDING_SCHEME
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_DELIVERY ld
            ON fe.LEARNING_DELIVERY_SKEY = ld.LEARNING_DELIVERY_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        ORDER BY FUNDING_SCHEME
        """
        return self.db.execute_query(query)

    def get_time_series_data(self, group_by='total', start_year=None, end_year=None, location=None):
        """Get time series data for forecasting."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        if group_by == 'provider':
            query = """
            SELECT
                ay.ACADEMIC_YEAR,
                p.PROVIDER_SKEY,
                p.PROVIDER_NAME,
                COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
            FROM PRESENTATION.FACT_ENROLMENT fe
            JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
            JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
            """
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
            query += """
            GROUP BY ay.ACADEMIC_YEAR, p.PROVIDER_SKEY, p.PROVIDER_NAME
            ORDER BY ay.ACADEMIC_YEAR
            """
        elif group_by == 'ssa':
            query = """
            SELECT
                ay.ACADEMIC_YEAR,
                ssa.SSA_TIER_1_DESCRIPTION AS SSA_TIER1,
                COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
            FROM PRESENTATION.FACT_ENROLMENT fe
            JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
            JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
            JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
            """
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
            query += """
            GROUP BY ay.ACADEMIC_YEAR, ssa.SSA_TIER_1_DESCRIPTION
            ORDER BY ay.ACADEMIC_YEAR
            """
        else:
            query = """
            SELECT
                ay.ACADEMIC_YEAR,
                COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
            FROM PRESENTATION.FACT_ENROLMENT fe
            JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
            JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
            """
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
            query += """
            GROUP BY ay.ACADEMIC_YEAR
            ORDER BY ay.ACADEMIC_YEAR
            """

        df = self.db.execute_query(query)

        if 'ACADEMIC_YEAR' in df.columns:
            df['YEAR'] = df['ACADEMIC_YEAR'].apply(self._academic_year_start)
            df['YEAR'] = pd.to_numeric(df['YEAR'], errors='coerce').astype('Int64')

        return df

    def get_dashboard_summary(self, start_year=None, end_year=None, location=None):
        """Get summary statistics for the dashboard using active filters."""
        conditions = self._build_common_conditions(start_year, end_year, location)

        query_total = """
        SELECT COUNT(DISTINCT fe.LEARNER_SKEY) AS TOTAL_LEARNERS
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        """
        if conditions:
            query_total += f" WHERE {' AND '.join(conditions)}"

        query_current = """
        SELECT
            ay.ACADEMIC_YEAR,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS CURRENT_LEARNERS
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        """
        if conditions:
            query_current += f" WHERE {' AND '.join(conditions)}"
        query_current += """
        GROUP BY ay.ACADEMIC_YEAR
        ORDER BY ay.ACADEMIC_YEAR DESC
        LIMIT 1
        """

        query_providers = """
        SELECT COUNT(DISTINCT fe.PROVIDER_SKEY) AS PROVIDER_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        """
        if conditions:
            query_providers += f" WHERE {' AND '.join(conditions)}"

        total = self.db.execute_query(query_total)
        current = self.db.execute_query(query_current)
        providers = self.db.execute_query(query_providers)

        return {
            'total_learners': int(total['TOTAL_LEARNERS'].iloc[0]) if len(total) > 0 and pd.notna(total['TOTAL_LEARNERS'].iloc[0]) else 0,
            'current_year': current['ACADEMIC_YEAR'].iloc[0] if len(current) > 0 else 'N/A',
            'current_learners': int(current['CURRENT_LEARNERS'].iloc[0]) if len(current) > 0 and pd.notna(current['CURRENT_LEARNERS'].iloc[0]) else 0,
            'provider_count': int(providers['PROVIDER_COUNT'].iloc[0]) if len(providers) > 0 and pd.notna(providers['PROVIDER_COUNT'].iloc[0]) else 0
        }
