"""
Forecast Service
Handles forecast retrieval and model switching
"""

import json
import os

import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX

from db.snowflake import get_db
from utils.filtering import (
    academic_year_start,
    academic_year_start_sql,
    build_forecast_sql_conditions,
    course_sql,
    funding_sql,
    level_sql,
    location_sql,
    normalize_provider_id,
    normalize_shared_filters,
    normalize_text_arg,
    ssa_sql,
)


class ForecastService:
    """Service for forecast operations"""

    def __init__(self, config_file, model_dir='saved_models'):
        self.db = get_db(config_file)
        self.model_dir = model_dir
        self.available_models = ['arima', 'sarima', 'lstm']
        self.forecast_periods = 3

    def _normalize_provider_id(self, provider_id):
        """Validate and normalize provider id from request/query params."""
        return normalize_provider_id(provider_id)

    def _normalize_funding_scheme(self, funding_scheme):
        """Normalize funding scheme from request/query params."""
        return self._normalize_text_filter(funding_scheme)

    @staticmethod
    def _normalize_text_filter(value):
        """Normalize a free-text filter value."""
        return normalize_text_arg(value)

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
    def _escape_sql_literal(value):
        """Escape single quotes for SQL literal usage."""
        return str(value).replace("'", "''")

    @staticmethod
    def _academic_year_start(academic_year):
        """Convert academic year strings like 23/24 or 2023/24 to start year int."""
        return academic_year_start(academic_year)

    @staticmethod
    def _academic_year_start_sql(year_column='ay.ACADEMIC_YEAR'):
        """SQL expression to convert academic year labels to start year integers."""
        return academic_year_start_sql(year_column)

    @staticmethod
    def _location_sql(location_column='p.CONTACT_TOWN'):
        """SQL expression for normalized provider town/city."""
        return location_sql(location_column)

    @staticmethod
    def _funding_sql(funding_column='ld.FUNDING_MODEL'):
        """SQL expression for normalized funding scheme."""
        return funding_sql(funding_column)

    @staticmethod
    def _ssa_sql(ssa_column='ssa.SSA_TIER_1_DESCRIPTION'):
        """SQL expression for normalized SSA tier 1."""
        return ssa_sql(ssa_column)

    @staticmethod
    def _level_sql(
        level_column='l.LEVEL_DESCRIPTION',
        level_fallback_1='la.NVQ_LEVEL_V2',
        level_fallback_2='la.NVQ_LEVEL'
    ):
        """SQL expression for normalized qualification level."""
        return level_sql(level_column, level_fallback_1, level_fallback_2)

    @staticmethod
    def _course_sql(
        title_column='la.LEARN_AIM_REF_TITLE',
        programme_column='ld.PROGRAMME_TYPE',
        reference_column='ld.LEARNING_AIM_REFERENCE',
        fallback_reference_column='la.LEARN_AIM_REF'
    ):
        """SQL expression for a readable course label."""
        return course_sql(
            title_column=title_column,
            programme_column=programme_column,
            reference_column=reference_column,
            fallback_reference_column=fallback_reference_column,
        )

    @staticmethod
    def _format_academic_year(start_year):
        """Format start year int to academic year label like 26/27."""
        year = int(start_year)
        return f"{str(year)[-2:]}/{str(year + 1)[-2:]}"

    @staticmethod
    def _empty_forecast_df():
        """Return an empty DataFrame using standard forecast columns."""
        return pd.DataFrame(
            columns=[
                'ACADEMIC_YEAR',
                'YEAR',
                'PREDICTED_ENROLMENTS',
                'LOWER_CI',
                'UPPER_CI',
                'MODEL_TYPE',
                'PROVIDER_KEY',
            ]
        )

    def _build_filter_conditions(
        self,
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None,
        exclude_filters=None,
        year_column='ay.ACADEMIC_YEAR',
        location_column='p.CONTACT_TOWN'
    ):
        """Build SQL filter clauses for forecast queries."""
        return build_forecast_sql_conditions(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level,
            exclude_filters=exclude_filters,
            year_column=year_column,
            location_column=location_column,
        )

    def _get_provider_historical_series(
        self,
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None
    ):
        """Get historical annual learner counts filtered by provider/funding scheme/location/year."""
        conditions, _ = self._build_filter_conditions(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level
        )

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_DELIVERY ld ON fe.LEARNING_DELIVERY_SKEY = ld.LEARNING_DELIVERY_SKEY
        LEFT JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
        LEFT JOIN PRESENTATION.DIM_LEVEL l ON fe.LEVEL_SKEY = l.LEVEL_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_AIM la ON fe.LEARNING_AIM_SKEY = la.LEARNING_AIM_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR
        ORDER BY ay.ACADEMIC_YEAR
        """

        history = self.db.execute_query(query)
        if len(history) == 0:
            return history

        history['YEAR'] = history['ACADEMIC_YEAR'].apply(self._academic_year_start)
        history['LEARNER_COUNT'] = pd.to_numeric(history['LEARNER_COUNT'], errors='coerce')
        history = history.dropna(subset=['YEAR', 'LEARNER_COUNT']).copy()

        if len(history) == 0:
            return history

        history['YEAR'] = history['YEAR'].astype(int)
        history = history.sort_values('YEAR')
        return history

    def _naive_forecast(self, series, periods):
        """Simple fallback forecast when statistical models cannot be fitted."""
        if len(series) == 0:
            return (
                np.array([0.0] * periods, dtype=float),
                np.array([0.0] * periods, dtype=float),
                np.array([0.0] * periods, dtype=float),
            )

        last_value = float(series.iloc[-1])
        predictions = np.array([last_value] * periods, dtype=float)

        std = float(series.std(ddof=0)) if len(series) > 1 else max(abs(last_value) * 0.1, 1.0)
        if not np.isfinite(std) or std == 0:
            std = max(abs(last_value) * 0.1, 1.0)

        margin = 1.96 * std
        lower = predictions - margin
        upper = predictions + margin
        return predictions, lower, upper

    def _forecast_with_arima(self, series, periods):
        """Forecast using ARIMA, with robust fallbacks."""
        candidate_orders = [(1, 1, 1), (0, 1, 1), (1, 0, 0), (0, 0, 0)]

        for order in candidate_orders:
            try:
                model = ARIMA(series, order=order)
                fitted = model.fit()
                forecast = fitted.get_forecast(steps=periods)

                mean = np.asarray(forecast.predicted_mean, dtype=float)
                conf_int = forecast.conf_int(alpha=0.05)
                if isinstance(conf_int, pd.DataFrame):
                    lower = np.asarray(conf_int.iloc[:, 0], dtype=float)
                    upper = np.asarray(conf_int.iloc[:, 1], dtype=float)
                else:
                    lower = np.asarray(conf_int[:, 0], dtype=float)
                    upper = np.asarray(conf_int[:, 1], dtype=float)

                return mean, lower, upper
            except Exception:
                continue

        return self._naive_forecast(series, periods)

    def _forecast_with_sarima(self, series, periods):
        """Forecast using SARIMA (non-seasonal fallback)."""
        candidate_orders = [(1, 1, 1), (0, 1, 1), (1, 0, 0)]

        for order in candidate_orders:
            try:
                model = SARIMAX(
                    series,
                    order=order,
                    seasonal_order=(0, 0, 0, 0),
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                fitted = model.fit(disp=False)
                forecast = fitted.get_forecast(steps=periods)

                mean = np.asarray(forecast.predicted_mean, dtype=float)
                conf_int = forecast.conf_int(alpha=0.05)
                if isinstance(conf_int, pd.DataFrame):
                    lower = np.asarray(conf_int.iloc[:, 0], dtype=float)
                    upper = np.asarray(conf_int.iloc[:, 1], dtype=float)
                else:
                    lower = np.asarray(conf_int[:, 0], dtype=float)
                    upper = np.asarray(conf_int[:, 1], dtype=float)

                return mean, lower, upper
            except Exception:
                continue

        return self._naive_forecast(series, periods)

    def _forecast_with_lstm_proxy(self, series, periods):
        """
        Lightweight LSTM-style forecast without TensorFlow dependency.
        Uses exponential smoothing plus recent trend.
        """
        values = np.asarray(series, dtype=float)
        if len(values) == 0:
            return self._naive_forecast(pd.Series(dtype=float), periods)

        alpha = 0.35
        smoothed = [values[0]]
        for value in values[1:]:
            smoothed.append(alpha * value + (1 - alpha) * smoothed[-1])

        if len(values) > 1:
            lookback = min(4, len(values) - 1)
            recent_diffs = np.diff(values[-(lookback + 1):])
            trend = float(np.mean(recent_diffs))
        else:
            trend = 0.0

        baseline = float(smoothed[-1])
        predictions = np.array([baseline + trend * (i + 1) for i in range(periods)], dtype=float)

        residuals = values - np.asarray(smoothed, dtype=float)
        std = float(np.std(residuals)) if len(residuals) > 1 else max(abs(baseline) * 0.1, 1.0)
        if not np.isfinite(std) or std == 0:
            std = max(abs(baseline) * 0.1, 1.0)

        margin = 1.64 * std
        lower = predictions - margin
        upper = predictions + margin
        return predictions, lower, upper

    def _forecast_series_for_model(self, model_type, series, periods):
        """Dispatch forecasting to selected model implementation."""
        model_type_normalized = str(model_type or 'sarima').lower()
        if model_type_normalized == 'arima':
            return self._forecast_with_arima(series, periods)
        if model_type_normalized == 'lstm':
            return self._forecast_with_lstm_proxy(series, periods)
        return self._forecast_with_sarima(series, periods)

    def _generate_provider_forecast(
        self,
        model_type,
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None,
        periods=3
    ):
        """Generate filtered forecast directly from historical data."""
        history = self._get_provider_historical_series(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level
        )
        if len(history) < 2:
            return self._empty_forecast_df()

        series = history.set_index('YEAR')['LEARNER_COUNT'].astype(float)
        model_type_normalized = str(model_type or 'sarima').lower()
        predicted, lower, upper = self._forecast_series_for_model(model_type_normalized, series, periods)

        predicted = np.maximum(np.asarray(predicted, dtype=float), 0)
        lower = np.maximum(np.asarray(lower, dtype=float), 0)
        upper = np.maximum(np.asarray(upper, dtype=float), predicted)

        last_year = int(history['YEAR'].max())
        forecast_years = [last_year + i for i in range(1, periods + 1)]

        return pd.DataFrame({
            'ACADEMIC_YEAR': [self._format_academic_year(year) for year in forecast_years],
            'YEAR': forecast_years,
            'PREDICTED_ENROLMENTS': predicted,
            'LOWER_CI': lower,
            'UPPER_CI': upper,
            'MODEL_TYPE': model_type_normalized.upper(),
            'PROVIDER_KEY': provider_id if provider_id is not None else None,
        })

    def _get_ssa_historical_data(
        self,
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None
    ):
        """Get historical learner counts by SSA with optional filters."""
        conditions, _ = self._build_filter_conditions(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level
        )

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            COALESCE(ssa.SSA_TIER_1_DESCRIPTION, 'Unknown') AS SSA_TIER1,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        LEFT JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_DELIVERY ld ON fe.LEARNING_DELIVERY_SKEY = ld.LEARNING_DELIVERY_SKEY
        LEFT JOIN PRESENTATION.DIM_LEVEL l ON fe.LEVEL_SKEY = l.LEVEL_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_AIM la ON fe.LEARNING_AIM_SKEY = la.LEARNING_AIM_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR, COALESCE(ssa.SSA_TIER_1_DESCRIPTION, 'Unknown')
        ORDER BY ay.ACADEMIC_YEAR, SSA_TIER1
        """

        df = self.db.execute_query(query)
        if len(df) == 0:
            return df

        df['YEAR'] = df['ACADEMIC_YEAR'].apply(self._academic_year_start)
        df['LEARNER_COUNT'] = pd.to_numeric(df['LEARNER_COUNT'], errors='coerce')
        df['SSA_TIER1'] = df['SSA_TIER1'].fillna('Unknown').astype(str)
        df = df.dropna(subset=['YEAR', 'LEARNER_COUNT']).copy()
        if len(df) == 0:
            return df

        df['YEAR'] = df['YEAR'].astype(int)
        df = df.sort_values(['SSA_TIER1', 'YEAR'])
        return df

    def _get_distinct_filter_values(
        self,
        expression,
        alias='FILTER_VALUE',
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None,
        exclude_filters=None,
        require_provider=False
    ):
        """Return distinct values for a college-forecast filter."""
        provider_key = self._normalize_provider_id(provider_id)
        if require_provider and provider_key is None:
            return []

        conditions, _ = self._build_filter_conditions(
            provider_id=provider_key,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level,
            exclude_filters=exclude_filters
        )

        query = f"""
        SELECT DISTINCT
            {expression} AS {alias}
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_DELIVERY ld ON fe.LEARNING_DELIVERY_SKEY = ld.LEARNING_DELIVERY_SKEY
        LEFT JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
        LEFT JOIN PRESENTATION.DIM_LEVEL l ON fe.LEVEL_SKEY = l.LEVEL_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_AIM la ON fe.LEARNING_AIM_SKEY = la.LEARNING_AIM_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += f" ORDER BY {alias}"

        df = self.db.execute_query(query)
        values = []
        for value in df.get(alias, pd.Series(dtype=object)).tolist():
            text = str(value).strip()
            if text:
                values.append(text)
        return values

    def get_college_forecast_filter_options(
        self,
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None
    ):
        """Get cascading filter options for the college forecast page."""
        provider_key = self._normalize_provider_id(provider_id)

        return {
            'funding_schemes': self._get_distinct_filter_values(
                self._funding_sql(),
                provider_id=provider_key,
                funding_scheme=funding_scheme,
                location=location,
                start_year=start_year,
                end_year=end_year,
                ssa=ssa,
                course=course,
                level=level,
                exclude_filters={'funding_scheme'}
            ),
            'ssa_options': self._get_distinct_filter_values(
                self._ssa_sql(),
                provider_id=provider_key,
                funding_scheme=funding_scheme,
                location=location,
                start_year=start_year,
                end_year=end_year,
                ssa=ssa,
                course=course,
                level=level,
                exclude_filters={'ssa'}
            ),
            'level_options': self._get_distinct_filter_values(
                self._level_sql(),
                provider_id=provider_key,
                funding_scheme=funding_scheme,
                location=location,
                start_year=start_year,
                end_year=end_year,
                ssa=ssa,
                course=course,
                level=level,
                exclude_filters={'level'}
            ),
            'course_options': self._get_distinct_filter_values(
                self._course_sql(),
                provider_id=provider_key,
                funding_scheme=funding_scheme,
                location=location,
                start_year=start_year,
                end_year=end_year,
                ssa=ssa,
                course=course,
                level=level,
                exclude_filters={'course'},
                require_provider=True
            ),
            'course_requires_college': provider_key is None,
        }

    def get_ssa_forecast_filtered(
        self,
        model_type='sarima',
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None,
        periods=3
    ):
        """Forecast by SSA based on selected provider, funding scheme, location, and year range."""
        _, normalized = self._build_filter_conditions(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level
        )

        history = self._get_ssa_historical_data(
            provider_id=normalized['provider_id'],
            funding_scheme=normalized['funding_scheme'],
            location=normalized['location'],
            start_year=normalized['start_year'],
            end_year=normalized['end_year'],
            ssa=normalized['ssa'],
            course=normalized['course'],
            level=normalized['level']
        )
        if len(history) == 0:
            return pd.DataFrame(
                columns=['SSA_TIER1', 'ACADEMIC_YEAR', 'PREDICTED_ENROLMENTS', 'LOWER_CI', 'UPPER_CI', 'MODEL_TYPE']
            )

        model_type_normalized = str(model_type or 'sarima').lower()
        global_last_year = int(history['YEAR'].max())
        forecast_years = [global_last_year + i for i in range(1, periods + 1)]

        rows = []
        for ssa_name, group in history.groupby('SSA_TIER1', sort=True):
            series = group.sort_values('YEAR').set_index('YEAR')['LEARNER_COUNT'].astype(float)
            if len(series) == 0:
                continue

            predicted, lower, upper = self._forecast_series_for_model(
                model_type_normalized,
                series,
                periods
            )

            predicted = np.maximum(np.asarray(predicted, dtype=float), 0)
            lower = np.maximum(np.asarray(lower, dtype=float), 0)
            upper = np.maximum(np.asarray(upper, dtype=float), predicted)

            for idx, year in enumerate(forecast_years):
                rows.append({
                    'SSA_TIER1': ssa_name,
                    'ACADEMIC_YEAR': self._format_academic_year(year),
                    'PREDICTED_ENROLMENTS': float(predicted[idx]) if idx < len(predicted) else 0.0,
                    'LOWER_CI': float(lower[idx]) if idx < len(lower) else 0.0,
                    'UPPER_CI': float(upper[idx]) if idx < len(upper) else 0.0,
                    'MODEL_TYPE': model_type_normalized.upper(),
                })

        result = pd.DataFrame(rows)
        if len(result) == 0:
            return pd.DataFrame(
                columns=['SSA_TIER1', 'ACADEMIC_YEAR', 'PREDICTED_ENROLMENTS', 'LOWER_CI', 'UPPER_CI', 'MODEL_TYPE']
            )

        result = result.sort_values(['SSA_TIER1', 'ACADEMIC_YEAR']).reset_index(drop=True)
        return result

    def get_forecast(
        self,
        model_type='sarima',
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None
    ):
        """Get forecast results for specified model and active filters."""
        _, normalized = self._build_filter_conditions(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level
        )

        requires_dynamic_forecast = any(
            value is not None
            for value in [
                normalized['funding_scheme'],
                normalized['location'],
                normalized['start_year'],
                normalized['end_year'],
                normalized['ssa'],
                normalized['course'],
                normalized['level'],
            ]
        )

        if requires_dynamic_forecast:
            return self._generate_provider_forecast(
                model_type=model_type,
                provider_id=normalized['provider_id'],
                funding_scheme=normalized['funding_scheme'],
                location=normalized['location'],
                start_year=normalized['start_year'],
                end_year=normalized['end_year'],
                ssa=normalized['ssa'],
                course=normalized['course'],
                level=normalized['level'],
                periods=self.forecast_periods,
            )

        try:
            query = f"""
            SELECT
                ACADEMIC_YEAR,
                PROVIDER_KEY,
                MODEL_TYPE,
                PREDICTED_ENROLMENTS,
                LOWER_CI,
                UPPER_CI,
                CREATED_AT
            FROM STAGING_ILR.FACT_ENROLMENT_FORECAST
            WHERE MODEL_TYPE = '{str(model_type).upper()}'
            """
            if normalized['provider_id'] is not None:
                query += f" AND PROVIDER_KEY = {normalized['provider_id']}"
            query += " ORDER BY ACADEMIC_YEAR"

            df = self.db.execute_query(query)
            if len(df) > 0:
                return df
        except Exception:
            pass

        file_forecast = self._load_forecast_from_file(model_type, normalized['provider_id'])
        if len(file_forecast) > 0:
            return file_forecast

        if normalized['provider_id'] is not None:
            generated = self._generate_provider_forecast(
                model_type=model_type,
                provider_id=normalized['provider_id'],
                funding_scheme=None,
                location=None,
                start_year=None,
                end_year=None,
                periods=self.forecast_periods,
            )
            if len(generated) > 0:
                return generated

        return self._empty_forecast_df()

    def _load_forecast_from_file(self, model_type, provider_id=None):
        """Load forecast from saved file."""
        filename = f"{str(model_type).lower()}_forecast.csv"
        filepath = os.path.join(self.model_dir, filename)

        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            if provider_id is not None:
                if 'PROVIDER_KEY' not in df.columns:
                    return self._empty_forecast_df()
                provider_keys = pd.to_numeric(df['PROVIDER_KEY'], errors='coerce')
                df = df[provider_keys == provider_id]
            return df

        return self._empty_forecast_df()

    def get_ssa_forecast(self, model_type='sarima', location=None, start_year=None, end_year=None):
        """Get forecast by SSA category, falling back to generated filtered data when needed."""
        start, end, location_value = self._normalize_filters(start_year, end_year, location)
        if location_value is not None or start is not None or end is not None:
            return self.get_ssa_forecast_filtered(
                model_type=model_type,
                location=location_value,
                start_year=start,
                end_year=end,
                periods=self.forecast_periods
            )

        filename = f"{model_type}_ssa_forecast.csv"
        filepath = os.path.join(self.model_dir, filename)

        if os.path.exists(filepath):
            return pd.read_csv(filepath)

        return pd.DataFrame(columns=[
            'ACADEMIC_YEAR', 'SSA_TIER1', 'PREDICTED_ENROLMENTS', 'LOWER_CI', 'UPPER_CI', 'MODEL_TYPE'
        ])

    def get_historical_with_forecast(
        self,
        model_type='sarima',
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None
    ):
        """Get combined historical and forecast data."""
        conditions, normalized = self._build_filter_conditions(
            provider_id=provider_id,
            funding_scheme=funding_scheme,
            location=location,
            start_year=start_year,
            end_year=end_year,
            ssa=ssa,
            course=course,
            level=level
        )

        query = """
        SELECT
            ay.ACADEMIC_YEAR,
            COUNT(DISTINCT fe.LEARNER_SKEY) AS LEARNER_COUNT,
            'Historical' AS DATA_TYPE
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_DELIVERY ld ON fe.LEARNING_DELIVERY_SKEY = ld.LEARNING_DELIVERY_SKEY
        LEFT JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
        LEFT JOIN PRESENTATION.DIM_LEVEL l ON fe.LEVEL_SKEY = l.LEVEL_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_AIM la ON fe.LEARNING_AIM_SKEY = la.LEARNING_AIM_SKEY
        """
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += """
        GROUP BY ay.ACADEMIC_YEAR
        ORDER BY ay.ACADEMIC_YEAR
        """

        historical = self.db.execute_query(query)
        historical['DATA_TYPE'] = 'Historical'
        historical['LOWER_CI'] = None
        historical['UPPER_CI'] = None
        historical['MODEL_TYPE'] = None

        forecast = self.get_forecast(
            model_type=model_type,
            provider_id=normalized['provider_id'],
            funding_scheme=normalized['funding_scheme'],
            location=normalized['location'],
            start_year=normalized['start_year'],
            end_year=normalized['end_year'],
            ssa=normalized['ssa'],
            course=normalized['course'],
            level=normalized['level']
        )

        if len(forecast) > 0:
            forecast_df = pd.DataFrame({
                'ACADEMIC_YEAR': forecast['ACADEMIC_YEAR'],
                'LEARNER_COUNT': forecast['PREDICTED_ENROLMENTS'],
                'DATA_TYPE': 'Forecast',
                'LOWER_CI': forecast.get('LOWER_CI', None),
                'UPPER_CI': forecast.get('UPPER_CI', None),
                'MODEL_TYPE': str(model_type).upper()
            })
            combined = pd.concat([historical, forecast_df], ignore_index=True)
        else:
            combined = historical

        return combined

    def compare_models(
        self,
        provider_id=None,
        funding_scheme=None,
        location=None,
        start_year=None,
        end_year=None,
        ssa=None,
        course=None,
        level=None
    ):
        """Compare forecasts from all available models."""
        results = {}

        for model in self.available_models:
            forecast = self.get_forecast(
                model,
                provider_id=provider_id,
                funding_scheme=funding_scheme,
                location=location,
                start_year=start_year,
                end_year=end_year,
                ssa=ssa,
                course=course,
                level=level
            )
            if len(forecast) > 0:
                results[model] = forecast

        return results

    def get_model_accuracy(self, model_type='sarima'):
        """Get model accuracy metrics."""
        filepath = os.path.join(self.model_dir, f"{model_type}_metrics.json")

        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as file_obj:
                return json.load(file_obj)

        return {
            'model': str(model_type).upper(),
            'mae': None,
            'rmse': None,
            'mape': None,
            'last_trained': None
        }

    def save_forecast_to_db(self, forecast_df, model_type):
        """Save forecast results to Snowflake."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS STAGING_ILR.FACT_ENROLMENT_FORECAST (
            ACADEMIC_YEAR VARCHAR(10),
            PROVIDER_KEY INTEGER,
            MODEL_TYPE VARCHAR(20),
            PREDICTED_ENROLMENTS FLOAT,
            LOWER_CI FLOAT,
            UPPER_CI FLOAT,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

        try:
            self.db.execute(create_table_sql)

            for _, row in forecast_df.iterrows():
                insert_sql = f"""
                INSERT INTO STAGING_ILR.FACT_ENROLMENT_FORECAST
                (ACADEMIC_YEAR, PROVIDER_KEY, MODEL_TYPE, PREDICTED_ENROLMENTS, LOWER_CI, UPPER_CI)
                VALUES ('{row['ACADEMIC_YEAR']}', {row.get('PROVIDER_KEY', 0)},
                        '{str(model_type).upper()}', {row['PREDICTED_ENROLMENTS']},
                        {row.get('LOWER_CI', 'NULL')}, {row.get('UPPER_CI', 'NULL')})
                """
                self.db.execute(insert_sql)

            print(f"[OK] Saved {len(forecast_df)} forecast records for {model_type}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to save forecast: {e}")
            return False
