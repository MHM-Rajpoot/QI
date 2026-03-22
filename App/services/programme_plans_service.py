"""
Programme Plans Service (Snowflake-backed)
Builds Programme Plans outputs from current Snowflake data.
"""

import os
from math import ceil
from typing import Dict, List, Optional, TypedDict

import pandas as pd

from db.snowflake import get_db


class PrimaryFilterSpec(TypedDict):
    """Configuration for promoted quick filters."""

    name: str
    max_values: int


class SnapshotCacheEntry(TypedDict):
    """Cached snapshot dataframe keyed by file stat values."""

    cache_key: tuple[int, int]
    df: pd.DataFrame


class ProgrammePlansService:
    """Service for Programme Plans data and summaries using live Snowflake data."""

    PAGE_SIZE_OPTIONS: List[int] = [50, 100, 250, 500]
    PRIMARY_FILTER_PRIORITY: List[PrimaryFilterSpec] = [
        {"name": "Approval Status", "max_values": 25},
        {"name": "Infill", "max_values": 12},
        {"name": "Site", "max_values": 60},
        {"name": "Level", "max_values": 25},
        {"name": "Parent", "max_values": 30},
        {"name": "CA Name", "max_values": 100},
    ]
    _snapshot_cache: Dict[str, SnapshotCacheEntry] = {}

    COMPAT_COLUMNS: List[str] = [
        "CA Name",
        "Prog Code",
        "Prog Name",
        "Aim",
        "Level",
        "Site",
        "Course ID",
        "Start",
        "End",
        "Main Qual GLH",
        "Tutorial Hours",
        "Blended Hours",
        "Review Hours",
        "As Hours",
        "2021-22 Groups",
        "2022-23 Groups",
        "16-18 Actual (21/22)",
        "AEB Actual (21/22)",
        "FCR Actual (21/22)",
        "Total Actual Learners (21/22)",
        "16-18 Target (22/23)",
        "AEB Target (22/23)",
        "Total Target Learners (22/23)",
        "CS Risk rating",
        "Total Income (22/23)",
        "Total Costs",
        "Contribution %",
        "Infill Parent Prog Code",
        "Parent",
        "Approval Status",
    ]

    SUMMARY_NUMERIC_COLUMNS: List[str] = [
        "Main Qual GLH",
        "Blended Hours",
        "Tutorial Hours",
    ]

    QUERY = """
    WITH year_stats AS (
        SELECT DISTINCT
            TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) AS year_key
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay
          ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        WHERE TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) IS NOT NULL
    ),
    year_window AS (
        SELECT
            MAX(year_key) AS target_year_key,
            MAX(year_key) - 1 AS actual_year_key
        FROM year_stats
    ),
    account_raw AS (
        SELECT
            fa.PROVIDER_SKEY,
            TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) AS year_key,
            SUM(COALESCE(fa.ADJUSTED_INCOME, 0)) AS total_income,
            SUM(
                COALESCE(fa.STAFF_COSTS, 0) +
                COALESCE(fa.TEACHING_COSTS, 0) +
                COALESCE(fa.TEACHING_SUPPORT_COSTS, 0) +
                COALESCE(fa.ADMINISTRATION_COSTS, 0) +
                COALESCE(fa.OPERATIONAL_MAINTENANCE_COSTS, 0) +
                COALESCE(fa.EXAMINATION_COSTS, 0) +
                COALESCE(fa.RENT_AND_LEASE_COSTS, 0)
            ) AS total_costs
        FROM PRESENTATION.FACT_ACCOUNT fa
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay
          ON fa.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN year_window yw
          ON TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) IN (yw.actual_year_key, yw.target_year_key)
        GROUP BY fa.PROVIDER_SKEY, TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2))
    ),
    account AS (
        SELECT
            PROVIDER_SKEY,
            year_key,
            total_income,
            total_costs,
            CASE
                WHEN NULLIF(total_income, 0) IS NULL THEN NULL
                ELSE (total_income - total_costs) / NULLIF(total_income, 0)
            END AS contribution_pct
        FROM account_raw
    ),
    base AS (
        SELECT
            COALESCE(ssa.SSA_TIER_1_DESCRIPTION, 'Unknown') AS ca_name,
            COALESCE(ld.PROGRAMME_TYPE, 'Unknown') AS prog_code,
            COALESCE(la.LEARN_AIM_REF_TITLE, ld.PROGRAMME_TYPE, 'Unknown') AS prog_name,
            COALESCE(la.LEARN_AIM_REF, ld.LEARNING_AIM_REFERENCE, 'Unknown') AS aim,
            COALESCE(lvl.LEVEL_DESCRIPTION, la.NVQ_LEVEL_V2, la.NVQ_LEVEL, 'Unknown') AS level_desc,
            COALESCE(
                NULLIF(TRIM(COALESCE(l.CAMPUS_IDENTIFIER::STRING, '')), ''),
                l.CAMPUS_ID::STRING,
                'Unknown'
            ) AS site,
            COALESCE(ld.LEARNING_AIM_REFERENCE, la.LEARN_AIM_REF, 'Unknown') AS course_id,
            MIN(fe.START_DATE) AS start_date,
            MAX(COALESCE(fe.PLANNED_END_DATE, fe.ACTUAL_END_DATE)) AS end_date,
            SUM(COALESCE(fe.GUIDED_LEARNING_HOURS, la.GUIDED_LEARNING_HOURS, 0)) AS main_qual_glh,
            CAST(0 AS FLOAT) AS tutorial_hours,
            CAST(0 AS FLOAT) AS blended_hours,
            CAST(0 AS FLOAT) AS review_hours,
            SUM(COALESCE(l.PLANNED_EEP_HOURS, 0)) AS as_hours,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.actual_year_key
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS groups_2021_22,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS groups_2022_23,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.actual_year_key
                     AND ld.FUNDING_MODEL = '16-19 (excluding Apprenticeships)'
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS actual_16_18_21_22,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.actual_year_key
                     AND ld.FUNDING_MODEL ILIKE '%Adult Skills%'
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS aeb_actual_21_22,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.actual_year_key
                     AND ld.FUNDING_MODEL ILIKE '%Apprenticeship%'
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS fcr_actual_21_22,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.actual_year_key
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS total_actual_learners_21_22,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                     AND ld.FUNDING_MODEL = '16-19 (excluding Apprenticeships)'
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS target_16_18_22_23,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                     AND ld.FUNDING_MODEL ILIKE '%Adult Skills%'
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS aeb_target_22_23,
            SUM(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                    THEN COALESCE(fe.ENROLMENT_COUNT, 0)
                    ELSE 0
                END
            ) AS total_target_learners_22_23,
            CAST(NULL AS VARCHAR) AS cs_risk_rating,
            MAX(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                    THEN acc.total_income
                END
            ) AS total_income_22_23,
            MAX(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                    THEN acc.total_costs
                END
            ) AS total_costs,
            MAX(
                CASE
                    WHEN TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = yw.target_year_key
                    THEN acc.contribution_pct
                END
            ) AS contribution_pct,
            CAST(NULL AS VARCHAR) AS infill_parent_prog_code,
            COALESCE(p.PROVIDER_NAME, 'Unknown') AS parent,
            COALESCE(
                NULLIF(TRIM(ld.COMPLETION_STATUS), ''),
                CASE ld.COMPLETION_STATUS_CODE
                    WHEN 1 THEN 'Continuing'
                    WHEN 2 THEN 'Completed'
                    WHEN 3 THEN 'Withdrawn'
                    WHEN 6 THEN 'Break in learning'
                    ELSE 'Unknown'
                END
            ) AS approval_status,
            CASE
                WHEN UPPER(COALESCE(ld.IS_NON_FUNDED::STRING, 'FALSE')) IN ('TRUE', '1', 'Y', 'YES') THEN 'Y'
                ELSE 'N'
            END AS infill
        FROM PRESENTATION.FACT_ENROLMENT fe
        JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay
          ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
        JOIN year_window yw
          ON TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) IN (yw.actual_year_key, yw.target_year_key)
        LEFT JOIN PRESENTATION.DIM_SSA ssa
          ON fe.SSA_SKEY = ssa.SSA_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_AIM la
          ON fe.LEARNING_AIM_SKEY = la.LEARNING_AIM_SKEY
        LEFT JOIN PRESENTATION.DIM_LEVEL lvl
          ON fe.LEVEL_SKEY = lvl.LEVEL_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNER l
          ON fe.LEARNER_SKEY = l.LEARNER_SKEY
        LEFT JOIN PRESENTATION.DIM_LEARNING_DELIVERY ld
          ON fe.LEARNING_DELIVERY_SKEY = ld.LEARNING_DELIVERY_SKEY
        LEFT JOIN PRESENTATION.DIM_PROVIDER p
          ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
        LEFT JOIN account acc
          ON fe.PROVIDER_SKEY = acc.PROVIDER_SKEY
         AND TRY_TO_NUMBER(SUBSTR(ay.ACADEMIC_YEAR, 1, 2)) = acc.year_key
        GROUP BY
            COALESCE(ssa.SSA_TIER_1_DESCRIPTION, 'Unknown'),
            COALESCE(ld.PROGRAMME_TYPE, 'Unknown'),
            COALESCE(la.LEARN_AIM_REF_TITLE, ld.PROGRAMME_TYPE, 'Unknown'),
            COALESCE(la.LEARN_AIM_REF, ld.LEARNING_AIM_REFERENCE, 'Unknown'),
            COALESCE(lvl.LEVEL_DESCRIPTION, la.NVQ_LEVEL_V2, la.NVQ_LEVEL, 'Unknown'),
            COALESCE(
                NULLIF(TRIM(COALESCE(l.CAMPUS_IDENTIFIER::STRING, '')), ''),
                l.CAMPUS_ID::STRING,
                'Unknown'
            ),
            COALESCE(ld.LEARNING_AIM_REFERENCE, la.LEARN_AIM_REF, 'Unknown'),
            COALESCE(p.PROVIDER_NAME, 'Unknown'),
            COALESCE(
                NULLIF(TRIM(ld.COMPLETION_STATUS), ''),
                CASE ld.COMPLETION_STATUS_CODE
                    WHEN 1 THEN 'Continuing'
                    WHEN 2 THEN 'Completed'
                    WHEN 3 THEN 'Withdrawn'
                    WHEN 6 THEN 'Break in learning'
                    ELSE 'Unknown'
                END
            ),
            CASE
                WHEN UPPER(COALESCE(ld.IS_NON_FUNDED::STRING, 'FALSE')) IN ('TRUE', '1', 'Y', 'YES') THEN 'Y'
                ELSE 'N'
            END
    )
    SELECT *
    FROM base
    WHERE
        groups_2021_22 > 0
        OR groups_2022_23 > 0
        OR total_actual_learners_21_22 > 0
        OR total_target_learners_22_23 > 0
    ORDER BY ca_name, prog_name, aim, site
    """

    def __init__(self, config_file: str, snapshot_csv: Optional[str] = None):
        self.db = get_db(config_file)
        self.snapshot_csv = snapshot_csv

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Snowflake returns uppercase column names by default; normalize first.
        df.columns = [str(c).strip().lower() for c in df.columns]

        rename_map = {
            "ca_name": "CA Name",
            "prog_code": "Prog Code",
            "prog_name": "Prog Name",
            "aim": "Aim",
            "level_desc": "Level",
            "site": "Site",
            "course_id": "Course ID",
            "start_date": "Start",
            "end_date": "End",
            "main_qual_glh": "Main Qual GLH",
            "tutorial_hours": "Tutorial Hours",
            "blended_hours": "Blended Hours",
            "review_hours": "Review Hours",
            "as_hours": "As Hours",
            "groups_2021_22": "2021-22 Groups",
            "groups_2022_23": "2022-23 Groups",
            "actual_16_18_21_22": "16-18 Actual (21/22)",
            "aeb_actual_21_22": "AEB Actual (21/22)",
            "fcr_actual_21_22": "FCR Actual (21/22)",
            "total_actual_learners_21_22": "Total Actual Learners (21/22)",
            "target_16_18_22_23": "16-18 Target (22/23)",
            "aeb_target_22_23": "AEB Target (22/23)",
            "total_target_learners_22_23": "Total Target Learners (22/23)",
            "cs_risk_rating": "CS Risk rating",
            "total_income_22_23": "Total Income (22/23)",
            "total_costs": "Total Costs",
            "contribution_pct": "Contribution %",
            "infill_parent_prog_code": "Infill Parent Prog Code",
            "parent": "Parent",
            "approval_status": "Approval Status",
            "infill": "Infill",
        }
        df = df.rename(columns=rename_map)

        # Ensure expected compatibility columns exist
        for col in self.COMPAT_COLUMNS + ["Infill"]:
            if col not in df.columns:
                df[col] = None

        # Numeric cleanup
        for col in self.SUMMARY_NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        num_cols = [
            "2021-22 Groups",
            "2022-23 Groups",
            "16-18 Actual (21/22)",
            "AEB Actual (21/22)",
            "FCR Actual (21/22)",
            "Total Actual Learners (21/22)",
            "16-18 Target (22/23)",
            "AEB Target (22/23)",
            "Total Target Learners (22/23)",
            "Total Income (22/23)",
            "Total Costs",
            "Contribution %",
            "As Hours",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop rows with no useful programme identity
        df = df.dropna(subset=["CA Name", "Prog Name"], how="all")

        return df

    def load_data(self) -> pd.DataFrame:
        """Load live Programme Plans data from Snowflake."""
        df = self.db.execute_query(self.QUERY)
        return self._normalize_dataframe(df)

    def refresh_snapshot(self) -> pd.DataFrame:
        """Pull live Snowflake data and optionally persist a local snapshot CSV."""
        df = self.load_data()
        if self.snapshot_csv:
            os.makedirs(os.path.dirname(self.snapshot_csv), exist_ok=True)
            df.to_csv(self.snapshot_csv, index=False)
            self._snapshot_cache.pop(self.snapshot_csv, None)
        return df

    @staticmethod
    def _sort_unique_values(values: List[str]) -> List[str]:
        """Sort unique string values consistently for filter dropdowns."""
        return sorted(values, key=lambda item: str(item).lower())

    def _get_snapshot_stat(self):
        """Return os.stat data for the local snapshot CSV when available."""
        if not self.snapshot_csv or not os.path.exists(self.snapshot_csv):
            return None
        return os.stat(self.snapshot_csv)

    def _load_snapshot_dataset(self) -> Optional[pd.DataFrame]:
        """Load the raw local snapshot CSV and cache it by file timestamp."""
        if not self.snapshot_csv or not os.path.exists(self.snapshot_csv):
            return None

        stat = os.stat(self.snapshot_csv)
        cached = self._snapshot_cache.get(self.snapshot_csv)
        cache_key = (stat.st_mtime_ns, stat.st_size)
        if cached and cached.get("cache_key") == cache_key:
            return cached["df"].copy()

        try:
            df = pd.read_csv(self.snapshot_csv, dtype=str, keep_default_na=False).fillna("")
        except Exception:
            return None

        self._snapshot_cache[self.snapshot_csv] = {
            "cache_key": cache_key,
            "df": df.copy(),
        }
        return df

    def _load_snapshot_compat_data(self) -> Optional[pd.DataFrame]:
        """Load compatibility rows from local snapshot CSV to preserve file row order."""
        df = self._load_snapshot_dataset()
        if df is None:
            return None

        for col in self.COMPAT_COLUMNS + ["Infill"]:
            if col not in df.columns:
                df[col] = ""

        return df

    def _load_explorer_data(self) -> pd.DataFrame:
        """Load Programme Plans data for the explorer view."""
        df = self._load_snapshot_dataset()
        if df is not None:
            return df

        df = self.load_data().fillna("")
        return df.astype(str).fillna("")

    @staticmethod
    def _normalize_column_filters(column_filters) -> Dict[str, str]:
        """Normalize column filters from request payloads."""
        normalized: Dict[str, str] = {}
        if not isinstance(column_filters, dict):
            return normalized

        for column_name, selected_value in column_filters.items():
            key = str(column_name).strip()
            if not key:
                continue

            value = "" if selected_value is None else str(selected_value)
            normalized[key] = value.strip()

        return normalized

    def _get_primary_filter_names(self, columns: List[str], unique_values: Dict[str, List[str]]) -> List[str]:
        """Choose a small set of quick filters for the explorer toolbar."""
        selected: List[str] = []
        used = set()

        for priority in self.PRIMARY_FILTER_PRIORITY:
            column_name = str(priority["name"])
            if column_name not in columns or column_name in used:
                continue

            unique_count = len(unique_values.get(column_name, []))
            if unique_count > 0 and unique_count <= int(priority["max_values"]):
                selected.append(column_name)
                used.add(column_name)

        for column_name in columns:
            if len(selected) >= 6 or column_name in used:
                continue

            unique_count = len(unique_values.get(column_name, []))
            if 1 < unique_count <= 20:
                selected.append(column_name)
                used.add(column_name)

        return selected[:6]

    def get_dataset_info(self) -> Dict[str, object]:
        """Return snapshot metadata for the Programme Plans explorer."""
        df = self._load_explorer_data()
        stat = self._get_snapshot_stat()

        return {
            "name": "Programme Plans Dataset",
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "last_modified": (
                pd.Timestamp(stat.st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")
                if stat is not None else None
            ),
            "source": "snapshot_csv" if stat is not None else "live_query",
        }

    def get_filters(self) -> Dict[str, object]:
        """Return Programme Plans filter metadata for both legacy and explorer views."""
        compat_df = self._load_snapshot_compat_data()
        if compat_df is None:
            compat_df = self.load_data().fillna("")

        explorer_df = self._load_explorer_data()
        columns = [str(column) for column in explorer_df.columns.tolist()]
        unique_values = {
            column: self._sort_unique_values(
                [
                    str(value)
                    for value in explorer_df[column].fillna("").astype(str).str.strip().unique().tolist()
                    if str(value).strip() != ""
                ]
            )
            for column in columns
        }

        infill_vals = sorted(
            [v for v in compat_df["Infill"].dropna().astype(str).str.strip().unique().tolist() if v]
        )
        approval_vals = sorted(
            [v for v in compat_df["Approval Status"].dropna().astype(str).str.strip().unique().tolist() if v]
        )

        return {
            "infill": infill_vals,
            "approval_status": approval_vals,
            "columns": columns,
            "quick_filters": self._get_primary_filter_names(columns, unique_values),
            "column_filters": [
                {
                    "name": column,
                    "options": unique_values[column],
                    "unique_count": len(unique_values[column]),
                }
                for column in columns
            ],
            "page_size_options": self.PAGE_SIZE_OPTIONS,
            "dataset": self.get_dataset_info(),
        }

    def _apply_filters(
        self,
        df: pd.DataFrame,
        infill: Optional[str] = None,
        approval_status: Optional[str] = None,
    ) -> pd.DataFrame:
        filtered = df.copy()

        if infill is not None and str(infill).strip() != "":
            filtered = filtered[filtered["Infill"].astype(str).str.strip() == str(infill).strip()]

        if approval_status is not None and str(approval_status).strip() != "":
            filtered = filtered[
                filtered["Approval Status"].astype(str).str.strip() == str(approval_status).strip()
            ]

        return filtered

    def get_hours_summary(
        self,
        infill: Optional[str] = None,
        approval_status: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Reproduce old pivot behavior:
        Row Labels = CA Name
        Values = sums of Main Qual GLH, Blended Hours, Tutorial Hours
        Filters = Infill, Approval Status
        """
        df = self.load_data()
        df = self._apply_filters(df, infill=infill, approval_status=approval_status)

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "Row Labels",
                    "Sum of Main Qual GLH",
                    "Sum of Blended Hours",
                    "Sum of Tutorial Hours",
                ]
            )

        summary = (
            df.groupby("CA Name", dropna=False)[self.SUMMARY_NUMERIC_COLUMNS]
            .sum(min_count=1)
            .reset_index()
            .rename(
                columns={
                    "CA Name": "Row Labels",
                    "Main Qual GLH": "Sum of Main Qual GLH",
                    "Blended Hours": "Sum of Blended Hours",
                    "Tutorial Hours": "Sum of Tutorial Hours",
                }
            )
            .sort_values("Row Labels")
        )

        for col in ["Sum of Main Qual GLH", "Sum of Blended Hours", "Sum of Tutorial Hours"]:
            summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0)

        grand_total = pd.DataFrame(
            [
                {
                    "Row Labels": "Grand Total",
                    "Sum of Main Qual GLH": summary["Sum of Main Qual GLH"].sum(),
                    "Sum of Blended Hours": summary["Sum of Blended Hours"].sum(),
                    "Sum of Tutorial Hours": summary["Sum of Tutorial Hours"].sum(),
                }
            ]
        )

        return pd.concat([summary, grand_total], ignore_index=True)

    def get_compat_data(
        self,
        infill: Optional[str] = None,
        approval_status: Optional[str] = None,
        limit: Optional[int] = 2000,
    ) -> pd.DataFrame:
        """Return row-level compatibility dataset for legacy dashboard columns."""
        df = self._load_snapshot_compat_data()
        if df is None:
            df = self.load_data()

        df = self._apply_filters(df, infill=infill, approval_status=approval_status)
        df = df[self.COMPAT_COLUMNS].copy()

        if limit is not None and limit > 0:
            df = df.head(limit)

        return df

    def get_paginated_data(
        self,
        *,
        page: int = 1,
        page_size: int = 100,
        search: Optional[str] = None,
        column_filters: Optional[Dict[str, str]] = None,
    ) -> Dict[str, object]:
        """Return server-side filtered and paginated Programme Plans explorer data."""
        df = self._load_explorer_data()
        columns = [str(column) for column in df.columns.tolist()]
        page = max(int(page), 1)
        page_size = max(int(page_size), 1)
        search_text = str(search or "").strip().lower()
        normalized_filters = self._normalize_column_filters(column_filters)

        filtered = df.copy()
        for column_name, selected_value in normalized_filters.items():
            if column_name not in filtered.columns:
                continue
            filtered = filtered[
                filtered[column_name].fillna("").astype(str).str.strip() == selected_value
            ]

        if search_text:
            contains_mask = filtered.fillna("").astype(str).apply(
                lambda column: column.str.contains(search_text, case=False, na=False, regex=False)
            )
            filtered = filtered[contains_mask.any(axis=1)]

        total_rows = int(len(filtered))
        total_pages = max(1, ceil(total_rows / page_size)) if total_rows else 1
        current_page = min(page, total_pages)
        start_index = (current_page - 1) * page_size
        end_index = start_index + page_size
        page_df = filtered.iloc[start_index:end_index]

        return {
            "columns": columns,
            "rows": page_df.fillna("").astype(str).values.tolist(),
            "pagination": {
                "page": current_page,
                "page_size": page_size,
                "total_rows": total_rows,
                "total_pages": total_pages,
                "has_previous": current_page > 1,
                "has_next": current_page < total_pages,
                "start_row": start_index + 1 if total_rows else 0,
                "end_row": min(end_index, total_rows),
            },
            "applied_filters": normalized_filters,
            "search": search_text,
            "dataset": self.get_dataset_info(),
        }
