"""Shared normalization and SQL filter helpers."""

from typing import Dict, Iterable, List, Optional, Tuple


def normalize_text_arg(value) -> Optional[str]:
    """Normalize a free-text value to stripped text or None."""
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def normalize_year_arg(value) -> Optional[int]:
    """Normalize a year-like value to int or None."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        year = int(text)
    except ValueError:
        return None

    return year if 1900 <= year <= 2100 else None


def normalize_provider_id(provider_id) -> Optional[int]:
    """Validate and normalize a provider id."""
    if provider_id is None:
        return None

    text = str(provider_id).strip()
    if not text:
        return None

    try:
        return int(text)
    except ValueError as exc:
        raise ValueError("provider_id must be an integer") from exc


def normalize_shared_filters(
    start_year=None,
    end_year=None,
    location=None,
) -> Dict[str, Optional[object]]:
    """Normalize shared dashboard filters."""
    start = normalize_year_arg(start_year)
    end = normalize_year_arg(end_year)
    location_value = normalize_text_arg(location)

    if start is not None and end is not None and start > end:
        start, end = end, start

    return {
        "start_year": start,
        "end_year": end,
        "location": location_value,
    }


def escape_sql_literal(value) -> str:
    """Escape a SQL string literal for direct interpolation."""
    return str(value).replace("'", "''")


def academic_year_start(academic_year) -> Optional[int]:
    """Convert academic year strings like 23/24 or 2023/24 to start year int."""
    if academic_year is None:
        return None

    text = str(academic_year).strip()
    if not text:
        return None

    start = text.split("/")[0].strip()
    if not start.isdigit():
        return None

    if len(start) == 2:
        return 2000 + int(start)
    return int(start)


def academic_year_start_sql(year_column: str = "ay.ACADEMIC_YEAR") -> str:
    """SQL expression to convert academic year labels to start year integers."""
    return f"""
    CASE
        WHEN TRY_TO_NUMBER(SPLIT_PART({year_column}, '/', 1)) IS NULL THEN NULL
        WHEN TRY_TO_NUMBER(SPLIT_PART({year_column}, '/', 1)) < 100
            THEN 2000 + TRY_TO_NUMBER(SPLIT_PART({year_column}, '/', 1))
        ELSE TRY_TO_NUMBER(SPLIT_PART({year_column}, '/', 1))
    END
    """


def location_sql(location_column: str = "p.CONTACT_TOWN") -> str:
    """SQL expression for normalized provider town/city."""
    return f"COALESCE(NULLIF(TRIM({location_column}), ''), 'Unknown')"


def funding_sql(funding_column: str = "ld.FUNDING_MODEL") -> str:
    """SQL expression for normalized funding scheme."""
    return f"COALESCE(NULLIF(TRIM({funding_column}), ''), 'Unknown')"


def ssa_sql(ssa_column: str = "ssa.SSA_TIER_1_DESCRIPTION") -> str:
    """SQL expression for normalized SSA tier 1."""
    return f"COALESCE(NULLIF(TRIM({ssa_column}), ''), 'Unknown')"


def level_sql(
    level_column: str = "l.LEVEL_DESCRIPTION",
    level_fallback_1: str = "la.NVQ_LEVEL_V2",
    level_fallback_2: str = "la.NVQ_LEVEL",
) -> str:
    """SQL expression for normalized qualification level."""
    return (
        "COALESCE("
        f"NULLIF(TRIM({level_column}), ''), "
        f"NULLIF(TRIM({level_fallback_1}), ''), "
        f"NULLIF(TRIM({level_fallback_2}), ''), "
        "'Unknown')"
    )


def course_sql(
    title_column: str = "la.LEARN_AIM_REF_TITLE",
    programme_column: str = "ld.PROGRAMME_TYPE",
    reference_column: str = "ld.LEARNING_AIM_REFERENCE",
    fallback_reference_column: str = "la.LEARN_AIM_REF",
) -> str:
    """SQL expression for a readable course label."""
    return f"""
    CASE
        WHEN NULLIF(TRIM({title_column}), '') IS NOT NULL
         AND COALESCE(
            NULLIF(TRIM({reference_column}), ''),
            NULLIF(TRIM({fallback_reference_column}), '')
         ) IS NOT NULL
        THEN TRIM({title_column}) || ' (' ||
            COALESCE(
                NULLIF(TRIM({reference_column}), ''),
                NULLIF(TRIM({fallback_reference_column}), '')
            ) || ')'
        ELSE COALESCE(
            NULLIF(TRIM({title_column}), ''),
            NULLIF(TRIM({programme_column}), ''),
            NULLIF(TRIM({reference_column}), ''),
            NULLIF(TRIM({fallback_reference_column}), ''),
            'Unknown'
        )
    END
    """


def build_shared_sql_conditions(
    start_year=None,
    end_year=None,
    location=None,
    year_column: str = "ay.ACADEMIC_YEAR",
    location_column: str = "p.CONTACT_TOWN",
) -> Tuple[List[str], Dict[str, Optional[object]]]:
    """Build shared SQL conditions for year range and location filters."""
    normalized = normalize_shared_filters(
        start_year=start_year,
        end_year=end_year,
        location=location,
    )

    conditions: List[str] = []
    year_expr = academic_year_start_sql(year_column)

    if normalized["start_year"] is not None:
        conditions.append(f"{year_expr} >= {normalized['start_year']}")
    if normalized["end_year"] is not None:
        conditions.append(f"{year_expr} <= {normalized['end_year']}")
    if normalized["location"] is not None:
        escaped = escape_sql_literal(normalized["location"])
        conditions.append(f"{location_sql(location_column)} = '{escaped}'")

    return conditions, normalized


def build_forecast_sql_conditions(
    provider_id=None,
    funding_scheme=None,
    location=None,
    start_year=None,
    end_year=None,
    ssa=None,
    course=None,
    level=None,
    exclude_filters: Optional[Iterable[str]] = None,
    year_column: str = "ay.ACADEMIC_YEAR",
    location_column: str = "p.CONTACT_TOWN",
) -> Tuple[List[str], Dict[str, Optional[object]]]:
    """Build shared SQL conditions for college forecast queries."""
    exclude = set(exclude_filters or [])
    provider_key = normalize_provider_id(provider_id)
    funding_value = normalize_text_arg(funding_scheme)
    ssa_value = normalize_text_arg(ssa)
    course_value = normalize_text_arg(course)
    level_value = normalize_text_arg(level)

    _, normalized_shared = build_shared_sql_conditions(
        start_year=start_year,
        end_year=end_year,
        location=location,
        year_column=year_column,
        location_column=location_column,
    )

    conditions: List[str] = []
    year_expr = academic_year_start_sql(year_column)

    if provider_key is not None and "provider_id" not in exclude:
        conditions.append(f"fe.PROVIDER_SKEY = {provider_key}")
    if funding_value is not None and "funding_scheme" not in exclude:
        escaped = escape_sql_literal(funding_value)
        conditions.append(f"{funding_sql()} = '{escaped}'")
    if normalized_shared["location"] is not None and "location" not in exclude:
        escaped = escape_sql_literal(normalized_shared["location"])
        conditions.append(f"{location_sql(location_column)} = '{escaped}'")
    if normalized_shared["start_year"] is not None and "start_year" not in exclude:
        conditions.append(f"{year_expr} >= {normalized_shared['start_year']}")
    if normalized_shared["end_year"] is not None and "end_year" not in exclude:
        conditions.append(f"{year_expr} <= {normalized_shared['end_year']}")
    if ssa_value is not None and "ssa" not in exclude:
        escaped = escape_sql_literal(ssa_value)
        conditions.append(f"{ssa_sql()} = '{escaped}'")
    if course_value is not None and "course" not in exclude:
        escaped = escape_sql_literal(course_value)
        conditions.append(f"{course_sql()} = '{escaped}'")
    if level_value is not None and "level" not in exclude:
        escaped = escape_sql_literal(level_value)
        conditions.append(f"{level_sql()} = '{escaped}'")

    return conditions, {
        "provider_id": provider_key,
        "funding_scheme": funding_value,
        "location": normalized_shared["location"],
        "start_year": normalized_shared["start_year"],
        "end_year": normalized_shared["end_year"],
        "ssa": ssa_value,
        "course": course_value,
        "level": level_value,
    }
