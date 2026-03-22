import pytest

from utils.filtering import (
    build_forecast_sql_conditions,
    normalize_provider_id,
    normalize_shared_filters,
)


def test_normalize_shared_filters_reorders_years():
    filters = normalize_shared_filters(start_year="2026", end_year="2024", location=" Leeds ")

    assert filters == {
        "start_year": 2024,
        "end_year": 2026,
        "location": "Leeds",
    }


def test_normalize_provider_id_rejects_invalid_value():
    with pytest.raises(ValueError, match="provider_id must be an integer"):
        normalize_provider_id("not-a-number")


def test_build_forecast_sql_conditions_escapes_text_filters():
    conditions, normalized = build_forecast_sql_conditions(
        provider_id="7",
        location="King's Lynn",
        funding_scheme="Adult Skills",
        ssa="Health, Public Services and Care",
    )

    assert "fe.PROVIDER_SKEY = 7" in conditions
    assert "King''s Lynn" in " ".join(conditions)
    assert normalized["provider_id"] == 7
    assert normalized["location"] == "King's Lynn"
