from services.programme_plans_service import ProgrammePlansService


def test_get_filters_exposes_dataset_and_quick_filters(sample_programme_plans_csv):
    service = ProgrammePlansService(config_file="unused", snapshot_csv=str(sample_programme_plans_csv))

    filters = service.get_filters()

    assert filters["dataset"]["row_count"] == 3
    assert filters["page_size_options"] == [50, 100, 250, 500]
    assert "Approval Status" in filters["quick_filters"]


def test_get_paginated_data_applies_search_and_filters(sample_programme_plans_csv):
    service = ProgrammePlansService(config_file="unused", snapshot_csv=str(sample_programme_plans_csv))

    result = service.get_paginated_data(
        page=1,
        page_size=10,
        search="nursing",
        column_filters={"Approval Status": "Completed"},
    )

    assert result["pagination"]["total_rows"] == 1
    assert result["rows"][0][1] == "Nursing Pathway"
