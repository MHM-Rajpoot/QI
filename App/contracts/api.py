"""Formal API contract registry and version helpers."""

from dataclasses import dataclass
from typing import Dict, Optional


API_OUTPUT_VERSION = "2026.03.0"
API_CONTRACT_DOC_VERSION = "v1"


@dataclass(frozen=True)
class ContractDescriptor:
    """A versioned API response contract."""

    id: str
    version: str
    summary: str
    category: str


DEFAULT_CONTRACT = ContractDescriptor(
    id="core.response",
    version="v1",
    summary="Default versioned dashboard API response envelope.",
    category="core",
)


CONTRACT_REGISTRY: Dict[str, ContractDescriptor] = {
    "api.health": ContractDescriptor("health.status", "v1", "Service health status response.", "system"),
    "api.database_status": ContractDescriptor("database.status", "v1", "Snowflake connectivity summary.", "system"),
    "api.summary": ContractDescriptor("dashboard.summary", "v1", "Dashboard KPI summary response.", "dashboard"),
    "api.enrolment_trends": ContractDescriptor("enrolment.trends", "v1", "Historical enrolment trend rows.", "dashboard"),
    "api.enrolment_by_provider": ContractDescriptor("enrolment.provider_rows", "v1", "Provider enrolment detail rows.", "dashboard"),
    "api.enrolment_by_ssa": ContractDescriptor("enrolment.ssa_rows", "v1", "SSA enrolment detail rows.", "dashboard"),
    "api.enrolment_by_age": ContractDescriptor("enrolment.age_rows", "v1", "Age breakdown detail rows.", "dashboard"),
    "api.enrolment_by_level": ContractDescriptor("enrolment.level_rows", "v1", "Level breakdown detail rows.", "dashboard"),
    "api.providers": ContractDescriptor("provider.list", "v1", "Provider option list rows.", "dashboard"),
    "api.ssa_list": ContractDescriptor("ssa.list", "v1", "Sector subject area lookup rows.", "dashboard"),
    "api.forecast": ContractDescriptor("forecast.rows", "v1", "Forecast-only series rows.", "forecasting"),
    "api.forecast_combined": ContractDescriptor("forecast.combined_rows", "v1", "Historical and forecast combined rows.", "forecasting"),
    "api.forecast_compare": ContractDescriptor("forecast.model_comparison", "v1", "Forecast outputs grouped by model.", "forecasting"),
    "api.forecast_filter_options": ContractDescriptor("forecast.filter_options", "v1", "College forecast cascading filter options.", "forecasting"),
    "api.forecast_subject_areas": ContractDescriptor("forecast.subject_area_table", "v1", "Subject area forecast rows and pivot table.", "forecasting"),
    "api.forecast_accuracy": ContractDescriptor("forecast.accuracy", "v1", "Saved model accuracy metrics.", "forecasting"),
    "api.timeseries": ContractDescriptor("timeseries.rows", "v1", "Forecast input time-series rows.", "forecasting"),
    "api.programme_plans_refresh": ContractDescriptor("programme_plans.refresh_job", "v1", "Queued Programme Plans refresh job.", "programme_plans"),
    "api.job_status": ContractDescriptor("jobs.status", "v1", "Background admin job status response.", "admin"),
    "api.programme_plans_filters": ContractDescriptor("programme_plans.filters", "v2", "Programme Plans explorer filter metadata.", "programme_plans"),
    "api.programme_plans_summary": ContractDescriptor("programme_plans.summary", "v1", "Programme Plans summary rows.", "programme_plans"),
    "api.programme_plans_data": ContractDescriptor("programme_plans.page", "v2", "Server-side paginated Programme Plans explorer rows.", "programme_plans"),
    "api.programme_plans_csv_data": ContractDescriptor("programme_plans.page_alias", "v2", "Backward-compatible alias for paginated Programme Plans rows.", "programme_plans"),
    "api.programme_plans_csv_download": ContractDescriptor("programme_plans.csv_download", "v1", "Programme Plans CSV download response.", "programme_plans"),
    "api.refresh_data": ContractDescriptor("admin.refresh_job", "v1", "Queued data refresh job.", "admin"),
    "api.metadata_database": ContractDescriptor("metadata.database", "v1", "Database metadata summary.", "metadata"),
    "api.metadata_schemas": ContractDescriptor("metadata.schemas", "v1", "Schema name list.", "metadata"),
    "api.metadata_schema_structure": ContractDescriptor("metadata.schema_structure", "v1", "Schema structure summary.", "metadata"),
    "api.train_models": ContractDescriptor("admin.train_job", "v1", "Queued model training job.", "admin"),
    "api.view_credentials": ContractDescriptor("credentials.summary", "v1", "Non-sensitive Snowflake credential summary.", "admin"),
    "api.save_credentials": ContractDescriptor("credentials.write_blocked", "v1", "Credential write-block response.", "admin"),
    "api.contract_registry": ContractDescriptor("contracts.registry", API_CONTRACT_DOC_VERSION, "Published API contract registry.", "system"),
}


def resolve_contract(endpoint_name: Optional[str]) -> ContractDescriptor:
    """Resolve the versioned contract descriptor for an endpoint."""
    if not endpoint_name:
        return DEFAULT_CONTRACT
    return CONTRACT_REGISTRY.get(endpoint_name, DEFAULT_CONTRACT)


def build_contract_version_payload(contract: ContractDescriptor) -> dict:
    """Build the standard version payload included in every API response."""
    return {
        "api": API_OUTPUT_VERSION,
        "contract": {
            "id": contract.id,
            "version": contract.version,
        },
    }


def build_contract_registry_payload() -> dict:
    """Return a serializable contract registry for discovery/documentation."""
    contracts = []
    for endpoint_name, descriptor in sorted(CONTRACT_REGISTRY.items()):
        contracts.append({
            "endpoint": endpoint_name,
            "id": descriptor.id,
            "version": descriptor.version,
            "category": descriptor.category,
            "summary": descriptor.summary,
        })

    return {
        "api_version": API_OUTPUT_VERSION,
        "response_envelope": {
            "status": "success|error",
            "version": {
                "api": API_OUTPUT_VERSION,
                "contract": {
                    "id": "contract identifier",
                    "version": "contract version",
                },
            },
            "data": "response payload when successful",
            "meta": "optional metadata",
            "message": "optional user-facing summary",
            "error": "error object when unsuccessful",
        },
        "contracts": contracts,
    }
