"""Shared API response and validation helpers."""

from typing import Iterable, Optional

import numpy as np
import pandas as pd
from flask import current_app, has_request_context, jsonify, request

from contracts import build_contract_version_payload, resolve_contract

from utils.filtering import normalize_text_arg, normalize_year_arg


class ApiValidationError(ValueError):
    """Raised when request input fails validation."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.field = field


def success_response(
    data=None,
    *,
    message: Optional[str] = None,
    meta: Optional[dict] = None,
    status_code: int = 200,
    contract_key: Optional[str] = None,
):
    """Return a consistent success envelope."""
    contract = resolve_contract(contract_key or (request.endpoint if has_request_context() else None))
    version_payload = build_contract_version_payload(contract)
    payload: dict[str, object] = {
        "status": "success",
        "version": version_payload,
        "data": data,
    }
    if message:
        payload["message"] = message
    if meta is not None:
        payload["meta"] = meta
    response = jsonify(payload)
    response.status_code = status_code
    response.headers["X-API-Version"] = str(version_payload["api"])
    response.headers["X-Contract-Id"] = contract.id
    response.headers["X-Contract-Version"] = contract.version
    return response


def error_response(
    message: str,
    *,
    status_code: int = 400,
    field: Optional[str] = None,
    errors: Optional[Iterable[dict]] = None,
    contract_key: Optional[str] = None,
):
    """Return a consistent error envelope."""
    contract = resolve_contract(contract_key or (request.endpoint if has_request_context() else None))
    version_payload = build_contract_version_payload(contract)
    error_payload: dict[str, object] = {
        "message": message,
    }
    if field:
        error_payload["field"] = field
    if errors:
        error_payload["details"] = list(errors)
    payload: dict[str, object] = {
        "status": "error",
        "version": version_payload,
        "error": error_payload,
    }
    response = jsonify(payload)
    response.status_code = status_code
    response.headers["X-API-Version"] = str(version_payload["api"])
    response.headers["X-Contract-Id"] = contract.id
    response.headers["X-Contract-Version"] = contract.version
    return response


def exception_response(
    action: str,
    *,
    message: str = "The request could not be completed.",
    status_code: int = 500,
    contract_key: Optional[str] = None,
):
    """Log an exception and return a safe API error."""
    current_app.logger.exception("API request failed: %s", action)
    return error_response(message, status_code=status_code, contract_key=contract_key)


def dataframe_to_safe_records(df: Optional[pd.DataFrame]):
    """Convert a DataFrame to records with NaN/inf normalized to None."""
    if df is None:
        return []

    safe_df = df.replace([np.inf, -np.inf], np.nan)
    safe_df = safe_df.astype(object).where(pd.notnull(safe_df), None)
    return safe_df.to_dict(orient="records")


def require_json_object(payload):
    """Validate that a parsed JSON request body is an object."""
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ApiValidationError("JSON request body must be an object")
    return payload


def parse_optional_text(value):
    """Parse an optional text value."""
    return normalize_text_arg(value)


def parse_optional_int(value, field_name: str, *, minimum=None, maximum=None):
    """Parse an optional integer value."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        number = int(text)
    except ValueError as exc:
        raise ApiValidationError(f"{field_name} must be an integer", field=field_name) from exc

    if minimum is not None and number < minimum:
        raise ApiValidationError(
            f"{field_name} must be greater than or equal to {minimum}",
            field=field_name,
        )
    if maximum is not None and number > maximum:
        raise ApiValidationError(
            f"{field_name} must be less than or equal to {maximum}",
            field=field_name,
        )
    return number


def parse_choice(value, field_name: str, allowed_values, *, default=None):
    """Parse a value constrained to a defined set."""
    if value is None or str(value).strip() == "":
        return default

    normalized = str(value).strip().lower()
    allowed = {str(item).lower(): item for item in allowed_values}
    if normalized not in allowed:
        allowed_display = ", ".join(str(item) for item in allowed_values)
        raise ApiValidationError(
            f"{field_name} must be one of: {allowed_display}",
            field=field_name,
        )
    return str(allowed[normalized]).lower()


def parse_shared_filters(source):
    """Parse shared dashboard filters with strict validation."""
    start_raw = source.get("start_year")
    end_raw = source.get("end_year")
    location = normalize_text_arg(source.get("location"))

    if start_raw not in (None, "") and normalize_year_arg(start_raw) is None:
        raise ApiValidationError("start_year must be a valid year", field="start_year")
    if end_raw not in (None, "") and normalize_year_arg(end_raw) is None:
        raise ApiValidationError("end_year must be a valid year", field="end_year")

    start_year = normalize_year_arg(start_raw)
    end_year = normalize_year_arg(end_raw)
    if start_year is not None and end_year is not None and start_year > end_year:
        start_year, end_year = end_year, start_year

    return {
        "start_year": start_year,
        "end_year": end_year,
        "location": location,
    }


def parse_pagination(source, *, default_page=1, default_page_size=100, max_page_size=500):
    """Parse generic pagination parameters."""
    page = parse_optional_int(source.get("page"), "page", minimum=1) or default_page
    page_size = (
        parse_optional_int(source.get("page_size"), "page_size", minimum=1, maximum=max_page_size)
        or default_page_size
    )
    return {
        "page": page,
        "page_size": page_size,
        "offset": (page - 1) * page_size,
    }
