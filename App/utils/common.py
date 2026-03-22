"""
Common utilities for the FE Dashboard
Shared functions to avoid code duplication
"""

import sys
import os
from flask import current_app, request

from utils.filtering import normalize_shared_filters


def get_project_root():
    """Get the project root directory"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_path():
    """Add project root to Python path"""
    root = get_project_root()
    if root not in sys.path:
        sys.path.insert(0, root)


def get_services():
    """
    Get service instances with config from Flask app
    
    Returns:
        tuple: (EnrolmentService, ForecastService)
    """
    from services.enrolment_service import EnrolmentService
    from services.forecast_service import ForecastService
    
    config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
    model_dir = current_app.config.get('MODEL_DIR', 'saved_models')
    
    enrolment_service = EnrolmentService(config_file)
    forecast_service = ForecastService(config_file, model_dir)
    
    return enrolment_service, forecast_service


def get_shared_filters(args=None):
    """Return normalized shared page filters from request args."""
    source = args if args is not None else request.args
    return normalize_shared_filters(
        start_year=source.get('start_year'),
        end_year=source.get('end_year'),
        location=source.get('location'),
    )
