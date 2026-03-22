"""
Dashboard Routes
Main dashboard pages and views
"""

from flask import Blueprint, render_template, request, current_app, url_for, redirect
from utils.common import get_services, get_shared_filters

dashboard = Blueprint('dashboard', __name__)


def _render_page_error(action, status_code=500):
    """Render a safe page-level error state and log the underlying exception."""
    current_app.logger.exception("Dashboard page failed: %s", action)
    return render_template(
        'error.html',
        error_code=status_code,
        error_message='The page could not be loaded right now. Please try again.'
    ), status_code


def _build_shared_filter_context(enrolment_service, endpoint_name, hidden_fields=None):
    """Build common template context for the shared dashboard filters."""
    filters = get_shared_filters()
    preserved = {
        key: value for key, value in (hidden_fields or {}).items()
        if value not in (None, '')
    }

    return {
        'available_years': enrolment_service.get_available_years(location=filters['location']),
        'available_locations': enrolment_service.get_available_locations(
            start_year=filters['start_year'],
            end_year=filters['end_year']
        ),
        'selected_start_year': filters['start_year'],
        'selected_end_year': filters['end_year'],
        'selected_location': filters['location'],
        'shared_filters': filters,
        'filter_hidden_fields': preserved,
        'filter_reset_url': url_for(endpoint_name, **preserved),
    }


@dashboard.route('/')
def index():
    """Main dashboard page"""
    try:
        enrolment_service, forecast_service = get_services()
        model_type = request.args.get('model', current_app.config.get('DEFAULT_MODEL', 'sarima'))
        filters = get_shared_filters()
        available_models = current_app.config.get('AVAILABLE_MODELS', ['arima', 'sarima', 'lstm'])
        if model_type not in available_models:
            model_type = current_app.config.get('DEFAULT_MODEL', 'sarima')
        
        # Get summary stats
        summary = enrolment_service.get_dashboard_summary(**filters)
        
        # Get enrolment trends
        trends = enrolment_service.get_enrolment_trends(**filters)

        # Get combined trend + forecast data for selected model
        combined_data = forecast_service.get_historical_with_forecast(model_type, **filters)

        # Get SSA breakdown
        ssa_data = enrolment_service.get_enrolment_by_ssa(**filters)

        # Get provider list and data
        providers = enrolment_service.get_providers_list(**filters)
        provider_data = enrolment_service.get_enrolment_by_provider(**filters)

        shared_context = _build_shared_filter_context(
            enrolment_service,
            'dashboard.index',
            hidden_fields={'model': model_type}
        )
        
        return render_template(
            'dashboard.html',
            summary=summary,
            trends=trends.to_dict(orient='records') if len(trends) > 0 else [],
            combined_data=combined_data.to_dict(orient='records') if len(combined_data) > 0 else [],
            ssa_data=ssa_data.to_dict(orient='records') if len(ssa_data) > 0 else [],
            providers=providers.to_dict(orient='records') if len(providers) > 0 else [],
            provider_data=provider_data.to_dict(orient='records') if len(provider_data) > 0 else [],
            available_models=available_models,
            selected_model=model_type,
            page_title='FE Enrolment Dashboard',
            **shared_context
        )
    except Exception:
        return _render_page_error('dashboard')

@dashboard.route('/demographics')
def demographics():
    """Demographics analysis page"""
    try:
        enrolment_service, _ = get_services()
        filters = get_shared_filters()
        
        # Get age breakdown
        age_data = enrolment_service.get_enrolment_by_age(**filters)
        
        # Get level breakdown
        level_data = enrolment_service.get_enrolment_by_level(**filters)

        shared_context = _build_shared_filter_context(
            enrolment_service,
            'dashboard.demographics'
        )
        
        return render_template(
            'demographics.html',
            age_data=age_data.to_dict(orient='records') if len(age_data) > 0 else [],
            level_data=level_data.to_dict(orient='records') if len(level_data) > 0 else [],
            page_title='Demographics',
            **shared_context
        )
    except Exception:
        return _render_page_error('demographics')


@dashboard.route('/forecast')
def forecast():
    """Forecast comparison page"""
    try:
        enrolment_service, forecast_service = get_services()
        
        model_type = request.args.get('model', current_app.config.get('DEFAULT_MODEL', 'sarima'))
        filters = get_shared_filters()
        available_models = current_app.config.get('AVAILABLE_MODELS', ['arima', 'sarima', 'lstm'])
        if model_type not in available_models:
            model_type = current_app.config.get('DEFAULT_MODEL', 'sarima')
        
        # Get forecast for selected model
        forecast_data = forecast_service.get_forecast(model_type, **filters)
        
        # Compare all models
        comparison = forecast_service.compare_models(**filters)
        
        # Get model accuracy
        accuracy = forecast_service.get_model_accuracy(model_type)
        
        # Get SSA forecast and group by SSA
        ssa_forecast_raw = forecast_service.get_ssa_forecast_filtered(
            model_type=model_type,
            location=filters['location'],
            start_year=filters['start_year'],
            end_year=filters['end_year'],
            periods=forecast_service.forecast_periods
        )
        ssa_forecast = ssa_forecast_raw.to_dict(orient='records') if len(ssa_forecast_raw) > 0 else []
        
        # Group SSA data by subject area
        ssa_grouped = {}
        for row in ssa_forecast:
            ssa_name = row.get('SSA_TIER1', 'Unknown')
            year = row.get('ACADEMIC_YEAR', '')
            predicted = row.get('PREDICTED_ENROLMENTS', 0)
            if ssa_name not in ssa_grouped:
                ssa_grouped[ssa_name] = {}
            ssa_grouped[ssa_name][year] = predicted
        
        return render_template(
            'forecast.html',
            forecast_data=forecast_data.to_dict(orient='records') if len(forecast_data) > 0 else [],
            comparison={k: v.to_dict(orient='records') for k, v in comparison.items()},
            accuracy=accuracy,
            ssa_forecast=ssa_forecast,
            ssa_grouped=ssa_grouped,
            selected_model=model_type,
            available_models=available_models,
            page_title='Forecast Analysis',
            **_build_shared_filter_context(
                enrolment_service,
                'dashboard.forecast',
                hidden_fields={'model': model_type}
            )
        )
    except Exception:
        return _render_page_error('forecast')


@dashboard.route('/college-forecast')
def college_forecast():
    """College-specific forecast page."""
    try:
        enrolment_service, forecast_service = get_services()
        filters = get_shared_filters()

        model_type = request.args.get('model', current_app.config.get('DEFAULT_MODEL', 'sarima'))
        selected_college_id = request.args.get('college_id', '')
        selected_funding_scheme = request.args.get('funding_scheme', '')
        selected_ssa = request.args.get('ssa', '')
        selected_course = request.args.get('course', '')
        selected_level = request.args.get('level', '')
        available_models = current_app.config.get('AVAILABLE_MODELS', ['arima', 'sarima', 'lstm'])
        if model_type not in available_models:
            model_type = current_app.config.get('DEFAULT_MODEL', 'sarima')

        provider_filter = int(selected_college_id) if str(selected_college_id).strip().isdigit() else None
        colleges = enrolment_service.get_providers_list(**filters)
        filter_options = forecast_service.get_college_forecast_filter_options(
            provider_id=provider_filter,
            funding_scheme=selected_funding_scheme,
            ssa=selected_ssa,
            course=selected_course,
            level=selected_level,
            **filters
        )

        return render_template(
            'college_forecast.html',
            colleges=colleges.to_dict(orient='records') if len(colleges) > 0 else [],
            selected_college_id=str(selected_college_id),
            selected_funding_scheme=str(selected_funding_scheme),
            selected_ssa=str(selected_ssa),
            selected_course=str(selected_course),
            selected_level=str(selected_level),
            funding_schemes=filter_options.get('funding_schemes', []),
            ssa_options=filter_options.get('ssa_options', []),
            course_options=filter_options.get('course_options', []),
            level_options=filter_options.get('level_options', []),
            course_requires_college=filter_options.get('course_requires_college', True),
            selected_model=model_type,
            available_models=available_models,
            page_title='College Forecast',
            **_build_shared_filter_context(
                enrolment_service,
                'dashboard.college_forecast',
                hidden_fields={
                    'model': model_type,
                    'college_id': str(selected_college_id).strip(),
                    'funding_scheme': str(selected_funding_scheme).strip(),
                    'ssa': str(selected_ssa).strip(),
                    'course': str(selected_course).strip(),
                    'level': str(selected_level).strip()
                }
            )
        )
    except Exception:
        return _render_page_error('college forecast')


@dashboard.route('/data-management')
def data_management():
    """Data management page"""
    try:
        import os
        from datetime import datetime
        from services.metadata_service import MetadataService
        
        data_dir = os.path.join(current_app.root_path, 'data')
        model_dir = current_app.config.get('MODEL_DIR', 'saved_models')
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        
        # Get database metadata
        metadata_service = MetadataService(config_file)
        db_info = metadata_service.get_database_info()
        schema_structure = metadata_service.get_schema_structure()
        
        # Check for data files
        data_files = []
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.endswith('.csv'):
                    filepath = os.path.join(data_dir, filename)
                    stat = os.stat(filepath)
                    data_files.append({
                        'name': filename,
                        'size': stat.st_size,
                        'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                    })
        
        # Check for model files
        model_files = []
        if os.path.exists(model_dir):
            for filename in os.listdir(model_dir):
                if filename.endswith(('.csv', '.json', '.pkl')):
                    filepath = os.path.join(model_dir, filename)
                    stat = os.stat(filepath)
                    model_files.append({
                        'name': filename,
                        'size': stat.st_size,
                        'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                    })
        
        return render_template(
            'data_management.html',
            db_info=db_info,
            schema_structure=schema_structure,
            data_files=data_files,
            model_files=model_files,
            page_title='Data Management'
        )
    except Exception:
        return _render_page_error('data management')


@dashboard.route('/programme-plans')
def programme_plans():
    """Programme Plans dashboard view."""
    try:
        return render_template(
            'programme_plans.html',
            page_title='Programme Plans Explorer',
            available_models=current_app.config.get('AVAILABLE_MODELS', []),
            selected_model=current_app.config.get('DEFAULT_MODEL', 'sarima')
        )
    except Exception:
        return _render_page_error('programme plans')


@dashboard.route('/dev')
def developer():
    """Developer contact page."""
    try:
        selected_model = request.args.get('model', current_app.config.get('DEFAULT_MODEL', 'sarima'))
        available_models = current_app.config.get('AVAILABLE_MODELS', ['arima', 'sarima', 'lstm'])
        if selected_model not in available_models:
            selected_model = current_app.config.get('DEFAULT_MODEL', 'sarima')

        return render_template(
            'developer.html',
            page_title='Developer',
            available_models=available_models,
            selected_model=selected_model
        )
    except Exception:
        return _render_page_error('developer page')


@dashboard.route('/4cast-legacy')
def legacy_programme_plans_redirect():
    """Legacy Programme Plans URL redirect."""
    return redirect(url_for('dashboard.programme_plans'), code=302)
