"""
API Routes
RESTful API endpoints for dashboard data
"""

import json
import os

from contracts import build_contract_registry_payload
from flask import Blueprint, request, current_app, send_file
from services.admin_jobs import (
    refresh_all_data_job,
    refresh_programme_plans_job,
    train_models_job,
)
from utils.api_contracts import (
    ApiValidationError,
    dataframe_to_safe_records,
    error_response,
    exception_response,
    parse_choice,
    parse_optional_int,
    parse_optional_text,
    parse_pagination,
    parse_shared_filters,
    require_json_object,
    success_response,
)
from utils.common import get_services
from utils.credentials import get_snowflake_connection_summary, save_snowflake_settings

api = Blueprint('api', __name__, url_prefix='/api')


def _parse_optional_provider_id(source):
    """Parse provider_id from request args."""
    return parse_optional_int(source.get('provider_id'), 'provider_id')


def _get_college_forecast_filters(source):
    """Return normalized college-forecast page filters."""
    return {
        'funding_scheme': parse_optional_text(source.get('funding_scheme')),
        'ssa': parse_optional_text(source.get('ssa')),
        'course': parse_optional_text(source.get('course')),
        'level': parse_optional_text(source.get('level')),
    }


def _parse_model_type(source):
    """Parse and validate a forecast model type."""
    available_models = current_app.config.get('AVAILABLE_MODELS', ['arima', 'sarima', 'lstm'])
    default_model = current_app.config.get('DEFAULT_MODEL', 'sarima')
    return parse_choice(source.get('model'), 'model', available_models, default=default_model)


def _parse_programme_plans_payload():
    """Parse Programme Plans explorer request data from JSON or query params."""
    body = require_json_object(request.get_json(silent=True)) if request.method == 'POST' else {}
    pagination_source = body if request.method == 'POST' else request.args

    filters = body.get('filters')
    if filters is None and request.args.get('filters'):
        try:
            filters = json.loads(request.args.get('filters', '{}'))
        except json.JSONDecodeError as exc:
            raise ApiValidationError('filters must be valid JSON', field='filters') from exc
    filters = require_json_object(filters)

    pagination = parse_pagination(pagination_source, default_page=1, default_page_size=100, max_page_size=500)
    return {
        'page': pagination['page'],
        'page_size': pagination['page_size'],
        'search': parse_optional_text(body.get('search', request.args.get('search'))),
        'filters': filters,
    }


@api.route('/health')
def health():
    """Health check endpoint"""
    return success_response(
        {'healthy': True},
        message='API is running',
    )


@api.route('/contracts')
def contract_registry():
    """Return the published API contract registry and response envelope shape."""
    return success_response(build_contract_registry_payload())


@api.route('/database/status')
def database_status():
    """Check Snowflake database connection and existence"""
    try:
        from db.snowflake import SnowflakeDB

        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        db = SnowflakeDB(config_file)

        if not db.connect():
            return success_response({
                'connected': False,
                'database': None,
                'message': 'Failed to connect. Check the configured Snowflake credentials.'
            })

        expected_db = 'COMMON_DATA_MODEL'
        if not db.database or db.database != expected_db:
            db.disconnect()
            return success_response({
                'connected': False,
                'database': db.database if db.database else None,
                'message': 'Database mismatch or not set'
            })

        try:
            db.execute_query("SELECT 1")
            return success_response({
                'connected': True,
                'database': db.database,
                'status': 'CONNECTED',
                'message': 'Database connection verified'
            })
        finally:
            db.disconnect()
    except Exception:
        current_app.logger.exception("Database status check failed")
        return success_response({
            'connected': False,
            'database': None,
            'message': 'Connection error'
        })


@api.route('/summary')
def summary():
    """Get dashboard summary statistics"""
    try:
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        data = enrolment_service.get_dashboard_summary(**filters)
        return success_response(data)
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('dashboard summary', message='Unable to load the dashboard summary.')


@api.route('/enrolment/trends')
def enrolment_trends():
    """Get enrolment trends by academic year"""
    try:
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        df = enrolment_service.get_enrolment_trends(**filters)
        return success_response(dataframe_to_safe_records(df))
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('enrolment trends', message='Unable to load enrolment trends.')


@api.route('/enrolment/by-provider')
def enrolment_by_provider():
    """Get enrolment by provider"""
    try:
        provider_id = _parse_optional_provider_id(request.args)
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        df = enrolment_service.get_enrolment_by_provider(provider_id, **filters)
        return success_response(dataframe_to_safe_records(df))
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except ValueError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('provider enrolment', message='Unable to load provider enrolment data.')


@api.route('/enrolment/by-ssa')
def enrolment_by_ssa():
    """Get enrolment by Sector Subject Area"""
    try:
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        df = enrolment_service.get_enrolment_by_ssa(**filters)
        return success_response(dataframe_to_safe_records(df))
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('SSA enrolment', message='Unable to load subject area enrolment data.')


@api.route('/enrolment/by-age')
def enrolment_by_age():
    """Get enrolment by age group"""
    try:
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        df = enrolment_service.get_enrolment_by_age(**filters)
        return success_response(dataframe_to_safe_records(df))
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('age enrolment', message='Unable to load age breakdown data.')


@api.route('/enrolment/by-level')
def enrolment_by_level():
    """Get enrolment by qualification level"""
    try:
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        df = enrolment_service.get_enrolment_by_level(**filters)
        return success_response(dataframe_to_safe_records(df))
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('level enrolment', message='Unable to load level breakdown data.')


@api.route('/providers')
def providers():
    """Get list of all providers"""
    try:
        enrolment_service, _ = get_services()
        filters = parse_shared_filters(request.args)
        df = enrolment_service.get_providers_list(**filters)
        return success_response(dataframe_to_safe_records(df))
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('providers list', message='Unable to load providers.')


@api.route('/ssa')
def ssa_list():
    """Get list of all SSA categories"""
    try:
        enrolment_service, _ = get_services()
        df = enrolment_service.get_ssa_list()
        return success_response(dataframe_to_safe_records(df))
    except Exception:
        return exception_response('SSA list', message='Unable to load subject areas.')


@api.route('/forecast')
def forecast():
    """
    Get forecast for specified model
    
    Query params:
        model: Model type (arima, sarima, lstm)
        provider_id: Optional provider filter
    """
    try:
        model_type = _parse_model_type(request.args)
        filters = parse_shared_filters(request.args)
        provider_id = _parse_optional_provider_id(request.args)
        college_filters = _get_college_forecast_filters(request.args)
        
        _, forecast_service = get_services()
        df = forecast_service.get_forecast(
            model_type,
            provider_id,
            **college_filters,
            **filters
        )
        
        return success_response(
            dataframe_to_safe_records(df),
            meta={
                'model': model_type,
                'provider_id': provider_id,
                **college_filters,
                **filters,
            }
        )
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except ValueError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('forecast data', message='Unable to load forecast data.')


@api.route('/forecast/combined')
def forecast_combined():
    """
    Get historical data combined with forecast
    
    Query params:
        model: Model type (arima, sarima, lstm)
        provider_id: Optional provider filter
    """
    try:
        model_type = _parse_model_type(request.args)
        filters = parse_shared_filters(request.args)
        provider_id = _parse_optional_provider_id(request.args)
        college_filters = _get_college_forecast_filters(request.args)
        
        _, forecast_service = get_services()
        df = forecast_service.get_historical_with_forecast(
            model_type,
            provider_id,
            **college_filters,
            **filters
        )
        return success_response(
            dataframe_to_safe_records(df),
            meta={
                'model': model_type,
                'provider_id': provider_id,
                **college_filters,
                **filters,
            }
        )
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except ValueError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('combined forecast data', message='Unable to load historical and forecast data.')


@api.route('/forecast/compare')
def forecast_compare():
    """Compare forecasts from all available models"""
    try:
        filters = parse_shared_filters(request.args)
        provider_id = _parse_optional_provider_id(request.args)
        college_filters = _get_college_forecast_filters(request.args)
        
        _, forecast_service = get_services()
        comparison = forecast_service.compare_models(
            provider_id,
            **college_filters,
            **filters
        )
        
        result = {}
        for model, df in comparison.items():
            result[model] = dataframe_to_safe_records(df)
        
        return success_response(result, meta={
            'provider_id': provider_id,
            **college_filters,
            **filters,
        })
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except ValueError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('forecast comparison', message='Unable to compare forecast models.')


@api.route('/forecast/filter-options')
def forecast_filter_options():
    """Get cascading filter options for the college forecast page."""
    try:
        provider_id = _parse_optional_provider_id(request.args)
        filters = parse_shared_filters(request.args)
        college_filters = _get_college_forecast_filters(request.args)

        _, forecast_service = get_services()
        options = forecast_service.get_college_forecast_filter_options(
            provider_id=provider_id,
            **college_filters,
            **filters
        )

        return success_response(
            options,
            meta={
                'provider_id': provider_id,
                **college_filters,
                **filters,
            }
        )
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except ValueError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('forecast filter options', message='Unable to load college forecast filters.')


@api.route('/forecast/subject-areas')
def forecast_subject_areas():
    """Get SSA forecast table filtered by provider and funding scheme."""
    try:
        model_type = _parse_model_type(request.args)
        provider_id = _parse_optional_provider_id(request.args)
        filters = parse_shared_filters(request.args)
        college_filters = _get_college_forecast_filters(request.args)

        _, forecast_service = get_services()
        df = forecast_service.get_ssa_forecast_filtered(
            model_type=model_type,
            provider_id=provider_id,
            **college_filters,
            location=filters['location'],
            start_year=filters['start_year'],
            end_year=filters['end_year'],
            periods=3
        )

        if len(df) == 0:
            return success_response({
                'years': [],
                'table': [],
                'rows': []
            }, meta={
                'model': model_type,
                'provider_id': provider_id,
                **college_filters,
                **filters,
            })

        years = sorted(df['ACADEMIC_YEAR'].dropna().unique().tolist())
        pivot = (
            df.pivot_table(
                index='SSA_TIER1',
                columns='ACADEMIC_YEAR',
                values='PREDICTED_ENROLMENTS',
                aggfunc='sum',
                fill_value=0
            )
            .reset_index()
            .rename(columns={'SSA_TIER1': 'Subject Area'})
        )

        for year in years:
            if year not in pivot.columns:
                pivot[year] = 0
        pivot = pivot[['Subject Area'] + years].sort_values('Subject Area')

        # Round for display table consistency
        for year in years:
            pivot[year] = pivot[year].round(0).astype(int)

        return success_response({
            'years': years,
            'table': dataframe_to_safe_records(pivot),
            'rows': dataframe_to_safe_records(df)
        }, meta={
            'model': model_type,
            'provider_id': provider_id,
            **college_filters,
            **filters,
        })
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except ValueError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('forecast subject areas', message='Unable to load forecast subject areas.')


@api.route('/forecast/accuracy')
def forecast_accuracy():
    """Get model accuracy metrics"""
    try:
        model_type = _parse_model_type(request.args)
        
        _, forecast_service = get_services()
        accuracy = forecast_service.get_model_accuracy(model_type)
        
        return success_response(accuracy, meta={'model': model_type})
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('forecast accuracy', message='Unable to load forecast accuracy metrics.')


@api.route('/timeseries')
def timeseries():
    """Get time series data for forecasting"""
    try:
        group_by = parse_choice(
            request.args.get('group_by'),
            'group_by',
            ['total', 'provider', 'ssa'],
            default='total'
        )
        filters = parse_shared_filters(request.args)
        
        enrolment_service, _ = get_services()
        df = enrolment_service.get_time_series_data(group_by, **filters)
        
        return success_response(dataframe_to_safe_records(df), meta={'group_by': group_by, **filters})
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('time series', message='Unable to load time series data.')


@api.route('/programme-plans/refresh', methods=['POST'])
@api.route('/4cast/refresh', methods=['POST'])
def programme_plans_refresh():
    """Refresh the local Programme Plans snapshot CSV from live Snowflake data."""
    try:
        flask_app = current_app._get_current_object()
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        csv_file = current_app.config.get('PROGRAMME_PLANS_CSV_FILE')
        job_manager = current_app.extensions['job_manager']
        job = job_manager.submit(
            'Programme Plans refresh',
            lambda: refresh_programme_plans_job(config_file=config_file, csv_file=csv_file),
            app=flask_app,
        )
        return success_response(
            job,
            message='Programme Plans refresh started',
            status_code=202,
        )
    except Exception:
        return exception_response(
            'programme plans refresh',
            message='Unable to refresh Programme Plans data right now.',
        )


@api.route('/jobs/<job_id>')
def job_status(job_id):
    """Return the current status for an asynchronous admin job."""
    job_manager = current_app.extensions['job_manager']
    job = job_manager.get_job(job_id)
    if job is None:
        return error_response('Job not found', status_code=404, field='job_id')
    return success_response(job)


@api.route('/programme-plans/filters')
@api.route('/4cast/filters')
def programme_plans_filters():
    """Get available Programme Plans filter values (e.g., Infill, Approval Status)."""
    try:
        from services.programme_plans_service import ProgrammePlansService

        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        csv_file = current_app.config.get('PROGRAMME_PLANS_CSV_FILE')

        service = ProgrammePlansService(config_file=config_file, snapshot_csv=csv_file)
        data = service.get_filters()

        return success_response(data)
    except Exception:
        return exception_response('programme plans filters', message='Unable to load Programme Plans filters.')


@api.route('/programme-plans/summary')
@api.route('/4cast/summary')
def programme_plans_summary():
    """
    Reproduce old pivot summary:
    rows by CA Name with sums of Main Qual GLH, Blended Hours, Tutorial Hours.
    """
    try:
        from services.programme_plans_service import ProgrammePlansService

        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        csv_file = current_app.config.get('PROGRAMME_PLANS_CSV_FILE')

        infill = parse_optional_text(request.args.get('infill'))
        approval_status = parse_optional_text(request.args.get('approval_status'))

        service = ProgrammePlansService(config_file=config_file, snapshot_csv=csv_file)
        df = service.get_hours_summary(infill=infill, approval_status=approval_status)

        return success_response(
            dataframe_to_safe_records(df),
            meta={
                'infill': infill,
                'approval_status': approval_status,
                'row_count': int(len(df)),
            }
        )
    except Exception:
        return exception_response('programme plans summary', message='Unable to load Programme Plans summary data.')


@api.route('/programme-plans/data', methods=['GET', 'POST'])
@api.route('/4cast/data', methods=['GET', 'POST'])
def programme_plans_data():
    """Get server-side filtered and paginated Programme Plans explorer rows."""
    try:
        from services.programme_plans_service import ProgrammePlansService

        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        csv_file = current_app.config.get('PROGRAMME_PLANS_CSV_FILE')
        payload = _parse_programme_plans_payload()

        service = ProgrammePlansService(config_file=config_file, snapshot_csv=csv_file)
        result = service.get_paginated_data(
            page=payload['page'],
            page_size=payload['page_size'],
            search=payload['search'],
            column_filters=payload['filters'],
        )

        return success_response(
            {
                'columns': result['columns'],
                'rows': result['rows'],
                'search': payload['search'] or '',
                'applied_filters': result['applied_filters'],
            },
            meta={
                'pagination': result['pagination'],
                'dataset': result['dataset'],
            }
        )
    except ApiValidationError as exc:
        return error_response(exc.message, status_code=400, field=exc.field)
    except Exception:
        return exception_response('programme plans detail data', message='Unable to load Programme Plans data.')


@api.route('/programme-plans/csv-data', methods=['GET', 'POST'])
@api.route('/4cast/csv-data', methods=['GET', 'POST'])
def programme_plans_csv_data():
    """Backward-compatible alias for paginated Programme Plans explorer data."""
    return programme_plans_data()


@api.route('/programme-plans/csv-download')
@api.route('/4cast/csv-download')
def programme_plans_csv_download():
    """Download the current local Programme Plans CSV snapshot."""
    try:
        csv_file = current_app.config.get('PROGRAMME_PLANS_CSV_FILE')
        if not csv_file:
            return error_response('Programme Plans dataset is not configured')
        if not os.path.exists(csv_file):
            return error_response('Programme Plans dataset is not available', status_code=404)

        return send_file(
            csv_file,
            mimetype='text/csv',
            as_attachment=True,
            download_name='programme-plans-export.csv'
        )
    except Exception:
        return exception_response('programme plans download', message='Unable to download the Programme Plans export.')


@api.route('/data/refresh', methods=['POST'])
def refresh_data():
    """Queue a refresh for all Snowflake-backed CSV data files."""
    try:
        flask_app = current_app._get_current_object()
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        root_path = current_app.root_path
        data_dir = os.path.join(root_path, 'data')
        programme_plans_csv = current_app.config.get('PROGRAMME_PLANS_CSV_FILE') or os.path.join(
            data_dir,
            'programme_plans.csv'
        )
        job_manager = current_app.extensions['job_manager']
        job = job_manager.submit(
            'Data refresh',
            lambda: refresh_all_data_job(
                config_file=config_file,
                root_path=root_path,
                data_dir=data_dir,
                programme_plans_csv=programme_plans_csv,
            ),
            app=flask_app,
        )
        return success_response(job, message='Data refresh started', status_code=202)
    except Exception:
        return exception_response('data refresh', message='Unable to start the data refresh job.')


@api.route('/metadata/database')
def metadata_database():
    """Get database information"""
    try:
        from services.metadata_service import MetadataService
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        metadata_service = MetadataService(config_file)
        info = metadata_service.get_database_info()
        return success_response(info)
    except Exception:
        return exception_response('metadata database info', message='Unable to load database metadata.')


@api.route('/metadata/schemas')
def metadata_schemas():
    """Get list of schemas"""
    try:
        from services.metadata_service import MetadataService
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        metadata_service = MetadataService(config_file)
        schemas = metadata_service.get_schemas()
        return success_response({'schemas': schemas})
    except Exception:
        return exception_response('metadata schemas', message='Unable to load schema metadata.')


@api.route('/metadata/schema-structure')
def metadata_schema_structure():
    """Get complete schema structure with tables and views"""
    try:
        from services.metadata_service import MetadataService
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        metadata_service = MetadataService(config_file)
        structure = metadata_service.get_schema_summary()
        return success_response(structure)
    except Exception:
        return exception_response('metadata schema structure', message='Unable to load schema structure.')


@api.route('/models/train', methods=['POST'])
def train_models():
    """Queue forecasting model training."""
    try:
        flask_app = current_app._get_current_object()
        project_root = current_app.root_path
        job_manager = current_app.extensions['job_manager']
        job = job_manager.submit(
            'Model training',
            lambda: train_models_job(project_root=project_root),
            app=flask_app,
        )
        return success_response(job, message='Model training started', status_code=202)
    except Exception:
        return exception_response('model training', message='Unable to train models right now.')

@api.route('/credentials/view', methods=['GET'])
def view_credentials():
    """View non-sensitive Snowflake credential metadata."""
    try:
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        summary = get_snowflake_connection_summary(config_file)
        return success_response(summary)
    except Exception:
        return exception_response('credentials summary', message='Unable to load Snowflake connection details.')


@api.route('/credentials/save', methods=['POST'])
def save_credentials():
    """Save Snowflake credential settings for local app usage."""
    try:
        payload = require_json_object(request.get_json(silent=True))
        config_file = current_app.config.get('SNOWFLAKE_CONFIG_FILE')
        summary = save_snowflake_settings(payload, config_file)
        return success_response(summary, message='Snowflake connection details saved successfully.')
    except RuntimeError as exc:
        return error_response(str(exc), status_code=400)
    except Exception:
        return exception_response('credentials save', message='Unable to save Snowflake connection details.')
