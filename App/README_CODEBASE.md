# FE Dashboard Codebase Guide

## Purpose

This document explains how the repository is structured, how requests move through the system, what each major module is responsible for, and where to make changes when extending the application.

Use this guide when you need to:

- onboard to the repository
- trace a page or API request end to end
- add a new route, service method, or page
- understand where data refresh, training, and Programme Plans logic live

## Architectural Summary

The application is a Flask server-rendered dashboard with a small JavaScript layer for asynchronous UI flows and paginated table interactions.

There are three runtime patterns to keep in mind:

1. server-rendered page requests
2. JSON API requests
3. background admin jobs

The main stack is:

1. `app.py` creates the Flask application
2. `routes/dashboard.py` handles page rendering
3. `routes/api.py` handles JSON endpoints
4. `services/` contains business logic and query composition
5. `db/snowflake.py` handles Snowflake connectivity
6. `templates/` and `static/js/` render the UI

## Repository Map

### Entry And App Setup

- `app.py`: Flask app factory, config loading, runtime validation, blueprint registration, shared error pages, and job manager setup

### Routing Layer

- `routes/dashboard.py`: page routes and Jinja rendering
- `routes/api.py`: JSON endpoints, validation boundaries, background job submission, metadata, credentials, and Programme Plans pagination

### Business Logic Layer

- `services/enrolment_service.py`: historical enrolment queries and dashboard summaries
- `services/forecast_service.py`: forecast retrieval, filtered forecast generation, SSA forecast creation, model comparison, and filter-option generation
- `services/programme_plans_service.py`: Programme Plans query, CSV snapshot loading, summary generation, filters, search, and pagination
- `services/metadata_service.py`: database and schema metadata helpers
- `services/admin_jobs.py`: background job manager and the concrete refresh/train job functions

### Shared Utility Layer

- `db/snowflake.py`: lazy Snowflake connection wrapper
- `utils/filtering.py`: shared normalization and SQL filter builders
- `utils/api_contracts.py`: response envelopes, parsing helpers, and validation errors
- `utils/credentials.py`: env-first Snowflake credential loading and config-file persistence
- `utils/common.py`: project-root helpers, shared filter extraction, and service creation
- `contracts/api.py`: versioned API contract registry

### Scripts

- `scripts/config.py`: runtime config classes and startup validation
- `scripts/fetch_data.py`: local dataset refresh path from Snowflake
- `scripts/train_local.py`: supported local training entry point
- `scripts/refresh_programme_plans_data.py`: Programme Plans refresh helper
- `scripts/pipeline.py`: broader pipeline orchestration helpers
- `scripts/save_forecasts.py`
- `scripts/base_trainer.py`
- `scripts/data_preprocessor.py`
- `scripts/dashboard_exporter.py`
- `scripts/forecasting_models.py`

### Models Folder

- `models/`: model-specific code artifacts still present in the repo

Important note:

- the active admin training workflow uses `scripts/train_local.py` and writes outputs into `saved_models/`
- the `models/` directory is not the main operational training entry point used by the current admin job flow

### Frontend

- `templates/base.html`: shared layout shell
- `templates/dashboard.html`
- `templates/demographics.html`
- `templates/forecast.html`
- `templates/college_forecast.html`
- `templates/programme_plans.html`
- `templates/data_management.html`
- `templates/error.html`
- `templates/developer.html`
- `static/js/app.js`: shared notifications
- `static/js/dashboard.js`: dashboard-specific interactivity
- `static/js/programme_plans.js`: Programme Plans state management, filter rendering, search, and pagination
- `static/css/style.css`: global styling, responsiveness, and accessibility work

### Runtime Assets

- `data/`: generated local CSV datasets and metadata exports
- `saved_models/`: forecast CSV outputs and metrics
- `secrets/`: `secret_key.txt`, `passcode.txt`, and setup notes
- `docs/`: supporting design and contract notes

### Tooling

- `tests/`: automated checks
- `pyproject.toml`: pytest, Ruff, and mypy configuration
- `requirements-dev.txt`: dev/test dependencies
- `.github/workflows/ci.yml`: CI pipeline

## Request Lifecycle Walkthroughs

### Page Request Example: `/forecast`

1. Flask dispatches the request to `dashboard.forecast` in `routes/dashboard.py`
2. the route reads query parameters and validates the selected model
3. shared filters are normalized through `utils.common.get_shared_filters()`
4. service instances are created through `utils.common.get_services()`
5. `ForecastService` loads forecast data, comparison data, metrics, and SSA forecast rows
6. the route shapes those results into template-friendly payloads
7. `templates/forecast.html` renders the page

### API Request Example: `POST /api/programme-plans/data`

1. Flask dispatches to `routes/api.py`
2. `_parse_programme_plans_payload()` validates JSON body and pagination
3. `ProgrammePlansService.get_paginated_data()` loads data, applies filters, search, and page slicing
4. `utils.api_contracts.success_response()` wraps the result in the standard versioned envelope
5. the browser receives rows, columns, applied filters, dataset metadata, and pagination metadata

### Admin Job Example: `POST /api/data/refresh`

1. the route captures config values and the current Flask app object
2. `BackgroundJobManager.submit()` stores a queued job in memory
3. a worker thread runs the job inside `app.app_context()`
4. the job updates status from `queued` to `running` to `completed` or `failed`
5. the UI polls `/api/jobs/<job_id>` for status

That app-context step matters because background work may indirectly need Flask config or other app-bound helpers.

## Route Map

### Server-Rendered Pages

- `/` -> `dashboard.index`
- `/demographics` -> `dashboard.demographics`
- `/forecast` -> `dashboard.forecast`
- `/college-forecast` -> `dashboard.college_forecast`
- `/data-management` -> `dashboard.data_management`
- `/programme-plans` -> `dashboard.programme_plans`
- `/dev` -> `dashboard.developer`
- `/4cast-legacy` -> redirect to `/programme-plans`

### API Areas

System:

- `/api/health`
- `/api/contracts`
- `/api/database/status`

Dashboard and lookups:

- `/api/summary`
- `/api/enrolment/trends`
- `/api/enrolment/by-provider`
- `/api/enrolment/by-ssa`
- `/api/enrolment/by-age`
- `/api/enrolment/by-level`
- `/api/providers`
- `/api/ssa`
- `/api/timeseries`

Forecasting:

- `/api/forecast`
- `/api/forecast/combined`
- `/api/forecast/compare`
- `/api/forecast/filter-options`
- `/api/forecast/subject-areas`
- `/api/forecast/accuracy`

Programme Plans:

- `/api/programme-plans/filters`
- `/api/programme-plans/summary`
- `/api/programme-plans/data`
- `/api/programme-plans/csv-data`
- `/api/programme-plans/csv-download`
- `/api/programme-plans/refresh`

Backward-compatible legacy aliases also exist under `/4cast/...` for Programme Plans-related endpoints.

Admin and metadata:

- `/api/data/refresh`
- `/api/models/train`
- `/api/jobs/<job_id>`
- `/api/metadata/database`
- `/api/metadata/schemas`
- `/api/metadata/schema-structure`
- `/api/credentials/view`
- `/api/credentials/save`

## Core Modules

### `app.py`

Responsibilities:

- resolve the active config class from environment
- load Flask config from `scripts.config`
- validate runtime settings
- register blueprints
- attach `BackgroundJobManager` at `app.extensions["job_manager"]`
- define shared 404 and 500 handlers

Important detail:

- the module-level `app = create_app(...)` means imports touching `app.py` can build the app immediately unless test configuration is already in place

### `routes/dashboard.py`

This is the page controller layer. It:

- reads query parameters
- normalizes shared filters
- validates selected models
- calls services
- converts DataFrames into template-friendly records
- renders Jinja templates
- catches exceptions and returns `error.html`

One important helper is `_build_shared_filter_context()`, which keeps common filter UI synchronized across multiple dashboard pages.

Page-specific notes:

- `/` combines summary KPIs, trends, provider data, SSA data, and selected-model forecast data
- `/forecast` adds comparison data, accuracy metrics, and SSA forecast shaping
- `/college-forecast` builds cascading filter choices
- `/data-management` inspects local resources and Snowflake metadata
- `/programme-plans` keeps most data loading on the API side

### `routes/api.py`

This is the API boundary. It is responsible for:

- parsing and validating request input
- dispatching to services
- wrapping responses with standard contracts/version metadata
- queueing background jobs
- serving paginated Programme Plans rows
- exposing metadata and credential endpoints

You can mentally split this file into six sections:

1. small parsing helpers
2. health and contract endpoints
3. dashboard and forecast endpoints
4. Programme Plans endpoints
5. admin job endpoints
6. metadata and credentials endpoints

### `services/enrolment_service.py`

Purpose:

- query Snowflake for historical dashboard data
- generate KPI summaries
- generate provider, SSA, age, and level breakdowns
- provide year and location filter options
- provide time-series data for forecasting views

Important methods:

- `get_dashboard_summary()`
- `get_enrolment_trends()`
- `get_enrolment_by_provider()`
- `get_enrolment_by_ssa()`
- `get_enrolment_by_age()`
- `get_enrolment_by_level()`
- `get_providers_list()`
- `get_available_years()`
- `get_available_locations()`
- `get_time_series_data()`

This service now relies on shared filter builders instead of duplicating query-condition logic.

### `services/forecast_service.py`

This is the most complex service in the repository.

It supports three forecast sources:

- stored forecast rows from Snowflake
- saved forecast files from `saved_models/`
- dynamic filtered forecast generation when a view needs a custom slice

Key responsibilities:

- `get_forecast()`
- `get_historical_with_forecast()`
- `compare_models()`
- `get_model_accuracy()`
- `get_ssa_forecast_filtered()`
- `get_college_forecast_filter_options()`

Important implementation details:

- ARIMA and SARIMA attempt fallback orders if fitting fails
- the LSTM path is a lightweight proxy forecast
- filtered requests can bypass saved forecasts and generate directly from historical series
- forecast outputs are normalized into a common shape for page and API consumers

### `services/programme_plans_service.py`

This service owns the Programme Plans area end to end.

Responsibilities:

- define the Snowflake query for Programme Plans
- normalize raw Snowflake columns into UI-friendly columns
- refresh and save the local snapshot CSV
- cache snapshot CSV loads by file timestamp and size
- expose filter metadata for the explorer
- support legacy summary/detail compatibility behavior
- apply server-side filtering, search, and pagination

Important methods:

- `refresh_snapshot()`
- `get_filters()`
- `get_hours_summary()`
- `get_compat_data()`
- `get_paginated_data()`
- `get_dataset_info()`

Important behavior:

- explorer requests prefer the local snapshot when it exists
- the service can fall back to a live Snowflake query when the snapshot is missing
- page responses return columns plus row arrays, not row dicts, for compactness
- quick filters are selected dynamically based on configured priority and manageable unique-value counts

### `services/admin_jobs.py`

Contains two layers:

1. `BackgroundJobManager`
2. concrete job functions

`BackgroundJobManager` uses a `ThreadPoolExecutor` and in-memory job storage.

Current job functions:

- `refresh_programme_plans_job()`
- `refresh_all_data_job()`
- `train_models_job()`

Important behavior:

- jobs can run inside a Flask app context when the app object is provided
- job records are not persistent
- restart the process and job history disappears

### `services/metadata_service.py`

Purpose:

- return database metadata for the Data Management page
- return schema lists
- return schema structure summaries

This is a thin wrapper around `db/snowflake.py`.

### `db/snowflake.py`

Purpose:

- load Snowflake credentials through `utils.credentials`
- create Snowflake connections lazily
- provide query helpers that return pandas DataFrames
- provide schema/table/view inspection helpers

Important implementation details:

- one `SnowflakeDB` instance uses one connection
- each query gets a short-lived cursor
- connection is established only when needed
- failures are logged and surfaced upstream as safe errors

## Shared Utilities

### `utils/filtering.py`

This module centralizes shared normalization and SQL filtering logic.

It contains:

- text normalization
- year normalization
- provider id normalization
- academic year parsing
- reusable SQL expressions for location, funding, SSA, level, and course
- shared SQL condition builders

Two especially important helpers:

- `build_shared_sql_conditions()`
- `build_forecast_sql_conditions()`

This file matters because it removed duplicated filtering behavior from multiple services.

### `utils/api_contracts.py`

This module defines the JSON API envelope and request parsing helpers.

Key functions:

- `success_response()`
- `error_response()`
- `exception_response()`
- `dataframe_to_safe_records()`
- `parse_shared_filters()`
- `parse_pagination()`
- `parse_choice()`
- `parse_optional_int()`
- `require_json_object()`

This is the main reason API clients get consistent error payloads instead of route-by-route custom responses.

### `contracts/api.py`

This module holds the formal API contract registry.

Each contract descriptor has:

- an id
- a version
- a summary
- a category

Important outputs:

- `resolve_contract()`
- `build_contract_version_payload()`
- `build_contract_registry_payload()`

Current notable contracts:

- `programme_plans.filters@v2`
- `programme_plans.page@v2`
- `jobs.status@v1`
- `forecast.model_comparison@v1`

### `utils/credentials.py`

Purpose:

- load Snowflake credentials from env vars or `passcode.txt`
- reject partially configured environments
- provide a UI-friendly credential summary
- save edited connection details back to `passcode.txt`
- update the running process environment after save

Important note:

- the current implementation intentionally supports plaintext local credential editing because the app was asked to support that local admin workflow

### `utils/common.py`

Purpose:

- expose `get_project_root()`
- create configured `EnrolmentService` and `ForecastService` instances
- normalize shared page filters from request args

This is the convenience glue between Flask routes and service construction.

## Scripts And Data Flow

### Data Lineage Summary

```text
Snowflake
   |
   v
Refresh Data
   |
   v
data/*.csv
   |
   v
Train Models
   |
   v
saved_models/*
   |
   v
Forecast Pages / APIs
```

At runtime, forecast pages can also fall back to:

- Snowflake stored forecast rows
- dynamic forecast generation from historical data

### Refresh Flow

Triggered by:

- `/api/data/refresh`

Current call chain:

1. `routes/api.py`
2. `services/admin_jobs.refresh_all_data_job()`
3. `scripts.fetch_data.fetch_and_save_data()`
4. `ProgrammePlansService.refresh_snapshot()`
5. metadata export query through `db/snowflake.py`

Outputs land in `data/`.

Current refresh artifacts mentioned explicitly by the job code:

- enrolment totals
- provider enrolment data
- SSA enrolment data
- age enrolment data
- level enrolment data
- Programme Plans snapshot
- Snowflake column metadata export

### Training Flow

Triggered by:

- `/api/models/train`

Current call chain:

1. `routes/api.py`
2. `services/admin_jobs.train_models_job()`
3. `scripts.train_local.train_all_models_from_local()`
4. ARIMA, SARIMA, LSTM-style, and SSA forecast helpers

Outputs land in `saved_models/`.

Important detail:

- the admin flow calls the supported training function directly rather than relying on a script `__main__` path

Saved outputs produced by the training flow include:

- `arima_forecast.csv`
- `sarima_forecast.csv`
- `lstm_forecast.csv`
- `arima_metrics.json`
- `sarima_metrics.json`
- `lstm_metrics.json`
- `arima_ssa_forecast.csv`
- `sarima_ssa_forecast.csv`
- `lstm_ssa_forecast.csv`

### Forecast Consumption Flow

Pages and APIs ask `ForecastService` for data. That service then decides whether to use:

- Snowflake forecast rows
- local saved forecast CSVs
- dynamic generation from historical data

This layered strategy is central to how the application balances speed with flexibility.

## Frontend Structure

The frontend is mostly server-rendered rather than SPA-driven.

### Template Pattern

`templates/base.html` provides the shared shell, navigation, and common accessibility hooks.

Most routes prepare nearly all required data before template rendering. That makes the application easier to trace than a client-heavy architecture.

### JavaScript Pattern

- `static/js/app.js` exposes `window.appUI.notify(...)` for shared toast notifications
- `static/js/programme_plans.js` manages explorer state, filter UI, search, pagination, and fetches
- `static/js/dashboard.js` handles dashboard-specific client behavior

Frontend behaviors worth knowing:

- Programme Plans search is client-triggered but server-executed
- the Data Management page keeps some page-specific admin behavior directly in the template
- page state is often preserved in query parameters for server-rendered pages

### Styling And Accessibility

`static/css/style.css` contains:

- global styling
- responsive behavior
- toast styles
- accessibility-focused updates from the UX pass

Recent accessibility-oriented patterns in the repo include:

- live regions
- skip-navigation support
- reduced-motion-aware behavior

## Testing And Quality

### Test Setup

`tests/conftest.py` builds a Flask test app with:

- a temporary Programme Plans CSV
- a test `SECRET_KEY`
- an isolated config class

### Test File Map

- `tests/test_api_endpoints.py`: API envelopes, version headers, credential save/view behavior, pagination validation, refresh route callback handling
- `tests/test_admin_jobs.py`: app-context behavior for background jobs and training-entry-point usage
- `tests/test_filtering.py`: normalization and shared filter helper behavior
- `tests/test_credentials.py`: credential loading and saving behavior
- `tests/test_programme_plans_service.py`: Programme Plans filtering, pagination, and dataset metadata behavior

### Tooling

`pyproject.toml` currently configures:

- `pytest`
- `ruff check .`
- `mypy`

Current notable settings:

- Ruff mainly enforces `F` and `E9`
- mypy runs over `app.py`, `contracts`, `db`, `routes`, `services`, and `utils`
- tests are excluded from mypy
- `venv/` is excluded from the quality tools

### CI

`.github/workflows/ci.yml` currently runs:

1. dependency installation
2. `pytest`
3. `ruff check .`
4. `mypy`

## Config And Secrets

### Runtime Config

`scripts/config.py` defines:

- `Config`
- `DevelopmentConfig`
- `ProductionConfig`

Notable config values:

- `MODEL_DIR`
- `PROGRAMME_PLANS_CSV_FILE`
- `FORECAST_PERIODS`
- `AVAILABLE_MODELS`
- `DEFAULT_MODEL`
- `ADMIN_JOB_MAX_WORKERS`

### Secret Precedence

Current precedence:

1. environment variables
2. local files in `secrets/`

Important secret files:

- `secrets/secret_key.txt`
- `secrets/passcode.txt`
- `secrets/environment_variables_setup.txt`

## Running The App Locally

### Install Dependencies

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

Why the launcher is useful:

- it starts the app from the project root
- it supports the repo's current local secret-loading setup
- it reduces setup friction for repeated local runs

Typical local runtime sequence:

1. install dependencies
2. ensure the local environment or secrets are ready
3. start the Flask app
4. refresh data from the UI when needed
5. retrain models when forecast outputs need updating

## Troubleshooting

### Refresh Data Is Not Working

Check:

- the `Data Management` tab database status
- admin job progress and output log messages
- whether Snowflake connectivity is currently valid

From a codebase perspective, the main components to inspect are:

- `templates/data_management.html`
- `routes/api.py`
- `services/admin_jobs.py`
- `scripts/fetch_data.py`
- `db/snowflake.py`

### Forecast Results Look Empty

Possible reasons:

- the latest data has not been refreshed
- model outputs have not been retrained
- current filters are too narrow
- the selected slice does not have enough matching history

The main components to inspect are:

- `services/forecast_service.py`
- `saved_models/`
- `data/`
- `routes/dashboard.py`
- `routes/api.py`

### Programme Plans Looks Empty

Check:

- active filters
- search terms
- whether the latest snapshot has been refreshed

The main components to inspect are:

- `services/programme_plans_service.py`
- `static/js/programme_plans.js`
- `templates/programme_plans.html`
- `data/programme_plans.csv`

The Programme Plans explorer is paginated by design, so it does not load the full dataset into the browser at once.

## Operational Notes

- data refresh and model retraining run asynchronously
- job progress is visible in the app and backed by the in-memory job manager
- forecasts are most useful when refresh and retraining are part of a regular cycle
- Programme Plans is designed for larger datasets and avoids full-browser loading
- restart the Flask process and in-memory admin job history is lost

## Extension Guide

### Add A New API Endpoint

Typical steps:

1. add the route in `routes/api.py`
2. use parsing helpers from `utils/api_contracts.py`
3. return a standard success or error envelope
4. register a contract in `contracts/api.py`
5. add tests under `tests/`

Also consider:

- whether the endpoint needs pagination
- whether a frontend page consumes it
- whether the contract version should be bumped

### Add A New Page

Typical steps:

1. add a route in `routes/dashboard.py`
2. reuse shared filter handling if appropriate
3. add or extend a service in `services/`
4. create a template in `templates/`
5. add page-specific JS only if needed

### Add A New Forecast Model

Typical steps:

1. extend `AVAILABLE_MODELS` in `scripts/config.py`
2. teach `ForecastService` how to load or generate the model output
3. update `scripts/train_local.py`
4. expose the model in relevant templates and filters
5. add tests for both retrieval and display behavior

## Important Behaviors To Remember

- admin job state is in memory only
- forecast results may come from Snowflake, local files, or dynamic generation
- Programme Plans explorer performance is best when the snapshot CSV exists
- the credentials UI writes plaintext local config because it is intended for trusted local operations
- dashboard routes prefer safe error pages over raw exceptions
- API routes prefer standard error envelopes over raw exceptions
