"""
Configuration settings for FE Enrolment Dashboard
"""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SECRETS_DIR = os.path.join(PROJECT_ROOT, 'secrets')
ENVIRONMENT_SETUP_GUIDE = os.path.join(PROJECT_ROOT, 'secrets', 'environment_variables_setup.txt')
DEFAULT_SECRET_KEY_FILE = os.path.join(SECRETS_DIR, 'secret_key.txt')
DEFAULT_SNOWFLAKE_CONFIG_FILE = os.path.join(SECRETS_DIR, 'passcode.txt')


def _env_flag(name, default=False):
    """Parse a boolean-style environment variable."""
    value = os.environ.get(name)
    if value is None:
        return default

    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _read_text_file(path):
    """Read a small text file value when present."""
    if not path or not os.path.exists(path):
        return None

    with open(path, 'r', encoding='utf-8') as file_obj:
        value = file_obj.read().strip()
    return value or None


class Config:
    SECRET_KEY_FILE = os.environ.get('SECRET_KEY_FILE') or DEFAULT_SECRET_KEY_FILE
    SECRET_KEY = os.environ.get('SECRET_KEY') or _read_text_file(SECRET_KEY_FILE)
    DEBUG = _env_flag('DEBUG', False) or _env_flag('FLASK_DEBUG', False)
    ENV_NAME = (os.environ.get('APP_ENV') or os.environ.get('FLASK_ENV') or 'production').lower()
    SNOWFLAKE_CONFIG_FILE = os.environ.get('SNOWFLAKE_CONFIG_FILE') or DEFAULT_SNOWFLAKE_CONFIG_FILE
    MODEL_DIR = os.path.join(PROJECT_ROOT, 'saved_models')

    PROGRAMME_PLANS_CSV_FILE = os.path.join(
        PROJECT_ROOT,
        'data',
        'programme_plans.csv'
    )

    FORECAST_PERIODS = 3  # Years to forecast
    CONFIDENCE_LEVEL = 0.95
    AVAILABLE_MODELS = ['arima', 'sarima', 'lstm']
    DEFAULT_MODEL = 'sarima'
    ADMIN_JOB_MAX_WORKERS = 2


class DevelopmentConfig(Config):
    DEBUG = _env_flag('DEBUG', False) or _env_flag('FLASK_DEBUG', False)
    ENV_NAME = 'development'


class ProductionConfig(Config):
    DEBUG = False
    ENV_NAME = 'production'


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': ProductionConfig,
}


def validate_runtime_settings(config_obj):
    """Validate security-sensitive runtime settings."""
    if config_obj.get('TESTING'):
        return

    if not config_obj.get('SECRET_KEY'):
        raise RuntimeError(
            'SECRET_KEY must be set either via the SECRET_KEY environment variable '
            f'or via {config_obj.get("SECRET_KEY_FILE")}. See {ENVIRONMENT_SETUP_GUIDE} for setup steps.'
        )
