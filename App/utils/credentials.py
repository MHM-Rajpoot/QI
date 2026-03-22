"""Credential helpers for Snowflake connection settings."""

import configparser
import os


ENV_VAR_MAP = {
    'account': 'SNOWFLAKE_ACCOUNT',
    'user': 'SNOWFLAKE_USER',
    'password': 'SNOWFLAKE_PASSWORD',
    'role': 'SNOWFLAKE_ROLE',
    'database': 'SNOWFLAKE_DATABASE',
    'warehouse': 'SNOWFLAKE_WAREHOUSE',
}

REQUIRED_FIELDS = ('account', 'user', 'password', 'role', 'database')
DEFAULT_WAREHOUSE = 'COMPUTE_WH'
CONFIG_SECTION = 'connections.my_example_connection'


def _normalize_value(value):
    """Return a stripped credential value or None."""
    if value is None:
        return None

    text = str(value).strip().strip('"')
    return text or None


def get_required_env_vars():
    """Return the environment variable names required for Snowflake auth."""
    return [ENV_VAR_MAP[field] for field in REQUIRED_FIELDS]


def _build_summary(payload):
    """Build a serializable credential summary."""
    settings = payload['settings']
    return {
        'configured': True,
        'source': payload['source'],
        'account': settings.get('account'),
        'user': settings.get('user'),
        'password': settings.get('password'),
        'role': settings.get('role'),
        'database': settings.get('database'),
        'warehouse': settings.get('warehouse'),
        'editable': True,
        'required_env': get_required_env_vars(),
    }


def load_snowflake_settings(config_file=None):
    """Load Snowflake settings from environment variables or a config file."""
    env_settings = {
        field: _normalize_value(os.environ.get(env_name))
        for field, env_name in ENV_VAR_MAP.items()
    }

    if all(env_settings.get(field) for field in REQUIRED_FIELDS):
        env_settings['warehouse'] = env_settings.get('warehouse') or DEFAULT_WAREHOUSE
        return {
            'source': 'environment',
            'settings': env_settings,
        }

    if any(env_settings.get(field) for field in REQUIRED_FIELDS):
        missing = [ENV_VAR_MAP[field] for field in REQUIRED_FIELDS if not env_settings.get(field)]
        raise RuntimeError(
            f'Incomplete Snowflake environment configuration. Missing: {", ".join(missing)}'
        )

    if config_file and os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        if CONFIG_SECTION not in config:
            raise RuntimeError('Snowflake configuration file is missing the required connection section.')

        section = config[CONFIG_SECTION]
        file_settings = {
            field: _normalize_value(section.get(field))
            for field in ENV_VAR_MAP
        }
        file_settings['warehouse'] = file_settings.get('warehouse') or DEFAULT_WAREHOUSE

        missing = [field for field in REQUIRED_FIELDS if not file_settings.get(field)]
        if missing:
            raise RuntimeError(
                'Snowflake configuration file is missing required settings: '
                + ', '.join(missing)
            )

        return {
            'source': 'config_file',
            'settings': file_settings,
        }

    raise RuntimeError(
        'Snowflake credentials are not configured. Set the SNOWFLAKE_* environment variables '
        'or provide a valid passcode.txt file.'
    )


def get_snowflake_connection_summary(config_file=None):
    """Return a non-sensitive credential summary for the UI/admin endpoints."""
    try:
        payload = load_snowflake_settings(config_file)
        return _build_summary(payload)
    except RuntimeError as exc:
        return {
            'configured': False,
            'source': 'unconfigured',
            'account': None,
            'user': None,
            'password': None,
            'role': None,
            'database': None,
            'warehouse': None,
            'editable': True,
            'required_env': get_required_env_vars(),
            'message': str(exc),
        }


def save_snowflake_settings(settings, config_file):
    """Persist Snowflake settings to a local config file and update the process environment."""
    if not config_file:
        raise RuntimeError('No Snowflake config file path is configured for saving credentials.')

    normalized = {
        field: _normalize_value(settings.get(field))
        for field in ENV_VAR_MAP
    }
    normalized['warehouse'] = normalized.get('warehouse') or DEFAULT_WAREHOUSE

    missing = [field for field in REQUIRED_FIELDS if not normalized.get(field)]
    if missing:
        raise RuntimeError(
            'Missing required Snowflake settings: ' + ', '.join(missing)
        )

    os.makedirs(os.path.dirname(config_file), exist_ok=True)

    config = configparser.ConfigParser()
    config[CONFIG_SECTION] = {
        field: normalized.get(field) or ''
        for field in ENV_VAR_MAP
    }
    with open(config_file, 'w', encoding='utf-8') as file_obj:
        config.write(file_obj)

    for field, env_name in ENV_VAR_MAP.items():
        os.environ[env_name] = normalized.get(field) or ''

    payload = {
        'source': 'config_file',
        'settings': normalized,
    }
    return _build_summary(payload)
