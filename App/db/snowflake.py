"""Snowflake database connection module."""

import logging

import pandas as pd
import snowflake.connector

from utils.credentials import load_snowflake_settings


logger = logging.getLogger(__name__)


class SnowflakeDB:
    """Snowflake database connection handler.

    Uses one connection per instance and creates short-lived cursors per query,
    which avoids cross-request cursor reuse issues in threaded Flask usage.
    """

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.conn = None
        self.database = None
        self.connection_source = None

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.disconnect()

    def connect(self):
        """Establish connection to Snowflake."""
        if self.conn is not None:
            return True

        try:
            credential_payload = load_snowflake_settings(self.config_file)
            connection_config = credential_payload["settings"]
            self.connection_source = credential_payload["source"]

            account = connection_config["account"]
            user = connection_config["user"]
            password = connection_config["password"]
            role = connection_config["role"]
            database = connection_config["database"]
            warehouse = connection_config["warehouse"]

            self.database = database
            self.conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                role=role,
                database=database,
                warehouse=warehouse if warehouse != "<none selected>" else None,
            )
            logger.info("Connected to Snowflake database '%s' using %s credentials", database, self.connection_source)
            return True
        except Exception:
            self.conn = None
            logger.exception("Snowflake connection failed")
            return False

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            finally:
                self.conn = None
                logger.info("Disconnected from Snowflake")

    def _ensure_connection(self):
        if self.conn is None and not self.connect():
            raise RuntimeError("Failed to connect to Snowflake.")

    def execute_query(self, query, params=None):
        """Execute SQL query and return a DataFrame."""
        self._ensure_connection()

        with self.conn.cursor() as cursor:
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            columns = [desc[0] for desc in (cursor.description or [])]
            data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)

    def execute(self, query, params=None):
        """Execute SQL query without returning results."""
        self._ensure_connection()

        with self.conn.cursor() as cursor:
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
        self.conn.commit()

    def execute_scalar(self, query, params=None, default=None):
        """Execute a query and return the first scalar value."""
        df = self.execute_query(query, params=params)
        if df.empty or len(df.columns) == 0:
            return default
        value = df.iloc[0, 0]
        return default if pd.isna(value) else value

    def get_all_schemas(self):
        """Get list of all schemas in current database."""
        self._ensure_connection()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SHOW SCHEMAS")
                schemas = cursor.fetchall()
            return [schema[1] for schema in schemas]
        except Exception:
            logger.exception("Failed to get schemas")
            return []

    def get_tables_in_schema(self, schema_name):
        """Get list of tables in a specific schema."""
        self._ensure_connection()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
                tables = cursor.fetchall()
            return [table[1] for table in tables]
        except Exception:
            logger.exception("Failed to get tables in schema '%s'", schema_name)
            return []

    def get_views_in_schema(self, schema_name):
        """Get list of views in a specific schema."""
        self._ensure_connection()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SHOW VIEWS IN SCHEMA {schema_name}")
                views = cursor.fetchall()
            return [view[1] for view in views]
        except Exception:
            logger.exception("Failed to get views in schema '%s'", schema_name)
            return []

    def get_schema_structure(self):
        """Get complete schema structure (schemas, tables, views)."""
        self._ensure_connection()

        structure = {}
        schemas = self.get_all_schemas()

        for schema in schemas:
            try:
                tables = self.get_tables_in_schema(schema)
                views = self.get_views_in_schema(schema)
                structure[schema] = {
                    "tables": tables,
                    "views": views,
                    "table_count": len(tables),
                    "view_count": len(views),
                }
            except Exception:
                logger.exception("Failed to get schema structure for '%s'", schema)
                structure[schema] = {
                    "tables": [],
                    "views": [],
                    "table_count": 0,
                    "view_count": 0,
                }

        return structure


def get_db(config_file=None):
    """Get a database instance with lazy connection handling."""
    return SnowflakeDB(config_file)
