import os
import getpass
import tomli
import snowflake.connector


TOML_PATH = os.path.join(os.path.dirname(__file__), "connection.toml")


def load_from_toml(path=TOML_PATH):
	"""Load Snowflake connection config from TOML file."""
	if not os.path.exists(path):
		return None
	try:
		with open(path, "rb") as f:
			data = tomli.load(f)
		return data.get("connections", {}).get("my_example_connection")
	except Exception:
		return None


def get_config():
	"""Get Snowflake configuration from TOML, environment variables, or prompts."""
	# 1) Try toml
	cfg = load_from_toml() or {}

	# 2) Override with environment variables if provided
	cfg = {
		"account": os.getenv("SNOWFLAKE_ACCOUNT", cfg.get("account", "OTRWMNU-PU63187")),
		"user": os.getenv("SNOWFLAKE_USER", cfg.get("user", "MHM")),
		"password": os.getenv("SNOWFLAKE_PASSWORD", cfg.get("password")),
		"role": os.getenv("SNOWFLAKE_ROLE", cfg.get("role", "ACCOUNTADMIN")),
		"warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", cfg.get("warehouse")),
		"database": os.getenv("SNOWFLAKE_DATABASE", cfg.get("database", "SNOWFLAKE_LEARNING_DB")),
		"schema": os.getenv("SNOWFLAKE_SCHEMA", cfg.get("schema", "MHM_LOAD_SAMPLE_DATA_FROM_S3")),
	}

	if not cfg["password"]:
		cfg["password"] = getpass.getpass("Snowflake password: ")
	return cfg


def mask_password(cfg):
	"""Return a copy of config with password masked."""
	masked = dict(cfg)
	if "password" in masked and masked["password"] is not None:
		masked["password"] = "********"
	return masked


def connect_to_snowflake():
	"""Connect to Snowflake and close the connection."""
	cfg = get_config()
	con = snowflake.connector.connect(
		account=cfg["account"],
		user=cfg["user"],
		password=cfg["password"],
		role=cfg["role"],
		warehouse=cfg["warehouse"],
		database=cfg["database"],
		schema=cfg["schema"],
	)
	con.close()
