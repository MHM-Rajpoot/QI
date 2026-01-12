#!/usr/bin/env python3
"""
Connect to Snowflake using connection.toml and show details of
`connections.my_example_connection`.

Place a `connection.toml` in the same directory with a section:

[connections.my_example_connection]
account = "..."
user = "..."
password = "..."
role = "..."
warehouse = "..."
database = "..."
schema = "..."

If `connection.toml` or the password is missing the script will
fall back to environment variables or prompt for the password.
"""

import os
import getpass
from pprint import pprint

import snowflake.connector
import tomli


TOML_PATH = os.path.join(os.path.dirname(__file__), "connection.toml")


def load_from_toml(path=TOML_PATH):
	if not os.path.exists(path):
		return None
	try:
		with open(path, "rb") as f:
			data = tomli.load(f)
		return data.get("connections", {}).get("my_example_connection")
	except Exception:
		return None


def get_config():
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
	masked = dict(cfg)
	if "password" in masked and masked["password"] is not None:
		masked["password"] = "********"
	return masked


def main():
	cfg = get_config()
	connections = {"my_example_connection": cfg}
	print("connections['my_example_connection'] (config):")
	pprint(mask_password(connections["my_example_connection"]))
	print()

	print("Attempting to connect to Snowflake...")
	try:
		con = snowflake.connector.connect(
			account=cfg["account"],
			user=cfg["user"],
			password=cfg["password"],
			role=cfg["role"],
			warehouse=cfg["warehouse"],
			database=cfg["database"],
			schema=cfg["schema"],
		)
		print("Connection successful")
		print("Connection summary:")
		print(f"  user: {cfg['user']}")
		print(f"  account: {cfg['account']}")
		print(f"  role: {cfg['role']}")
		print(f"  database: {cfg.get('database')}")
		print(f"  schema: {cfg.get('schema')}")
		print(f"  warehouse: {cfg.get('warehouse')}")
		cur = con.cursor()
		try:
			cur.execute("""
				SELECT
				  current_account() AS account,
				  current_user() AS user,
				  current_role() AS role,
				  current_database() AS database,
				  current_schema() AS schema,
				  current_warehouse() AS warehouse
			""")
			row = cur.fetchone()
			print("Runtime connection details from Snowflake:")
			print({
				"account": row[0],
				"user": row[1],
				"role": row[2],
				"database": row[3],
				"schema": row[4],
				"warehouse": row[5],
			})
		finally:
			cur.close()
			con.close()
	except Exception as e:
		print("Connection failed:", e)


if __name__ == "__main__":
	main()
