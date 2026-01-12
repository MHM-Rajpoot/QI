"""
Snowflake data query module.
Handles all data queries and returns results as pandas DataFrames.
"""

import pandas as pd
from connection import get_config
import snowflake.connector


def query_snowflake_data():
	"""Connect to Snowflake and execute the Tasty Bytes queries."""
	cfg = get_config()
	
	# Build connection params, excluding None values
	conn_params = {
		"account": cfg["account"],
		"user": cfg["user"],
		"password": cfg["password"],
		"role": cfg["role"],
		"database": cfg["database"],
		"schema": cfg["schema"],
	}
	
	if cfg["warehouse"]:
		conn_params["warehouse"] = cfg["warehouse"]
	
	con = snowflake.connector.connect(**conn_params)
	
	try:
		cur = con.cursor()
		# If no warehouse was set in connection, use the first available one
		if not cfg["warehouse"]:
			cur.execute("SHOW WAREHOUSES")
			warehouses = cur.fetchall()
			if warehouses:
				warehouse_name = warehouses[0][0]
				cur.execute(f"USE WAREHOUSE {warehouse_name}")
		
		# Query 1: Row count
		print("\n" + "="*100)
		print("Query 1: How many rows are in the MENU table?")
		print("="*100)
		cur.execute("SELECT COUNT(*) AS row_count FROM menu;")
		df1 = cur.fetch_pandas_all()
		print(df1)
		
		# Query 2: Top 10 rows
		print("\n" + "="*100)
		print("Query 2: What do the top 10 rows look like?")
		print("="*100)
		cur.execute("SELECT TOP 10 * FROM menu;")
		df2 = cur.fetch_pandas_all()
		print(df2)
		
		# Query 3: Freezing Point menu items
		print("\n" + "="*100)
		print("Query 3: What menu items does the Freezing Point brand sell?")
		print("="*100)
		cur.execute("SELECT menu_item_name FROM menu WHERE truck_brand_name = 'Freezing Point';")
		df3 = cur.fetch_pandas_all()
		print(df3)
		
		# Query 4: Mango Sticky Rice profit
		print("\n" + "="*100)
		print("Query 4: What is the profit on Mango Sticky Rice?")
		print("="*100)
		cur.execute("""
			SELECT
				menu_item_name,
				(sale_price_usd - cost_of_goods_usd) AS profit_usd
			FROM menu
			WHERE truck_brand_name = 'Freezing Point'
			AND menu_item_name = 'Mango Sticky Rice';
		""")
		df4 = cur.fetch_pandas_all()
		print(df4)
		
		# Query 5: Extract ingredients
		print("\n" + "="*100)
		print("Query 5: Extract Mango Sticky Rice ingredients")
		print("="*100)
		cur.execute("""
			SELECT
				m.menu_item_name,
				obj.value:"ingredients"::ARRAY AS ingredients
			FROM menu m,
				LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
			WHERE truck_brand_name = 'Freezing Point'
			AND menu_item_name = 'Mango Sticky Rice';
		""")
		df5 = cur.fetch_pandas_all()
		print(df5)
		
		return df2  # Return the main data
	finally:
		cur.close()
		con.close()
