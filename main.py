
from query import query_snowflake_data
import pandas as pd


def main():
	try:
		query_snowflake_data()
		print("\n✓ All queries executed successfully!")
	except Exception as e:
		print("FAIL")
		print(f"Error: {e}")


if __name__ == "__main__":
	main()
