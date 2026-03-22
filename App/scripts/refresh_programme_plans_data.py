"""
Refresh the local Programme Plans snapshot CSV from live Snowflake data.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.config import Config
from services.programme_plans_service import ProgrammePlansService


def main():
    service = ProgrammePlansService(
        config_file=Config.SNOWFLAKE_CONFIG_FILE,
        snapshot_csv=Config.PROGRAMME_PLANS_CSV_FILE,
    )
    df = service.refresh_snapshot()
    print("=" * 60)
    print("PROGRAMME PLANS DATA REFRESH COMPLETE")
    print("=" * 60)
    print("Source:   Snowflake (environment credentials)")
    print(f"CSV:      {Config.PROGRAMME_PLANS_CSV_FILE}")
    print(f"Rows:     {len(df)}")


if __name__ == "__main__":
    main()
