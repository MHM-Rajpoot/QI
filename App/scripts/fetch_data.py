"""
Fetch and Save Data Locally
Downloads enrolment data from Snowflake and saves to local CSV files
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.snowflake import get_db


def fetch_and_save_data(config_file, output_dir='data'):
    """
    Fetch enrolment data from Snowflake and save locally
    """
    print("=" * 60)
    print("FETCHING DATA FROM SNOWFLAKE")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to Snowflake
    print("\n[1] Connecting to Snowflake...")
    db = get_db(config_file)
    
    # Fetch total enrolment by year
    print("\n[2] Fetching total enrolment trends...")
    query_total = """
    SELECT 
        ay.ACADEMIC_YEAR,
        COUNT(DISTINCT fe.LEARNER_SKEY) as LEARNER_COUNT,
        SUM(fe.ENROLMENT_COUNT) as ENROLMENT_COUNT
    FROM PRESENTATION.FACT_ENROLMENT fe
    JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
    GROUP BY ay.ACADEMIC_YEAR
    ORDER BY ay.ACADEMIC_YEAR
    """
    df_total = db.execute_query(query_total)
    df_total.to_csv(os.path.join(output_dir, 'enrolment_total.csv'), index=False)
    print(f"    Saved: {len(df_total)} rows to enrolment_total.csv")
    print(df_total)
    
    # Fetch enrolment by SSA
    print("\n[3] Fetching enrolment by Subject Area...")
    query_ssa = """
    SELECT 
        ay.ACADEMIC_YEAR,
        ssa.SSA_TIER_1_DESCRIPTION as SSA_TIER1,
        COUNT(DISTINCT fe.LEARNER_SKEY) as LEARNER_COUNT,
        SUM(fe.ENROLMENT_COUNT) as ENROLMENT_COUNT
    FROM PRESENTATION.FACT_ENROLMENT fe
    JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
    JOIN PRESENTATION.DIM_SSA ssa ON fe.SSA_SKEY = ssa.SSA_SKEY
    GROUP BY ay.ACADEMIC_YEAR, ssa.SSA_TIER_1_DESCRIPTION
    ORDER BY ay.ACADEMIC_YEAR, ssa.SSA_TIER_1_DESCRIPTION
    """
    df_ssa = db.execute_query(query_ssa)
    df_ssa.to_csv(os.path.join(output_dir, 'enrolment_by_ssa.csv'), index=False)
    print(f"    Saved: {len(df_ssa)} rows to enrolment_by_ssa.csv")
    
    # Fetch enrolment by provider
    print("\n[4] Fetching enrolment by Provider...")
    query_provider = """
    SELECT 
        ay.ACADEMIC_YEAR,
        p.PROVIDER_SKEY,
        p.PROVIDER_NAME,
        p.COLLEGE_TYPE,
        COUNT(DISTINCT fe.LEARNER_SKEY) as LEARNER_COUNT,
        SUM(fe.ENROLMENT_COUNT) as ENROLMENT_COUNT
    FROM PRESENTATION.FACT_ENROLMENT fe
    JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
    JOIN PRESENTATION.DIM_PROVIDER p ON fe.PROVIDER_SKEY = p.PROVIDER_SKEY
    GROUP BY ay.ACADEMIC_YEAR, p.PROVIDER_SKEY, p.PROVIDER_NAME, p.COLLEGE_TYPE
    ORDER BY ay.ACADEMIC_YEAR, p.PROVIDER_NAME
    """
    df_provider = db.execute_query(query_provider)
    df_provider.to_csv(os.path.join(output_dir, 'enrolment_by_provider.csv'), index=False)
    print(f"    Saved: {len(df_provider)} rows to enrolment_by_provider.csv")
    
    # Fetch enrolment by age
    print("\n[5] Fetching enrolment by Age Group...")
    query_age = """
    SELECT 
        ay.ACADEMIC_YEAR,
        a.AGE_GROUP_FES as AGE_GROUP,
        COUNT(DISTINCT fe.LEARNER_SKEY) as LEARNER_COUNT
    FROM PRESENTATION.FACT_ENROLMENT fe
    JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
    JOIN PRESENTATION.DIM_AGE a ON fe.AGE_SKEY = a.AGE_SKEY
    GROUP BY ay.ACADEMIC_YEAR, a.AGE_GROUP_FES
    ORDER BY ay.ACADEMIC_YEAR, a.AGE_GROUP_FES
    """
    df_age = db.execute_query(query_age)
    df_age.to_csv(os.path.join(output_dir, 'enrolment_by_age.csv'), index=False)
    print(f"    Saved: {len(df_age)} rows to enrolment_by_age.csv")
    
    # Fetch enrolment by level
    print("\n[6] Fetching enrolment by Level...")
    query_level = """
    SELECT 
        ay.ACADEMIC_YEAR,
        l.LEVEL_DESCRIPTION as LEVEL_DESC,
        COUNT(DISTINCT fe.LEARNER_SKEY) as LEARNER_COUNT
    FROM PRESENTATION.FACT_ENROLMENT fe
    JOIN PRESENTATION.DIM_ACADEMIC_YEAR ay ON fe.ACADEMIC_YEAR_SKEY = ay.ACADEMIC_YEAR_SKEY
    JOIN PRESENTATION.DIM_LEVEL l ON fe.LEVEL_SKEY = l.LEVEL_SKEY
    GROUP BY ay.ACADEMIC_YEAR, l.LEVEL_DESCRIPTION
    ORDER BY ay.ACADEMIC_YEAR, l.LEVEL_DESCRIPTION
    """
    df_level = db.execute_query(query_level)
    df_level.to_csv(os.path.join(output_dir, 'enrolment_by_level.csv'), index=False)
    print(f"    Saved: {len(df_level)} rows to enrolment_by_level.csv")
    
    # Connection auto-managed by singleton
    
    print("\n" + "=" * 60)
    print("DATA FETCH COMPLETE")
    print(f"All files saved to: {os.path.abspath(output_dir)}")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    config_path = os.environ.get('SNOWFLAKE_CONFIG_FILE')
    fetch_and_save_data(config_path, output_dir='data')
