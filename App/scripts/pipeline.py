"""
FE College Enrolment Forecasting Pipeline
Main orchestration script for the complete ML pipeline
"""

import os

from data_extractor import DataExtractor
from data_preprocessor import DataPreprocessor
from forecasting_models import EnrolmentForecaster, TimeSeriesForecaster
from dashboard_exporter import DashboardExporter


def run_full_pipeline(config_path, export_for_powerbi=True):
    """
    Complete pipeline for FE College Enrolment Forecasting
    
    Steps:
    1. Extract data from Snowflake
    2. Preprocess and engineer features
    3. Train forecasting models
    4. Generate predictions
    5. Export for Power BI dashboard
    """
    
    print("=" * 80)
    print("FE COLLEGE ENROLMENT FORECASTING PIPELINE")
    print("=" * 80)
    
    # =========================================================================
    # STEP 1: DATA EXTRACTION
    # =========================================================================
    print("\n[STEP 1] EXTRACTING DATA FROM SNOWFLAKE...")
    print("-" * 80)
    
    extractor = DataExtractor(config_path)
    
    # Get enrolment trends
    df_trends = extractor.get_enrolment_trends()
    print(f"  Enrolment trends: {len(df_trends)} years")
    
    # Get enrolment by SSA
    df_ssa = extractor.get_enrolment_by_ssa()
    print(f"  SSA breakdown: {len(df_ssa)} records")
    
    # Get enrolment by age
    df_age = extractor.get_enrolment_by_age()
    print(f"  Age breakdown: {len(df_age)} records")
    
    # Get funding trends
    df_funding = extractor.get_funding_trends()
    print(f"  Funding trends: {len(df_funding)} records")
    
    # Get comprehensive ML dataset
    df_ml = extractor.get_ml_training_dataset()
    print(f"  ML training data: {len(df_ml)} records")
    
    extractor.close()
    
    # =========================================================================
    # STEP 2: DATA PREPROCESSING
    # =========================================================================
    print("\n[STEP 2] PREPROCESSING DATA...")
    print("-" * 80)
    
    preprocessor = DataPreprocessor()
    
    # Clean the ML dataset
    df_clean = preprocessor.clean_data(df_ml)
    
    # Create time features
    df_features = preprocessor.create_time_features(df_clean)
    
    # Encode categorical variables
    categorical_cols = ['PROVIDER_NAME', 'PROVIDER_TYPE', 'SSA_TIER1', 'SSA_TIER2', 
                        'QUAL_LEVEL', 'FUNDING_TYPE_DESC', 'AGE_GROUP']
    categorical_cols = [col for col in categorical_cols if col in df_features.columns]
    df_encoded = preprocessor.encode_categorical(df_features, categorical_cols)
    
    # Get feature summary
    preprocessor.get_feature_summary(df_encoded)
    
    # =========================================================================
    # STEP 3: TRAIN FORECASTING MODELS
    # =========================================================================
    print("\n[STEP 3] TRAINING FORECASTING MODELS...")
    print("-" * 80)
    
    # Prepare features for ML
    feature_cols = [col for col in df_encoded.columns if '_encoded' in col or col in ['YEAR', 'MONTH_SIN', 'MONTH_COS']]
    
    if len(feature_cols) > 0 and 'LEARNER_COUNT' in df_encoded.columns:
        # Remove rows with NaN
        df_model = df_encoded.dropna(subset=feature_cols + ['LEARNER_COUNT'])
        
        if len(df_model) > 20:  # Minimum samples for training
            X_train, X_test, y_train, y_test = preprocessor.prepare_for_training(
                df_model, 
                target_col='LEARNER_COUNT',
                feature_cols=feature_cols
            )
            
            # Train regression models
            forecaster = EnrolmentForecaster()
            results = forecaster.train_regression_models(X_train, y_train, X_test, y_test)
            
            # Get feature importance
            forecaster.get_feature_importance(feature_cols)
        else:
            print("[WARN] Insufficient data for ML model training")
            forecaster = None
            results = None
    else:
        print("[WARN] Required columns not found for ML training")
        forecaster = None
        results = None
    
    # =========================================================================
    # STEP 4: TIME SERIES FORECASTING
    # =========================================================================
    print("\n[STEP 4] TIME SERIES FORECASTING...")
    print("-" * 80)
    
    # Prepare time series data
    ts_data = preprocessor.prepare_time_series(
        df_features, 
        target_col='LEARNER_COUNT',
        date_col='YEAR'
    )
    
    if len(ts_data) >= 3:
        ts_forecaster = TimeSeriesForecaster()
        forecast_df = ts_forecaster.forecast(
            ts_data, 
            target_col='LEARNER_COUNT',
            periods=3,
            method='holt'
        )
        print("\nForecast for next 3 years:")
        print(forecast_df.to_string(index=False))
    else:
        print("[WARN] Insufficient time series data for forecasting")
        forecast_df = None
    
    # =========================================================================
    # STEP 5: EXPORT FOR DASHBOARD
    # =========================================================================
    if export_for_powerbi:
        print("\n[STEP 5] EXPORTING FOR POWER BI DASHBOARD...")
        print("-" * 80)
        
        exporter = DashboardExporter('dashboard_data')
        
        # Export enrolment summary
        summary = exporter.prepare_enrolment_summary(df_features)
        exporter.export_to_csv(summary, 'enrolment_summary', include_timestamp=False)
        
        # Export SSA breakdown
        if 'SSA_TIER1' in df_features.columns:
            ssa_breakdown = exporter.prepare_ssa_breakdown(df_features)
            exporter.export_to_csv(ssa_breakdown, 'ssa_breakdown', include_timestamp=False)
        
        # Export trends
        exporter.export_to_csv(df_trends, 'enrolment_trends', include_timestamp=False)
        
        # Export age breakdown
        exporter.export_to_csv(df_age, 'age_breakdown', include_timestamp=False)
        
        # Export funding
        exporter.export_to_csv(df_funding, 'funding_trends', include_timestamp=False)
        
        # Export forecast
        if forecast_df is not None:
            combined = exporter.prepare_forecast_data(ts_data, forecast_df)
            exporter.export_to_csv(combined, 'forecast_combined', include_timestamp=False)
        
        # Export KPIs
        if len(df_features) > 0:
            kpis = exporter.create_kpi_metrics(df_features)
            exporter.export_to_csv(kpis, 'kpi_metrics', include_timestamp=False)
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print("\nOutputs:")
    print("  - Data extracted from Snowflake")
    print("  - Features engineered and encoded")
    if results is not None:
        print(f"  - Best model: {forecaster.best_model_name}")
    if forecast_df is not None:
        print(f"  - Forecasts generated for {len(forecast_df)} periods")
    if export_for_powerbi:
        print("  - Dashboard data exported to 'dashboard_data/' folder")
    
    return {
        'trends': df_trends,
        'ssa': df_ssa,
        'age': df_age,
        'funding': df_funding,
        'ml_data': df_encoded,
        'forecast': forecast_df,
        'model_results': results
    }


if __name__ == "__main__":
    config_path = os.environ.get('SNOWFLAKE_CONFIG_FILE')
    
    # Run the complete pipeline
    results = run_full_pipeline(config_path, export_for_powerbi=True)
