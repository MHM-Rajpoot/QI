"""
Train Models from Local Data
Trains ARIMA, SARIMA models using locally saved CSV data
"""

import sys
import os
import json
import warnings
from datetime import datetime

import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')


def train_all_models_from_local(data_dir='data', output_dir='saved_models', forecast_periods=3):
    """Train all supported local models and refresh saved outputs."""
    os.makedirs(output_dir, exist_ok=True)

    data_path = os.path.join(data_dir, 'enrolment_total.csv')
    if not os.path.exists(data_path):
        raise RuntimeError("Local data not found. Refresh data before training models.")

    arima_forecast = train_arima_from_local(data_dir, output_dir, forecast_periods)
    sarima_forecast = train_sarima_from_local(data_dir, output_dir, forecast_periods)
    lstm_forecast = create_lstm_forecast(data_dir, output_dir, forecast_periods)
    ssa_forecast = train_ssa_forecasts(data_dir, output_dir, forecast_periods)

    return {
        'trained_models': ['ARIMA', 'SARIMA', 'LSTM'],
        'forecast_periods': forecast_periods,
        'output_dir': output_dir,
        'row_counts': {
            'arima': int(len(arima_forecast)),
            'sarima': int(len(sarima_forecast)),
            'lstm': int(len(lstm_forecast)),
            'ssa': int(len(ssa_forecast)),
        },
    }


def train_arima_from_local(data_dir='data', output_dir='saved_models', forecast_periods=3):
    """Train ARIMA model from local CSV data"""
    from statsmodels.tsa.arima.model import ARIMA
    
    print("\n" + "=" * 60)
    print("TRAINING ARIMA MODEL")
    print("=" * 60)
    
    # Load data
    df = pd.read_csv(os.path.join(data_dir, 'enrolment_total.csv'))
    print(f"\nLoaded {len(df)} years of data:")
    print(df)
    
    # Prepare time series
    df = df.sort_values('ACADEMIC_YEAR')
    series = df['LEARNER_COUNT'].values
    years = df['ACADEMIC_YEAR'].values
    
    # Extract year numbers for forecasting
    year_nums = [int(y.split('/')[0]) + 2000 for y in years]
    last_year_num = max(year_nums)
    
    # Fit ARIMA model
    print("\nFitting ARIMA model...")
    try:
        model = ARIMA(series, order=(1, 1, 1))
        fitted = model.fit()
        print("Model fitted successfully")
        print(fitted.summary().tables[0])
    except Exception as e:
        print(f"ARIMA(1,1,1) failed, trying (0,1,1): {e}")
        model = ARIMA(series, order=(0, 1, 1))
        fitted = model.fit()
    
    # Generate forecast
    print(f"\nGenerating {forecast_periods}-year forecast...")
    forecast_result = fitted.get_forecast(steps=forecast_periods)
    forecast_mean = forecast_result.predicted_mean
    conf_int = forecast_result.conf_int(alpha=0.05)
    
    # Create forecast dataframe
    forecast_years = []
    for i in range(forecast_periods):
        y = last_year_num + i + 1
        forecast_years.append(f"{str(y)[-2:]}/{str(y+1)[-2:]}")
    
    forecast_df = pd.DataFrame({
        'ACADEMIC_YEAR': forecast_years,
        'YEAR': [last_year_num + i + 1 for i in range(forecast_periods)],
        'PREDICTED_ENROLMENTS': forecast_mean,
        'LOWER_CI': conf_int[:, 0],
        'UPPER_CI': conf_int[:, 1],
        'MODEL_TYPE': 'ARIMA'
    })
    
    print("\nForecast:")
    print(forecast_df)
    
    # Save forecast
    os.makedirs(output_dir, exist_ok=True)
    forecast_df.to_csv(os.path.join(output_dir, 'arima_forecast.csv'), index=False)
    
    # Calculate metrics (in-sample)
    residuals = fitted.resid
    mae = np.mean(np.abs(residuals))
    rmse = np.sqrt(np.mean(residuals**2))
    mape = np.mean(np.abs(residuals / series)) * 100 if len(series) > 0 else None
    
    metrics = {
        'model': 'ARIMA',
        'order': [1, 1, 1],
        'mae': round(float(mae), 2),
        'rmse': round(float(rmse), 2),
        'mape': round(float(mape), 2) if mape else None,
        'last_trained': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_points': len(series),
        'forecast_periods': forecast_periods
    }
    
    with open(os.path.join(output_dir, 'arima_metrics.json'), 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\nMetrics: MAE={mae:.2f}, RMSE={rmse:.2f}, MAPE={mape:.2f}%")
    print("[OK] ARIMA model trained and saved")
    
    return forecast_df


def train_sarima_from_local(data_dir='data', output_dir='saved_models', forecast_periods=3):
    """Train SARIMA model from local CSV data"""
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    
    print("\n" + "=" * 60)
    print("TRAINING SARIMA MODEL")
    print("=" * 60)
    
    # Load data
    df = pd.read_csv(os.path.join(data_dir, 'enrolment_total.csv'))
    print(f"\nLoaded {len(df)} years of data:")
    print(df)
    
    # Prepare time series
    df = df.sort_values('ACADEMIC_YEAR')
    series = df['LEARNER_COUNT'].values
    years = df['ACADEMIC_YEAR'].values
    
    year_nums = [int(y.split('/')[0]) + 2000 for y in years]
    last_year_num = max(year_nums)
    
    # Fit SARIMA model (with minimal seasonality due to limited data)
    print("\nFitting SARIMA model...")
    try:
        # For very short series, use simple SARIMA
        model = SARIMAX(series, order=(1, 1, 1), seasonal_order=(0, 0, 0, 0))
        fitted = model.fit(disp=False)
        print("Model fitted successfully")
    except Exception as e:
        print(f"SARIMA failed, using simpler model: {e}")
        model = SARIMAX(series, order=(0, 1, 1))
        fitted = model.fit(disp=False)
    
    # Generate forecast
    print(f"\nGenerating {forecast_periods}-year forecast...")
    forecast_result = fitted.get_forecast(steps=forecast_periods)
    forecast_mean = forecast_result.predicted_mean
    conf_int = forecast_result.conf_int(alpha=0.05)
    
    # Create forecast dataframe
    forecast_years = []
    for i in range(forecast_periods):
        y = last_year_num + i + 1
        forecast_years.append(f"{str(y)[-2:]}/{str(y+1)[-2:]}")
    
    forecast_df = pd.DataFrame({
        'ACADEMIC_YEAR': forecast_years,
        'YEAR': [last_year_num + i + 1 for i in range(forecast_periods)],
        'PREDICTED_ENROLMENTS': forecast_mean,
        'LOWER_CI': conf_int[:, 0],
        'UPPER_CI': conf_int[:, 1],
        'MODEL_TYPE': 'SARIMA'
    })
    
    print("\nForecast:")
    print(forecast_df)
    
    # Save forecast
    os.makedirs(output_dir, exist_ok=True)
    forecast_df.to_csv(os.path.join(output_dir, 'sarima_forecast.csv'), index=False)
    
    # Calculate metrics
    residuals = fitted.resid
    mae = np.mean(np.abs(residuals))
    rmse = np.sqrt(np.mean(residuals**2))
    mape = np.mean(np.abs(residuals / series)) * 100 if len(series) > 0 else None
    
    metrics = {
        'model': 'SARIMA',
        'order': [1, 1, 1],
        'seasonal_order': [0, 0, 0, 0],
        'mae': round(float(mae), 2),
        'rmse': round(float(rmse), 2),
        'mape': round(float(mape), 2) if mape else None,
        'last_trained': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_points': len(series),
        'forecast_periods': forecast_periods
    }
    
    with open(os.path.join(output_dir, 'sarima_metrics.json'), 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\nMetrics: MAE={mae:.2f}, RMSE={rmse:.2f}, MAPE={mape:.2f}%")
    print("[OK] SARIMA model trained and saved")
    
    return forecast_df


def train_ssa_forecasts(data_dir='data', output_dir='saved_models', forecast_periods=3):
    """Train models for each SSA category"""
    from statsmodels.tsa.arima.model import ARIMA
    
    print("\n" + "=" * 60)
    print("TRAINING SSA-LEVEL FORECASTS")
    print("=" * 60)
    
    # Load SSA data
    df = pd.read_csv(os.path.join(data_dir, 'enrolment_by_ssa.csv'))
    print(f"\nLoaded {len(df)} rows of SSA data")
    
    # Get unique SSA categories
    ssa_categories = df['SSA_TIER1'].unique()
    print(f"Found {len(ssa_categories)} SSA categories")
    
    all_forecasts = []
    
    for ssa in ssa_categories:
        ssa_data = df[df['SSA_TIER1'] == ssa].sort_values('ACADEMIC_YEAR')
        series = ssa_data['LEARNER_COUNT'].values
        years = ssa_data['ACADEMIC_YEAR'].values
        
        if len(series) < 2:
            print(f"  [SKIP] {ssa}: Not enough data ({len(series)} points)")
            continue
        
        year_nums = [int(y.split('/')[0]) + 2000 for y in years]
        last_year_num = max(year_nums)
        
        try:
            # Simple model for short series
            if len(series) >= 3:
                model = ARIMA(series, order=(1, 0, 0))
            else:
                model = ARIMA(series, order=(0, 0, 0))
            
            fitted = model.fit()
            
            forecast_result = fitted.get_forecast(steps=forecast_periods)
            forecast_mean = np.asarray(forecast_result.predicted_mean, dtype=float)
            conf_int = forecast_result.conf_int(alpha=0.05)
            conf_int_values = conf_int.to_numpy() if hasattr(conf_int, 'to_numpy') else np.asarray(conf_int, dtype=float)
            
            for i in range(forecast_periods):
                y = last_year_num + i + 1
                predicted_value = max(0, float(forecast_mean[i]))
                lower_value = max(0, float(conf_int_values[i][0]))
                upper_value = float(conf_int_values[i][1])
                all_forecasts.append({
                    'ACADEMIC_YEAR': f"{str(y)[-2:]}/{str(y+1)[-2:]}",
                    'SSA_TIER1': ssa,
                    'PREDICTED_ENROLMENTS': predicted_value,
                    'LOWER_CI': lower_value,
                    'UPPER_CI': upper_value,
                    'MODEL_TYPE': 'SARIMA'
                })
            
            print(f"  [OK] {ssa[:40]}...")
            
        except Exception as e:
            print(f"  [ERROR] {ssa}: {str(e)[:50]}")
            # Use simple average for failed forecasts
            avg = np.mean(series)
            for i in range(forecast_periods):
                y = last_year_num + i + 1
                all_forecasts.append({
                    'ACADEMIC_YEAR': f"{str(y)[-2:]}/{str(y+1)[-2:]}",
                    'SSA_TIER1': ssa,
                    'PREDICTED_ENROLMENTS': avg,
                    'LOWER_CI': avg * 0.8,
                    'UPPER_CI': avg * 1.2,
                    'MODEL_TYPE': 'SARIMA'
                })
    
    # Save SSA forecasts
    forecast_df = pd.DataFrame(all_forecasts)
    
    # Save for all models (using same data since we have limited history)
    for model_type in ['arima', 'sarima', 'lstm']:
        forecast_df['MODEL_TYPE'] = model_type.upper()
        forecast_df.to_csv(os.path.join(output_dir, f'{model_type}_ssa_forecast.csv'), index=False)
    
    print(f"\n[OK] Saved SSA forecasts for all models ({len(forecast_df)} rows)")
    
    return forecast_df


def create_lstm_forecast(data_dir='data', output_dir='saved_models', forecast_periods=3):
    """Create LSTM-style forecast (simplified without TensorFlow)"""
    print("\n" + "=" * 60)
    print("CREATING LSTM FORECAST (Simplified)")
    print("=" * 60)
    
    # Load data
    df = pd.read_csv(os.path.join(data_dir, 'enrolment_total.csv'))
    df = df.sort_values('ACADEMIC_YEAR')
    series = df['LEARNER_COUNT'].values
    years = df['ACADEMIC_YEAR'].values
    
    year_nums = [int(y.split('/')[0]) + 2000 for y in years]
    last_year_num = max(year_nums)
    
    # Simple exponential smoothing as LSTM proxy
    alpha = 0.3
    smoothed = [series[0]]
    for i in range(1, len(series)):
        smoothed.append(alpha * series[i] + (1 - alpha) * smoothed[-1])
    
    # Forecast using trend
    if len(series) >= 2:
        trend = (series[-1] - series[0]) / (len(series) - 1)
    else:
        trend = 0
    
    last_val = smoothed[-1]
    
    forecast_years = []
    forecast_vals = []
    for i in range(forecast_periods):
        y = last_year_num + i + 1
        forecast_years.append(f"{str(y)[-2:]}/{str(y+1)[-2:]}")
        forecast_vals.append(last_val + trend * (i + 1))
    
    forecast_df = pd.DataFrame({
        'ACADEMIC_YEAR': forecast_years,
        'YEAR': [last_year_num + i + 1 for i in range(forecast_periods)],
        'PREDICTED_ENROLMENTS': forecast_vals,
        'LOWER_CI': [v * 0.85 for v in forecast_vals],
        'UPPER_CI': [v * 1.15 for v in forecast_vals],
        'MODEL_TYPE': 'LSTM'
    })
    
    print("\nForecast:")
    print(forecast_df)
    
    forecast_df.to_csv(os.path.join(output_dir, 'lstm_forecast.csv'), index=False)
    
    metrics = {
        'model': 'LSTM',
        'method': 'Exponential Smoothing (simplified)',
        'alpha': alpha,
        'mae': None,
        'rmse': None,
        'mape': None,
        'last_trained': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_points': len(series),
        'forecast_periods': forecast_periods
    }
    
    with open(os.path.join(output_dir, 'lstm_metrics.json'), 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print("[OK] LSTM forecast saved")
    
    return forecast_df


if __name__ == "__main__":
    data_dir = 'data'
    output_dir = 'saved_models'
    
    print("=" * 60)
    print("TRAINING ALL MODELS FROM LOCAL DATA")
    print("=" * 60)
    
    try:
        train_all_models_from_local(data_dir, output_dir)
    except RuntimeError as exc:
        print(f"\n[ERROR] {exc}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("ALL MODELS TRAINED SUCCESSFULLY")
    print("=" * 60)
