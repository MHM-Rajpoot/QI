"""
Base Model Trainer
Common functionality for all model training scripts
"""

import os
import json
from datetime import datetime
import pandas as pd
import numpy as np


class BaseModelTrainer:
    """Base class for model training with common utilities"""
    
    def __init__(self, model_name, output_dir='saved_models'):
        self.model_name = model_name
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def log_header(self):
        """Print training header"""
        print("=" * 80)
        print(f"{self.model_name.upper()} MODEL TRAINING")
        print("=" * 80)
    
    def log_step(self, step_num, message):
        """Print training step"""
        print(f"\n[{step_num}] {message}")
    
    def prepare_series(self, df, year_col='ACADEMIC_YEAR', value_col='LEARNER_COUNT'):
        """Prepare time series from dataframe"""
        df = df.sort_values(year_col)
        series = df[value_col].values
        years = df[year_col].values
        return series, years
    
    def create_forecast_df(self, forecast_mean, conf_int, last_year_str, forecast_periods, model_type):
        """Create standardized forecast dataframe"""
        # Parse last year from format "YY/YY"
        if '/' in str(last_year_str):
            last_year_num = int(last_year_str.split('/')[0]) + 2000
        else:
            last_year_num = int(last_year_str)
        
        # Generate future academic years
        forecast_years = []
        for i in range(forecast_periods):
            y = last_year_num + i + 1
            forecast_years.append(f"{str(y)[-2:]}/{str(y+1)[-2:]}")
        
        # Create dataframe
        forecast_df = pd.DataFrame({
            'ACADEMIC_YEAR': forecast_years,
            'YEAR': [last_year_num + i + 1 for i in range(forecast_periods)],
            'PREDICTED_ENROLMENTS': forecast_mean,
            'LOWER_CI': conf_int[:, 0] if conf_int.ndim > 1 else conf_int,
            'UPPER_CI': conf_int[:, 1] if conf_int.ndim > 1 else conf_int,
            'MODEL_TYPE': model_type
        })
        
        return forecast_df
    
    def save_forecast(self, forecast_df, filename):
        """Save forecast to CSV"""
        filepath = os.path.join(self.output_dir, filename)
        forecast_df.to_csv(filepath, index=False)
        print(f"    Forecast saved to: {filepath}")
        return filepath
    
    def save_metrics(self, metrics_dict, filename):
        """Save metrics to JSON"""
        metrics_dict['last_trained'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(metrics_dict, f, indent=2)
        print(f"    Metrics saved to: {filepath}")
        return filepath
    
    def calculate_metrics(self, residuals):
        """Calculate standard metrics from residuals"""
        mae = np.mean(np.abs(residuals))
        rmse = np.sqrt(np.mean(residuals**2))
        
        return {
            'MAE': float(mae),
            'RMSE': float(rmse)
        }
