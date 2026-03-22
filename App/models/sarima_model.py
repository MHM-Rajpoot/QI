"""
SARIMA Model for Enrolment Forecasting
Seasonal Autoregressive Integrated Moving Average
"""

import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle
import os
import warnings
warnings.filterwarnings('ignore')


class SARIMAModel:
    """SARIMA forecasting model with seasonality for enrolment prediction"""

    def save_forecast(self, forecast_df, filename='saved_models/sarima_forecast.csv'):
        """Save forecast DataFrame to CSV with year columns"""
        # Add year columns if missing
        if 'ACADEMIC_YEAR' not in forecast_df.columns or 'YEAR' not in forecast_df.columns:
            forecast_df = forecast_df.copy()
            forecast_df['YEAR'] = [2026 + i for i in range(len(forecast_df))]
            forecast_df['ACADEMIC_YEAR'] = [f"{str(y)[-2:]}/{str(y+1)[-2:]}" for y in forecast_df['YEAR']]
            cols = ['ACADEMIC_YEAR', 'YEAR', 'PREDICTED_ENROLMENTS', 'LOWER_CI', 'UPPER_CI', 'MODEL_TYPE']
            forecast_df = forecast_df[cols]
        forecast_df.to_csv(filename, index=False)
        print(f'[OK] Saved SARIMA forecast to {filename}')
    
    def __init__(self, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12)):
        """
        Initialize SARIMA model
        
        Args:
            order: (p, d, q) - AR order, differencing, MA order
            seasonal_order: (P, D, Q, s) - Seasonal AR, differencing, MA, and period
        """
        self.order = order
        self.seasonal_order = seasonal_order
        self.model = None
        self.fitted_model = None
        self.metrics = {}
    
    def fit(self, series, exog=None):
        """
        Fit SARIMA model to time series data
        
        Args:
            series: pandas Series with datetime/year index
            exog: Optional exogenous variables
        """
        try:
            self.model = SARIMAX(
                series,
                order=self.order,
                seasonal_order=self.seasonal_order,
                exog=exog,
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            self.fitted_model = self.model.fit(disp=False)
            print(f"[OK] SARIMA{self.order}x{self.seasonal_order} model fitted successfully")
            print(f"     AIC: {self.fitted_model.aic:.2f}")
            return True
        except Exception as e:
            print(f"[ERROR] SARIMA fitting failed: {e}")
            return False
    
    def predict(self, periods=3, exog_future=None, confidence=0.95):
        """
        Generate forecast for future periods
        
        Args:
            periods: Number of periods to forecast
            exog_future: Exogenous variables for future periods
            confidence: Confidence level for intervals
            
        Returns:
            DataFrame with predictions and confidence intervals
        """
        if self.fitted_model is None:
            print("[ERROR] Model not fitted. Call fit() first.")
            return None
        
        # Generate forecast
        forecast = self.fitted_model.get_forecast(steps=periods, exog=exog_future)
        forecast_mean = forecast.predicted_mean
        conf_int = forecast.conf_int(alpha=1-confidence)
        
        # Build result DataFrame
        result = pd.DataFrame({
            'PREDICTED_ENROLMENTS': forecast_mean.values,
            'LOWER_CI': conf_int.iloc[:, 0].values,
            'UPPER_CI': conf_int.iloc[:, 1].values,
            'MODEL_TYPE': 'SARIMA'
        })
        
        return result
    
    def evaluate(self, train_series, test_series):
        """
        Evaluate model performance using train/test split
        
        Args:
            train_series: Training data
            test_series: Test data for evaluation
            
        Returns:
            Dictionary with evaluation metrics
        """
        # Fit on training data
        self.fit(train_series)
        
        # Predict test periods
        predictions = self.predict(len(test_series))
        
        if predictions is not None:
            y_true = test_series.values
            y_pred = predictions['PREDICTED_ENROLMENTS'].values
            
            self.metrics = {
                'MAE': mean_absolute_error(y_true, y_pred),
                'RMSE': np.sqrt(mean_squared_error(y_true, y_pred)),
                'MAPE': np.mean(np.abs((y_true - y_pred) / y_true)) * 100
            }
            
            print(f"[METRICS] MAE: {self.metrics['MAE']:.2f}")
            print(f"          RMSE: {self.metrics['RMSE']:.2f}")
            print(f"          MAPE: {self.metrics['MAPE']:.2f}%")
        
        return self.metrics
    
    def save(self, filepath):
        """Save fitted model to file"""
        if self.fitted_model is None:
            print("[ERROR] No model to save")
            return False
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'wb') as f:
            pickle.dump({
                'model': self.fitted_model,
                'order': self.order,
                'seasonal_order': self.seasonal_order,
                'metrics': self.metrics
            }, f)
        print(f"[OK] Model saved to {filepath}")
        return True
    
    def load(self, filepath):
        """Load model from file"""
        if not os.path.exists(filepath):
            print(f"[ERROR] File not found: {filepath}")
            return False
        
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            self.fitted_model = data['model']
            self.order = data['order']
            self.seasonal_order = data['seasonal_order']
            self.metrics = data.get('metrics', {})
        
        print(f"[OK] Model loaded from {filepath}")
        return True
    
    def get_diagnostics(self):
        """Get model diagnostics"""
        if self.fitted_model is None:
            print("[ERROR] Model not fitted")
            return None
        
        return {
            'aic': self.fitted_model.aic,
            'bic': self.fitted_model.bic,
            'log_likelihood': self.fitted_model.llf,
            'params': self.fitted_model.params.to_dict()
        }


class SARIMAForFE(SARIMAModel):
    """
    SARIMA model tuned for FE academic year cycles
    Uses annual seasonality (s=1 for yearly data, s=12 for monthly)
    """
    
    def __init__(self, data_frequency='yearly'):
        """
        Initialize SARIMA for FE data
        
        Args:
            data_frequency: 'yearly' or 'monthly'
        """
        if data_frequency == 'monthly':
            # Monthly data with annual seasonality
            order = (1, 1, 1)
            seasonal_order = (1, 1, 1, 12)
        else:
            # Yearly data - use simpler seasonal pattern
            order = (1, 1, 1)
            seasonal_order = (0, 0, 0, 0)  # No seasonality for yearly
        
        super().__init__(order=order, seasonal_order=seasonal_order)
        self.data_frequency = data_frequency
    
    def fit_for_fe(self, df, target_col='LEARNER_COUNT', date_col='YEAR'):
        """
        Fit model specifically for FE enrolment data
        
        Args:
            df: DataFrame with enrolment data
            target_col: Column with target values
            date_col: Column with year/date
        """
        # Sort by date
        df = df.sort_values(date_col)
        
        # Create series
        series = pd.Series(
            df[target_col].values,
            index=df[date_col].values
        )
        
        return self.fit(series)


# if __name__ == "__main__":
#     # Test with sample data
#     print("[TEST] Testing SARIMA Model...")
    
#     # Sample enrolment data
#     years = range(2015, 2025)
#     enrolments = [10000, 10500, 10200, 10800, 11000, 10900, 11200, 11500, 11300, 11800]
    
#     series = pd.Series(enrolments, index=years)
    
#     # Create and fit model (yearly data, no seasonality)
#     model = SARIMAModel(order=(1, 1, 1), seasonal_order=(0, 0, 0, 0))
#     model.fit(series)
    
#     # Generate forecast
#     forecast = model.predict(periods=3)
#     print("\nForecast:")
#     print(forecast)
#     model.save_forecast(forecast)
#     # Get diagnostics
#     print("\nDiagnostics:")
#     diag = model.get_diagnostics()
#     print(f"  AIC: {diag['aic']:.2f}")
#     print(f"  BIC: {diag['bic']:.2f}")
#     print("\n[OK] SARIMA test complete!")
