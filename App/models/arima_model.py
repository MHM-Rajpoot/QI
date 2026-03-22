"""
ARIMA Model for Enrolment Forecasting
Autoregressive Integrated Moving Average
"""

import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle
import os
import warnings
warnings.filterwarnings('ignore')


class ARIMAModel:
    """ARIMA forecasting model for enrolment prediction"""

    def save_forecast(self, forecast_df, filename='saved_models/arima_forecast.csv'):
        """Save forecast DataFrame to CSV with year columns (always overwrite)"""
        forecast_df = forecast_df.copy()
        forecast_df['YEAR'] = [2026 + i for i in range(len(forecast_df))]
        forecast_df['ACADEMIC_YEAR'] = [f"{str(y)[-2:]}/{str(y+1)[-2:]}" for y in forecast_df['YEAR']]
        cols = ['ACADEMIC_YEAR', 'YEAR', 'PREDICTED_ENROLMENTS', 'LOWER_CI', 'UPPER_CI', 'MODEL_TYPE']
        forecast_df = forecast_df[cols]
        forecast_df.to_csv(filename, index=False)
        print(f'[OK] Saved ARIMA forecast to {filename}')
    
    def __init__(self, order=(1, 1, 1)):
        """
        Initialize ARIMA model
        
        Args:
            order: (p, d, q) - AR order, differencing, MA order
        """
        self.order = order
        self.model = None
        self.fitted_model = None
        self.metrics = {}
    
    def fit(self, series):
        """
        Fit ARIMA model to time series data
        
        Args:
            series: pandas Series with datetime/year index
        """
        try:
            self.model = ARIMA(series, order=self.order)
            self.fitted_model = self.model.fit()
            print(f"[OK] ARIMA{self.order} model fitted successfully")
            print(f"     AIC: {self.fitted_model.aic:.2f}")
            return True
        except Exception as e:
            print(f"[ERROR] ARIMA fitting failed: {e}")
            return False
    
    def predict(self, periods=3, confidence=0.95):
        """
        Generate forecast for future periods
        
        Args:
            periods: Number of periods to forecast
            confidence: Confidence level for intervals
            
        Returns:
            DataFrame with predictions and confidence intervals
        """
        if self.fitted_model is None:
            print("[ERROR] Model not fitted. Call fit() first.")
            return None
        
        # Generate forecast
        forecast = self.fitted_model.get_forecast(steps=periods)
        forecast_mean = forecast.predicted_mean
        conf_int = forecast.conf_int(alpha=1-confidence)
        
        # Build result DataFrame
        result = pd.DataFrame({
            'PREDICTED_ENROLMENTS': forecast_mean.values,
            'LOWER_CI': conf_int.iloc[:, 0].values,
            'UPPER_CI': conf_int.iloc[:, 1].values,
            'MODEL_TYPE': 'ARIMA'
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
            self.metrics = data.get('metrics', {})
        
        print(f"[OK] Model loaded from {filepath}")
        return True
    
    def auto_select_order(self, series, max_p=3, max_d=2, max_q=3):
        """
        Automatically select best ARIMA order using AIC
        
        Args:
            series: Time series data
            max_p, max_d, max_q: Maximum values to search
            
        Returns:
            Best order tuple (p, d, q)
        """
        best_aic = float('inf')
        best_order = (1, 1, 1)
        
        print("[INFO] Auto-selecting ARIMA order...")
        
        for p in range(max_p + 1):
            for d in range(max_d + 1):
                for q in range(max_q + 1):
                    try:
                        model = ARIMA(series, order=(p, d, q))
                        fitted = model.fit()
                        
                        if fitted.aic < best_aic:
                            best_aic = fitted.aic
                            best_order = (p, d, q)
                    except:
                        continue
        
        print(f"[OK] Best order: {best_order} (AIC: {best_aic:.2f})")
        self.order = best_order
        return best_order


# if __name__ == "__main__":
#     # Test with sample data
#     print("[TEST] Testing ARIMA Model...")
#     # Sample enrolment data
#     years = range(2015, 2025)
#     enrolments = [10000, 10500, 10200, 10800, 11000, 10900, 11200, 11500, 11300, 11800]
#     series = pd.Series(enrolments, index=years)
#     # Create and fit model
#     model = ARIMAModel(order=(1, 1, 1))
#     model.fit(series)
#     # Generate forecast
#     forecast = model.predict(periods=3)
#     print("\nForecast:")
#     print(forecast)
#     model.save_forecast(forecast)
#     print("\n[OK] ARIMA test complete!")
