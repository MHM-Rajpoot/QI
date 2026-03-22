"""
Enrolment Forecasting Models
Multiple ML approaches for FE college enrolment prediction
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')


class EnrolmentForecaster:
    """Forecasting models for learner enrolment prediction"""
    
    def __init__(self):
        self.models = {}
        self.best_model = None
        self.best_model_name = None
        self.metrics = {}
    
    def train_regression_models(self, X_train, y_train, X_test, y_test):
        """Train multiple regression models and compare performance"""
        
        models = {
            'Linear Regression': LinearRegression(),
            'Ridge Regression': Ridge(alpha=1.0),
            'Lasso Regression': Lasso(alpha=0.1),
            'Random Forest': RandomForestRegressor(n_estimators=100, random_state=42),
            'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, random_state=42)
        }
        
        print("\n" + "=" * 80)
        print("TRAINING REGRESSION MODELS")
        print("=" * 80)
        
        results = []
        
        for name, model in models.items():
            print(f"\n[TRAINING] {name}...")
            
            # Train
            model.fit(X_train, y_train)
            
            # Predict
            y_pred = model.predict(X_test)
            
            # Calculate metrics
            mae = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2 = r2_score(y_test, y_pred)
            mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
            
            results.append({
                'Model': name,
                'MAE': mae,
                'RMSE': rmse,
                'R2': r2,
                'MAPE': mape
            })
            
            self.models[name] = model
            self.metrics[name] = {'MAE': mae, 'RMSE': rmse, 'R2': r2, 'MAPE': mape}
            
            print(f"  MAE: {mae:.2f} | RMSE: {rmse:.2f} | R2: {r2:.4f} | MAPE: {mape:.2f}%")
        
        # Find best model (lowest RMSE)
        results_df = pd.DataFrame(results)
        best_idx = results_df['RMSE'].idxmin()
        self.best_model_name = results_df.loc[best_idx, 'Model']
        self.best_model = self.models[self.best_model_name]
        
        print("\n" + "-" * 80)
        print(f"[BEST MODEL] {self.best_model_name}")
        print("-" * 80)
        
        return results_df
    
    def get_feature_importance(self, feature_names):
        """Get feature importance from tree-based models"""
        importance_dict = {}
        
        for name, model in self.models.items():
            if hasattr(model, 'feature_importances_'):
                importance = pd.DataFrame({
                    'Feature': feature_names,
                    'Importance': model.feature_importances_
                }).sort_values('Importance', ascending=False)
                importance_dict[name] = importance
                
                print(f"\n[FEATURE IMPORTANCE] {name}")
                print("-" * 40)
                for _, row in importance.head(10).iterrows():
                    print(f"  {row['Feature']}: {row['Importance']:.4f}")
        
        return importance_dict
    
    def predict(self, X, model_name=None):
        """Make predictions using specified or best model"""
        if model_name and model_name in self.models:
            model = self.models[model_name]
        else:
            model = self.best_model
        
        return model.predict(X)
    
    def forecast_future(self, historical_df, periods=3, target_col='LEARNER_COUNT'):
        """Simple trend-based forecasting for future periods"""
        # Get historical trend
        yearly_data = historical_df.groupby('YEAR')[target_col].sum().reset_index()
        yearly_data = yearly_data.sort_values('YEAR')
        
        # Fit linear trend
        X = yearly_data['YEAR'].values.reshape(-1, 1)
        y = yearly_data[target_col].values
        
        lr = LinearRegression()
        lr.fit(X, y)
        
        # Forecast future years
        last_year = yearly_data['YEAR'].max()
        future_years = np.array([last_year + i for i in range(1, periods + 1)]).reshape(-1, 1)
        future_predictions = lr.predict(future_years)
        
        forecast_df = pd.DataFrame({
            'YEAR': future_years.flatten(),
            'PREDICTED_' + target_col: future_predictions,
            'TREND': 'Forecast'
        })
        
        # Calculate growth rate
        if len(yearly_data) >= 2:
            growth_rate = (yearly_data[target_col].iloc[-1] - yearly_data[target_col].iloc[-2]) / yearly_data[target_col].iloc[-2] * 100
        else:
            growth_rate = 0
        
        print(f"\n[FORECAST] Next {periods} Years")
        print("-" * 40)
        print(f"Historical Growth Rate: {growth_rate:.2f}%")
        print(forecast_df.to_string(index=False))
        
        return forecast_df


class TimeSeriesForecaster:
    """Time series specific forecasting (ARIMA-like)"""
    
    def __init__(self):
        self.model = None
    
    def simple_moving_average(self, series, window=3):
        """Simple Moving Average forecast"""
        return series.rolling(window=window).mean()
    
    def exponential_smoothing(self, series, alpha=0.3):
        """Simple Exponential Smoothing"""
        result = [series.iloc[0]]
        for i in range(1, len(series)):
            result.append(alpha * series.iloc[i] + (1 - alpha) * result[-1])
        return pd.Series(result, index=series.index)
    
    def holt_linear_trend(self, series, alpha=0.3, beta=0.1, periods=3):
        """Holt's Linear Trend Method"""
        # Initialize
        level = series.iloc[0]
        trend = series.iloc[1] - series.iloc[0]
        
        levels = [level]
        trends = [trend]
        
        # Fit
        for i in range(1, len(series)):
            new_level = alpha * series.iloc[i] + (1 - alpha) * (level + trend)
            new_trend = beta * (new_level - level) + (1 - beta) * trend
            level, trend = new_level, new_trend
            levels.append(level)
            trends.append(trend)
        
        # Forecast
        forecasts = []
        for i in range(1, periods + 1):
            forecasts.append(level + i * trend)
        
        return forecasts, levels, trends
    
    def forecast(self, df, target_col, date_col='YEAR', periods=3, method='holt'):
        """Main forecasting method"""
        series = df.set_index(date_col)[target_col]
        
        if method == 'holt':
            forecasts, _, _ = self.holt_linear_trend(series, periods=periods)
        elif method == 'ema':
            smoothed = self.exponential_smoothing(series)
            # Simple extrapolation
            trend = smoothed.diff().mean()
            forecasts = [smoothed.iloc[-1] + trend * i for i in range(1, periods + 1)]
        else:
            # SMA
            ma = self.simple_moving_average(series)
            forecasts = [ma.iloc[-1]] * periods
        
        last_date = df[date_col].max()
        forecast_df = pd.DataFrame({
            date_col: [last_date + i for i in range(1, periods + 1)],
            f'FORECAST_{target_col}': forecasts
        })
        
        return forecast_df


if __name__ == "__main__":
    # Test with sample data
    print("[TEST] Testing Forecasting Models...")
    
    # Sample training data
    np.random.seed(42)
    n_samples = 100
    
    X_train = np.random.randn(n_samples, 5)
    y_train = 1000 + 50 * X_train[:, 0] + 30 * X_train[:, 1] + np.random.randn(n_samples) * 10
    
    X_test = np.random.randn(20, 5)
    y_test = 1000 + 50 * X_test[:, 0] + 30 * X_test[:, 1] + np.random.randn(20) * 10
    
    # Train models
    forecaster = EnrolmentForecaster()
    results = forecaster.train_regression_models(X_train, y_train, X_test, y_test)
    
    print("\n[RESULTS]")
    print(results.to_string(index=False))
    
    # Test time series
    print("\n\n[TEST] Time Series Forecaster...")
    ts_data = pd.DataFrame({
        'YEAR': [2019, 2020, 2021, 2022, 2023, 2024],
        'LEARNER_COUNT': [10000, 10500, 9800, 10200, 10800, 11200]
    })
    
    ts_forecaster = TimeSeriesForecaster()
    forecast = ts_forecaster.forecast(ts_data, 'LEARNER_COUNT', periods=3)
    print(forecast)
    
    print("\n[OK] Forecaster test complete!")
