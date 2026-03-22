import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.arima_model import ARIMAModel
from models.sarima_model import SARIMAModel

# Sample enrolment data (replace with real data loading if needed)
years = range(2015, 2025)
enrolments = [10000, 10500, 10200, 10800, 11000, 10900, 11200, 11500, 11300, 11800]
series = pd.Series(enrolments, index=years)

# ARIMA
arima = ARIMAModel(order=(1, 1, 1))
arima.fit(series)
arima_forecast = arima.predict(periods=3)
arima_forecast['ACADEMIC_YEAR'] = ["26/27", "27/28", "28/29"]
arima_forecast['YEAR'] = [2026, 2027, 2028]
arima_forecast['MODEL_TYPE'] = 'ARIMA'
arima_forecast = arima_forecast[['ACADEMIC_YEAR','YEAR','PREDICTED_ENROLMENTS','LOWER_CI','UPPER_CI','MODEL_TYPE']]
arima_forecast.to_csv('saved_models/arima_forecast.csv', index=False)
print('[OK] Saved ARIMA forecast to saved_models/arima_forecast.csv')

# SARIMA
sarima = SARIMAModel(order=(1, 1, 1), seasonal_order=(0, 0, 0, 0))
sarima.fit(series)
sarima_forecast = sarima.predict(periods=3)
sarima_forecast['ACADEMIC_YEAR'] = ["26/27", "27/28", "28/29"]
sarima_forecast['YEAR'] = [2026, 2027, 2028]
sarima_forecast['MODEL_TYPE'] = 'SARIMA'
sarima_forecast = sarima_forecast[['ACADEMIC_YEAR','YEAR','PREDICTED_ENROLMENTS','LOWER_CI','UPPER_CI','MODEL_TYPE']]
sarima_forecast.to_csv('saved_models/sarima_forecast.csv', index=False)
print('[OK] Saved SARIMA forecast to saved_models/sarima_forecast.csv')
