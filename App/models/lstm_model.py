"""
LSTM Model for Enrolment Forecasting
Long Short-Term Memory Neural Network
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle
import os
import warnings
warnings.filterwarnings('ignore')

# TensorFlow import with fallback
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    from tensorflow.keras.callbacks import EarlyStopping
    TF_AVAILABLE = True
except ImportError:
    TF_AVAILABLE = False
    print("[WARN] TensorFlow not installed. LSTM model will not be available.")


class LSTMModel:
    """LSTM neural network for enrolment forecasting"""

    def save_forecast(self, forecast_df, filename='saved_models/lstm_forecast.csv'):
        """Save forecast DataFrame to CSV with year columns"""
        # Add year columns if missing
        if 'ACADEMIC_YEAR' not in forecast_df.columns or 'YEAR' not in forecast_df.columns:
            forecast_df = forecast_df.copy()
            forecast_df['YEAR'] = [2026 + i for i in range(len(forecast_df))]
            forecast_df['ACADEMIC_YEAR'] = [f"{str(y)[-2:]}/{str(y+1)[-2:]}" for y in forecast_df['YEAR']]
            cols = ['ACADEMIC_YEAR', 'YEAR', 'PREDICTED_ENROLMENTS', 'LOWER_CI', 'UPPER_CI', 'MODEL_TYPE']
            forecast_df = forecast_df[cols]
        forecast_df.to_csv(filename, index=False)
        print(f'[OK] Saved LSTM forecast to {filename}')
    
    def __init__(self, sequence_length=3, units=50, epochs=100, batch_size=1):
        """
        Initialize LSTM model
        
        Args:
            sequence_length: Number of time steps to look back
            units: Number of LSTM units
            epochs: Training epochs
            batch_size: Training batch size
        """
        if not TF_AVAILABLE:
            raise ImportError("TensorFlow is required for LSTM model")
        
        self.sequence_length = sequence_length
        self.units = units
        self.epochs = epochs
        self.batch_size = batch_size
        
        self.model = None
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.metrics = {}
        self.history = None
    
    def _create_sequences(self, data):
        """Create sequences for LSTM training"""
        X, y = [], []
        for i in range(len(data) - self.sequence_length):
            X.append(data[i:(i + self.sequence_length)])
            y.append(data[i + self.sequence_length])
        return np.array(X), np.array(y)
    
    def _build_model(self, input_shape):
        """Build LSTM architecture"""
        model = Sequential([
            LSTM(self.units, activation='relu', input_shape=input_shape, return_sequences=True),
            Dropout(0.2),
            LSTM(self.units // 2, activation='relu'),
            Dropout(0.2),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        return model
    
    def fit(self, series, verbose=0):
        """
        Fit LSTM model to time series data
        
        Args:
            series: pandas Series or numpy array
            verbose: Training verbosity (0=silent, 1=progress, 2=detailed)
        """
        try:
            # Convert to numpy and reshape
            if isinstance(series, pd.Series):
                data = series.values.reshape(-1, 1)
            else:
                data = np.array(series).reshape(-1, 1)
            
            # Scale data
            scaled_data = self.scaler.fit_transform(data)
            
            # Create sequences
            X, y = self._create_sequences(scaled_data)
            
            if len(X) == 0:
                print(f"[ERROR] Insufficient data for sequence length {self.sequence_length}")
                return False
            
            # Reshape for LSTM [samples, time steps, features]
            X = X.reshape((X.shape[0], X.shape[1], 1))
            
            # Build model
            self.model = self._build_model((self.sequence_length, 1))
            
            # Early stopping
            early_stop = EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)
            
            # Train
            self.history = self.model.fit(
                X, y,
                epochs=self.epochs,
                batch_size=self.batch_size,
                callbacks=[early_stop],
                verbose=verbose
            )
            
            print(f"[OK] LSTM model trained for {len(self.history.epoch)} epochs")
            print(f"     Final Loss: {self.history.history['loss'][-1]:.6f}")
            
            # Store last sequence for prediction
            self._last_sequence = scaled_data[-self.sequence_length:]
            
            return True
            
        except Exception as e:
            print(f"[ERROR] LSTM training failed: {e}")
            return False
    
    def predict(self, periods=3, confidence=0.95):
        """
        Generate forecast for future periods
        
        Args:
            periods: Number of periods to forecast
            confidence: Confidence level for intervals (approximated)
            
        Returns:
            DataFrame with predictions and confidence intervals
        """
        if self.model is None:
            print("[ERROR] Model not fitted. Call fit() first.")
            return None
        
        predictions = []
        current_sequence = self._last_sequence.copy()
        
        for _ in range(periods):
            # Reshape for prediction
            X = current_sequence.reshape((1, self.sequence_length, 1))
            
            # Predict next value
            pred = self.model.predict(X, verbose=0)
            predictions.append(pred[0, 0])
            
            # Update sequence
            current_sequence = np.append(current_sequence[1:], pred)
        
        # Inverse transform predictions
        predictions = np.array(predictions).reshape(-1, 1)
        predictions_original = self.scaler.inverse_transform(predictions)
        
        # Approximate confidence intervals (using training std)
        std_estimate = np.std(predictions_original) * 0.1  # Rough estimate
        z_score = 1.96 if confidence == 0.95 else 1.645
        
        result = pd.DataFrame({
            'PREDICTED_ENROLMENTS': predictions_original.flatten(),
            'LOWER_CI': predictions_original.flatten() - z_score * std_estimate,
            'UPPER_CI': predictions_original.flatten() + z_score * std_estimate,
            'MODEL_TYPE': 'LSTM'
        })
        
        return result
    
    def evaluate(self, train_series, test_series):
        """
        Evaluate model performance
        
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
            y_true = np.array(test_series).flatten()
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
        """Save model to file"""
        if self.model is None:
            print("[ERROR] No model to save")
            return False
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Save Keras model
        self.model.save(filepath.replace('.pkl', '_keras.h5'))
        
        # Save scaler and config
        with open(filepath, 'wb') as f:
            pickle.dump({
                'scaler': self.scaler,
                'sequence_length': self.sequence_length,
                'units': self.units,
                'metrics': self.metrics,
                'last_sequence': self._last_sequence
            }, f)
        
        print(f"[OK] Model saved to {filepath}")
        return True
    
    def load(self, filepath):
        """Load model from file"""
        if not os.path.exists(filepath):
            print(f"[ERROR] File not found: {filepath}")
            return False
        
        try:
            # Load Keras model
            self.model = tf.keras.models.load_model(filepath.replace('.pkl', '_keras.h5'))
            
            # Load scaler and config
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
                self.scaler = data['scaler']
                self.sequence_length = data['sequence_length']
                self.units = data['units']
                self.metrics = data.get('metrics', {})
                self._last_sequence = data['last_sequence']
            
            print(f"[OK] Model loaded from {filepath}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to load model: {e}")
            return False


class LSTMMultivariate(LSTMModel):
    """
    Multivariate LSTM for forecasting with multiple features
    (e.g., enrolments + funding + demographics)
    """
    
    def __init__(self, sequence_length=3, units=64, epochs=100):
        super().__init__(sequence_length, units, epochs)
        self.feature_scalers = {}
    
    def fit_multivariate(self, df, target_col='LEARNER_COUNT', feature_cols=None):
        """
        Fit LSTM with multiple input features
        
        Args:
            df: DataFrame with features
            target_col: Target column name
            feature_cols: List of feature columns (if None, uses all numeric)
        """
        if feature_cols is None:
            feature_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if target_col in feature_cols:
                feature_cols.remove(target_col)
        
        # Scale features
        scaled_features = []
        for col in [target_col] + feature_cols:
            scaler = MinMaxScaler()
            scaled = scaler.fit_transform(df[col].values.reshape(-1, 1))
            scaled_features.append(scaled)
            self.feature_scalers[col] = scaler
        
        # Stack features
        data = np.hstack(scaled_features)
        
        # Create sequences
        X, y = [], []
        for i in range(len(data) - self.sequence_length):
            X.append(data[i:(i + self.sequence_length)])
            y.append(data[i + self.sequence_length, 0])  # Only target
        
        X = np.array(X)
        y = np.array(y)
        
        # Build model
        self.model = self._build_multivariate_model((self.sequence_length, len(feature_cols) + 1))
        
        # Train
        early_stop = EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)
        self.history = self.model.fit(X, y, epochs=self.epochs, callbacks=[early_stop], verbose=0)
        
        print(f"[OK] Multivariate LSTM trained with {len(feature_cols) + 1} features")
        return True
    
    def _build_multivariate_model(self, input_shape):
        """Build multivariate LSTM"""
        model = Sequential([
            LSTM(self.units, activation='relu', input_shape=input_shape, return_sequences=True),
            Dropout(0.2),
            LSTM(self.units // 2, activation='relu'),
            Dropout(0.2),
            Dense(32, activation='relu'),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        return model


# if __name__ == "__main__":
#     if TF_AVAILABLE:
#         print("[TEST] Testing LSTM Model...")
        
#         # Sample enrolment data
#         years = range(2010, 2025)
#         enrolments = [9000, 9500, 9800, 10000, 10500, 10200, 10800, 11000, 
#                       10900, 11200, 11500, 11300, 11800, 12000, 12200]
        
#         series = pd.Series(enrolments, index=years)
        
#         # Create and fit model
#         model = LSTMModel(sequence_length=3, epochs=50)
#         model.fit(series, verbose=0)
        
#         # Generate forecast
#         forecast = model.predict(periods=3)
#         print("\nForecast:")
#         print(forecast)
#         model.save_forecast(forecast)
#         print("\n[OK] LSTM test complete!")
#     else:
#         print("[SKIP] TensorFlow not available, skipping LSTM test")
