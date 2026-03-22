"""
Data Preprocessor Module
Prepares data for ML model training
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split


class DataPreprocessor:
    """Preprocess data for ML model training"""
    
    def __init__(self):
        self.label_encoders = {}
        self.scaler = None
        self.feature_columns = []
        self.target_column = None
    
    def clean_data(self, df):
        """Clean and handle missing values"""
        print(f"[INFO] Original shape: {df.shape}")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        # Fill numeric with median
        for col in numeric_cols:
            df[col] = df[col].fillna(df[col].median())
        
        # Fill categorical with mode
        for col in categorical_cols:
            df[col] = df[col].fillna(df[col].mode()[0] if len(df[col].mode()) > 0 else 'Unknown')
        
        print(f"[INFO] Cleaned shape: {df.shape}")
        return df
    
    def encode_categorical(self, df, columns):
        """Encode categorical variables"""
        df_encoded = df.copy()
        
        for col in columns:
            if col in df_encoded.columns:
                le = LabelEncoder()
                df_encoded[col + '_encoded'] = le.fit_transform(df_encoded[col].astype(str))
                self.label_encoders[col] = le
                print(f"[INFO] Encoded {col}: {len(le.classes_)} categories")
        
        return df_encoded
    
    def create_time_features(self, df, date_column='ACADEMIC_YEAR'):
        """Create time-based features for forecasting"""
        df = df.copy()
        
        # Extract year from academic year (e.g., "2023/24" -> 2023)
        if date_column in df.columns:
            df['YEAR'] = df[date_column].astype(str).str[:4].astype(int)
            
            # Create lag features if we have monthly data
            if 'MONTH' in df.columns:
                df['MONTH_NUM'] = df['MONTH'].astype(int)
                df['QUARTER_NUM'] = ((df['MONTH_NUM'] - 1) // 3) + 1
                
                # Cyclical encoding for month
                df['MONTH_SIN'] = np.sin(2 * np.pi * df['MONTH_NUM'] / 12)
                df['MONTH_COS'] = np.cos(2 * np.pi * df['MONTH_NUM'] / 12)
        
        return df
    
    def create_lag_features(self, df, target_col, group_cols, lags=[1, 2, 3]):
        """Create lag features for time series"""
        df = df.copy()
        df = df.sort_values(group_cols + ['YEAR'])
        
        for lag in lags:
            df[f'{target_col}_LAG_{lag}'] = df.groupby(group_cols)[target_col].shift(lag)
        
        # Rolling statistics
        df[f'{target_col}_ROLLING_MEAN_3'] = df.groupby(group_cols)[target_col].transform(
            lambda x: x.rolling(window=3, min_periods=1).mean()
        )
        df[f'{target_col}_ROLLING_STD_3'] = df.groupby(group_cols)[target_col].transform(
            lambda x: x.rolling(window=3, min_periods=1).std()
        )
        
        return df
    
    def scale_features(self, df, columns, method='standard'):
        """Scale numerical features"""
        df_scaled = df.copy()
        
        if method == 'standard':
            self.scaler = StandardScaler()
        else:
            self.scaler = MinMaxScaler()
        
        df_scaled[columns] = self.scaler.fit_transform(df[columns])
        print(f"[INFO] Scaled {len(columns)} columns using {method} scaling")
        
        return df_scaled
    
    def prepare_for_training(self, df, target_col, feature_cols, test_size=0.2):
        """Prepare final train/test split"""
        self.target_column = target_col
        self.feature_columns = feature_cols
        
        X = df[feature_cols]
        y = df[target_col]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        print(f"[INFO] Training set: {X_train.shape[0]} samples")
        print(f"[INFO] Test set: {X_test.shape[0]} samples")
        print(f"[INFO] Features: {len(feature_cols)}")
        
        return X_train, X_test, y_train, y_test
    
    def prepare_time_series(self, df, target_col, date_col='YEAR', group_cols=None):
        """Prepare data for time series forecasting"""
        df = df.copy()
        
        if group_cols:
            # Group by categories and aggregate over time
            agg_df = df.groupby(group_cols + [date_col]).agg({
                target_col: 'sum'
            }).reset_index()
        else:
            # Simple time series
            agg_df = df.groupby(date_col).agg({
                target_col: 'sum'
            }).reset_index()
        
        agg_df = agg_df.sort_values(date_col)
        print(f"[INFO] Time series prepared: {len(agg_df)} time points")
        
        return agg_df
    
    def get_feature_summary(self, df):
        """Get summary statistics of features"""
        summary = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': len(df.select_dtypes(include=['object']).columns),
            'missing_values': df.isnull().sum().sum()
        }
        
        print("\n" + "=" * 60)
        print("FEATURE SUMMARY")
        print("=" * 60)
        for key, value in summary.items():
            print(f"  {key}: {value}")
        print("=" * 60)
        
        return summary


if __name__ == "__main__":
    # Example usage with sample data
    print("[TEST] Testing DataPreprocessor...")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'ACADEMIC_YEAR': ['2021/22', '2021/22', '2022/23', '2022/23', '2023/24', '2023/24'],
        'SSA_TIER1': ['Science', 'Business', 'Science', 'Business', 'Science', 'Business'],
        'AGE_GROUP': ['16-18', '19-23', '16-18', '19-23', '16-18', '19-23'],
        'LEARNER_COUNT': [1500, 2000, 1600, 2100, 1700, 2200]
    })
    
    preprocessor = DataPreprocessor()
    
    # Clean data
    df_clean = preprocessor.clean_data(sample_data)
    
    # Create time features
    df_time = preprocessor.create_time_features(df_clean)
    
    # Encode categorical
    df_encoded = preprocessor.encode_categorical(df_time, ['SSA_TIER1', 'AGE_GROUP'])
    
    # Get summary
    preprocessor.get_feature_summary(df_encoded)
    
    print("\n[OK] Preprocessor test complete!")
    print(df_encoded)
