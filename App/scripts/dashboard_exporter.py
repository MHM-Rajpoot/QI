"""
Dashboard Data Exporter
Exports processed data for Power BI visualization
"""

import pandas as pd
import os
from datetime import datetime


class DashboardExporter:
    """Export data for Power BI dashboard"""
    
    def __init__(self, output_dir='dashboard_data'):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"[OK] Created output directory: {output_dir}")
    
    def export_to_csv(self, df, filename, include_timestamp=True):
        """Export DataFrame to CSV for Power BI"""
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join(self.output_dir, f"{filename}_{timestamp}.csv")
        else:
            filepath = os.path.join(self.output_dir, f"{filename}.csv")
        
        df.to_csv(filepath, index=False)
        print(f"[OK] Exported: {filepath} ({len(df)} rows)")
        return filepath
    
    def export_to_excel(self, dataframes_dict, filename):
        """Export multiple DataFrames to Excel sheets"""
        filepath = os.path.join(self.output_dir, f"{filename}.xlsx")
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for sheet_name, df in dataframes_dict.items():
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Excel max 31 chars
                print(f"  - Sheet '{sheet_name}': {len(df)} rows")
        
        print(f"[OK] Exported: {filepath}")
        return filepath
    
    def prepare_enrolment_summary(self, df):
        """Prepare enrolment summary for dashboard"""
        summary = df.groupby('ACADEMIC_YEAR').agg({
            'LEARNER_COUNT': 'sum',
            'ENROLMENT_COUNT': 'sum' if 'ENROLMENT_COUNT' in df.columns else 'sum'
        }).reset_index()
        
        # Add year-over-year change
        summary['YOY_CHANGE'] = summary['LEARNER_COUNT'].pct_change() * 100
        summary['YOY_CHANGE'] = summary['YOY_CHANGE'].round(2)
        
        return summary
    
    def prepare_ssa_breakdown(self, df):
        """Prepare SSA breakdown for dashboard"""
        if 'SSA_TIER1' not in df.columns:
            print("[WARN] SSA_TIER1 column not found")
            return df
        
        breakdown = df.groupby(['ACADEMIC_YEAR', 'SSA_TIER1']).agg({
            'LEARNER_COUNT': 'sum'
        }).reset_index()
        
        # Calculate percentage of total per year
        total_per_year = breakdown.groupby('ACADEMIC_YEAR')['LEARNER_COUNT'].transform('sum')
        breakdown['PERCENTAGE'] = (breakdown['LEARNER_COUNT'] / total_per_year * 100).round(2)
        
        return breakdown
    
    def prepare_forecast_data(self, historical_df, forecast_df, target_col='LEARNER_COUNT'):
        """Combine historical and forecast data for dashboard"""
        # Mark historical data
        historical = historical_df.copy()
        historical['DATA_TYPE'] = 'Historical'
        
        # Mark forecast data
        forecast = forecast_df.copy()
        forecast['DATA_TYPE'] = 'Forecast'
        
        # Rename forecast column to match
        if f'PREDICTED_{target_col}' in forecast.columns:
            forecast = forecast.rename(columns={f'PREDICTED_{target_col}': target_col})
        elif f'FORECAST_{target_col}' in forecast.columns:
            forecast = forecast.rename(columns={f'FORECAST_{target_col}': target_col})
        
        # Combine
        combined = pd.concat([historical, forecast], ignore_index=True)
        
        return combined
    
    def create_kpi_metrics(self, df, current_year=None):
        """Create KPI metrics for dashboard cards"""
        if current_year is None:
            current_year = df['ACADEMIC_YEAR'].max() if 'ACADEMIC_YEAR' in df.columns else df['YEAR'].max()
        
        # Current year metrics
        current_data = df[df.get('ACADEMIC_YEAR', df.get('YEAR', None)) == current_year]
        
        kpis = {
            'Total Learners': df['LEARNER_COUNT'].sum(),
            'Current Year Learners': current_data['LEARNER_COUNT'].sum() if len(current_data) > 0 else 0,
            'Average per Year': df.groupby(df.columns[0])['LEARNER_COUNT'].sum().mean(),
            'Peak Year': df.loc[df.groupby(df.columns[0])['LEARNER_COUNT'].sum().idxmax(), df.columns[0]] if len(df) > 0 else 'N/A'
        }
        
        kpi_df = pd.DataFrame([kpis])
        return kpi_df


class PowerBIDataModel:
    """Create Power BI compatible data model"""
    
    def __init__(self, exporter):
        self.exporter = exporter
    
    def create_date_dimension(self, start_year=2019, end_year=2030):
        """Create a date dimension table"""
        years = range(start_year, end_year + 1)
        months = range(1, 13)
        
        records = []
        for year in years:
            for month in months:
                academic_year = f"{year}/{str(year+1)[-2:]}" if month >= 8 else f"{year-1}/{str(year)[-2:]}"
                quarter = (month - 1) // 3 + 1
                
                records.append({
                    'DATE_KEY': int(f"{year}{month:02d}"),
                    'YEAR': year,
                    'MONTH': month,
                    'MONTH_NAME': pd.Timestamp(year=year, month=month, day=1).strftime('%B'),
                    'QUARTER': quarter,
                    'QUARTER_NAME': f'Q{quarter}',
                    'ACADEMIC_YEAR': academic_year,
                    'IS_ACADEMIC_START': 1 if month == 9 else 0
                })
        
        return pd.DataFrame(records)
    
    def export_full_model(self, fact_enrolment, dim_ssa, dim_age, dim_provider, forecast_df=None):
        """Export complete data model for Power BI"""
        
        exports = {
            'FACT_Enrolment': fact_enrolment,
            'DIM_Date': self.create_date_dimension(),
            'DIM_SSA': dim_ssa,
            'DIM_Age': dim_age,
            'DIM_Provider': dim_provider
        }
        
        if forecast_df is not None:
            exports['FACT_Forecast'] = forecast_df
        
        filepath = self.exporter.export_to_excel(exports, 'PowerBI_DataModel')
        
        print("\n[INFO] Power BI Data Model Structure:")
        print("  - FACT_Enrolment: Main fact table with enrolment metrics")
        print("  - DIM_Date: Date dimension with academic year mapping")
        print("  - DIM_SSA: Sector Subject Area hierarchy")
        print("  - DIM_Age: Age group dimension")
        print("  - DIM_Provider: College/Provider dimension")
        if forecast_df is not None:
            print("  - FACT_Forecast: Predicted enrolment values")
        
        return filepath


if __name__ == "__main__":
    print("[TEST] Testing Dashboard Exporter...")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'ACADEMIC_YEAR': ['2021/22', '2021/22', '2022/23', '2022/23', '2023/24', '2023/24'],
        'SSA_TIER1': ['Science', 'Business', 'Science', 'Business', 'Science', 'Business'],
        'LEARNER_COUNT': [1500, 2000, 1600, 2100, 1700, 2200]
    })
    
    forecast_data = pd.DataFrame({
        'YEAR': [2025, 2026, 2027],
        'FORECAST_LEARNER_COUNT': [2400, 2550, 2700]
    })
    
    # Test exporter
    exporter = DashboardExporter('dashboard_data')
    
    # Export summary
    summary = exporter.prepare_enrolment_summary(sample_data)
    exporter.export_to_csv(summary, 'enrolment_summary', include_timestamp=False)
    
    # Export SSA breakdown
    ssa_breakdown = exporter.prepare_ssa_breakdown(sample_data)
    exporter.export_to_csv(ssa_breakdown, 'ssa_breakdown', include_timestamp=False)
    
    # Export KPIs
    kpis = exporter.create_kpi_metrics(sample_data, '2023/24')
    exporter.export_to_csv(kpis, 'kpi_metrics', include_timestamp=False)
    
    print("\n[OK] Dashboard Exporter test complete!")
