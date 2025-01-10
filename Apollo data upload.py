import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
FILE_DIR = os.environ.get('FILE_ADDRESS')

# Setup connection engine and connection string
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')

# Parameters
start_date = "2024-07-25"  # Modify as needed
end_date = "2024-12-19"  # Modify as needed
table_name = "Meter_Output_Detail"

# Specify the 6 meters to exclude
excluded_meters = [
    "RMT-APL-01-MSB-MSB-01-40002624-DL1",
    "RMT-APL-01-MSB-MDB1-01-75000025-DL1",
    "RMT-APL-01-MDB2-3-MDB2-3-01-75000043-DL1",
    "RMT-APL-01-MDB4-5-MDB4-5-01-75000038-DL1",
    "RMT-APL-01-MSB-UMS-01-75000029-DL1",
    "RMT-APL-01-MSB-CMON-01-75000040-DL1"
]

# Step 1: Fetch data from the database
query = f"""
SELECT * 
FROM {table_name}
WHERE DateTime >= '{start_date}' 
  AND DateTime <= '{end_date}'
"""
data = pd.read_sql(query, engine)


# Cleaning Logic
def detect_outliers_by_difference(df, columns, group_col, threshold=200):
    """
    Detect and handle outliers based on the difference in specified columns.
    """
    df_copy = df.copy()

    for col in columns:
        df_copy = df_copy.sort_values([group_col, 'DateTime'])
        df_copy[f'{col}_diff'] = df_copy.groupby(group_col)[col].diff()
        outliers = (df_copy[f'{col}_diff'].abs() > threshold) | (df_copy[f'{col}_diff'].abs().shift(-1) > threshold)
        df_copy.loc[outliers, col] = np.nan
        df_copy = df_copy.drop(f'{col}_diff', axis=1)

    return df_copy


def clean_meter_data(df):
    """
    Cleans the meter data, filling missing values, handling outliers,
    and calculating differences for kWh_IMP.
    """
    process_cols = ['kWh_IMP']

    # Replace zero values with NaN for processing
    df[process_cols] = df[process_cols].replace(0, np.nan)

    # Detect and handle outliers
    df = detect_outliers_by_difference(df, process_cols, group_col='Meter', threshold=200)

    # Forward-fill and backward-fill missing values within each meter group
    df[process_cols] = df.groupby('Meter')[process_cols].ffill().bfill()

    # Calculate previous values (Prev_kWh_IMP)
    df['Prev_kWh_IMP'] = df.groupby('Meter')['kWh_IMP'].shift(1)

    # Calculate differences (Diff_kWh_IMP)
    df['Diff_kWh_IMP'] = df['kWh_IMP'] - df['Prev_kWh_IMP']
    df['Diff_KWH_IMP'] = df['Diff_kWh_IMP']

    # Set kWh_EXP to None
    if 'kWh_EXP' in df.columns:
        df['kWh_EXP'] = None

    return df


# Step 2: Clean the data
cleaned_data = clean_meter_data(data)

# Exclude the first row for each Meter group
cleaned_data = cleaned_data.groupby('Meter').apply(lambda group: group.iloc[1:]).reset_index(drop=True)

# Drop the 'Diff_kWh_IMP' column
cleaned_data.drop(columns=['Diff_kWh_IMP'], inplace=True)

# Step 3: Process all meters except the excluded ones
with engine.begin() as conn:
    try:
        # Get unique meters from the cleaned data
        unique_meters = cleaned_data['Meter'].unique()

        for meter in unique_meters:
            if meter in excluded_meters:
                print(f"Skipping Meter: {meter}")
                continue

            # Delete data for each meter
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE Meter = '{meter}' 
              AND DateTime >= '{start_date}' 
              AND DateTime <= '{end_date}'
            """
            conn.execute(text(delete_query))
            print(f"Rows deleted successfully for Meter: {meter} between {start_date} and {end_date}")

            # Filter cleaned data for the current meter
            meter_data = cleaned_data[cleaned_data['Meter'] == meter]

            # Upload cleaned data for the current meter
            meter_data.to_sql(table_name, con=conn, if_exists='append', index=False)
            print(f"Cleaned data uploaded successfully for Meter: {meter}")

    except Exception as e:
        # Rollback is automatic with `engine.begin()` context manager
        print(f"Error occurred while processing meters: {e}")
        raise
