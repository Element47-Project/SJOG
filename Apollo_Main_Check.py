import pyodbc
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

process_date = '2024-11-18'
log_file_path = r"C:\Users\Shane\Desktop\Apllo\apollo_MainCheck_upload.log"
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
FILE_DIR = os.environ.get('FILE_ADDRESS')

# Setup connection engine and connection string
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};'
    f'DATABASE={SQL_DB_NAME};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
)


def detect_outliers_by_difference(df, columns, group_col, threshold=200):
    df_copy = df.copy()

    for col in columns:
        df_copy = df_copy.sort_values([group_col, 'DateTime'])
        df_copy[f'{col}_diff'] = df_copy.groupby(group_col)[col].diff()
        outliers = (df_copy[f'{col}_diff'].abs() > threshold) | (df_copy[f'{col}_diff'].abs().shift(-1) > threshold)
        df_copy.loc[outliers, col] = np.nan
        df_copy = df_copy.drop(f'{col}_diff', axis=1)

    return df_copy


def process_data_for_date(date=None):
    if date is None:
        date = datetime.today().date() - timedelta(days=1)
    else:
        date = datetime.strptime(date, "%Y-%m-%d").date()

    conn = None
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        conn.autocommit = False

        # Backup query
        backup_query = f"""
            INSERT INTO [dbo].[Apollo_Main_Check_Backup]
            SELECT *
            FROM [dbo].[Apollo_Main_Check]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        cursor.execute(backup_query)
        conn.commit()
        logging.info(f"Backup completed for {date} into Apollo_Main_Check_Backup.")

        # Fetch data for processing
        query = f"""
            SELECT [DateTime], [kWh_Import_Total], [kWh_Import_Previous], 
                   [KWh_Import_Diff], [kWh_Export_Total], [kvarh_Import_Total], 
                   [kvarh_Import_Previous], [kvarh_Import_Diff], [kvarh_Export_Total], 
                   [kVAh_Total], [kVAh_Import_Total], [kVAh_Export_Total], 
                   [V12], [V23], [V13], [I1], [I2], [I3], [KW1], [KW2], [KW3], 
                   [I1_Current_THD], [I2_Current_THD], [I3_Current_THD], [Meter]
            FROM [dbo].[Apollo_Main_Check]
            WHERE CONVERT(date, [DateTime]) = '{date}'
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logging.warning(f"No data found for {date}.")
            return

        # Delete existing data for the date
        delete_query = f"DELETE FROM [dbo].[Apollo_Main_Check] WHERE CONVERT(date, [DateTime]) = '{date}'"
        cursor.execute(delete_query)
        conn.commit()
        logging.info(f"Deleted existing data for {date}.")

        # Columns to clean
        cols_to_clean = [
            'kWh_Import_Total', 'kvarh_Import_Total', 'kvarh_Export_Total',
            'kVAh_Total', 'V12', 'V23', 'V13', 'I1', 'I2', 'I3',
            'KW1', 'KW2', 'KW3', 'I1_Current_THD', 'I2_Current_THD', 'I3_Current_THD'
        ]
        df[cols_to_clean] = df[cols_to_clean].replace(0, np.nan)

        # Detect and handle outliers
        df = detect_outliers_by_difference(df, cols_to_clean, group_col='Meter', threshold=200)
        df[cols_to_clean] = df.groupby('Meter')[cols_to_clean].ffill().bfill()

        # Fetch previous day's data for the first entry of each meter
        prev_entry_query = f"""
            SELECT [Meter], [kWh_Import_Total], [kvarh_Import_Total], [kvarh_Export_Total], [kVAh_Total]
            FROM [dbo].[Apollo_Main_Check] AS main
            WHERE [DateTime] = (
                SELECT MAX([DateTime])
                FROM [dbo].[Apollo_Main_Check] AS sub
                WHERE sub.[DateTime] < '{date}' AND sub.[Meter] = main.[Meter]
            )
        """
        prev_day_df = pd.read_sql(prev_entry_query, engine)
        df = df.sort_values(by=['Meter', 'DateTime'])

        # Initialize previous columns
        for col in ['kWh_Import_Previous', 'kvarh_Import_Previous', 'kVAh_Total']:
            df[col] = np.nan

        for meter in df['Meter'].unique():
            prev_row = prev_day_df[prev_day_df['Meter'] == meter]
            if not prev_row.empty:
                mask = (df['Meter'] == meter) & (df['DateTime'] == df[df['Meter'] == meter]['DateTime'].min())
                df.loc[mask, 'kWh_Import_Previous'] = prev_row['kWh_Import_Total'].values[0]
                df.loc[mask, 'kvarh_Import_Previous'] = prev_row['kvarh_Import_Total'].values[0]

        # Shift previous values for the rest of the day
        df['kWh_Import_Previous'] = df.groupby('Meter')['kWh_Import_Total'].shift(1)
        df['kvarh_Import_Previous'] = df.groupby('Meter')['kvarh_Import_Total'].shift(1)

        # Calculate differences
        df['KWh_Import_Diff'] = df['kWh_Import_Total'] - df['kWh_Import_Previous']
        df['kvarh_Import_Diff'] = df['kvarh_Import_Total'] - df['kvarh_Import_Previous']

        # Check for abnormal data
        alarm_rows_gt_5 = df[df['KWh_Import_Diff'] > 5]
        alarm_rows_lt_0 = df[df['KWh_Import_Diff'] < 0]

        if not alarm_rows_gt_5.empty:
            for _, row in alarm_rows_gt_5.iterrows():
                logging.warning(f"High KWh_Import_Diff: Meter: {row['Meter']}, "
                                f"Timestamp: {row['DateTime']}, Diff: {row['KWh_Import_Diff']}")

        if not alarm_rows_lt_0.empty:
            for _, row in alarm_rows_lt_0.iterrows():
                logging.warning(f"Negative KWh_Import_Diff: Meter: {row['Meter']}, "
                                f"Timestamp: {row['DateTime']}, Diff: {row['KWh_Import_Diff']}")

        # Prepare for upload
        df_to_upload = df[[
            'DateTime', 'kWh_Import_Total', 'kWh_Import_Previous', 'KWh_Import_Diff',
            'kWh_Export_Total', 'kvarh_Import_Total', 'kvarh_Import_Previous', 'kvarh_Import_Diff',
            'kvarh_Export_Total', 'kVAh_Total', 'kVAh_Import_Total', 'kVAh_Export_Total',
            'V12', 'V23', 'V13', 'I1', 'I2', 'I3', 'KW1', 'KW2', 'KW3',
            'I1_Current_THD', 'I2_Current_THD', 'I3_Current_THD', 'Meter'
        ]]
        df_to_upload.to_sql('Apollo_Main_Check', con=engine, if_exists='append', index=False)
        conn.commit()
        logging.info(f"Data successfully processed and uploaded for {date}.")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error while processing data for {date}: {e}")
    finally:
        if conn:
            conn.close()


# Example usage:
if __name__ == "__main__":
    if process_date:
        process_data_for_date(process_date)
    else:
        process_data_for_date()