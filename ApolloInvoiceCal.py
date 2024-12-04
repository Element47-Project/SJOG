import pyodbc
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

process_date = '2024-10-29'  # "%Y-%m-%d"
log_file_path = r"C:\Users\Shane\Desktop\Apllo\apollo_upload.log"
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

        # Step 1: Backup existing data
        backup_query = f"""
            INSERT INTO [dbo].[ApolloTesting2]
            SELECT * FROM [dbo].[ApolloTesting]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        cursor.execute(backup_query)
        conn.commit()
        logging.info(f"Backup completed for {date} into ApolloTesting2.")

        # Step 2: Fetch the data for the given date
        query = f"""
            SELECT * FROM [dbo].[ApolloTesting]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logging.warning(f"No data found for {date}.")
            return

        # Step 3: Delete existing data for the date
        delete_query = f"DELETE FROM [dbo].[ApolloTesting] WHERE CONVERT(date, [DateTime]) = '{date}'"
        cursor.execute(delete_query)
        conn.commit()
        logging.info(f"Deleted existing data for {date}.")

        # Step 4: Clean and preprocess data
        cols_to_clean = ['kWh_IMP', 'kvarh_IMP', 'kvarh_EXP', 'kVAh', 'V', 'I', 'kW', 'I_THD']
        df[cols_to_clean] = df[cols_to_clean].replace(0, np.nan)

        # Fix the conditional update for kWh_IMP
        df.loc[df['kWh_IMP'] == 0, 'kWh_IMP'] = df['Prev_kWh_IMP']

        # Detect and handle outliers
        df = detect_outliers_by_difference(df, cols_to_clean, group_col='Meter', threshold=200)
        df[cols_to_clean] = df.groupby('Meter')[cols_to_clean].ffill().bfill()

        # Step 5: Add previous day's values for the first row of each Meter
        prev_entry_query = f"""
            SELECT [Meter], [kWh_IMP], [kvarh_IMP], [kvarh_EXP], [kVAh]
            FROM [dbo].[ApolloTesting] AS main
            WHERE [DateTime] = (
                SELECT MAX([DateTime])
                FROM [dbo].[ApolloTesting] AS sub
                WHERE sub.[DateTime] < '{date}' AND sub.[Meter] = main.[Meter]
            )
        """
        prev_day_df = pd.read_sql(prev_entry_query, engine)
        df = df.sort_values(by=['Meter', 'DateTime'])

        for meter in df['Meter'].unique():
            prev_row = prev_day_df[prev_day_df['Meter'] == meter]
            if not prev_row.empty:
                mask = (df['Meter'] == meter) & (df['DateTime'] == df[df['Meter'] == meter]['DateTime'].min())
                df.loc[mask, 'Prev_kWh_IMP'] = prev_row['kWh_IMP'].values[0]
                df.loc[mask, 'Prev_kvarh_IMP'] = prev_row['kvarh_IMP'].values[0]
                df.loc[mask, 'Prev_kvarh_EXP'] = prev_row['kvarh_EXP'].values[0]
                df.loc[mask, 'Prev_kVAh'] = prev_row['kVAh'].values[0]

        # Calculate previous values for the rest of the day
        df['Prev_kWh_IMP'] = df.groupby('Meter')['kWh_IMP'].shift(1).fillna(df['Prev_kWh_IMP'])
        df['Prev_kvarh_IMP'] = df.groupby('Meter')['kvarh_IMP'].shift(1).fillna(df['Prev_kvarh_IMP'])
        df['Prev_kvarh_EXP'] = df.groupby('Meter')['kvarh_EXP'].shift(1).fillna(df['Prev_kvarh_EXP'])
        df['Prev_kVAh'] = df.groupby('Meter')['kVAh'].shift(1).fillna(df['Prev_kVAh'])

        # Step 6: Calculate differences
        df['KWH_IMP_Diff'] = df['kWh_IMP'] - df['Prev_kWh_IMP']
        df['kvarh_IMP_Diff'] = df['kvarh_IMP'] - df['Prev_kvarh_IMP']
        df['kvarh_EXP_Diff'] = df['kvarh_EXP'] - df['Prev_kvarh_EXP']
        df['kVAh_Diff'] = df['kVAh'] - df['Prev_kVAh']

        # Step 7: Upload cleaned data back to ApolloTesting
        df_to_upload = df[['DateTime', 'Meter', 'kWh_IMP', 'Prev_kWh_IMP', 'KWH_IMP_Diff',
                           'kWh_EXP', 'kvarh_IMP', 'Prev_kvarh_IMP', 'kvarh_IMP_Diff',
                           'kvarh_EXP', 'Prev_kvarh_EXP', 'kvarh_EXP_Diff', 'kVAh',
                           'Prev_kVAh', 'kVAh_Diff', 'V', 'I', 'kW', 'I_THD']]
        df_to_upload.to_sql('ApolloTesting', con=engine, if_exists='append', index=False)
        conn.commit()

        logging.info(f"Successfully processed and uploaded data for {date}.")
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error while processing data for {date}: {e}")
    finally:
        if conn:
            conn.close()


def get_consumption(date_str):
    try:
        query = f"""
            SELECT CAST(DateTime AS DATE) as Date, 
                   Meter, 
                   SUM(KWH_IMP_Diff) as Consumption
            FROM ApolloTesting
            WHERE CAST(DateTime AS DATE) = '{date_str}' 
            GROUP BY CAST(DateTime AS DATE), Meter;
        """
        df = pd.read_sql(query, engine)
        df['Consumption'] = df['Consumption'].round(3)
        return df

    except Exception as e:
        logging.error(f"Error while fetching consumption data for {date_str}: {e}")
        return None


def get_tariff(date_str):
    try:
        query = f"""
            SELECT TOP 1 Supply, Flat
            FROM ElecTariff
            WHERE '{date_str}' >= StartTime AND '{date_str}' <= EndTime
            ORDER BY StartTime DESC;
        """
        tariff_data = pd.read_sql(query, engine)
        return tariff_data.iloc[0] if not tariff_data.empty else None

    except Exception as e:
        logging.error(f"Error while fetching tariff data for {date_str}: {e}")
        return None


def calculate_invoice(consumption, tariff):
    try:
        if tariff is None or consumption is None:
            logging.error("Tariff data or consumption data is missing.")
            return None

        supply_charge = tariff['Supply']
        flat_rate_per_kwh = tariff['Flat']

        consumption['Supply_Charge'] = supply_charge
        consumption['Elec_Charge'] = round(consumption['Consumption'] * flat_rate_per_kwh, 4)
        consumption['Total_Charge'] = round(consumption['Supply_Charge'] + consumption['Elec_Charge'], 4)

        return consumption

    except Exception as e:
        logging.error(f"Error while calculating the invoice: {e}")
        return None


# Function to upload the invoice to Apollo_Invoice table
def upload_to_sql(df, table_name='Apollo_Invoice'):
    try:
        if df is not None:
            df = df[['Date', 'Meter', 'Supply_Charge', 'Elec_Charge', 'Total_Charge', 'Consumption']]
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            logging.info(f"Data uploaded successfully to {table_name}.")
        else:
            logging.error(f"Dataframe is None. Nothing to upload.")
    except Exception as e:
        logging.error(f"Error while uploading data to SQL: {e}")


def main(start_date=None):
    try:
        # Determine the start and end dates
        if start_date is None:
            logging.error("Start date is required to run the process.")
            return

        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        yesterday_dt = datetime.today().date() - timedelta(days=1)

        # Loop through each date from start_date to yesterday
        current_date = start_date_dt
        while current_date <= yesterday_dt:
            process_date_str = current_date.strftime("%Y-%m-%d")

            logging.info(f"Starting processing for {process_date_str}...")

            # Step 1: Process data for the current date
            process_data_for_date(process_date_str)

            # Step 2: Calculate consumption for the current date
            consumption_data = get_consumption(process_date_str)

            if consumption_data is None or consumption_data.empty:
                logging.warning(f"No consumption data found for {process_date_str}. Skipping invoice calculation.")
                current_date += timedelta(days=1)
                continue

            # Step 3: Fetch tariff for the current date
            tariff_data = get_tariff(process_date_str)

            if tariff_data is None:
                logging.warning(f"No tariff data found for {process_date_str}. Skipping invoice calculation.")
                current_date += timedelta(days=1)
                continue

            # Step 4: Calculate invoice
            invoice_data = calculate_invoice(consumption_data, tariff_data)

            if invoice_data is None:
                logging.warning(f"Invoice calculation failed for {process_date_str}.")
                current_date += timedelta(days=1)
                continue

            # Step 5: Upload invoice data to SQL
            upload_to_sql(invoice_data)

            logging.info(f"Processing completed for {process_date_str}.")
            current_date += timedelta(days=1)

        logging.info("Processing completed for all dates up to yesterday.")

    except Exception as e:
        logging.error(f"An error occurred in the main process: {e}")


# Example usage:
if __name__ == "__main__":
    main(start_date=process_date)

