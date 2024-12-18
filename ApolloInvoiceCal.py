import pyodbc
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

process_date = ''  # "%Y-%m-%d"
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
ALL_UNITS_METERS = {
    "RMT-APL-01-MSB-APR11-01-50002646-DL1",
    "RMT-APL-01-MSB-APR12-01-50002646-DL2",
    "RMT-APL-01-MSB-APR13-01-50002646-DL3",
    "RMT-APL-01-MSB-APR14-01-50002728-DL1",
    "RMT-APL-01-MSB-APR15-01-50002728-DL2",
    "RMT-APL-01-MSB-APR37-01-50002764-DL1",
    "RMT-APL-01-MSB-APR38-01-50002764-DL2",
    "RMT-APL-01-MSB-APR39-01-50002764-DL3",
    "RMT-APL-01-MSB-APR73-01-50002728-DL3",
    "RMT-APL-01-MDB1-APR01-01-50002745-DL1",
    "RMT-APL-01-MDB1-APR02-01-50002745-DL2",
    "RMT-APL-01-MDB1-APR03-01-50002745-DL3",
    "RMT-APL-01-MDB1-APR04-01-50002756-DL1",
    "RMT-APL-01-MDB1-APR05-01-50002756-DL2",
    "RMT-APL-01-MDB1-APR06-01-50002756-DL3",
    "RMT-APL-01-MDB1-APR07-01-50002692-DL1",
    "RMT-APL-01-MDB1-APR08-01-50002692-DL2",
    "RMT-APL-01-MDB1-APR09-01-50002692-DL3",
    "RMT-APL-01-MDB1-APR10-01-50002676-DL1",
    "RMT-APL-01-MDB2-APR27-01-50002663-DL1",
    "RMT-APL-01-MDB2-APR28-01-50002663-DL2",
    "RMT-APL-01-MDB2-APR29-01-50002663-DL3",
    "RMT-APL-01-MDB2-APR30-01-50002733-DL1",
    "RMT-APL-01-MDB2-APR31-01-50002733-DL2",
    "RMT-APL-01-MDB2-APR32-01-50002733-DL3",
    "RMT-APL-01-MDB2-APR34-01-50002751-DL2",
    "RMT-APL-01-MDB2-APR35-01-50002751-DL3",
    "RMT-APL-01-MDB2-APR36-01-50002683-DL1",
    "RMT-APL-01-MDB3-APR16-01-50002687-DL1",
    "RMT-APL-01-MDB3-APR17-01-50002687-DL2",
    "RMT-APL-01-MDB3-APR18-01-50002687-DL3",
    "RMT-APL-01-MDB3-APR19-01-50002760-DL1",
    "RMT-APL-01-MDB3-APR20-01-50002760-DL2",
    "RMT-APL-01-MDB3-APR21-01-50002760-DL3",
    "RMT-APL-01-MDB3-APR22-01-50002668-DL1",
    "RMT-APL-01-MDB3-APR23-01-50002668-DL2",
    "RMT-APL-01-MDB3-APR24-01-50002668-DL3",
    "RMT-APL-01-MDB3-APR25-01-50002685-DL1",
    "RMT-APL-01-MDB3-APR26-01-50002685-DL2",
    "RMT-APL-01-MDB4-APR40-01-50002690-DL1",
    "RMT-APL-01-MDB4-APR41-01-50002690-DL2",
    "RMT-APL-01-MDB4-APR42-01-50002690-DL3",
    "RMT-APL-01-MDB4-APR43-01-50002684-DL1",
    "RMT-APL-01-MDB4-APR44-01-50002684-DL2",
    "RMT-APL-01-MDB4-APR45-01-50002684-DL3",
    "RMT-APL-01-MDB4-APR46-01-50002725-DL1",
    "RMT-APL-01-MDB4-APR47-01-50002725-DL2",
    "RMT-APL-01-MDB4-APR65-01-50002681-DL1",
    "RMT-APL-01-MDB4-APR66-01-50002681-DL2",
    "RMT-APL-01-MDB4-APR67-01-50002681-DL3",
    "RMT-APL-01-MDB4-APR68-01-50002747-DL1",
    "RMT-APL-01-MDB4-APR69-01-50002747-DL2",
    "RMT-APL-01-MDB4-APR70-01-50002747-DL3",
    "RMT-APL-01-MDB4-APR71-01-50002975-DL1",
    "RMT-APL-01-MDB4-APR72-01-50002975-DL2",
    "RMT-APL-01-MDB5-APR48-01-50002673-DL1",
    "RMT-APL-01-MDB5-APR49-01-50002673-DL2",
    "RMT-APL-01-MDB5-APR50-01-50002673-DL3",
    "RMT-APL-01-MDB5-APR51-01-50002759-DL1",
    "RMT-APL-01-MDB5-APR52-01-50002759-DL2",
    "RMT-APL-01-MDB5-APR53-01-50002759-DL3",
    "RMT-APL-01-MDB5-APR54-01-50002686-DL1",
    "RMT-APL-01-MDB5-APR55-01-50002686-DL2",
    "RMT-APL-01-MDB5-APR56-01-50002686-DL3",
    "RMT-APL-01-MDB5-APR57-01-50002561-DL1",
    "RMT-APL-01-MDB5-APR58-01-50002561-DL2",
    "RMT-APL-01-MDB5-APR59-01-50002561-DL3",
    "RMT-APL-01-MDB5-APR60-01-50002758-DL1",
    "RMT-APL-01-MDB5-APR61-01-50002758-DL2",
    "RMT-APL-01-MDB5-APR62-01-50002758-DL3",
    "RMT-APL-01-MDB5-APR63-01-50002679-DL1",
    "RMT-APL-01-MDB5-APR64-01-50002679-DL2",
}

ALL_MAIN_METERS = {
    "RMT-APL-01-MSB-UMS-01-75000029-DL1",
    "RMT-APL-01-MSB-CMON-01-75000040-DL1",
    "RMT-APL-01-MSB-MSB-01-40002624-DL1",
    "RMT-APL-01-MDB4-5-MDB4-5-01-75000038-DL1",
    "RMT-APL-01-MDB2-3-MDB2-3-01-75000043-DL1",
    "RMT-APL-01-MSB-MDB1-01-75000025-DL1"
}


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

        # Step 1: Delete existing data for the given date from ApolloTesting2
        delete_query = f"""
            DELETE FROM [dbo].[ApolloTesting2]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        cursor.execute(delete_query)
        logging.info(f"Deleted existing data for {date} from ApolloTesting2.")

        # Step 2: Copy data from ApolloTesting to ApolloTesting2
        backup_query = f"""
            INSERT INTO [dbo].[ApolloTesting2]
            SELECT * FROM [dbo].[ApolloTesting]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        cursor.execute(backup_query)
        conn.commit()
        logging.info(f"Backup completed for {date} from ApolloTesting to ApolloTesting2.")

        # Step 3: Fetch the data for verification
        query = f"""
            SELECT * FROM [dbo].[ApolloTesting]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logging.warning(f"No data found for {date}.")
            return

        # Step 2.1: Check for missing meters
        existing_meters = set(df['Meter'].unique())
        missing_meters = ALL_UNITS_METERS - existing_meters
        if missing_meters:
            logging.warning(f"Missing meters for {date}: {missing_meters}")

        # Step 3: Delete existing data for the date
        delete_query = f"DELETE FROM [dbo].[ApolloTesting] WHERE CONVERT(date, [DateTime]) = '{date}'"
        cursor.execute(delete_query)
        conn.commit()

        # Step 4: Clean and preprocess data
        cols_to_clean = ['kWh_IMP', 'kvarh_IMP', 'kvarh_EXP', 'kVAh']
        df[cols_to_clean] = df[cols_to_clean].replace(0, np.nan)

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
        df = df.drop_duplicates(subset=['DateTime', 'Meter'], keep='last')
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

        if df.empty:
            logging.warning(f"No consumption data found for {date_str}. Initializing all meters with zero consumption.")
            df = pd.DataFrame(columns=['Date', 'Meter', 'Consumption'])

        # Round the existing consumption values
        df['Consumption'] = df['Consumption'].fillna(0).round(3)

        # Identify missing meters
        existing_meters = set(df['Meter'].unique())
        missing_meters = ALL_UNITS_METERS - existing_meters

        # Add missing meters with zero consumption
        if missing_meters:
            logging.info(f"Adding missing meters for {date_str}: {missing_meters}")
            # Create a DataFrame for missing meters
            missing_data_df = pd.DataFrame(
                [{'Date': date_str, 'Meter': meter, 'Consumption': 0.0} for meter in missing_meters])

            # Check for non-empty DataFrame and exclude all-NA rows
            if not missing_data_df.isna().all(axis=1).all():
                df = pd.concat([df, missing_data_df], ignore_index=True)

        # Ensure all meters are present and sorted
        df = df.sort_values(by=['Meter']).reset_index(drop=True)
        logging.info(f"Consumption data successfully fetched and completed for {date_str}.")

        return df

    except Exception as e:
        logging.error(f"Error while fetching consumption data for {date_str}: {e}")
        return None


def process_main_check_data(date: str = None):
    if date is None:
        date = datetime.today().date() - timedelta(days=1)
    else:
        date = datetime.strptime(date, "%Y-%m-%d").date()

    conn = None
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        conn.autocommit = False

        # Step 1: Backup data for the given date
        logging.info(f"Deleting backup data for {date}...")
        delete_backup_query = f"""
            DELETE FROM [dbo].[Apollo_Main_Check_Backup]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        backup_query = f"""
            INSERT INTO [dbo].[Apollo_Main_Check_Backup]
            SELECT * FROM [dbo].[Apollo_Main_Check]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        cursor.execute(delete_backup_query)
        cursor.execute(backup_query)
        conn.commit()
        logging.info(f"Backup completed for {date}.")

        # Step 2: Fetch data for cleaning
        query = f"""
            SELECT [DateTime], [kWh_Import_Total], [kWh_Export_Total], [kvarh_Import_Total],
                   [kvarh_Export_Total], [kVAh_Total], [kVAh_Import_Total], [kVAh_Export_Total],
                   [V12], [V23], [V13], [I1], [I2], [I3], [KW1], [KW2], [KW3],
                   [I1_Current_THD], [I2_Current_THD], [I3_Current_THD], [Meter]
            FROM [dbo].[Apollo_Main_Check]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logging.warning(f"No data found for {date}.")
            return

        # Step 2.1: Check for missing meters
        existing_meters = set(df['Meter'].unique())
        missing_meters = ALL_MAIN_METERS - existing_meters
        if missing_meters:
            logging.warning(f"Missing meters for {date}: {missing_meters}")
            for meter in missing_meters:
                missing_row = {'DateTime': date, 'Meter': meter,
                               'kWh_Import_Total': 0, 'kWh_Export_Total': 0,
                               'kvarh_Import_Total': 0, 'kvarh_Export_Total': 0,
                               'kVAh_Total': 0, 'kVAh_Import_Total': 0, 'kVAh_Export_Total': 0}
                df = pd.concat([df, pd.DataFrame([missing_row])], ignore_index=True)

        # Step 3: Get previous day's values
        prev_entry_query = f"""
            SELECT [Meter], [kWh_Import_Total], [kWh_Export_Total], [kvarh_Import_Total]
            FROM [dbo].[Apollo_Main_Check] AS main
            WHERE [DateTime] = (
                SELECT MAX([DateTime])
                FROM [dbo].[Apollo_Main_Check] AS sub
                WHERE sub.[DateTime] < '{date}' AND sub.[Meter] = main.[Meter]
            )
        """
        prev_day_df = pd.read_sql(prev_entry_query, engine)
        df = df.sort_values(by=['Meter', 'DateTime'])

        # Step 4: Add previous values for first row of each meter
        for meter in df['Meter'].unique():
            prev_row = prev_day_df[prev_day_df['Meter'] == meter]
            if not prev_row.empty:
                mask = (df['Meter'] == meter) & (df['DateTime'] == df[df['Meter'] == meter]['DateTime'].min())
                df.loc[mask, 'kWh_Import_Previous'] = prev_row['kWh_Import_Total'].values[0]
                df.loc[mask, 'kWh_Export_Previous'] = prev_row['kWh_Export_Total'].values[0]
                df.loc[mask, 'kvarh_Import_Previous'] = prev_row['kvarh_Import_Total'].values[0]

        # Step 5: Remove duplicates and calculate previous values for the rest of the day
        df = df.drop_duplicates(subset=['DateTime', 'Meter'], keep='last')
        df['kWh_Import_Previous'] = df.groupby('Meter')['kWh_Import_Total'].shift(1).fillna(df['kWh_Import_Previous'])
        df['kWh_Export_Previous'] = df.groupby('Meter')['kWh_Export_Total'].shift(1).fillna(df['kWh_Export_Previous'])
        df['kvarh_Import_Previous'] = df.groupby('Meter')['kvarh_Import_Total'].shift(1).fillna(
            df['kvarh_Import_Previous'])

        # Step 6: Calculate differences
        df['kWh_Import_Diff'] = df['kWh_Import_Total'] - df['kWh_Import_Previous']
        df['kWh_Export_Diff'] = df['kWh_Export_Total'] - df['kWh_Export_Previous']
        df['kvarh_Import_Diff'] = df['kvarh_Import_Total'] - df['kvarh_Import_Previous']

        # Step 7: Clean and handle outliers
        cols_to_clean = ['kWh_Import_Total', 'kWh_Export_Total', 'kvarh_Import_Total']
        df[cols_to_clean] = df[cols_to_clean].replace(0, np.nan)
        df = detect_outliers_by_difference(df, cols_to_clean, group_col='Meter', threshold=200)
        df[cols_to_clean] = df.groupby('Meter')[cols_to_clean].ffill().bfill()

        # Step 8: Upload cleaned data back to Apollo_Main_Check
        columns_order = ['DateTime', 'kWh_Import_Total', 'kWh_Import_Previous', 'kWh_Import_Diff',
                         'kWh_Export_Total', 'kWh_Export_Previous', 'kWh_Export_Diff',
                         'kvarh_Import_Total', 'kvarh_Import_Previous', 'kvarh_Import_Diff',
                         'kvarh_Export_Total', 'kVAh_Total', 'kVAh_Import_Total', 'kVAh_Export_Total',
                         'V12', 'V23', 'V13', 'I1', 'I2', 'I3', 'KW1', 'KW2', 'KW3',
                         'I1_Current_THD', 'I2_Current_THD', 'I3_Current_THD', 'Meter']

        df_to_upload = df[columns_order]

        # Delete existing data for the date before uploading
        delete_query = f"""
            DELETE FROM [dbo].[Apollo_Main_Check]
            WHERE CONVERT(date, [DateTime]) = '{date}';
        """
        cursor.execute(delete_query)

        # Upload the processed data
        df_to_upload.to_sql('Apollo_Main_Check', con=engine, if_exists='append', index=False)
        conn.commit()
        logging.info(f"Data processing completed for {date}.")

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_main_check_consumption(date_str):
    try:
        query = f"""
            SELECT 
                CAST(DateTime AS DATE) AS Date, 
                Meter, 
                SUM(CAST(COALESCE(kWh_Import_Diff, 0) AS FLOAT)) AS Consumption_IMP, 
                SUM(CAST(COALESCE(kWh_Export_Diff, 0) AS FLOAT)) AS Consumption_EXP
            FROM Apollo_Main_Check
            WHERE CAST(DateTime AS DATE) = '{date_str}'
            GROUP BY CAST(DateTime AS DATE), Meter;
        """
        df = pd.read_sql(query, engine)

        # Step 1: Handle empty result (initialize DataFrame if no data found)
        if df.empty:
            logging.warning(f"No consumption data found for {date_str}.")
            return None

        # Step 3: Melt the DataFrame to create a single 'Consumption' column
        if not df.empty:  # Proceed only if DataFrame has data
            df_melted = df.melt(
                id_vars=['Date', 'Meter'],
                value_vars=['Consumption_IMP', 'Consumption_EXP'],
                var_name='Type',
                value_name='Consumption'
            )

            # Adjust the 'Meter' column to distinguish between IMP and EXP
            df_melted['Meter'] = df_melted.apply(
                lambda x: f"{x['Meter']}_IMP" if x['Type'] == 'Consumption_IMP'
                else f"{x['Meter']}_EXP", axis=1
            )

            # Drop the 'Type' column as it's no longer needed
            df_final = df_melted.drop(columns=['Type'])

            # Reorder and sort for consistency
            df_final = df_final[['Date', 'Meter', 'Consumption']].sort_values(by=['Meter']).reset_index(drop=True)
        else:
            df_final = pd.DataFrame(columns=['Date', 'Meter', 'Consumption'])  # Handle empty DataFrame edge case

        logging.info(f"Main check consumption processed successfully for {date_str}.")
        return df_final

    except Exception as e:
        logging.error(f"Error fetching main check consumption for {date_str}: {e}")
        return None


def get_tariff(date_str):
    try:
        query = f"""
            SELECT TOP 1 Supply, Flat
            FROM ElecTariff
            WHERE Tariff = 'Apollo' and '{date_str}' >= StartTime AND '{date_str}' <= EndTime
            ORDER BY StartTime DESC;
        """
        tariff_data = pd.read_sql(query, engine)
        return tariff_data.iloc[0] if not tariff_data.empty else None

    except Exception as e:
        logging.error(f"Error while fetching tariff data for {date_str}: {e}")
        return None


def calculate_invoice(consumption, tariff):
    try:
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

            # Step 1: Process unit meters
            process_data_for_date(process_date_str)
            consumption_data = get_consumption(process_date_str)
            if consumption_data is not None:
                tariff_data = get_tariff(process_date_str)
                if tariff_data is not None:
                    invoice_data = calculate_invoice(consumption_data, tariff_data)
                    upload_to_sql(invoice_data)

            # Step 2: Process main check meters
            process_main_check_data(process_date_str)
            consumption_data_main = get_main_check_consumption(process_date_str)
            if consumption_data_main is not None:
                tariff_data_main = get_tariff(process_date_str)
                if tariff_data_main is not None:
                    invoice_data_main = calculate_invoice(consumption_data_main, tariff_data_main)
                    upload_to_sql(invoice_data_main)

            logging.info(f"Processing completed for {process_date_str}.")
            current_date += timedelta(days=1)

        logging.info("Processing completed for all dates up to yesterday.")

    except Exception as e:
        logging.error(f"An error occurred in the main process: {e}")


# Example usage:
if __name__ == "__main__":
    main(start_date=process_date)
