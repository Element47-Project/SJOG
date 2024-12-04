import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

# All settings in the .env file, including SQL information.
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
file_name = 'ApolloUserInfoDemo.csv'

engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')


def fetch_existing_data():
    """Fetches the existing data from the Apollo_User_Info table."""
    query = "SELECT * FROM Apollo_User_Info"
    try:
        existing_df = pd.read_sql(query, engine)
        return existing_df
    except pyodbc.Error as e:
        print(f"Error fetching existing data: {e}")
        return None


def remove_duplicates_and_upload(df):
    """Removes duplicates between the DataFrame (df) and the existing database records, then uploads new records."""
    existing_df = fetch_existing_data()
    existing_df['Unit_Number'] = existing_df['Unit_Number'].astype(str)

    df_unique = df[~df['Unit_Number'].isin(existing_df['Unit_Number'])]
    if not df_unique.empty:
        print(f"Uploading {len(df_unique)} new records...")
        df_unique.to_sql('Apollo_User_Info', engine, if_exists='append', index=False)
    else:
        print("No new records to upload.")


def clean_and_convert_dates(df):
    """Converts columns related to dates to datetime format to avoid SQL conversion errors."""
    date_columns = ['Estimate_Occupancy_Time', 'Estimate_Vacancy_Time', 'Actual_Occupancy_Time', 'Actual_Vacancy_Time']

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

    return df


def main():
    df = pd.read_csv(file_name, encoding='ISO-8859-1')
    mapping = {
        'MSB MAIN CHECK METER': 'RMT-APL-01-MSB-MSB-01-40002624-DL1',
        'MSB Grid Solar Export Control Meter': 'RMT-APL-01-MSB-MSB-03-40002624-',
        'MDB1': 'RMT-APL-01-MSB-MDB1-01-75000025-DL1',
        'MDB 2/3': 'RMT-APL-01-MDB2-3-MDB2-3-01-75000043-DL1',
        'MDB 4/5': 'RMT-APL-01-MDB4-5-MDB4-5-01-75000038-DL1',
        'Common, Commercial and PV': 'RMT-APL-01-MSB-CMON-01-75000040-DL1',
        'SMSB Unit Main Switches': 'RMT-APL-01-MSB-UMS-01-75000029-DL1',
        'Solar PV - Solis 30kW @Reception DB': 'RMT-APL-01-RDB-SPV-01-50003223-DL1',
        '1': 'RMT-APL-01-MDB1-APR01-01-50002745-DL1',
        '2': 'RMT-APL-01-MDB1-APR02-01-50002745-DL2',
        '3': 'RMT-APL-01-MDB1-APR03-01-50002745-DL3',
        '4': 'RMT-APL-01-MDB1-APR04-01-50002756-DL1',
        '5': 'RMT-APL-01-MDB1-APR05-01-50002756-DL2',
        '6': 'RMT-APL-01-MDB1-APR06-01-50002756-DL3',
        '7': 'RMT-APL-01-MDB1-APR07-01-50002692-DL1',
        '8': 'RMT-APL-01-MDB1-APR08-01-50002692-DL2',
        '9': 'RMT-APL-01-MDB1-APR09-01-50002692-DL3',
        '10': 'RMT-APL-01-MDB1-APR10-01-50002676-DL1',
        '11': 'RMT-APL-01-MSB-APR11-01-50002646-DL1',
        '12': 'RMT-APL-01-MSB-APR12-01-50002646-DL2',
        '13': 'RMT-APL-01-MSB-APR13-01-50002646-DL3',
        '14': 'RMT-APL-01-MSB-APR14-01-50002728-DL1',
        '15': 'RMT-APL-01-MSB-APR15-01-50002728-DL2',
        '16': 'RMT-APL-01-MDB3-APR16-01-50002687-DL1',
        '17': 'RMT-APL-01-MDB3-APR17-01-50002687-DL2',
        '18': 'RMT-APL-01-MDB3-APR18-01-50002687-DL3',
        '19': 'RMT-APL-01-MDB3-APR19-01-50002760-DL1',
        '20': 'RMT-APL-01-MDB3-APR20-01-50002760-DL2',
        '21': 'RMT-APL-01-MDB3-APR21-01-50002760-DL3',
        '22': 'RMT-APL-01-MDB3-APR22-01-50002668-DL1',
        '23': 'RMT-APL-01-MDB3-APR23-01-50002668-DL2',
        '24': 'RMT-APL-01-MDB3-APR24-01-50002668-DL3',
        '25': 'RMT-APL-01-MDB3-APR25-01-50002685-DL1',
        '26': 'RMT-APL-01-MDB3-APR26-01-50002685-DL2',
        '27': 'RMT-APL-01-MDB2-APR27-01-50002663-DL1',
        '28': 'RMT-APL-01-MDB2-APR28-01-50002663-DL2',
        '29': 'RMT-APL-01-MDB2-APR29-01-50002663-DL3',
        '30': 'RMT-APL-01-MDB2-APR30-01-50002733-DL1',
        '31': 'RMT-APL-01-MDB2-APR31-01-50002733-DL2',
        '32': 'RMT-APL-01-MDB2-APR32-01-50002733-DL3',
        '33': 'RMT-APL-01-MDB2-APR33-01-50002751-DL1',
        '34': 'RMT-APL-01-MDB2-APR34-01-50002751-DL2',
        '35': 'RMT-APL-01-MDB2-APR35-01-50002751-DL3',
        '36': 'RMT-APL-01-MDB2-APR36-01-50002683-DL1',
        '37': 'RMT-APL-01-MSB-APR37-01-50002764-DL1',
        '38': 'RMT-APL-01-MSB-APR38-01-50002764-DL2',
        '39': 'RMT-APL-01-MSB-APR39-01-50002764-DL3',
        '40': 'RMT-APL-01-MDB4-APR40-01-50002690-DL1',
        '41': 'RMT-APL-01-MDB4-APR41-01-50002690-DL2',
        '42': 'RMT-APL-01-MDB4-APR42-01-50002690-DL3',
        '43': 'RMT-APL-01-MDB4-APR43-01-50002684-DL1',
        '44': 'RMT-APL-01-MDB4-APR44-01-50002684-DL2',
        '45': 'RMT-APL-01-MDB4-APR45-01-50002684-DL3',
        '46': 'RMT-APL-01-MDB4-APR46-01-50002725-DL1',
        '47': 'RMT-APL-01-MDB4-APR47-01-50002725-DL2',
        '48': 'RMT-APL-01-MDB5-APR48-01-50002673-DL1',
        '49': 'RMT-APL-01-MDB5-APR49-01-50002673-DL2',
        '50': 'RMT-APL-01-MDB5-APR50-01-50002673-DL3',
        '51': 'RMT-APL-01-MDB5-APR51-01-50002759-DL1',
        '52': 'RMT-APL-01-MDB5-APR52-01-50002759-DL2',
        '53': 'RMT-APL-01-MDB5-APR53-01-50002759-DL3',
        '54': 'RMT-APL-01-MDB5-APR54-01-50002686-DL1',
        '55': 'RMT-APL-01-MDB5-APR55-01-50002686-DL2',
        '56': 'RMT-APL-01-MDB5-APR56-01-50002686-DL3',
        '57': 'RMT-APL-01-MDB5-APR57-01-50002561-DL1',
        '58': 'RMT-APL-01-MDB5-APR58-01-50002561-DL2',
        '59': 'RMT-APL-01-MDB5-APR59-01-50002561-DL3',
        '60': 'RMT-APL-01-MDB5-APR60-01-50002758-DL1',
        '61': 'RMT-APL-01-MDB5-APR61-01-50002758-DL2',
        '62': 'RMT-APL-01-MDB5-APR62-01-50002758-DL3',
        '63': 'RMT-APL-01-MDB5-APR63-01-50002679-DL1',
        '64': 'RMT-APL-01-MDB5-APR64-01-50002679-DL2',
        '65': 'RMT-APL-01-MDB4-APR65-01-50002681-DL1',
        '66': 'RMT-APL-01-MDB4-APR66-01-50002681-DL2',
        '67': 'RMT-APL-01-MDB4-APR67-01-50002681-DL3',
        '68': 'RMT-APL-01-MDB4-APR68-01-50002747-DL1',
        '69': 'RMT-APL-01-MDB4-APR69-01-50002747-DL2',
        '70': 'RMT-APL-01-MDB4-APR70-01-50002747-DL3',
        '71': 'RMT-APL-01-MDB4-APR71-01-50002975-DL1',
        '72': 'RMT-APL-01-MDB4-APR72-01-50002975-DL2',
        '73': 'RMT-APL-01-MSB-APR73-01-50002728-DL3',
    }
    df['Unit_Number'] = df['Unit_Number'].astype(str)
    df['Unit_Number'] = df['Unit_Number'].map(mapping)
    df = clean_and_convert_dates(df)
    remove_duplicates_and_upload(df)


if __name__ == "__main__":
    main()