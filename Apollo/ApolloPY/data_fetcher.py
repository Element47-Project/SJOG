import pandas as pd
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')

engine = create_engine(
    f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server')


def fetch_meter_data():
    try:
        query = """
            SELECT
                [ProjectName],
                [Meter],
                [Tariff],
                [Status],
                [EXP]
            FROM [dbo].[Meter_Table]
            WHERE [Status] IN ('Running', 'Processing')
        """
        meter_data = pd.read_sql(query, engine)
        return meter_data
    except Exception as e:
        logging.error(f"Error fetching meter data: {e}")
        return None


def fetch_all_tariff_data(date):
    try:
        query = """
            SELECT
                *
            FROM [dbo].[Tariff_All]
            WHERE StartDate <= ?
                  AND DATEADD(year, 1, StartDate) > ?
        """
        params = (date, date)
        tariff_data = pd.read_sql(query, engine, params=params)
        return tariff_data
    except Exception as e:
        logging.error(f"Error fetching tariff data: {e}")
        return None


def fetch_meter_data_with_previous(meter, date):
    try:
        query = f"""
            SELECT TOP 1 [DateTime], [Meter], [kWh_IMP], [kWh_EXP]
            FROM [dbo].[Meter_Output_Detail]
            WHERE [Meter] = '{meter}' AND [DateTime] < '{date} 00:00:00'
            ORDER BY [DateTime] DESC;

            SELECT [DateTime], [kWh_IMP], [kWh_EXP], [kvarh_IMP],
                   [kvarh_EXP], [kVAh], [V1], [V2], [V3], [I1], [I2], [I3],
                   [kW1], [kW2], [kW3], [Meter]
            FROM [dbo].[Meter_Output_RAW]
            WHERE [Meter] = '{meter}' AND CONVERT(date, [DateTime]) = '{date}'
            ORDER BY [DateTime];
        """
        prev_data = pd.read_sql(query.split(';')[0], engine)
        curr_data = pd.read_sql(query.split(';')[1], engine)
        return prev_data, curr_data
    except Exception as e:
        logging.error(f"Error fetching data for meter '{meter}': {e}")
        return None, None
