import pyodbc
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

process_date = '2024-10-07'
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
date = '2024-10-03'
conn = pyodbc.connect(CONNECTION_STRING)
cursor = conn.cursor()
delete_query = f"DELETE FROM [dbo].[ApolloTesting] WHERE CONVERT(date, [DateTime]) = '{date}'"
cursor.execute(delete_query)
conn.commit()
restore_query = f"""
    INSERT INTO [dbo].[ApolloTesting] ([DateTime], [Meter], [kWh_IMP], [Prev_kWh_IMP], 
                                       [KWH_IMP_Diff], [kWh_EXP], [kvarh_IMP], 
                                       [Prev_kvarh_IMP], [kvarh_IMP_Diff], [kvarh_EXP], 
                                       [Prev_kvarh_EXP], [kvarh_EXP_Diff], [kVAh], 
                                       [Prev_kVAh], [kVAh_Diff], [V], [I], [kW], [I_THD])
    SELECT [DateTime], [Meter], [kWh_IMP], [Prev_kWh_IMP], 
           [KWH_IMP_Diff], [kWh_EXP], [kvarh_IMP], 
           [Prev_kvarh_IMP], [kvarh_IMP_Diff], [kvarh_EXP], 
           [Prev_kvarh_EXP], [kvarh_EXP_Diff], [kVAh], 
           [Prev_kVAh], [kVAh_Diff], [V], [I], [kW], [I_THD]
    FROM [dbo].[ApolloTesting2]
    WHERE CONVERT(date, [DateTime]) = '{date}';
"""
cursor.execute(restore_query)
conn.commit()
conn.close()
