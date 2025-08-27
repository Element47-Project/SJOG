from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
SQL_SERVER = os.getenv('AZURE_SQL_SERVER')
SQL_DB_NAME = os.getenv('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.getenv('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.getenv('AZURE_SQL_PASSWORD')

class AzureConnector:
    def __init__(self):
        self.connection_string = (f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}"
                                  f"?driver=ODBC+Driver+17+for+SQL+Server")
        self.engine = create_engine(self.connection_string)

    def fetch_data(self, start, end):
        query = f"""
            SELECT 
                [DateTime], 
                kWh_IMP, 
                kWh_EXP, 
                d.Meter, 
                Unit AS display_name
            FROM 
                Meter_Output_RAW d
            INNER JOIN 
                Meter_Table m ON d.Meter = m.Meter
            WHERE 
                m.ProjectName = 'Dehavilland Apartment'
            AND 
                [DateTime] BETWEEN '{start}' AND '{end}'
            ORDER BY 
                d.Meter;
        """
        return pd.read_sql(query, self.engine)

