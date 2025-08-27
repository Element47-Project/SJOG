from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv()

# SQL connection info
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')

table_name = 'Meter_Output_RAW'
folder_path = r'C:\Users\Shane\PycharmProjects\pythonProject\element 47\SJOG\Standard\Testing_v2\testing\Data\DeHaviland'
# folder_path = os.path.expanduser('~/APP/testing/Data/DeHaviland')

# Set up SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server')


def parse_datetime_flexibly(x):
    """尝试匹配多种时间格式"""
    for fmt in ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M']:
        try:
            dt = datetime.strptime(x.strip(), fmt)
            return dt
        except ValueError:
            continue
    return pd.NaT  # 如果都失败，返回 NaT


# Loop through all CSV files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        meter = os.path.splitext(filename)[0]

        print(f"\n📂 Processing file: {filename} as Meter: {meter}")

        df = pd.read_csv(file_path)

        df['DateTime'] = df['DateTime'].astype(str).apply(parse_datetime_flexibly)
        # df['DateTime'] = df['DateTime'].astype(str).str.strip().apply(parse_datetime_flexibly)
        df = df[df['DateTime'].dt.minute.isin([0, 20, 40])]

        # Query the latest datetime from the table for this meter
        query = f"SELECT TOP(1) DateTime FROM {table_name} WHERE Meter = '{meter}' ORDER BY DateTime DESC"
        try:
            latest_time_df = pd.read_sql(query, engine)
            if not latest_time_df.empty:
                latest_time = pd.to_datetime(latest_time_df.iloc[0]['DateTime'])
                df = df[df['DateTime'] > latest_time]
                print(f"📌 Filtering rows after {latest_time}")
            else:
                print("ℹ️ No existing records, inserting all data.")
        except Exception as e:
            print(f"❌ SQL query failed: {e}")
            latest_time = None

        df.drop_duplicates(subset=['DateTime'], keep='last', inplace=True)

        # Upload to database
        if not df.empty:
            try:
                df.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"✅ Uploaded {len(df)} new rows to {table_name}")
            except Exception as e:
                print(f"❌ Upload failed: {e}")
        else:
            print("⚠️ No new data to upload.")
