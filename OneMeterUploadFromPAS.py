import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')

# Setup connection engine
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}?' 
                       f'driver=ODBC+Driver+18+for+SQL+Server')

# File path
file_path = 'Apollo/Data/SMB_30.csv'

# Read the CSV file
df = pd.read_csv(file_path)

# Ensure DateTime is parsed as a datetime object
# df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce', format='%YY-%m-%d %H:%M:%S')
# Ensure DateTime column is sorted in ascending order
df = df.sort_values(by='DateTime')

# Filter rows within a specific datetime range
filtered_df = df.loc[(df['DateTime'] >= '2024-10-11 02:00:00') & (df['DateTime'] <= '2024-11-18 08:30:00')].copy()

# Add the Meter column with the fixed value
filtered_df['Meter'] = 'RMT-APL-01-MSB-MSB-01-40002624-DL1'

# Remove duplicates within filtered_df
filtered_df = filtered_df.drop_duplicates(subset=['DateTime', 'Meter'])


# Replace NaT and NaN values
filtered_df['DateTime'] = filtered_df['DateTime'].fillna(pd.Timestamp.min)

# Debug: Print a sample of the filtered data
print(filtered_df.head().to_string())

# Upload the filtered data into the 'Meter_Output_Detail' table
try:
    filtered_df.to_sql('Meter_Output_Detail', con=engine, if_exists='append', index=False)
    print("File uploaded successfully!")
except Exception as e:
    print(f"Error during upload: {e}")
