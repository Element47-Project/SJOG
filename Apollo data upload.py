import os
import io
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from Apollo import upload_apollo

# Load environment variables
load_dotenv()

# SQL settings
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')

engine = create_engine(
    f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server')
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};'
    f'DATABASE={SQL_DB_NAME};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
)


def connect_to_db(conn_str):
    """Establishes a connection to the database."""
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor


def get_latest_datetime_for_meter(cursor, meter):
    """Fetch the latest datetime for a given meter from the Apollo_Units table."""
    query = f"SELECT MAX([DateTime]) FROM Apollo_Units WHERE [Meter] = ?"
    cursor.execute(query, meter)
    result = cursor.fetchone()
    return result[0] if result[0] else None


def process_csv_files(file_path, cursor):
    """Processes CSV files and uploads them to Apollo_5MINS."""
    filename = os.path.splitext(os.path.basename(file_path))[0]
    csv_data = pd.read_csv(file_path)

    # Process the CSV data using upload_apollo function
    processed_data = upload_apollo(csv_data, filename)

    # Fetch the latest datetime for the meter from the database
    latest_datetime = get_latest_datetime_for_meter(cursor, filename)

    if latest_datetime:
        latest_datetime += timedelta(seconds=2)
        processed_data['DateTime'] = pd.to_datetime(processed_data['DateTime'], errors='coerce')
        processed_data = processed_data[processed_data['DateTime'] > latest_datetime]

    if processed_data.empty:
        print(f"No new rows to insert for file: {file_path}")
        delete_file(file_path)
        return

    try:
        print("Start to Upload...")
        processed_data.to_sql('Apollo_Units', engine, if_exists='append', index=False)
        print(f"Insert Successful for file: {file_path}")
        delete_file(file_path)
    except pyodbc.Error as e:
        print(e)


def delete_file(file_path):
    """Delete the file at the specified file path."""
    try:
        os.remove(file_path)
        print(f"Deleted file: {file_path}")
    except OSError as e:
        print(f"Error deleting file: {file_path}")
        print(e)


def main():
    directory_path = 'Apollo\Data'
    print("Connecting to SQL Database...")
    conn, cursor = connect_to_db(CONNECTION_STRING)
    print("Connected. Loading the Information from Database...")

    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            print(f"Processing file: {file_path}")
            extension = os.path.splitext(file)[1].lower()
            if extension == '.csv':
                process_csv_files(file_path, cursor)

    cursor.close()
    conn.close()
    print("All files have been processed and uploaded.")


if __name__ == "__main__":
    main()
