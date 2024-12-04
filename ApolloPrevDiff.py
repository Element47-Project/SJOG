import pyodbc
import pandas as pd
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Logging setup
log_file_path = r"C:\Users\Shane\Desktop\Apllo\apollo_upload.log"
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')

# Setup connection engine and connection string
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};'
    f'DATABASE={SQL_DB_NAME};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
)


# Function to process and upload all interval data
def upload_all_interval_data():
    conn = None
    try:
        # Step 1: Establish database connection
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()

        conn.autocommit = False

        query = """
            SELECT [DateTime], [Meter], [kWh_IMP], [kWh_EXP], 
                   [kvarh_IMP], [kvarh_EXP], [kVAh], [V], [I], [kW], [I_THD]
            FROM [dbo].[ApolloTesting]
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logging.warning("No data found in the interval table.")
            return

        # Step 3: Delete all data from the ApolloTesting table (do not commit yet)
        delete_query = "DELETE FROM [dbo].[ApolloTesting]"
        cursor.execute(delete_query)
        logging.info("Deleted all existing data from ApolloTesting (pending commit).")

        # Step 4: Sort the data by Meter and DateTime
        df = df.sort_values(by=['Meter', 'DateTime'])

        # Step 5: Calculate the previous values and differences
        # Get the previous values for each Meter (shift by one row, grouped by Meter)
        df['Prev_kWh_IMP'] = df.groupby('Meter')['kWh_IMP'].shift(1)
        df['Prev_kvarh_IMP'] = df.groupby('Meter')['kvarh_IMP'].shift(1)
        df['Prev_kvarh_EXP'] = df.groupby('Meter')['kvarh_EXP'].shift(1)
        df['Prev_kVAh'] = df.groupby('Meter')['kVAh'].shift(1)

        # Calculate the differences
        df['KWH_IMP_Diff'] = df['kWh_IMP'] - df['Prev_kWh_IMP']
        df['kvarh_IMP_Diff'] = df['kvarh_IMP'] - df['Prev_kvarh_IMP']
        df['kvarh_EXP_Diff'] = df['kvarh_EXP'] - df['Prev_kvarh_EXP']
        df['kVAh_Diff'] = df['kVAh'] - df['Prev_kVAh']

        # Fill NaN values (the first record for each meter) with 0
        df.fillna(0, inplace=True)

        # Step 6: Upload the processed data back into ApolloTesting table
        df_to_upload = df[['DateTime', 'Meter', 'kWh_IMP', 'Prev_kWh_IMP', 'KWH_IMP_Diff',
                           'kWh_EXP', 'kvarh_IMP', 'Prev_kvarh_IMP', 'kvarh_IMP_Diff',
                           'kvarh_EXP', 'Prev_kvarh_EXP', 'kvarh_EXP_Diff', 'kVAh',
                           'Prev_kVAh', 'kVAh_Diff', 'V', 'I', 'kW', 'I_THD']]

        # Use to_sql to upload data
        df_to_upload.to_sql('ApolloTesting', con=engine, if_exists='append', index=False)
        logging.info("Processed data uploaded successfully to ApolloTesting.")

        # Step 7: Commit the transaction if everything succeeds
        conn.commit()
        logging.info("Transaction committed successfully.")

    except Exception as e:
        # Rollback if any error occurs
        if conn:
            conn.rollback()
        logging.error(f"Error while processing interval data: {e}")

    finally:
        if conn:
            conn.close()


# Example usage:
if __name__ == "__main__":
    upload_all_interval_data()
