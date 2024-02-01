import os
import io
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from exchangelib import Credentials, Account, DELEGATE, FileAttachment
import openpyxl
import xlrd
from Gas_csv_Formatting import consumption
from fuzzywuzzy import process
import pdfplumber

# Local imports
import openmeteo_requests
import requests_cache
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

EMAIL_ADDRESS = os.environ.get("EMAIL_ADDRESS")
# Constants
PDF_DIR = '/pdfs'
DESIRED_DOMAINS = ['@gmail.com']
PASSWORD = os.environ.get('PASSWORD')
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};'
    f'DATABASE={SQL_DB_NAME};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
)


def connect_to_db(conn_str):
    """Establishes a connection to the database."""
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
    return conn, cursor


def get_all_table_columns(cursor):
    tables_columns = {}
    cursor.execute("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME, "
                   "ORDINAL_POSITION")
    for row in cursor.fetchall():
        table_name, column_name = row
        if table_name not in tables_columns:
            tables_columns[table_name] = []
        tables_columns[table_name].append(column_name)
    return tables_columns


def get_all_table_primary_keys(cursor):
    """
    Retrieves all primary key columns for each table in the database.
    Args:
        cursor: The cursor object from an active database connection.
    Returns:
        A dictionary mapping table names to their primary key columns.
    """
    # Retrieve all base tables
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    table_names = [row.TABLE_NAME for row in cursor.fetchall()]

    # Dictionary to hold table primary key information
    primary_keys = {}

    for table_name in table_names:
        # Query to find primary key columns for the current table
        pk_query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = ? AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(pk_query, [table_name])
        pk_columns = cursor.fetchall()

        # Add table primary key info to the dictionary
        primary_keys[table_name] = [col.COLUMN_NAME for col in pk_columns]
    return primary_keys, table_names


# def get_last_records_by_primary_key(cur, table, table_dict, records_per_nmi=1):
#     """
#     Retrieves the last record(s) based on the primary key(s) from a specified table.
#     Supports tables with single, composite primary keys, and tables without primary keys.
#     """
#     if table not in table_dict or not table_dict[table]:
#         return "Table does not have primary keys or not found in table dictionary!"
#
#     primary_keys = table_dict[table]
#
#     if len(primary_keys) == 1:
#         # Single primary key
#         query = f"""
#         SELECT TOP 1 *
#         FROM [{table}]
#         ORDER BY [{primary_keys[0]}] DESC
#         """
#     elif len(primary_keys) == 2:
#         query = f"""
#         WITH RankedRecords AS (
#             SELECT *, ROW_NUMBER() OVER (PARTITION BY ['NMI'] ORDER BY ['END INTERVAL'] DESC) AS rn
#             FROM [{table}]
#         )
#         SELECT *
#         FROM RankedRecords
#         WHERE rn <= {records_per_nmi}
#         """
#     else:
#         # Other composite primary keys or unsupported configurations
#         return "Table has a composite primary key that is not supported by this function!"
#
#     cur.execute(query)
#     rows = cur.fetchall()
#
#     if rows:
#         column_names = [desc[0] for desc in cur.description]
#         return [dict(zip(column_names, row)) for row in rows]
#     else:
#         return None


def find_header_row(df, expected_columns):
    expected_columns_set = set(expected_columns)
    for i, row in df.iterrows():
        # Extract non-null values from the row and convert them to a set
        row_values_set = set(row.dropna())

        # Optionally convert to the same case for case-insensitive comparison
        # row_values_set = set(value.lower() for value in row.dropna())
        # expected_columns_set = set(column.lower() for column in expected_columns)

        if row_values_set == expected_columns_set:
            print(f"Matching header row found at index: {i}")
            return i

    return None


def process_xlsx_attachments(attachment, table_dict, cursor, conn, all_tables_columns):
    try:
        excel_stream = io.BytesIO(attachment.content)
        workbook = openpyxl.load_workbook(excel_stream, read_only=True)

        # Reset stream to the beginning after getting total rows
        excel_stream.seek(0)

        header_row_index = None
        upload_table = None
        # Compare with all table column names to find header row index
        temp_df = pd.read_excel(excel_stream, header=None, nrows=20)
        for table_name, azure_columns in all_tables_columns.items():
            header_row_index = find_header_row(temp_df, azure_columns)
            if header_row_index is not None:
                upload_table = table_name
                # Break if we find a matching header
                break

        # If header_row_index is None, the header was not found
        if header_row_index is None:
            print(f"Header row not found in the Excel file {attachment.name}")
            return
        batch_df = pd.read_excel(excel_stream, skiprows=header_row_index)
        batch_df.columns = temp_df.iloc[header_row_index]
        # Process the Excel file in chunks
        upload_dataframe_to_azure_sql(batch_df, upload_table, cursor, table_dict, conn)

    except xlrd.biffh.XLRDError as e:
        if str(e) == "Workbook is encrypted":
            print(f"Cannot process encrypted file: {attachment.name}")
        else:
            raise


def process_csv_attachments(attachment, table_dict, cursor, conn):
    file_name_without_extension = attachment.name.rsplit('.', 1)[0]
    csv_header = pd.read_csv(io.BytesIO(attachment.content), nrows=0).columns.tolist()
    if file_name_without_extension in table_dict:
        csv_data = pd.read_csv(io.BytesIO(attachment.content))
        upload_dataframe_to_azure_sql(csv_data, file_name_without_extension, cursor, table_dict, conn)
    elif csv_header == ['NMI', 'CHECKSUM', 'GASDAY', 'READ_TYPE', 'DAILY_HEAT_VALUE',
                        'CONSUMPTION_HR01', 'CONSUMPTION_HR02', 'CONSUMPTION_HR03', 'CONSUMPTION_HR04',
                        'CONSUMPTION_HR05', 'CONSUMPTION_HR06', 'CONSUMPTION_HR07', 'CONSUMPTION_HR08',
                        'CONSUMPTION_HR09', 'CONSUMPTION_HR10', 'CONSUMPTION_HR11', 'CONSUMPTION_HR12',
                        'CONSUMPTION_HR13', 'CONSUMPTION_HR14', 'CONSUMPTION_HR15', 'CONSUMPTION_HR16',
                        'CONSUMPTION_HR17', 'CONSUMPTION_HR18', 'CONSUMPTION_HR19', 'CONSUMPTION_HR20',
                        'CONSUMPTION_HR21', 'CONSUMPTION_HR22', 'CONSUMPTION_HR23', 'CONSUMPTION_HR24',
                        'TOTAL_DAILY_CONSUMPTION', 'PEAK_RATE']:
        csv_data = pd.read_csv(io.BytesIO(attachment.content))
        df_csv = consumption(csv_data)
        upload_dataframe_to_azure_sql(df_csv, 'TestingGas', cursor, table_dict, conn)


def process_pdf_attachments(attachment, table_dict, cursor, conn):
    pass


def process_email_attachments(cursor, attachment_files, table_dict, conn):
    all_tables_columns = get_all_table_columns(cursor)
    """ for table, columns in all_tables_columns.items():
        print(f"Table: {table}, Columns: {columns}") """
    for item in attachment_files:
        # print("Attachments in the email:", len(item.attachments))
        if item.attachments:
            attachments = item.attachments
            for attachment in attachments:

                print(f"Processing attachment: {attachment.name}")

                (filename, extension) = os.path.splitext(attachment.name)
                if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment, FileAttachment):
                    process_xlsx_attachments(attachment, table_dict, cursor, conn, all_tables_columns)

                elif extension == '.csv' and isinstance(attachment, FileAttachment):
                    process_csv_attachments(attachment, table_dict, cursor, conn)

                elif extension == '.pdf' and isinstance(attachment, FileAttachment):
                    process_pdf_attachments(attachment, table_dict, cursor, conn)
                #     try:
                #         pdf_dataframes = process_pdf_tables(io.BytesIO(attachment.content), filename=f"{filename}.pdf")
                #         for pdf_df in pdf_dataframes:
                #             # Clean column names by replacing newlines and extra spaces
                #             pdf_df.columns = [col.replace('\n', ' ').strip().title() for col in pdf_df.columns]
                #             # print(f"Expected columns for {table_name}: {azure_columns}")
                #
                #             # Implement fuzzy matching here
                #             for col in list(pdf_df.columns):  # Use list to avoid iterating over changing dict
                #                 for table_name, azure_columns in all_tables_columns.items():
                #                     best_match = find_best_match(col, azure_columns)
                #                     if best_match:
                #                         pdf_df.rename(columns={col: best_match}, inplace=True)
                #
                #             # Upload the processed data to Azure SQL
                #             # Note: Ensure that the pdf_df now aligns with the table structure of Azure SQL
                #             upload_dataframe_to_azure_sql(pdf_df, table_name, connection_string)
                #             # print("Data uploaded successfully to Azure SQL.")
                #     except Exception as e:
                #         print(f"Error processing PDF tables in file: {attachment.name}. Error: {e}")
                #     item.is_read = True
                else:
                    pass
                # Mark the item as read after processing
                item.is_read = True


def upload_temperature_to_azure_sql(df, table_name, conn_str, batch_size=2000):
    """
    Uploads temperature data to the Azure SQL database.
    Args:
        df: The pandas DataFrame containing temperature data.
        table_name: The name of the destination table in the database.
        conn_str: The database connection string.
        batch_size: The size of the data batches to upload.
    """
    # Your existing upload_temperature_to_azure_sql function code goes here.
    # ...


def upload_dataframe_to_azure_sql(df, table_name, cursor, table_dict, conn):
    print(f"Uploading data to {table_name}...")
    if table_name not in table_dict or not table_dict[table_name]:
        print("Table does not have primary keys or not found in table dictionary.")
        return

    primary_keys = table_dict[table_name]
    print(primary_keys)
    query = ""
    if len(primary_keys) == 1:
        # Handle single primary key
        query = f"SELECT TOP 1 * FROM [{table_name}] ORDER BY [{primary_keys[0]}] DESC"
    elif len(primary_keys) > 1:
        query = f"""
        WITH RankedRecords AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [NMI] ORDER BY [END INTERVAL] DESC) AS rn
            FROM [{table_name}]
        )
        SELECT * FROM RankedRecords WHERE rn = 1
        """
    else:
        print("Unsupported primary key configuration.")
        return

    cursor.execute(query)
    last_records = cursor.fetchall()
    print(last_records)
    column_names = [desc[0] for desc in cursor.description] if cursor.description else []
    last_records_dicts = [dict(zip(column_names, row)) for row in last_records]

    if not last_records_dicts:
        print("No last records found. Uploading all data.")
    else:
        if len(primary_keys) == 1:
            pk_col = primary_keys[0]
            last_value = last_records_dicts[0][pk_col]
            df = df[df[pk_col] > last_value]
        elif len(primary_keys) > 1:
            for record in last_records_dicts:
                df = df[(df['NMI'] == record['NMI']) & (df['END INTERVAL'] > record['END INTERVAL'])]
                print(df)

    if df.empty:
        print("No new rows to insert after filtering with last records.")
        return

    # Define the SQL INSERT query
    df_columns = df.columns.tolist()
    sql_columns = ', '.join([f"[{col}]" for col in df_columns])
    placeholders = ', '.join(['?'] * len(df_columns))
    insert_query = f"INSERT INTO [{table_name}] ({sql_columns}) VALUES ({placeholders})"

    # Convert DataFrame to list of tuples
    data_for_insert = [tuple(x) for x in df.to_numpy()]
    batch_insert(cursor, insert_query, data_for_insert, conn)


def batch_insert(cursor, insert_query, data, conn, batch_size=1000):
    """
    Inserts data into a database in batches. Adjusts batch size and retries if a batch fails.
    """
    for start in range(0, len(data), batch_size):
        batch = data[start:start + batch_size]
        try:
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f'Batch {start//batch_size + 1} has been processed.')
        except pyodbc.Error as e:
            print(f"Error occurred: {e}")
            conn.rollback()
            if batch_size > 50:
                # Retry with smaller batch size if not already at the minimum size
                print(f"Batch insert failed, retrying with smaller batches.")
                new_batch_size = max(batch_size // 2, 50)  # Ensure batch size does not go below 50
                batch_insert(cursor, insert_query, batch, conn, new_batch_size)
            else:
                # If batch size is already at the minimum, attempt to insert row by row
                for row in batch:
                    try:
                        cursor.execute(insert_query, row)
                        conn.commit()
                    except pyodbc.Error as e:
                        print(f"Row insert failed: {e}")
                        conn.rollback()


def main():
    # Set up the email account
    print("Connecting to SQL Database...")
    credentials = Credentials(EMAIL_ADDRESS, PASSWORD)
    account = Account(
        EMAIL_ADDRESS,
        credentials=credentials,
        autodiscover=True,
        access_type=DELEGATE
    )
    conn, cursor = connect_to_db(CONNECTION_STRING)
    print("Connected. Loading the information from Database...")
    table_dict, all_tables = get_all_table_primary_keys(cursor)
    print(table_dict, all_tables)
    # Process unread emails
    all_unread_emails = account.inbox.filter(is_read=False).order_by('-datetime_received')
    filtered_unread_emails = [
        email for email in all_unread_emails
        if email.sender and email.sender.email_address and
        any(email.sender.email_address.strip().lower().endswith(domain) for domain in DESIRED_DOMAINS)
    ]
    process_email_attachments(cursor, filtered_unread_emails, table_dict, conn)
    conn.close()


if __name__ == "__main__":
    main()
