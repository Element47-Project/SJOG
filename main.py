import os
from dotenv import load_dotenv
from exchangelib import Credentials, Account, DELEGATE, FileAttachment
import pandas as pd
import io
import xlrd
import time
import pyodbc
# import pickle
# import datetime
# import pytz
# import ntplib
# from time import ctime

import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime as dt, timedelta

import openpyxl
import pdfplumber
from fuzzywuzzy import process
# from datetime import datetime
from Gas_csv_Formatting import consumption

# print("Script started")
load_dotenv()
# email account
password = os.environ.get('password')
credentials = Credentials(
    'element47testing@outlook.com',
    password
)
account = Account(
    'element47testing@outlook.com',
    credentials=credentials,
    autodiscover=True,
    access_type=DELEGATE)

# azure account
sql_server = os.environ.get('AZURE_SQL_SERVER')
sql_db_name = os.environ.get('AZURE_SQL_DB_NAME')
sql_username = os.environ.get('AZURE_SQL_USERNAME')
sql_password = os.environ.get('AZURE_SQL_PASSWORD')
connection_string = (f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={sql_server};'
                     f'DATABASE={sql_db_name};UID={sql_username};PWD={sql_password}')

# get the path for this script to save pdfs
script_path = __file__
# To get the absolute path to the script file, use abspath
absolute_script_path = os.path.abspath(__file__)
# To get the directory containing the script, use dir name
script_dir = os.path.dirname(absolute_script_path)
pdf_dir = script_dir + '/pdfs'


def get_total_rows(excel_stream):
    workbook = openpyxl.load_workbook(excel_stream, read_only=True)
    first_sheet = workbook.worksheets[0]
    return first_sheet.max_row


def find_best_match(header, choices, threshold=95):
    best_match, score = process.extractOne(header, choices)
    return best_match if score >= threshold else None


# check the extension of the attachment file
# read the attachments
# handling error for encrypted files


def get_all_table_names(conn_str):
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        return [row.TABLE_NAME for row in cursor.fetchall()]


def process_email_attachments(attachment_files):
    # print("Processing email attachments:", len(attachment_files))
    all_tables_columns = get_all_table_columns(connection_string)
    all_tables_names = get_all_table_names(connection_string)
    chunk_size = 2000
    """ for table, columns in all_tables_columns.items():
        print(f"Table: {table}, Columns: {columns}") """
    for item in attachment_files:
        # print("Attachments in the email:", len(item.attachments))
        if item.attachments:
            attachments = item.attachments
            for attachment in attachments:

                print(f"Processing attachment: {attachment.name}")

                (filename, extension) = os.path.splitext(attachment.name)
                if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment,
                                                                                FileAttachment):
                    # Ensure it's a FileAttachment type
                    try:
                        # Convert bytes to a DataFrame
                        excel_stream = io.BytesIO(attachment.content)

                        # Read the first 20 rows to find header
                        temp_df = pd.read_excel(excel_stream, header=None, nrows=20)
                        header_row_index = None
                        # Compare with all table column names
                        for table_name, azure_columns in all_tables_columns.items():
                            header_row_index = find_header_row(temp_df, azure_columns)
                            # print(f"Header row index for {table_name}: {header_row_index}")
                            if header_row_index is not None:
                                break

                        total_rows = get_total_rows(excel_stream)

                        # Ensure the stream is reset to the beginning for reading
                        excel_stream.seek(0)

                        # Read the file in batches, starting after the header row
                        start_row = header_row_index + 1
                        while start_row < total_rows:
                            end_row = min(start_row + chunk_size - 1, total_rows)

                            if header_row_index is not None:
                                # If a header row index is found, use it to set column names in the DataFrame
                                skip = list(range(0, header_row_index)) + list(range(header_row_index + 1, start_row))
                                batch_df = pd.read_excel(excel_stream, skiprows=skip, nrows=end_row - start_row + 1)
                            else:
                                # If no header row index is found, proceed without headers

                                batch_df = pd.read_excel(excel_stream, skiprows=start_row,
                                                         nrows=end_row - start_row + 1, header=None)

                            # print("DataFrame columns:", batch_df.columns)

                            upload_dataframe_to_azure_sql(batch_df, table_name, connection_string)
                            start_row += chunk_size

                        item.is_read = True
                    except xlrd.biffh.XLRDError as e:
                        if str(e) == "Workbook is encrypted":
                            print(f"Cannot process encrypted file: {attachment.name}")
                        else:
                            raise e

                elif extension == '.csv' and isinstance(attachment, FileAttachment):
                    file_name_without_extension = attachment.name.rsplit('.', 1)[0]
                    csv_header = pd.read_csv(io.BytesIO(attachment.content), nrows=0).columns.tolist()
                    if file_name_without_extension in all_tables_names:
                        csv_data = pd.read_csv(io.BytesIO(attachment.content))
                        # The first data frame is from 01/11/2023
                        upload_dataframe_to_azure_sql(csv_data, file_name_without_extension, connection_string)
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
                        upload_dataframe_to_azure_sql(df_csv, 'TestingGas', connection_string)
                    item.is_read = True

                elif extension == '.pdf' and isinstance(attachment, FileAttachment):

                    try:
                        pdf_dataframes = process_pdf_tables(io.BytesIO(attachment.content), filename=f"{filename}.pdf")
                        for pdf_df in pdf_dataframes:
                            # Clean column names by replacing newlines and extra spaces
                            pdf_df.columns = [col.replace('\n', ' ').strip().title() for col in pdf_df.columns]
                            # print(f"Expected columns for {table_name}: {azure_columns}")

                            # Implement fuzzy matching here
                            for col in list(pdf_df.columns):  # Use list to avoid iterating over changing dict
                                for table_name, azure_columns in all_tables_columns.items():
                                    best_match = find_best_match(col, azure_columns)
                                    if best_match:
                                        pdf_df.rename(columns={col: best_match}, inplace=True)

                            # Upload the processed data to Azure SQL
                            # Note: Ensure that the pdf_df now aligns with the table structure of Azure SQL
                            upload_dataframe_to_azure_sql(pdf_df, table_name, connection_string)
                            # print("Data uploaded successfully to Azure SQL.")
                    except Exception as e:
                        print(f"Error processing PDF tables in file: {attachment.name}. Error: {e}")
                    item.is_read = True
                else:
                    pass
                # Mark the item as read after processing
                item.is_read = True


# filter the senders
def is_desired_domain(email_address, domain_list):
    return any(email_address.strip().lower().endswith(domain) for domain in domain_list)


# Define the domains you want to filter by
desired_domains = ['@gmail.com']


# fetch the tables from pdf attachments
def process_pdf_tables(attachment_content, directory=pdf_dir, filename=None):
    # Ensure the directory exists
    if not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    # If no filename is given, generate one with a timestamp
    if filename is None:
        filename = f"pdf_table_{int(time.time())}.pdf"
    file_path = os.path.join(directory, filename)
    # Save the PDF to the specified directory
    with open(file_path, "wb") as f:
        f.write(attachment_content.read())

    dataframes = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            # Extract table from the page
            table = page.extract_table()
            if table:
                # Convert table (list of lists) to DataFrame
                df = pd.DataFrame(table[1:], columns=table[0])
                df.columns = [col.replace('\n', ' ').strip().title() for col in df.columns]  # Clean column names
                dataframes.append(df)

    print(f"Number of tables extracted: {len(dataframes)}")

    for i, df in enumerate(dataframes):
        print(f"Preview of table {i}:")
        print(df.head())  # Print first few rows of each table

    # Optionally, you can delete the PDF file after processing
    # os.remove(file_path)
    return dataframes


# Function to find header row in Excel file
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


def get_all_table_columns(conn_string):
    tables_columns = {}
    with pyodbc.connect(conn_string) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME, ORDINAL_POSITION")
        for row in cursor.fetchall():
            table_name, column_name = row
            if table_name not in tables_columns:
                tables_columns[table_name] = []
            tables_columns[table_name].append(column_name)
    return tables_columns


def batch_insert(cursor, insert_query, data, batch_size):
    for start in range(0, len(data), batch_size):
        end = start + batch_size
        batch = data[start:end]

        # Clean the data in the batch (convert NaN to None)
        cleaned_batch = [tuple(None if pd.isnull(item) else item for item in row) for row in batch]

        try:
            cursor.executemany(insert_query, cleaned_batch)
            cursor.commit()
            print('Batch has been processed.')
        except pyodbc.Error:
            if batch_size > 50:
                # Retry with smaller batch size
                print(f"Batch insert failed, retrying with smaller batches.")
                batch_insert(cursor, insert_query, batch, batch_size // 2)
            else:
                for row in cleaned_batch:
                    try:
                        cursor.execute(insert_query, row)
                        cursor.commit()
                    except pyodbc.Error as e:
                        print(f"Duplicate row, skipping insertion:{e}")
                        continue


def upload_dataframe_to_azure_sql(df, table_name, conn_string):
    print(f"Uploading data to {table_name}")
    # insert_query = ""  # Initialize insert_query to an empty string
    # Convert date columns to the format expected by SQL Server
    date_columns = ['Read Date']  # Add all date column names here
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
    # Connect to the Azure SQL database
    with pyodbc.connect(conn_string) as conn:
        cursor = conn.cursor()
        # Retrieve DataFrame column names
        df_columns = df.columns.tolist()
        # Construct SQL column names part for INSERT statement
        sql_columns = ', '.join([f'[{col}]' for col in df_columns])
        # Construct placeholders part for INSERT statement
        placeholders = ', '.join(['?'] * len(df_columns))

        # SQL INSERT statement
        insert_query = f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})"

        # Prepare data for batch insert
        data_for_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

        try:
            # Perform batch insert
            batch_insert(cursor, insert_query, data_for_insert, len(data_for_insert))
            # print('Batch data successfully uploaded.')
        except pyodbc.Error as e:
            print(f"Error during batch insert: {e}")
            conn.rollback()
        # Commit the transaction
        # conn.commit()


# fetch unread files
all_unread_emails = account.inbox.filter(is_read=False).order_by('-datetime_received')
# filter out the emails from the specific domains
filtered_unread_emails = [email for email in all_unread_emails if
                          is_desired_domain(email.sender.email_address, desired_domains)]
process_email_attachments(filtered_unread_emails)

# fetch read files
all_read_emails = account.inbox.filter(is_read=True).order_by('-datetime_received')
# filter out the emails from the specific domains
filtered_read_emails = [email for email in all_read_emails if
                        is_desired_domain(email.sender.email_address, desired_domains)]
# process_email_attachments(filtered_read_emails)


# filename = "time_list.pkl"
#
# def load_time_list():
#     if os.path.exists(filename):
#         with open(filename, 'rb') as file:
#             return pickle.load(file)
#     else:
#         return []
#
#
# def save_time_list(time_list):
#     with open(filename, 'wb') as file:
#         pickle.dump(time_list, file)
#
# def get_time(server="pool.ntp.org"):
#     client = ntplib.NTPClient()
#     response = client.request(server)
#     utc_time = datetime.datetime.utcfromtimestamp(response.tx_time)  # Create a UTC datetime object
#     utc_time = utc_time.replace(tzinfo=pytz.utc)  # Make it timezone-aware
#     return utc_time
#
#
# time_list = load_time_list()
# start_time = time_list[-1] if time_list else None
# email_filter = {}
# if start_time is not None:
#     email_filter['datetime_received__gte'] = start_time
# emails = account.inbox.filter(**email_filter).order_by('-datetime_received')
# # filter out the emails from the specific domains
# emails = [email for email in emails if
#           is_desired_domain(email.sender.email_address, desired_domains)]
# # process_email_attachments(emails)
# if not emails:
#     print("There are no new attachments")
# else:
#     process_email_attachments(emails)
#     process_status = process_email_attachments(emails)
#
#     # If processing is successful, proceed to get and save the current time
#     if process_status:
#         current_time = get_time()  # Get current time in UTC
#         time_list.append(current_time)
#         save_time_list(time_list)
#         # print(time_list)
#     else:
#         print("Error occurred during email attachment processing")


# save the file on Box
# upload the file on Azure
# Daily temperature data


# Set up the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def fetch_latest_date_from_azure(conn_string, table_name):
    with pyodbc.connect(conn_string) as conn:
        cursor = conn.cursor()
        query = f"SELECT MAX(Date_Time) FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result[0] is not None else None


latest_date = fetch_latest_date_from_azure(connection_string, 'Temperature_hourly')
# Define the start and end dates for the data fetching
start_date = (latest_date - timedelta(days=1 / 3)).strftime("%Y-%m-%d") if latest_date else "2020-01-01"
end_date = (dt.now() - timedelta(days=3)).strftime("%Y-%m-%d")

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": -31.9522,
    "longitude": 115.8614,
    "start_date": start_date,
    "end_date": end_date,
    "hourly": "temperature_2m",
    "timezone": "auto"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.

# The timezone has been modified in the Power BI by adding extra column(Real time) consisting with Perth local time.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
    start=pd.to_datetime(hourly.Time(), unit="s"),
    end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
    freq=pd.Timedelta(seconds=hourly.Interval()),
    inclusive="left"
), "temperature_2m": hourly_temperature_2m}
""" hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe) """


def upload_temperature_to_azure_sql(df, table_name, conn_string, batch_size=2000):
    print(f"Uploading data to {table_name}")
    # insert_query = ""  # Initialize insert_query to an empty string
    # Connect to the Azure SQL database
    with pyodbc.connect(conn_string) as conn:
        cursor = conn.cursor()
        # Retrieve DataFrame column names
        df_columns = df.columns.tolist()
        # Construct SQL column names part for INSERT statement
        sql_columns = ', '.join([f'[{col}]' for col in df_columns])
        # Construct placeholders part for INSERT statement
        placeholders = ', '.join(['?'] * len(df_columns))

        # SQL INSERT statement
        insert_query = f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})"

        # Prepare data for batch insert
        data_for_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

        try:
            # Perform batch insert
            batch_insert(cursor, insert_query, data_for_insert, batch_size)
            conn.commit()
            # print('Batch data successfully uploaded.')
        except pyodbc.Error as e:
            print(f"Error during batch insert: {e}")
            conn.rollback()


def process_temp(df, table_name, conn_string, chunk_size=2000):
    total_rows = len(df)
    start_row = 0
    while start_row < total_rows:
        end_row = min(start_row + chunk_size, total_rows)
        batch_df = df.iloc[start_row:end_row]
        upload_dataframe_to_azure_sql(batch_df, table_name, conn_string)
        start_row += chunk_size
        print('Temperature batch data has been uploaded.')


df_weather = pd.DataFrame(data=hourly_data)
df_weather.columns = ['Date_Time', 'Temperature']
# Upload temp
# process_temp(df_weather,'Temperature_hourly',connection_string)

