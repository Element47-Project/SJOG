import os
from dotenv import load_dotenv
from exchangelib import Credentials,Account,DELEGATE,FileAttachment
import pandas as pd
import io
import xlrd
import camelot
import time
import pyodbc
import pickle
#import datetime
import pytz
import ntplib
from time import ctime

import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime as dt


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
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={sql_server};DATABASE={sql_db_name};UID={sql_username};PWD={sql_password}'

# get the path for this script to save pdfs
script_path = __file__
# To get the absolute path to the script file, use abspath
absolute_script_path = os.path.abspath(__file__)
# To get the directory containing the script, use dirname
script_dir = os.path.dirname(absolute_script_path)
pdf_dir = script_dir + '/pdfs'


# check the extension of the attachment file
# read the attachments
# handling error for encrypted files
def process_email_attachments(attachment_files):
    all_tables_columns = get_all_table_columns(connection_string)
    """ for table, columns in all_tables_columns.items():
        print(f"Table: {table}, Columns: {columns}") """
    for item in attachment_files:
        if (item.attachments):
            attachements = item.attachments
            for attachment in attachements:
                (filename, extension) = os.path.splitext(attachment.name)
                if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment,
                                                                                FileAttachment):  # Ensure it's a FileAttachment type
                    try:
                        # Convert bytes to a DataFrame
                        excel_stream = io.BytesIO(attachment.content)
                        # Read the first 20 rows to find header
                        temp_df = pd.read_excel(excel_stream, header=None, nrows=20)
                        # Compare with all table column names
                        for table_name, azure_columns in all_tables_columns.items():
                            header_row_index = find_header_row(temp_df, azure_columns)
                            if header_row_index is not None:
                                # Read the full data starting from the header row
                                excel_data = pd.read_excel(excel_stream, header=header_row_index)
                                # Upload to Azure SQL
                                upload_dataframe_to_azure_sql(excel_data, table_name, connection_string)
                                break
                        item.is_read = True
                    except xlrd.biffh.XLRDError as e:
                        if str(e) == "Workbook is encrypted":
                            print(f"Cannot process encrypted file: {attachment.name}")
                        else:
                            raise e
                        return False

                elif extension == '.csv' and isinstance(attachment, FileAttachment):
                    csv_data = pd.read_csv(io.BytesIO(attachment.content), sheet_name=None)
                    for c in csv_data.items():
                        print(c)
                        # You can add code here to upload to Azure
                        item.is_read = True

                elif extension == '.pdf':
                    # Handle PDF files
                    try:
                        # Extract filename without extension to use as a basis for the PDF file
                        pdf_filename = f"{filename}.pdf"
                        tables = process_pdf_tables(io.BytesIO(attachment.content), filename=pdf_filename)
                        for i, table in enumerate(tables):
                            print(f"Table {i} from {pdf_filename}:")
                            print(table)
                            # You can add code here to upload to Azure or handle the DataFrame as needed
                    except Exception as e:
                        print(f"Error processing PDF tables in file: {pdf_filename}. Error: {e}")
                        return False
                    item.is_read = True
                else:
                    pass
                # Mark the item as read after processing
                item.is_read = True
    return True

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
    # Now read the PDF with Camelot
    tables = camelot.read_pdf(file_path, flavor='stream', pages='all')
    # Convert tables to DataFrames
    dataframes = [table.df for table in tables]
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
            return i
    return None


def get_all_table_columns(connection_string):
    tables_columns = {}
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME, ORDINAL_POSITION")
        for row in cursor.fetchall():
            table_name, column_name = row
            if table_name not in tables_columns:
                tables_columns[table_name] = []
            tables_columns[table_name].append(column_name)
    return tables_columns


def upload_dataframe_to_azure_sql(df, table_name, connection_string):
    # Connect to the Azure SQL database
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        # Retrieve DataFrame column names
        df_columns = df.columns.tolist()
        # Construct SQL column names part for INSERT statement
        sql_columns = ', '.join([f'[{col}]' for col in df_columns])
        # Construct placeholders part for INSERT statement
        placeholders = ', '.join(['?'] * len(df_columns))
        # SQL INSERT statement
        insert_query = f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})"
        # Iterate over DataFrame rows as tuples
        for row in df.itertuples(index=False, name=None):
            # Clean the data - convert NaN to None
            cleaned_data = [None if pd.isnull(item) else item for item in row]
            # Execute the query with cleaned data
            #cursor.execute(insert_query, cleaned_data)
            try:
                # Execute the query with cleaned data
                cursor.execute(insert_query, cleaned_data)
            except pyodbc.IntegrityError as e:
                # Handle primary key conflict
                print(f"Skipping row due to primary key conflict: {e}")
                continue  # Skip this row and continue with the next row
        print('THE DATA IS BEEN PROCESSED.')

        # Commit the transaction
        conn.commit()
    #print('THE DATA IS BEEN PROCESSED.')


# fetch unread files
all_unread_emails = account.inbox.filter(is_read=False).order_by('-datetime_received')
# filter out the emails from the specific domains
filtered_unread_emails = [email for email in all_unread_emails if
                          is_desired_domain(email.sender.email_address, desired_domains)]
#process_email_attachments(filtered_unread_emails)

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


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Define the start and end dates for the data fetching
start_date = "2023-12-01"
end_date = dt.now().strftime("%Y-%m-%d")  # Gets the current date

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
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s"),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}
hourly_data["temperature_2m"] = hourly_temperature_2m
""" hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe) """

def upload_temperature_to_azure_sql(df, table_name, connection_string):
    # Connect to the Azure SQL database
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()

        # Modify this part based on the column names of your Azure table
        df_columns = ['Date_time', 'Temperature']  # Update with actual column names
        sql_columns = ', '.join([f'[{col}]' for col in df_columns])
        placeholders = ', '.join(['?'] * len(df_columns))
        insert_query = f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})"

        for row in df.itertuples(index=False, name=None):
            cleaned_data = [None if pd.isnull(item) else item for item in row]
            try:
                cursor.execute(insert_query, cleaned_data)
            except pyodbc.IntegrityError as e:
                # Handle primary key conflict
                print(f"Skipping row due to primary key conflict: {e}")
                continue  # Skip this row and continue with the next row

        conn.commit()


df_weather = pd.DataFrame(data=hourly_data)
table_name = 'Temperature_hourly'
# Upload the data to Azure
upload_temperature_to_azure_sql(df_weather, table_name, connection_string)

