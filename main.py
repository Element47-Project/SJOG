import os
from dotenv import load_dotenv
from exchangelib import Credentials,Account,DELEGATE,FileAttachment
from exchangelib import Credentials,Account,DELEGATE,FileAttachment
import pandas as pd
import io
import xlrd
import camelot
import time
import pyodbc
import openpyxl


#print("Script started")
load_dotenv()
#email account
password =  os.environ.get('password')
credentials = Credentials(
    'element47testing@outlook.com',
    password
)
account = Account(
    'element47testing@outlook.com',
    credentials=credentials,
    autodiscover=True,
    access_type=DELEGATE)

#azure account
sql_server = os.environ.get('AZURE_SQL_SERVER')
sql_db_name = os.environ.get('AZURE_SQL_DB_NAME')
sql_username = os.environ.get('AZURE_SQL_USERNAME')
sql_password = os.environ.get('AZURE_SQL_PASSWORD')
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={sql_server};DATABASE={sql_db_name};UID={sql_username};PWD={sql_password}'


        
#get the path for this script to save pdfs
script_path = __file__
# To get the absolute path to the script file, use abspath
absolute_script_path = os.path.abspath(__file__)
# To get the directory containing the script, use dirname
script_dir = os.path.dirname(absolute_script_path)
pdf_dir = script_dir + '/pdfs'

def get_total_rows(excel_stream):
    workbook = openpyxl.load_workbook(excel_stream, read_only=True)
    first_sheet = workbook.worksheets[0]
    return first_sheet.max_row

#check the extension of the attachment file
#read the attachments
#handling error for encrypted files
def process_email_attachments(attachment_files):
    #print("Processing email attachments:", len(attachment_files))
    all_tables_columns = get_all_table_columns(connection_string)
    chunk_size = 2000
    """ for table, columns in all_tables_columns.items():
        print(f"Table: {table}, Columns: {columns}") """
    for item in attachment_files:
        #print("Attachments in the email:", len(item.attachments))
        if (item.attachments):
            attachements = item.attachments
            for attachment in attachements:
                #print(f"Processing attachment: {attachment.name}")
                (filename,extension) = os.path.splitext(attachment.name)
                if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment, FileAttachment):   # Ensure it's a FileAttachment type
                    try:
                            # Convert bytes to a DataFrame
                        excel_stream = io.BytesIO(attachment.content)
                        
                        # Read the first 20 rows to find header
                        temp_df = pd.read_excel(excel_stream, header=None, nrows=20)
                        header_row_index = None
                        # Compare with all table column names
                        for table_name, azure_columns in all_tables_columns.items():
                            header_row_index = find_header_row(temp_df, azure_columns)
                            #print(f"Header row index for {table_name}: {header_row_index}")

                            if header_row_index is not None:
                                # Read the full data starting from the header row
                                #excel_data = pd.read_excel(excel_stream, header=header_row_index)
                                # Upload to Azure SQL
                                #upload_dataframe_to_azure_sql(excel_data, table_name, connection_string)
                               break

                        if header_row_index is None:
                             #print(f"No header found in file: {attachment.name}")
                             continue 
                         # Get the total number of rows in the Excel file
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
                          
                            batch_df = pd.read_excel(excel_stream, skiprows=start_row, nrows=end_row - start_row + 1, header=None)

                          #print("DataFrame columns:", batch_df.columns)
                          
                          upload_dataframe_to_azure_sql(batch_df, table_name, connection_string)
                          start_row += chunk_size
                          print('THE DATA IS SUCCESSFULLY UPLOADED.')
                         

                        item.is_read = True
                    except xlrd.biffh.XLRDError as e:
                        if str(e) == "Workbook is encrypted":
                            print(f"Cannot process encrypted file: {attachment.name}")
                        else:
                            raise e
                
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
                        item.is_read = True
                else:
                    pass
                # Mark the item as read after processing
                item.is_read = True
                


#filter the senders
def is_desired_domain(email_address, domain_list):
    return any(email_address.strip().lower().endswith(domain) for domain in domain_list)
# Define the domains you want to filter by
desired_domains = ['@gmail.com']

#fetch the tables from pdf attachments
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
        cursor.execute("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME, ORDINAL_POSITION")
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
        except pyodbc.Error as e:
            if batch_size > 1:
                # Retry with smaller batch size
                print(f"Batch insert failed, retrying with smaller batches: {e}")
                batch_insert(cursor, insert_query, batch, batch_size // 2)
            else:
                # Handle individual row error
                print(f"Error inserting row: {e}")


def upload_dataframe_to_azure_sql(df, table_name, connection_string):
    print(f"Uploading data to {table_name}")
    #insert_query = ""  # Initialize insert_query to an empty string
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
        
        # Prepare data for batch insert
        data_for_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        try:
                  # Perform batch insert
            batch_insert(cursor, insert_query, data_for_insert, len(data_for_insert))
            conn.commit()
            #print('Batch data successfully uploaded.')
        except pyodbc.Error as e:
            print(f"Error during batch insert: {e}")
            conn.rollback()
                # Handle primary key conflict
                #print(f"Skipping row due to primary key conflict: {e}")
                #continue 
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


