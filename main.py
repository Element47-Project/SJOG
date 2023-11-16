import os
from dotenv import load_dotenv
from exchangelib import Credentials,Account,DELEGATE,FileAttachment
import pandas as pd
import io
import xlrd
#from sqlalchemy import create_engine
import camelot
import time
import pyodbc


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

# Establish a connection to the Azure SQL database
""" def connect_and_test_azure_sql():
    try:
        connection = pyodbc.connect(connection_string, timeout=60)
        print("Successfully connected to Azure SQL database.")
        
        # Perform a test query to ensure the connection is valid
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone()[0] == 1:
                print("Test query executed successfully. Connection is valid.")
            else:
                print("Test query did not return expected result. Check connection details.")
        return connection
    except Exception as e:
        print(f"An error occurred: {e}")
        return None """

# Check the connection to Azure SQL Database
""" azure_connection = connect_and_test_azure_sql()
if azure_connection is not None:
    # Connection is successful
    azure_connection.close()  # Close the connection if it's no longer needed here
else:
    # Handle connection failure
    print("Failed to connect to Azure SQL Database.") """

        
#get the path for this script to save pdfs
script_path = __file__
# To get the absolute path to the script file, use abspath
absolute_script_path = os.path.abspath(__file__)
# To get the directory containing the script, use dirname
script_dir = os.path.dirname(absolute_script_path)
pdf_dir = script_dir + '/pdfs'


#check the extension of the attachment file
#read the attachments
#handling error for encrypted files
def process_email_attachments(attachment_files):
    for item in attachment_files:
        if (item.attachments):
            attachements = item.attachments
            for attachment in attachements:
                (filename,extension) = os.path.splitext(attachment.name)
                if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment, FileAttachment):   # Ensure it's a FileAttachment type
                    # Convert bytes from the attachment directly to a pandas dataframe
                    #print(attachment.name)
                    try:
                        #excel_data = pd.read_excel(io.BytesIO(attachment.content), sheet_name=None)
                        """ for e in excel_data.items():
                            print(e) """
                        """ for sheet_name, df in excel_data.items():
                            print(f"Sheet name: {sheet_name}")
                            print(df) """
                            # You can add code here to upload to Azure
                            # Convert bytes to a DataFrame
                        excel_stream = io.BytesIO(attachment.content)
                        # Read the first 20 rows to find header
                        temp_df = pd.read_excel(excel_stream, header=None, nrows=20)
                        #print(temp_df)
                        azure_columns = get_azure_table_columns(connection_string,'TestingPerthEle')
                        #print(azure_columns)
                        header_row_index = find_header_row(temp_df, azure_columns)
                        if header_row_index is not None:
                            # Read the full data starting from the header row
                            excel_data = pd.read_excel(excel_stream, header=header_row_index)
                            #print(excel_data)  
                            # Assuming 'excel_data' is the DataFrame you want to upload
                            upload_dataframe_to_azure_sql(excel_data, 'TestingPerthEle', connection_string)

                        else:
                            print(f"No matching header row found in {filename}")



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
                            #base_filename = os.path.splitext(attachment.name)[0]

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
                

    # Example call to upload the dataframe
    #upload_dataframe_to_azure_sql(df, "YourAzureTableName")
    #item.is_read = True

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
def find_header_row(df, expected_columns,threshold=0.8):
    required_matches = int(len(expected_columns) * threshold)
    for i, row in df.iterrows():
        matched_columns = sum([col in row.values for col in expected_columns])
        if matched_columns >= required_matches:
            return i
    return None

def get_azure_table_columns(connection_string, azure_table_name):
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{azure_table_name}' ORDER BY ORDINAL_POSITION"
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
    return columns


def upload_dataframe_to_azure_sql(df, table_name, connection_string):
    # Connect to the Azure SQL database
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        
        # Iterate over DataFrame rows as tuples
        for row in df.itertuples(index=False, name=None):
            # Clean the data - convert NaN to None
            cleaned_data = [None if pd.isnull(item) else item for item in row]
            
            # SQL INSERT statement
            insert_query = f"""INSERT INTO {table_name} ([ACCOUNT NUMBER], [ACNAME], [NMI], [METER], [SITE ADDRESS], [END INTERVAL], [PERIOD], [E-STREAM(KWH)], [Q-STREAM(KVARH)], [T-STREAM(KVAH)], [SOLAR KWH])
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            
            # Execute the query with cleaned data
            cursor.execute(insert_query, cleaned_data)

        # Commit the transaction
        conn.commit()





# fetch unread files
all_unread_emails = account.inbox.filter(is_read=False).order_by('-datetime_received')
# filter out the emails from the specific domains
filtered_unread_emails = [email for email in all_unread_emails if is_desired_domain(email.sender.email_address, desired_domains)]
process_email_attachments(filtered_unread_emails) 


# fetch read files
all_read_emails = account.inbox.filter(is_read=True).order_by('-datetime_received')
# filter out the emails from the specific domains
filtered_read_emails = [email for email in all_read_emails if is_desired_domain(email.sender.email_address, desired_domains)]
#process_email_attachments(filtered_read_emails)



    
# save the file on Box
# upload the file on Azure
# Daily temperature data 



