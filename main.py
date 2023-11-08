import os
from dotenv import load_dotenv
from exchangelib import Credentials,Account,DELEGATE,FileAttachment,Mailbox
import pandas as pd
import io
import xlrd
#from sqlalchemy import create_engine
#import fitz
import camelot
import time

load_dotenv()
#email account
password =  os.environ.get('password')
credentials = Credentials(
    'element47testing@outlook.com',
    password
)
#azure account
""" sql_server = os.environ.get('AZURE_SQL_SERVER')
sql_db_name = os.environ.get('AZURE_SQL_DB_NAME')
sql_username = os.environ.get('AZURE_SQL_USERNAME')
sql_password = os.environ.get('AZURE_SQL_PASSWORD') """


account = Account(
    'element47testing@outlook.com',
    credentials=credentials,
    autodiscover=True,
    access_type=DELEGATE)

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
                        excel_data = pd.read_excel(io.BytesIO(attachment.content), sheet_name=None)
                        for e in excel_data.items():
                            print(e)
                            # You can add code here to upload to Azure
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


#filter the senders
def is_desired_domain(email_address, domain_list):
    return any(email_address.strip().lower().endswith(domain) for domain in domain_list)
# Define the domains you want to filter by
desired_domains = ['@gmail.com']


def process_pdf_tables(attachment_content, directory="/Users/duanyangdu/Documents/Element47/untitled folder/pdfs", filename=None):
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

# fetch unread files
""" unread_files = account.inbox.filter(is_read=False, sender=sender_email).order_by('-datetime_received')
process_email_attachments(unread_files) """
all_unread_emails = account.inbox.filter(is_read=False).order_by('-datetime_received')
# filter out the emails from the specific domains
filtered_unread_emails = [email for email in all_unread_emails if is_desired_domain(email.sender.email_address, desired_domains)]
process_email_attachments(filtered_unread_emails)


# fetch read files
""" read_files = account.inbox.filter(is_read=True, sender=sender_email).order_by('-datetime_received')
process_email_attachments(read_files) """

# save the file on Box
# upload the file on Azure