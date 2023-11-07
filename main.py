import os
from dotenv import load_dotenv
from exchangelib import Credentials,Account,DELEGATE,FileAttachment
import pandas as pd
import io
import xlrd

load_dotenv()
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

# check if the email is unread
# read content from .xlsx attachment
# save the file on Box
# upload the file on Azure

#unread files
unread_files = account.inbox.filter(is_read=False).order_by('-datetime_received')
for item in unread_files:
    if (item.attachments):
        attachements = item.attachments
        for attachment in attachements:
            #print(attachment.name)
            (filename,extension) = os.path.splitext(attachment.name)
            #Not working for .xls
            if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment, FileAttachment):   # Ensure it's a FileAttachment type
                # Convert bytes from the attachment directly to a pandas dataframe
                #print(attachment.name)
                try:
                    excel_data = pd.read_excel(io.BytesIO(attachment.content), sheet_name=None)
                    for e in excel_data.items():
                        print(e)
                        item.is_read = True
                except xlrd.biffh.XLRDError as e:
                    if str(e) == "Workbook is encrypted":
                        print(f"Cannot process encrypted file: {attachment.name}")
                    else:
                        raise e
                
            else:
                if extension == '.csv' and isinstance(attachment, FileAttachment):
                    csv_data = pd.read_csv(io.BytesIO(attachment.content), sheet_name=None)
                    for c in csv_data.items():
                        print(c)
                        item.is_read = True

#read files
read_files = account.inbox.filter(is_read=True).order_by('-datetime_received')
for item in read_files:
    if (item.attachments):
        attachements = item.attachments
        for attachment in attachements:
            #print(attachment.name)
            (filename,extension) = os.path.splitext(attachment.name)
            if (extension == '.xlsx' or extension == '.xls') and isinstance(attachment, FileAttachment):   # Ensure it's a FileAttachment type
                # Convert bytes from the attachment directly to a pandas dataframe
                #print(attachment.name)
                """ excel_data = pd.read_excel(io.BytesIO(attachment.content), sheet_name=None)
                for e in excel_data.items():
                    print(e)
                    item.is_read = True """
                try:
                    excel_data = pd.read_excel(io.BytesIO(attachment.content), sheet_name=None)
                    for e in excel_data.items():
                        print(e)
                        item.is_read = True
                except xlrd.biffh.XLRDError as e:
                    if str(e) == "Workbook is encrypted":
                        print(f"Cannot process encrypted file: {attachment.name}")
                    else:
                        raise e
                
            else:
                if extension == '.csv' and isinstance(attachment, FileAttachment):
                    csv_data = pd.read_csv(io.BytesIO(attachment.content), sheet_name=None)
                    for c in csv_data.items():
                        print(c)
                        item.is_read = True


            

