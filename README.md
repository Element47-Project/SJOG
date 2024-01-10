# Element47

EMAIL AND WEATHER DATA PROCESSOR
This repository contains a Python script designed to automate the processing and uploading of email attachments and weather data to an Azure SQL database. The script handles Excel, CSV, and PDF attachments, automatically determining their structure and contents for efficient database storage. It also retrieves weather data from the Open-Meteo API and uploads it to Azure SQL.

FEATURES
- Email Attachment Processing: Handles Excel, CSV, and PDF files, extracting and uploading their contents to Azure SQL.
- Weather Data Retrieval and Upload: Fetches historical weather data and uploads it to Azure SQL.
- Automatic File Type Detection: Identifies the file type of email attachments and processes them accordingly.
- Database Integration: Seamlessly integrates with Azure SQL for data storage.
- Error Handling: Robust error handling for encrypted or unsupported file types.

REQUIREMENTS
- Python 3.x
- dotenv: To manage environment variables.
- exchangelib: To interact with email accounts.
- pandas: For data manipulation and analysis.
- pyodbc: For Azure SQL database connectivity.
- openpyxl, xlrd, pdfplumber: For reading Excel and PDF files.
- fuzzywuzzy: For string matching.
- ntplib, pytz: For network time protocol operations.
- openmeteo_requests, requests_cache: For weather data retrieval.

SETUP
1. Clone the Repository:
   git clone https://github.com/your-repository.git

2. Install Dependencies:
   pip install -r requirements.txt

3. Configure Environment Variables: Set up a .env file with the following variables:
   - AZURE_SQL_SERVER
   - AZURE_SQL_DB_NAME
   - AZURE_SQL_USERNAME
   - AZURE_SQL_PASSWORD
   - EMAIL_ACCOUNT
   - EMAIL_PASSWORD
  
AZURE DATA UPLOAD
Setting up Azure SQL Database:
To ensure smooth operation of the script, set up your Azure SQL Database with the required tables and schemas:
1. Database Configuration: Follow the Azure documentation to set up your SQL database.
2. Required Tables and Schema: Create tables to store email and weather data. Example SQL commands for table creation are provided in the sql_scripts directory.

Uploading Process:
- The script automatically processes and transforms data from emails and weather sources to fit the Azure SQL schema.
- Data is uploaded in batches for efficiency and to minimize the load on the server.
- Error handling mechanisms are in place to address issues such as data format mismatches or network interruptions.

USAGE
1. Run the Script:
   python main.py

2. Check Output: The script prints logs to the console for progress tracking and debugging.

FUNCTIONALITY OVERVIEW
1. Email Processing:
  - Connects to an email account and fetches unread emails.
  - Filters emails based on the sender's domain.
  - Processes attachments (Excel, CSV, PDF) and uploads data to Azure SQL.

2. Weather Data Retrieval:
  - Fetches historical weather data using Open-Meteo API.
  - Processes and uploads this data to Azure SQL.

4. Database Operations:
  - Functions for batch inserts and data uploads.
  - Handles data formatting and column matching.


NOTES
- Ensure that the Azure SQL database has the necessary tables and schema set up.
- Adjust the email domain filter as per your requirements.
- Modify the weather data retrieval parameters according to your needs.
- The data upload takes a bit of time. (For 2000 lines it takes around 10 minutes to upload in Azure).
