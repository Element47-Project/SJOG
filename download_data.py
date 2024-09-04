from flask import Flask, jsonify, request
import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# All settings in the .env file, including SQL information.
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
FILE_DIR = os.environ.get('FILE_ADDRESS')

app = Flask(__name__)

# Step 1: Set up the SQL Database connection
def get_db_connection(SQL_SERVER=SQL_SERVER,SQL_DB_NAME=SQL_DB_NAME,SQL_USERNAME=SQL_USERNAME,SQL_PASSWORD=SQL_PASSWORD):
    server = SQL_SERVER  
    database = SQL_DB_NAME  
    username = SQL_USERNAME  
    password = SQL_PASSWORD  

    # Connection string
    connection_string = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'Connection Timeout=30;'
    )

    # Create a new database connection
    conn = pyodbc.connect(connection_string)
    return conn

# Step 2: Define the API endpoint to fetch data from the SQL database
@app.route('/get-data', methods=['GET'])
def get_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Execute SQL query
        query = """
        SELECT TOP (20) [DateTime]
        ,[kWh_IMP],[kWh_EXP],[kvarh_IMP]
        ,[kvarh_EXP],[kVAh]
        ,[V],[I],[kW],[I_THD],[Meter] 
        FROM [dbo].[Apollo_Units] 
        ORDER BY [DateTime] DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Fetch column names
        columns = [column[0] for column in cursor.description]

        # Convert rows to a list of dictionaries
        data = []
        for row in rows:
            data.append(dict(zip(columns, row)))

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Return JSON response
        return jsonify(data)
    
    except Exception as e:
        return jsonify({"error": str(e)})

# Step 3: Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
