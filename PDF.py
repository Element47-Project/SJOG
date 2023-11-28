import tabula
import pandas as pd
import pyodbc
def PDF_upload():
    table_name = 'Water Use History Statement 1 CLAYTON ST MIDLAND LOT 515.pdf'
    # Read PDF tables
    tables = tabula.read_pdf(table_name, pages='all', multiple_tables=True)

    correct_headers = ['Water Use Year', 'Read Date', 'Reading', 'Dial Reading', 'Kilolitres Used', 'Consumption Year to Date', 'Daily Rate']

    # Example of manually setting column names
    processed_tables = []
    for table in tables:
        if not table.empty:
            table.columns = correct_headers  # Set new column names
            processed_table = table.iloc[2:].reset_index(drop=True)  # Skip first two rows
            processed_tables.append(processed_table)
    processed_table['Read Date'] = pd.to_datetime(processed_table['Read Date']).dt.strftime('%Y-%m-%d')
    print(processed_table)


    server = 'sqlddatabasedemo.database.windows.net'
    database = 'SampleDB'
    username = 'sqladmin'
    password = 'Reviveyourbody47'
    driver= '{ODBC Driver 18 for SQL Server}'

    # Connect to the database
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()


    for row in processed_table.itertuples(index=False, name=None):
        # Clean the data - convert NaN to None
        cleaned_data = [None if pd.isnull(item) else item for item in row]

        # SQL INSERT statement
        insert_query = '''INSERT INTO dbo.TestingWater ([Water Use Year], [Read Date], [Reading], [Dial Reading], [Kilolitres Used], [Consumption Year to Date], [Daily Rate])
            VALUES (?, ?, ?, ?, ?, ?, ?)'''
        # Execute the query with cleaned data
        cursor.execute(insert_query, cleaned_data)

    # Commit the transaction and close the connection
    cnxn.commit()
    cursor.close()
    cnxn.close()


