import tabula
import pandas as pd


def pdf_upload():
    table_name = 'Water Use History Statement 1 CLAYTON ST MIDLAND LOT 515.pdf'
    tables = tabula.read_pdf(table_name, pages='all', multiple_tables=True)
    correct_headers = ['Water Use Year', 'Read Date', 'Reading', 'Dial Reading', 'Kilolitres Used',
                       'Consumption Year to Date', 'Daily Rate']
    print(tables)
    processed_tables = []
    for table in tables:
        if not table.empty:
            table.columns = correct_headers
            processed_table = table.iloc[2:].reset_index(drop=True)
            processed_tables.append(processed_table)
    processed_table['Read Date'] = pd.to_datetime(processed_table['Read Date']).dt.strftime('%Y-%m-%d')

    for row in processed_table.itertuples(index=False, name=None):
        cleaned_data = [None if pd.isnull(item) else item for item in row]
    print(cleaned_data)



