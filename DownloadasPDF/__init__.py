import azure.functions as func
import logging
import pyodbc
from fpdf import FPDF
from io import BytesIO
from datetime import datetime
import os
import sys

TABLE_NAME = 'dbo.Apollo_Units'

def checkinput(req: func.HttpRequest):
    # Get parameters from the query string or request body
    unit_no = req.params.get('unit_no')
    start_time = req.params.get('start_time')

    if not unit_no or not start_time:
        try:
            req_body = req.get_json()
            logging.info(f"Request body received: {req_body}")
            if isinstance(req_body, dict): # Ensure the body is a dictionary 
                unit_no = req_body.get('unit_no')
                start_time = req_body.get('start_time')
                logging.info(f"Parsed JSON body - unit_no: {unit_no}, start_time: {start_time}")
            else:
                logging.error("Request body is not a valid JSON object.")
                raise ValueError("Request body is not a valid JSON object.")
        except ValueError as e:
            logging.error(f'Error parsing JSON: {e}')
            return func.HttpResponse(
                "Invalid request body format. Please send a valid JSON object.",
                status_code=400
            )
        except Exception as e:
            logging.error(f'Unexpected error occurred while parsing request: {e}')
            return func.HttpResponse(
                "An unexpected error occurred while processing the request.",
                status_code=500
            )

    # Check which parameter is missing
    if not unit_no and not start_time:
        logging.error('Both "unit_no" and "start_time" are missing.')
        return func.HttpResponse(
            "Both 'unit_no' and 'start_time' are required.",
            status_code=400
        )
    elif not unit_no:
        logging.error('"unit_no" is missing.')
        return func.HttpResponse(
            "'unit_no' is required.",
            status_code=400
        )
    elif not start_time:
        logging.error('"start_time" is missing.')
        return func.HttpResponse(
            "'start_time' is required.",
            status_code=400
        )

    # Convert start_time to match the database format 'yyyy-MM-dd HH:mm:ss.SSS'
    try:
        start_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        start_time_sql = start_time_dt.strftime('%Y-%m-%d %H:%M:%S.000')
        logging.info(f"Converted start_time to SQL format: {start_time_sql}")
    except ValueError as ve:
        logging.error(f"Invalid date format for start_time: {start_time}")
        return func.HttpResponse(
            "Invalid date format. Please use 'yyyy-MM-dd HH:mm:ss'.",
            status_code=400
        )

    return unit_no,start_time_sql

def connect_to_db():
    # Initialize the connection variable
    conn = None
    cursor = None

    # Database connection parameters from environment variables
    server = os.getenv('AZURE_SQL_SERVER')
    database = os.getenv('AZURE_SQL_DB_NAME')
    username = os.getenv('AZURE_SQL_USERNAME')
    password = os.getenv('AZURE_SQL_PASSWORD')
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    logging.info(f"Connecting to database {server} with user {username}")

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        logging.info("Connected to database using ODBC Driver 17 for SQL Server")

    except pyodbc.Error as db_err_17:
        logging.warning(f"ODBC Driver 17 connection failed: {db_err_17}")

        # Try connecting with ODBC Driver 18 if Driver 17 fails
        try:
            driver = '{ODBC Driver 18 for SQL Server}'
            conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            logging.info("Connected to database using ODBC Driver 18 for SQL Server")

        except pyodbc.Error as db_err_18:
            logging.error(f"Both ODBC Driver 17 and 18 failed: {db_err_18}")
            return func.HttpResponse(
                f"Database connection error with both drivers: {str(db_err_18)}",
                status_code=500
            )

    return conn, cursor

def create_pdf(unit_no, rows):
    # Create a PDF document using FPDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=8)

    # Add a title to the PDF
    pdf.cell(200, 10, txt=f"Report for Unit {unit_no}", ln=True, align='C')

    # Add column headers with appropriate width
    header = ['DateTime', 'kWh_IMP', 'kWh_EXP', 'kvarh_IMP', 'kvarh_EXP', 'kVAh', 'V', 'I', 'kW', 'I_THD', 'Meter']
    header_widths = [30, 20, 20, 25, 25, 20, 10, 10, 10, 15, 60]

    for i in range(len(header)-1):
        pdf.cell(header_widths[i], 10, txt=header[i], border=1)
    pdf.ln()

    # Add rows to the PDF
    for row in rows:
        for i in range(len(row)-1):
            pdf.cell(header_widths[i], 6, txt=str(row[i]), border=1)
        pdf.ln()

        # Check if we need to add a page break (optional, depending on your content)
        if pdf.get_y() > 270:  # Assuming standard A4 page length
            pdf.add_page()
            # Re-add header if needed
            for i in range(len(header)-1):
                pdf.cell(header_widths[i], 10, txt=header[i], border=1)
            pdf.ln()

    # Save PDF to a bytes buffer
    pdf_buffer = BytesIO()
    pdf_data = pdf.output(pdf_buffer, dest='S').encode('latin1')
    pdf_buffer.write(pdf_data)
    pdf_buffer.seek(0)

    # Read the raw bytes from the BytesIO buffer 
    pdf_bytes = pdf_buffer.read()

    logging.info(f"Generated PDF with {len(pdf_bytes)} bytes.")

    return pdf_bytes



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        check_result = checkinput(req)

        # Check if the return value is an HttpResponse (indicating an error case)
        if isinstance(check_result, func.HttpResponse):
            return check_result
        
        # Unpack the tuple returned by checkinput (if no errors)
        unit_no, start_time_sql = check_result

        conn, cursor = connect_to_db()

        

        try:
            # SQL query to fetch required columns where the unit number matches and start_time matches
            query = """
            SELECT [DateTime], [kWh_IMP], [kWh_EXP], [kvarh_IMP], [kvarh_EXP], [kVAh], [V], [I], [kW], [I_THD], [Meter]
            FROM [dbo].[Apollo_Units]
            WHERE 
                SUBSTRING([Meter], CHARINDEX('APR', [Meter]) + 3, 2) = ?
                AND [DateTime] > ?
            """
            logging.info(f"Executing query: with unit_no={unit_no} and start_time_sql={start_time_sql}")
            cursor.execute(query, (unit_no, start_time_sql))
            rows = cursor.fetchall()

            if not rows:
                logging.warning("No data found for the provided parameters.")
                return func.HttpResponse(
                    "No data found for the provided parameters.",
                    status_code=404
                )
            
            logging.info(f"Fetched {len(rows)} rows from database.")

            # Create a PDF bytes
            pdf_bytes = create_pdf(unit_no, rows)

            # Return the PDF file as an HTTP response 
            return func.HttpResponse(
                body=pdf_bytes,
                status_code=200,
                headers={
                    'Content-Type': 'application/pdf',
                    'Content-Disposition': f'attachment; filename="Unit_{unit_no}_report.pdf"'
                }
            )


        except Exception as e:
            logging.error(f"Unexpected error during data processing or PDF generation: {e}")
            return func.HttpResponse(
                f"An unexpected error occurred: {str(e)}",
                status_code=500
            )

        finally:
            if conn:
                cursor.close()
                conn.close()

    except Exception as global_err:
        logging.error(f'Unhandled error: {global_err}, Trace: {str(sys.exc_info()[2])}')
        return func.HttpResponse(f"An unhandled error occurred: {str(global_err)}", status_code=500)
