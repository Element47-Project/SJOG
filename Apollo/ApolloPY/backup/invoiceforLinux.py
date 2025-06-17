import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Initialize logging
log_file_path = os.path.join(os.path.dirname(__file__), "logs", "apollo_invoice.log")
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
EMAIL_USERNAME = os.environ.get('EMAIL_USERNAME')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')


# Configure SQL database connection
def get_engine():
    return create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                         f'?driver=ODBC+Driver+18+for+SQL+Server')


# Define target date and date range
def get_date_range(target_date):
    return target_date - timedelta(hours=5), target_date + timedelta(hours=5)


# Query Meter and Date data
def query_meter_data(engine, target_date):
    logging.info("Querying Meter and Date data")
    meter_date_query = """
    WITH NearestData AS (
        SELECT 
            t.[Meter],
            d.[DateTime],
            d.[kWh_IMP],
            d.[kWh_EXP],
            ABS(DATEDIFF(SECOND, d.[DateTime], ?)) AS TimeDifference,
            t.[EXP],
            t.[Unit]
        FROM 
            Meter_Output_Detail d
        JOIN 
            Meter_Table t
        ON 
            d.Meter = t.Meter
        WHERE 
            t.ProjectName = 'Apollo' AND d.[DateTime] BETWEEN DATEADD(DAY, -3, ?) AND DATEADD(DAY, 3, ?)
    )
    SELECT 
        Meter,
        Unit,
        NULL AS InitialTimestamp,
        NULL AS InitialReading,
        FinalTimestamp,
        FinalReading
    FROM (
        SELECT 
            nd.[Meter] AS Meter,
            nd.[Unit],
            nd.[DateTime] AS FinalTimestamp,
            nd.[kWh_IMP] AS FinalReading
        FROM 
            (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY [Meter] ORDER BY TimeDifference ASC) AS RowNum
                FROM 
                    NearestData
            ) nd
        WHERE 
            nd.RowNum = 1
        UNION ALL
        SELECT 
            nd.[Meter] + ' EXP' AS Meter,
            nd.[Unit],
            nd.[DateTime] AS FinalTimestamp,
            nd.[kWh_EXP] AS FinalReading
        FROM 
            (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY [Meter] ORDER BY TimeDifference ASC) AS RowNum
                FROM 
                    NearestData
            ) nd
        WHERE 
            nd.RowNum = 1 AND nd.[EXP] = 1
    ) AllData
    ORDER BY 
        Unit;
    """
    return pd.read_sql_query(meter_date_query, con=engine, params=(target_date, target_date, target_date))


# Update Units with descriptions
def map_meter_descriptions(meter_date_df):
    meter_description_map = {
        "RMT-APL-01-MDB2-3-MDB2-3-01-75000043-DL1": "MDB 2/3",
        "RMT-APL-01-MDB4-5-MDB4-5-01-75000038-DL1": "MDB 4/5",
        "RMT-APL-01-MSB-CMON-01-75000040-DL1": "Common, Commercial and PV Import kWh",
        "RMT-APL-01-MSB-MDB1-01-75000025-DL1": "MDB1",
        "RMT-APL-01-MSB-MSB-01-40002624-DL1": "MSB MAIN CHECK METER Import kWh",
        "RMT-APL-01-MSB-UMS-01-75000029-DL1": "SMSB Unit Main Switches",
        "RMT-APL-01-MSB-CMON-01-75000040-DL1 EXP": "Common, Commercial and PV Export Meter",
        "RMT-APL-01-MSB-MSB-01-40002624-DL1 EXP": "MSB MAIN CHECK METER Export kWh",
    }
    meter_date_df['Unit'] = meter_date_df.apply(
        lambda row: meter_description_map[row['Meter']] if row['Meter'] in meter_description_map else row['Unit'],
        axis=1
    )


# Update initial values from processed data
def update_initial_values(meter_date_df, processed_data_path):
    processed_data_df = pd.read_excel(processed_data_path, 'Processed 123')
    for index, row in meter_date_df.iterrows():
        match = processed_data_df.loc[processed_data_df['Meter'] == row['Meter']]
        if not match.empty:
            meter_date_df.at[index, 'InitialTimestamp'] = match.iloc[0]['FinalTimestamp']
            meter_date_df.at[index, 'InitialReading'] = match.iloc[0]['FinalReading']


# Calculate Days, Consumption, and Average Consumption per Day
def calculate_additional_columns(meter_date_df):
    # Convert to datetime for calculations
    meter_date_df['InitialTimestamp'] = pd.to_datetime(meter_date_df['InitialTimestamp'], format='%d/%m/%Y %H:%M'
                                                       , errors='coerce')
    meter_date_df['FinalTimestamp'] = pd.to_datetime(meter_date_df['FinalTimestamp'], format='%d/%m/%Y %H:%M'
                                                     , errors='coerce')

    # Convert readings to numeric
    meter_date_df['InitialReading'] = pd.to_numeric(meter_date_df['InitialReading'], errors='coerce')
    meter_date_df['FinalReading'] = pd.to_numeric(meter_date_df['FinalReading'], errors='coerce')
    meter_date_df['Consumption'] = round((meter_date_df['FinalReading'] - meter_date_df['InitialReading']), 3)

    # Format datetime columns to match expected format
    meter_date_df['InitialTimestamp'] = meter_date_df['InitialTimestamp'].dt.strftime('%d/%m/%Y %H:%M')
    meter_date_df['FinalTimestamp'] = meter_date_df['FinalTimestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')


# Save results to a CSV file
def aggregate_meter_consumption(meter_date_df):
    meter_groups = {
        "SMSB Unit Main Switches": ["MSB-APR"],
        "MDB1": ["MDB1-APR"],
        "MDB 2/3": ["MDB2-APR", "MDB3-APR"],
        "MDB 4/5": ["MDB4-APR", "MDB5-APR"]
    }
    aggregated_data = []

    for group, prefixes in meter_groups.items():
        total_consumption = meter_date_df[meter_date_df['Meter'].str.contains('|'.join(prefixes), na=False)][
            'Consumption'].sum()
        aggregated_data.append([group, total_consumption])

    return pd.DataFrame(aggregated_data, columns=['MeterGroup', 'TotalConsumption'])


# Calculate variance
def calculate_variance(meter_date_df, aggregated_df):
    comparison_map = {
        "SMSB Unit Main Switches": "RMT-APL-01-MSB-UMS-01-75000029-DL1",
        "MDB1": "RMT-APL-01-MSB-MDB1-01-75000025-DL1",
        "MDB 2/3": "RMT-APL-01-MDB2-3-MDB2-3-01-75000043-DL1",
        "MDB 4/5": "RMT-APL-01-MDB4-5-MDB4-5-01-75000038-DL1"
    }
    variance_data = []
    check_meter_value = meter_date_df.loc[meter_date_df['Meter'] == "RMT-APL-01-MSB-MSB-01-40002624-DL1", 'Consumption'].sum()
    calculated_value = (
        meter_date_df.loc[meter_date_df['Meter'].str.contains("MDB1"), 'Consumption'].sum() +
        meter_date_df.loc[meter_date_df['Meter'].str.contains("MDB2-3"), 'Consumption'].sum() +
        meter_date_df.loc[meter_date_df['Meter'].str.contains("MDB4-5"), 'Consumption'].sum() +
        meter_date_df.loc[meter_date_df['Meter'] == "RMT-APL-01-MSB-UMS-01-75000029-DL1", 'Consumption'].sum() +
        meter_date_df.loc[meter_date_df['Meter'] == "RMT-APL-01-MSB-CMON-01-75000040-DL1", 'Consumption'].sum() -
        meter_date_df.loc[meter_date_df['Meter'] == "RMT-APL-01-MSB-CMON-01-75000040-DL1 EXP", 'Consumption'].sum() -
        meter_date_df.loc[meter_date_df['Meter'] == "RMT-APL-01-MSB-MSB-01-40002624-DL1 EXP", 'Consumption'].sum()
    )
    variance = round(((calculated_value - check_meter_value) / check_meter_value), 3)
    variance_data.append(["MSB MAIN CHECK METER", calculated_value, check_meter_value, variance])
    for group, meter in comparison_map.items():
        check_meter_value = meter_date_df.loc[meter_date_df['Meter'] == meter, 'Consumption'].sum()
        calculated_value = aggregated_df.loc[aggregated_df['MeterGroup'] == group, 'TotalConsumption'].sum()
        variance = round(((calculated_value - check_meter_value) / check_meter_value), 3)
        variance_data.append([group, calculated_value, check_meter_value, variance])

    return pd.DataFrame(variance_data,
                        columns=['MeterGroup', 'CalculatedConsumption', 'CheckMeterConsumption', 'Variance'])


def save_to_excel(meter_date_df, variance_df, output_path):
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        meter_date_df.to_excel(writer, sheet_name="Processed 123", index=False)
        variance_df.to_excel(writer, sheet_name="Variance 123", index=False)


def send_email_with_attachment(file_path, recipient_email, sender_email, sender_password):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Processed Meter 123 Report"

    body = "Please find the attached processed meter data report."
    msg.attach(MIMEText(body, 'plain'))

    with open(file_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
        msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com', 587)  # Modify SMTP server as needed
    server.starttls()
    server.login(sender_email, sender_password)
    server.sendmail(sender_email, recipient_email, msg.as_string())
    server.quit()


# Main execution
def main():
    today = datetime.today()
    target_date = datetime(datetime.today().year, datetime.today().month, 1, 0, 0, 0)
    last_processed_month = today.month - 2
    last_processed_year = today.year if last_processed_month > 0 else today.year - 1
    last_processed_month = last_processed_month if last_processed_month > 0 else 12
    engine = get_engine()
    next_processed_month = (last_processed_month + 2) if (last_processed_month + 2) <= 12 else (
                last_processed_month + 2 - 12)
    next_processed_year = last_processed_year if (last_processed_month + 2) <= 12 else last_processed_year + 1
    processed_data_path = f"processed_data_{last_processed_year}_{last_processed_month}.xlsx"
    output_file = f"processed_data_{next_processed_year}_{next_processed_month}.xlsx"
    engine = get_engine()
    meter_date_df = query_meter_data(engine, target_date)
    map_meter_descriptions(meter_date_df)
    update_initial_values(meter_date_df, processed_data_path)
    calculate_additional_columns(meter_date_df)
    aggregated_df = aggregate_meter_consumption(meter_date_df)
    variance_df = calculate_variance(meter_date_df, aggregated_df)
    save_to_excel(meter_date_df, variance_df, output_file)

    # Send email and delete file
    send_email_with_attachment(output_file, "zhengliangqiu50@gmail.com",
                               EMAIL_USERNAME, EMAIL_PASSWORD)


if __name__ == "__main__":
    main()
