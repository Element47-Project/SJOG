import numpy as np
import pandas as pd
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta

process_date = '2025-01-09'  # "%Y-%m-%d"
log_file_path = r"C:\Users\Shane\Desktop\Apllo\apollo_upload.log"
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
FILE_DIR = os.environ.get('FILE_ADDRESS')

# Setup connection engine and connection string
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};'
    f'DATABASE={SQL_DB_NAME};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
)


def get_time_ranges(tariff_type):
    """
    Define time ranges for different tariff types, including weekday and weekend logic.
    """
    if tariff_type == 1:  # Anytime
        return {
            "Weekday": {
                "Anytime": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]
            },
            "Weekend": {
                "Anytime": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]
            }
        }
    elif tariff_type == 2:  # Peak, Shoulder, Off-Peak
        return {
            "Weekday": {
                "Peak": [(pd.to_datetime("17:00:00").time(), pd.to_datetime("20:00:00").time())],
                "Shoulder": [
                    (pd.to_datetime("07:00:00").time(), pd.to_datetime("17:00:00").time()),
                    (pd.to_datetime("20:00:00").time(), pd.to_datetime("22:00:00").time())
                ],
                "Off-Peak": [
                    (pd.to_datetime("22:00:00").time(), pd.to_datetime("23:59:59").time()),
                    (pd.to_datetime("00:00:00").time(), pd.to_datetime("07:00:00").time())
                ]
            },
            "Weekend": {
                "Off-Peak": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]
            }
        }
    elif tariff_type == 3:  # Overnight and additional rates
        return {
            "Weekday": {
                "Peak": [(pd.to_datetime("17:00:00").time(), pd.to_datetime("20:00:00").time())],
                "Shoulder": [
                    (pd.to_datetime("07:00:00").time(), pd.to_datetime("17:00:00").time()),
                    (pd.to_datetime("20:00:00").time(), pd.to_datetime("22:00:00").time())
                ],
                "Off-Peak": [
                    (pd.to_datetime("22:00:00").time(), pd.to_datetime("23:59:59").time()),
                    (pd.to_datetime("00:00:00").time(), pd.to_datetime("07:00:00").time())
                ],
                "Overnight": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("06:00:00").time())],
                "Super_Off_Peak": [(pd.to_datetime("01:00:00").time(), pd.to_datetime("05:00:00").time())]
            },
            "Weekend": {
                "Off-Peak": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]
            }
        }
    else:
        return {}


def fetch_meter_data():
    try:
        query = f"""
            SELECT 
                [ProjectName], 
                [Meter], 
                [Tariff],
                [Status],
                [EXP]
            FROM [dbo].[Meter_Table]
            WHERE [Status] IN ('Running', 'Processing')
        """

        meter_data = pd.read_sql(query, engine)
        return meter_data

    except Exception as e:
        logging.error(f"Error fetching meter data: {e}")
        return None


def fetch_all_tariff_data(date):
    """
    Fetch all tariff data for the given date in bulk.
    """
    try:
        query = f"""
            SELECT 
                *
            FROM [dbo].[Tariff_All]
            WHERE StartDate <= ? 
                  AND DATEADD(year, 1, StartDate) > ?
        """
        params = (date, date)
        tariff_data = pd.read_sql(query, engine, params=params)

        if tariff_data.empty:
            logging.warning(f"No tariff data found for date '{date}'.")
        else:
            logging.info(f"Successfully fetched all tariff data for date '{date}'")

        return tariff_data

    except Exception as e:
        logging.error(f"Error fetching tariff data: {e}")
        return None


def fetch_meter_data_with_previous(meter, date):
    try:
        query = f"""
            SELECT TOP 1 [DateTime], [Meter], [kWh_IMP], [kWh_EXP]
            FROM [dbo].[Meter_Output_Detail]
            WHERE [Meter] = '{meter}' AND [DateTime] < '{date} 00:00:00'
            ORDER BY [DateTime] DESC;

            SELECT [DateTime], [kWh_IMP], [kWh_EXP], [kvarh_IMP],
                   [kvarh_EXP], [kVAh], [V1], [V2], [V3], [I1], [I2], [I3],
                   [kW1], [kW2], [kW3], [Meter]
            FROM [dbo].[Meter_Output_RAW]
            WHERE [Meter] = '{meter}' AND CONVERT(date, [DateTime]) = '{date}'
            ORDER BY [DateTime];
        """

        prev_data = pd.read_sql(query.split(';')[0], engine)
        if prev_data is None or prev_data.empty:
            prev_data = pd.DataFrame({
                'DateTime': [pd.Timestamp('1900-01-01 00:00:00')],
                'Meter': [meter],
                'kWh_IMP': [0],
                'kWh_EXP': [0]
            })
        curr_data = pd.read_sql(query.split(';')[1], engine)
        if curr_data.empty:
            logging.warning(f"No data for meter '{meter}' on date '{date}'.")
            return None, prev_data, None

        # Ensure the columns match expected structure
        combined_data = pd.concat([prev_data, curr_data]).sort_values(by='DateTime').reset_index(drop=True)

        required_columns = [
            'DateTime', 'kWh_IMP', 'kWh_EXP', 'kvarh_IMP',
            'kvarh_EXP', 'kVAh', 'V1', 'V2', 'V3', 'I1', 'I2', 'I3',
            'kW1', 'kW2', 'kW3', 'Meter'
        ]

        for column in required_columns:
            if column not in combined_data.columns:
                combined_data[column] = 0
        return combined_data, prev_data, curr_data

    except Exception as e:
        logging.error(f"Error fetching data for meter '{meter}': {e}")
        return None, None, None


def detect_outliers_by_difference(df, columns, group_col, threshold=200):
    df_copy = df.copy()

    for col in columns:
        df_copy = df_copy.sort_values([group_col, 'DateTime'])
        df_copy[f'{col}_diff'] = df_copy.groupby(group_col)[col].diff()
        outliers = (df_copy[f'{col}_diff'].abs() > threshold) | (df_copy[f'{col}_diff'].abs().shift(-1) > threshold)
        df_copy.loc[outliers, col] = np.nan
        df_copy = df_copy.drop(f'{col}_diff', axis=1)

    return df_copy


def clean_meter_data(df, pre_data):
    try:
        process_cols = ['kWh_IMP', 'kWh_EXP']
        df[process_cols] = df[process_cols].replace(0, np.nan)
        df = detect_outliers_by_difference(df, process_cols, group_col='Meter', threshold=200)
        df[process_cols] = df.groupby('Meter')[process_cols].ffill().bfill()

        for meter in df['Meter'].unique():
            prev_row = pre_data[pre_data['Meter'] == meter]
            if not prev_row.empty:
                mask = (df['Meter'] == meter) & (df['DateTime'] == df[df['Meter'] == meter]['DateTime'].min())
                for col in ['kWh_IMP', 'kWh_EXP']:
                    df.loc[mask, f'Prev_{col}'] = prev_row[col].values[0]

        df['Prev_kWh_IMP'] = df.groupby('Meter')['kWh_IMP'].shift(1).fillna(df['Prev_kWh_IMP'])
        df['Prev_kWh_EXP'] = df.groupby('Meter')['kWh_EXP'].shift(1).fillna(df['Prev_kWh_EXP'])

        for col in process_cols:
            df[f'Diff_{col}'] = df[col] - df[f'Prev_{col}']
            df[f'Diff_{col}'] = df[f'Diff_{col}'].fillna(0)

        return df

    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        return None


def calculate_consumption(cleaned_data, tariff_type, exp):
    """
    Calculate consumption for all time ranges based on tariff type.
    Output all kinds of time range data, leave null if no data.
    """
    try:
        if cleaned_data is None or cleaned_data.empty:
            return {
                "Anytime": None, "Peak": None, "Shoulder": None, "Off-Peak": None,
                "Overnight": None, "Super_Off_Peak": None, "EXP": None
            }

        # Get time ranges for the tariff type
        time_ranges = get_time_ranges(tariff_type)

        # Initialize consumption categories
        consumption_imp = {
            "Anytime": 0,
            "Peak": 0,
            "Shoulder": 0,
            "Off-Peak": 0,
            "Overnight": 0,
            "Super_Off_Peak": 0,
            "EXP": 0
        }

        consumption_exp = {
            "Anytime": 0,
            "Peak": 0,
            "Shoulder": 0,
            "Off-Peak": 0,
            "Overnight": 0,
            "Super_Off_Peak": 0,
            "EXP": 1
        }

        # Add Weekday and Time columns for filtering
        cleaned_data['Weekday'] = cleaned_data['DateTime'].dt.weekday
        cleaned_data['Time'] = cleaned_data['DateTime'].dt.time

        # Separate weekday and weekend data
        weekday_data = cleaned_data[cleaned_data['Weekday'] < 5]
        weekend_data = cleaned_data[cleaned_data['Weekday'] >= 5]

        # Calculate weekday consumption
        if "Weekday" in time_ranges:
            for period, time_range in time_ranges["Weekday"].items():
                for start_time, end_time in time_range:
                    if start_time == pd.to_datetime("00:00:00").time():
                        filtered_period = weekday_data[(weekday_data['Time'] >= start_time)
                                                       & (weekday_data['Time'] <= end_time)]
                    else:
                        filtered_period = weekday_data[(weekday_data['Time'] > start_time)
                                                       & (weekday_data['Time'] <= end_time)]
                    consumption_imp[period] += filtered_period['Diff_kWh_IMP'].sum()
                    if exp == 1 and pd.notnull(filtered_period['Diff_kWh_EXP'].sum()):
                        consumption_exp[period] += filtered_period['Diff_kWh_EXP'].sum()

        # Calculate weekend consumption
        if "Weekend" in time_ranges:
            for period, time_range in time_ranges["Weekend"].items():
                for start_time, end_time in time_range:
                    if start_time == pd.to_datetime("00:00:00").time():
                        filtered_period = weekend_data[(weekend_data['Time'] >= start_time)
                                                       & (weekend_data['Time'] <= end_time)]
                    else:
                        filtered_period = weekend_data[(weekend_data['Time'] > start_time)
                                                       & (weekend_data['Time'] <= end_time)]
                    consumption_imp[period] += filtered_period['Diff_kWh_IMP'].sum()
                    if exp == 1 and pd.notnull(filtered_period['Diff_kWh_EXP'].sum()):
                        consumption_exp[period] += filtered_period['Diff_kWh_EXP'].sum()

        # Round consumption values and set nulls if no data
        consumption_imp = {key: (round(value, 3) if value >= 0 else None) for key, value in consumption_imp.items()}
        consumption_exp = {key: (round(value, 3) if value >= 0 else None) for key, value in consumption_exp.items()}

        if exp == 0:
            return consumption_imp
        else:
            return [consumption_imp, consumption_exp]

    except Exception as e:
        logging.error(f"Error calculating consumption: {e}")
        return [
            {
                "Anytime": None, "Peak": None, "Shoulder": None, "Off-Peak": None,
                "Overnight": None, "Super_Off_Peak": None, "EXP": 0
            },
            {
                "Anytime": None, "Peak": None, "Shoulder": None, "Off-Peak": None,
                "Overnight": None, "Super_Off_Peak": None, "EXP": 1
            }
        ]


def generate_invoice(consumption, tariff, date, meter):
    """
    Generate invoice data for a specific meter.
    """
    try:
        invoice_rows = []

        for cons in (consumption if isinstance(consumption, list) else [consumption]):
            invoice_row = {
                "Date": date,
                "Meter": meter,
                "EXP": cons.get("EXP"),
                "Consumption": sum([value for key, value in cons.items() if key != "EXP" and value is not None]),
                "Fixed_Daily": tariff.get("Fixed_Price", 0),
                "Anytime": (cons.get("Anytime") or 0) * tariff.get("Anytime_Rate", 0),
                "On_Peak": (cons.get("Peak") or 0) * tariff.get("On_Peak_Rate", 0),
                "Off_Peak": (cons.get("Off-Peak") or 0) * tariff.get("Off_Peak_Rate", 0),
                "Shoulder": (cons.get("Shoulder") or 0) * tariff.get("Shoulder_Rate", 0),
                "Overnight": (cons.get("Overnight") or 0) * tariff.get("Overnight_Rate", 0),
                "Super_Off_Peak": (cons.get("Super_Off_Peak") or 0) * tariff.get("Super_Off_Peak_Rate", 0),
                "Total": sum([
                    tariff.get("Fixed_Price", 0),
                    (cons.get("Anytime") or 0) * tariff.get("Anytime_Rate", 0),
                    (cons.get("Peak") or 0) * tariff.get("On_Peak_Rate", 0),
                    (cons.get("Off-Peak") or 0) * tariff.get("Off_Peak_Rate", 0),
                    (cons.get("Shoulder") or 0) * tariff.get("Shoulder_Rate", 0),
                    (cons.get("Overnight") or 0) * tariff.get("Overnight_Rate", 0),
                    (cons.get("Super_Off_Peak") or 0) * tariff.get("Super_Off_Peak_Rate", 0)
                ])
            }
            invoice_rows.append(invoice_row)

        return pd.DataFrame(invoice_rows)

    except Exception as e:
        logging.error(f"Error generating invoice: {e}")
        return None


def main(date):
    if not date:
        date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    logging.info(f"Processing data for date: {date}")

    CLEANED_DATA_COLUMNS = [
        'DateTime', 'kWh_IMP', 'Prev_kWh_IMP', 'Diff_kWh_IMP',
        'kWh_EXP', 'Prev_kWh_EXP', 'Diff_kWh_EXP', 'kvarh_IMP',
        'kvarh_EXP', 'kVAh', 'V1', 'V2', 'V3', 'I1', 'I2', 'I3',
        'kW1', 'kW2', 'kW3', 'Meter'
    ]

    INVOICE_COLUMNS = [
        'Date', 'Meter', 'EXP', 'Consumption', 'Fixed_Daily',
        'Anytime', 'On_Peak', 'Off_Peak', 'Shoulder', 'Overnight',
        'Super_Off_Peak', 'Total'
    ]
    # Fetch meter data
    meter_data = fetch_meter_data()

    # Fetch tariff data
    all_tariff_data = fetch_all_tariff_data(date)
    all_tariff_data.fillna(0, inplace=True)

    # Find unique projects
    unique_projects = meter_data['ProjectName'].unique()

    for project_name in unique_projects:
        # Get tariff data for the project
        project_tariff_data = all_tariff_data[all_tariff_data['Bundled_Tariff']
                                              == meter_data[meter_data['ProjectName']
                                                            == project_name]['Tariff'].iloc[0]]
        if not project_tariff_data.empty:
            # Filter meters for this project
            project_meters = meter_data[meter_data['ProjectName'] == project_name]

            for _, meter_row in project_meters.iterrows():
                meter = meter_row['Meter']
                status = meter_row['Status']
                exp_flag = meter_row['EXP']
                print(f"Processing Meter '{meter}' in Project '{project_name}' with status '{status}' "
                      f"and EXP flag '{exp_flag}'.")

                # Fetch data for the specific meter
                meter_data_details, prev_data, curr_data = fetch_meter_data_with_previous(meter, date)
                if meter_data_details is not None:
                    cleaned_data = clean_meter_data(meter_data_details, prev_data)

                    if cleaned_data is not None:
                        # Determine calculation type
                        tariff_type = project_tariff_data['TYPE'].iloc[0]
                        consumption = calculate_consumption(cleaned_data, tariff_type, exp_flag)

                        # Generate invoice
                        invoice = generate_invoice(consumption, project_tariff_data.iloc[0].to_dict(), date, meter)
                        try:
                            cleaned_data[CLEANED_DATA_COLUMNS].iloc[1:].to_sql(
                                'Meter_Output_Detail', con=engine, if_exists='append', index=False)

                            invoice[INVOICE_COLUMNS].to_sql('Invoice_All', con=engine, if_exists='append', index=False)
                        except Exception as e:
                            logging.error(f"Error uploading data for Meter '{meter}': {e}")
        else:
            logging.warning(f"No tariff data found for Project '{project_name}'.")


if __name__ == "__main__":
    if not process_date:
        process_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    current_date = pd.to_datetime(process_date)
    end_date = pd.to_datetime(datetime.today() - timedelta(days=1))

    while current_date <= end_date:
        main(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
