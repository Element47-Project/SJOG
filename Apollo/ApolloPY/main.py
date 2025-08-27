import logging
from datetime import datetime, timedelta
from data_fetcher import fetch_meter_data, fetch_all_tariff_data, fetch_meter_data_with_previous
from data_cleaner import clean_meter_data
from data_processor import calculate_consumption
from invoice_generator import generate_invoice
from config import engine

process_date = '2025-02-18'
log_file_path = r"C:\Users\Shane\Desktop\Apllo\apollo_upload.log"
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
    start_date = datetime.strptime(process_date, "%Y-%m-%d")
    end_date = datetime.today() - timedelta(days=1)  # Yesterday's date

    current_date = start_date
    while current_date <= end_date:
        main(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
