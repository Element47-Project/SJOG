def main(date):
    if not date:
        date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    logging.info(f"Processing data for date: {date}")

    # Fetch meter data
    meter_data = fetch_meter_data()

    # Fetch tariff data
    all_tariff_data = fetch_all_tariff_data(date)
    unique_projects = meter_data['ProjectName'].unique()
    for project_name in unique_projects:
        project_meters = meter_data[meter_data['ProjectName'] == project_name]

        for _, meter_row in project_meters.iterrows():
            meter = meter_row['Meter']
            status = meter_row['Status']
            exp_flag = meter_row['EXP']
            print(
                f"Processing Meter '{meter}' in Project '{project_name}' with status '{status}' and EXP flag '{exp_flag}'.")

        if meter_data_details is not None:
            cleaned_data_imp = clean_meter_data(meter_data_details, identifier='kWh_IMP')
            tariff_type = project_tariff_data['TYPE'].iloc[0]
            if tariff_type == 1:
                consumption_imp = calculate_anytime_consumption(cleaned_data_imp, identifier='kWh_IMP')
                invoice1(consumption_imp)
            elif tariff_type == 2:
                consumption_imp = calculate_peak_offpeak_consumption(cleaned_data_imp)
                invoice2(consumption_imp)
            elif tariff_type == 3:
                consumption_imp = calculate_overnight_consumption(cleaned_data_imp)
                invoice3(consumption_imp)

            if exp_flag == '1':
                cleaned_data_exp = clean_meter_data(meter_data_details, identifier='kWh_EXP')
                if tariff_type == 1:
                    consumption_exp = calculate_anytime_consumption(cleaned_data_imp, identifier='kWh_IMP')
                    invoice1(consumption_exp)
                elif tariff_type == 2:
                    consumption_exp = calculate_peak_offpeak_consumption(cleaned_data_imp)
                    invoice2(consumption_exp)
                elif tariff_type == 3:
                    consumption_exp = calculate_overnight_consumption(cleaned_data_imp)
                    invoice3(consumption_exp)


            if invoice_df is not None:
                logging.info(f"Invoice generated for Meter {meter}:
                {invoice_df}
                ")
        upload_data(cleaned_data, invoice_data)

if __name__ == "__main__":
    main(process_date)
