import pandas as pd
import logging


def get_time_ranges(tariff_type):
    if tariff_type == 1:
        return {
            "Weekday": {"Anytime": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]},
            "Weekend": {"Anytime": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]}
        }
    elif tariff_type == 2:
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
            "Weekend": {"Off-Peak": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]}
        }
    elif tariff_type == 3:
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
            "Weekend": {"Off-Peak": [(pd.to_datetime("00:00:00").time(), pd.to_datetime("23:59:59").time())]}
        }
    else:
        return {}


def calculate_consumption(cleaned_data, tariff_type, exp):
    try:
        if cleaned_data is None or cleaned_data.empty:
            return {
                "Anytime": None, "Peak": None, "Shoulder": None, "Off-Peak": None,
                "Overnight": None, "Super_Off_Peak": None, "EXP": None
            }

        time_ranges = get_time_ranges(tariff_type)
        consumption_imp = {
            "Anytime": 0, "Peak": 0, "Shoulder": 0, "Off-Peak": 0,
            "Overnight": 0, "Super_Off_Peak": 0, "EXP": 0
        }
        consumption_exp = {
            "Anytime": 0, "Peak": 0, "Shoulder": 0, "Off-Peak": 0,
            "Overnight": 0, "Super_Off_Peak": 0, "EXP": 1
        }

        cleaned_data['Weekday'] = cleaned_data['DateTime'].dt.weekday
        cleaned_data['Time'] = cleaned_data['DateTime'].dt.time

        weekday_data = cleaned_data[cleaned_data['Weekday'] < 5]
        weekend_data = cleaned_data[cleaned_data['Weekday'] >= 5]

        if "Weekday" in time_ranges:
            for period, time_range in time_ranges["Weekday"].items():
                for start_time, end_time in time_range:
                    if start_time == pd.to_datetime("00:00:00").time():
                        filtered_period = weekday_data[
                            (weekday_data['Time'] >= start_time) & (weekday_data['Time'] <= end_time)]
                    else:
                        filtered_period = weekday_data[
                            (weekday_data['Time'] > start_time) & (weekday_data['Time'] <= end_time)]
                    consumption_imp[period] += filtered_period['Diff_kWh_IMP'].sum()
                    if exp == 1 and pd.notnull(filtered_period['Diff_kWh_EXP'].sum()):
                        consumption_exp[period] += filtered_period['Diff_kWh_EXP'].sum()

        if "Weekend" in time_ranges:
            for period, time_range in time_ranges["Weekend"].items():
                for start_time, end_time in time_range:
                    if start_time == pd.to_datetime("00:00:00").time():
                        filtered_period = weekend_data[
                            (weekend_data['Time'] >= start_time) & (weekend_data['Time'] <= end_time)]
                    else:
                        filtered_period = weekend_data[
                            (weekend_data['Time'] > start_time) & (weekend_data['Time'] <= end_time)]
                    consumption_imp[period] += filtered_period['Diff_kWh_IMP'].sum()
                    if exp == 1 and pd.notnull(filtered_period['Diff_kWh_EXP'].sum()):
                        consumption_exp[period] += filtered_period['Diff_kWh_EXP'].sum()

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
