import subprocess
import threading
import EM133XM_HMI_Library as HMI_133Library
import BFM136_HMI_Library as HMI_136Library
from datetime import datetime, timedelta
from config import map_133_subsets, map_136
import signal
from dateutil import parser
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
load_dotenv()

SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')


engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};'
    f'DATABASE={SQL_DB_NAME};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
)

MAX_RETRIES = 3


def signal_handler(sig, frame):
    print("\n\u26d4 User requested stop. Saving data before exiting...")


def run_all_meters():
    threads = []
    for subset in map_133_subsets:
        thread = threading.Thread(target=process_subset, args=(subset,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def process_subset(subset):
    for key, value in subset.items():
        ip, port, node, data_logger_no = value.split(",")
        folder_name, file_name = key.split("_", 1)
        file_path = f"testing/Data/{folder_name}/{file_name}.csv"
        meter = 'EM133'
        last_timestamp = get_last_timestamp(file_path)

        if last_timestamp and (datetime.now() - last_timestamp) < timedelta(minutes=100):
            print(f"✅ Skipping {key}.")
            continue
        else:
            if last_timestamp:
                last_timestamp_st = last_timestamp.strftime("%d/%m/%Y %H:%M:%S")
            else:
                last_timestamp_st = None

        run_with_retries(file_path, meter, ip, int(port), int(node), int(data_logger_no), last_timestamp_st, file_name)


def run_with_retries(file_path, meter, ip, port, node, data_logger_no, last_timestamp, file_name):
    for attempt in range(MAX_RETRIES):
        print(f"🔄 Attempt {attempt + 1} for Meter={meter}, IP={ip}, Node={node}, Port={port}, Logger={data_logger_no}")
        success = run(file_path, meter, ip, port, node, data_logger_no, last_timestamp, file_name)
        if success:
            return
        print(f"⚠️ Retrying {file_path} (Attempt {attempt + 1}/{MAX_RETRIES})")

    print(f"❌ Failed after {MAX_RETRIES} attempts: {file_path}")


def run(file_path, meter, ip, port, node, data_logger_no, last_timestamp, file_name):
    TwoDArray = []
    print(f"🔄File={file_path}, Start={last_timestamp if last_timestamp else 'Full Range'}")
    if meter == 'BFM136':
        # DataLoggerInstance = HMI_136Library.DataLogger(ip, node, port, start_time=last_timestamp)
        # DataLoggerInstance.ReadDatalogger(data_logger_no, node)
        # TwoDArray = DataLoggerInstance.GetDataMatrix()
        pass
    elif meter == 'EM133':
        DataLoggerInstance = HMI_133Library.DataLogger(ip, node, port, start_time=last_timestamp)
        DataLoggerInstance.ReadDatalogger(data_logger_no, node)
        TwoDArray = DataLoggerInstance.GetDataMatrix()

    if not TwoDArray:
        print(f"⚠️ No new data for {file_path}.")
        return False
    TwoDArray = pd.DataFrame(TwoDArray, columns=[
        "Index", "DateTime", "kWh_IMP", "kWh_EXP", "kvarh_IMP", "kvarh_EXP", "kVAh", "kVAh_EXPORT"
    ])
    TwoDArray["Meter"] = file_name
    final_data = format_data(TwoDArray)
    # upload(final_data)
    generate_csv(final_data, file_path)
    return True


def get_last_timestamp(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
        last_timestamp_str = df["DateTime"].iloc[-1]
        try:
            last_timestamp = parser.parse(last_timestamp_str, dayfirst=True)
        except ValueError:
            print(f"Error parsing date: {last_timestamp_str}")
            return None
        return last_timestamp
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def generate_csv(data, file_path):
    file_exists = os.path.isfile(file_path)
    data.to_csv(file_path, mode='a', index=False, header=not file_exists, encoding="utf-8")
    print(f"✅ Ready Finished {file_path}")
    return file_path


def format_data(data):
    data = data.drop(columns=["Index", "kVAh_EXPORT"])
    for col in data.columns:
        if col != "DateTime" and col != "Meter":
            data[col] = data[col].astype(float) * 10
    data = data[1:]

    return data


def upload(data):
    table_name = 'Meter_Output_RAW'
    data['DateTime'] = pd.to_datetime(data['DateTime'], format='%d/%m/%Y %H:%M:%S ', errors='coerce')
    data = data[data['DateTime'].dt.minute.isin([0, 20, 40])]
    data.to_sql(table_name, engine, if_exists='append', index=False)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    run_all_meters()
