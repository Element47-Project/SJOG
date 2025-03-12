import EM133XM_HMI_Library as HMI_133Library
import BFM136_HMI_Library as HMI_136Library
from datetime import datetime, timedelta
from config import map_133, map_136
import signal
from dateutil import parser
import os
import pandas as pd

MAX_RETRIES = 3


def signal_handler(sig, frame):
    print("\n\u26d4 User requested stop. Saving data before exiting...")


def run_all_meters():
    for key, value in map_133.items():
        ip, port, node, data_logger_no = value.split(",")
        folder_name, file_name = key.split("_", 1)
        file_path = f"Data/{folder_name}/{file_name}.csv"
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

        run_with_retries(file_path, meter, ip, int(port), int(node), int(data_logger_no), last_timestamp_st)


def run_with_retries(file_path, meter, ip, port, node, data_logger_no, last_timestamp):
    for attempt in range(MAX_RETRIES):
        print(f"🔄 Attempt {attempt + 1} for Meter={meter}, IP={ip}, Node={node}, Port={port}, Logger={data_logger_no}")
        success = run(file_path, meter, ip, port, node, data_logger_no, last_timestamp)

        if success:
            return

        print(f"⚠️ Retrying {file_path} (Attempt {attempt + 1}/{MAX_RETRIES})")

    print(f"❌ Failed after {MAX_RETRIES} attempts: {file_path}")


def run(file_path, meter, ip, port, node, data_logger_no, last_timestamp):
    TwoDArray = []
    print(f"🔄File={file_path}, Start={last_timestamp if last_timestamp else 'Full Range'}")
    if meter == 'BFM136':
        DataLoggerInstance = HMI_136Library.DataLogger(ip, node, port, start_time=last_timestamp)
        DataLoggerInstance.ReadDatalogger(data_logger_no, node)
        TwoDArray = DataLoggerInstance.GetDataMatrix()
    elif meter == 'EM133':
        DataLoggerInstance = HMI_133Library.DataLogger(ip, node, port, start_time=last_timestamp)
        DataLoggerInstance.ReadDatalogger(data_logger_no, node)
        TwoDArray = DataLoggerInstance.GetDataMatrix()

    if not TwoDArray:
        print(f"⚠️ No new data for {file_path}.")
        return False
    df = pd.DataFrame(TwoDArray, columns=[
        "Index", "timeStamp", "import", "export", "Shoulder kW", "Off Peak kW", "Mx Dmd kW", "Mx Dmd kVA"
    ])
    df = clean_data(df, last_timestamp)
    generate_csv(df, file_path)
    return True


def get_last_timestamp(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            return None
        last_timestamp_str = df["Time Stamp"].iloc[-1]
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
    data[1:].to_csv(file_path, mode='a', index=False, header=not file_exists, encoding="utf-8")
    print(f"✅ Ready Finished {file_path}")
    return file_path


def clean_data(df, last_timestamp):
    # Convert 'timeStamp' to datetime
    df['timeStamp'] = pd.to_datetime(df['timeStamp'], dayfirst=True)

    # Sort the DataFrame by 'timeStamp'
    df.sort_values(by='timeStamp', inplace=True)

    # Reset the index
    df.reset_index(drop=True, inplace=True)

    # Check for duplicate rows based on 'timeStamp' and 'meterUid'
    df.drop_duplicates(subset=['timeStamp', 'meterUid'], inplace=True)

    # Check if the start time of the data is within 4 hours of the last_timestamp
    if last_timestamp and (df['timeStamp'].iloc[0] - last_timestamp).total_seconds() > 4 * 3600:
        raise ValueError("123 start time is more than 4 hours after the last timestamp.")

    # Check for data continuity within 4 hours
    df['time_diff'] = df['timeStamp'].diff().dt.total_seconds().div(3600).fillna(0)
    df = df[df['time_diff'] <= 4]

    # Compare with last_timestamp
    if last_timestamp:
        df = df[df['timeStamp'] > last_timestamp]

    return df


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    run_all_meters()
