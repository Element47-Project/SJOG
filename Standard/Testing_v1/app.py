from flask import Flask
import EM133XM_HMI_Library as HMI_133Library
import BFM136_HMI_Library as HMI_136Library
from datetime import datetime
import os
import pandas as pd
from config import map_133, map_136

app = Flask(__name__)


def run_all_meters():
    for key, value in map_133.items():
        ip, port, node, data_logger_no = value.split(",")
        file_path = f"Data/{key}.csv"
        meter = 'EM133'
        last_timestamp = get_last_timestamp(file_path)
        run(meter, ip, int(port), int(node), int(data_logger_no), last_timestamp)

    for key, value in map_136.items():
        ip, port, node, data_logger_no = value.split(",")
        file_path = f"{key}.csv"
        meter = 'BFM136'
        last_timestamp = get_last_timestamp(file_path)
        run(file_path, meter, ip, int(port), int(node), int(data_logger_no), last_timestamp)
    return


def run(file_path, meter, ip, port, node, data_logger_no, last_timestamp):
    print(f"🔄 Running data collection for: Meter={meter}, IP={ip}, Node={node}, Port={port}, Logger={data_logger_no}")
    print(f"🕒 Time Range: Start={last_timestamp if last_timestamp else 'Full Range'}")

    if meter == 'BFM136':
        pass
    else:
        DataLoggerInstance = HMI_133Library.DataLogger(ip, node, port, start_time=last_timestamp)
        print(f"DataLoggerInstance = HMI_133Library.DataLogger({ip}, {node}, {port}, start_time={last_timestamp})")
        DataLoggerInstance.ReadDatalogger(data_logger_no, node)
        TwoDArray = DataLoggerInstance.GetDataMatrix()

    if not TwoDArray:
        print(f"⚠️ No new data for {ip} (Node {node}). Skipping.")
        return

    generate_csv(TwoDArray, file_path)
    print(f"✅ Data collection complete: {file_path}")


def get_last_timestamp(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
        if df.empty or "Time Stamp" not in df.columns:
            return None
        return df["Time Stamp"].iloc[-1]
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def generate_csv(data, file_path):
    df = pd.DataFrame(data, columns=[
        "Index", "Time Stamp", "Total kW", "Peak kW", "Shoulder kW", "Off Peak kW", "Mx Dmd kW", "Mx Dmd kVA"
    ])

    file_exists = os.path.isfile(file_path)
    df.to_csv(file_path, mode='a', index=False, header=not file_exists, encoding="utf-8")
    return file_path


if __name__ == "__main__":
    # last_timestamp = '' # when want to define the start time
    run_all_meters()
