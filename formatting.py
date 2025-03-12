import os
import pandas as pd
from glob import glob
from dateutil import parser


def custom_date_parser(date_str):
    try:
        return parser.parse(date_str, dayfirst=True)
    except ValueError:
        raise ValueError(f"no valid date format found for {date_str}")


def process_one_file(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

    try:
        df["Time Stamp"] = df["Time Stamp"].apply(custom_date_parser)
    except ValueError as e:
        print(f"Error parsing dates in {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

    df.dropna(subset=["Time Stamp"], inplace=True)

    cutoff = pd.Timestamp("22/02/2025 00:00:00")
    cuton = pd.Timestamp("04/03/2025 00:00:00")

    df = df[(df["Time Stamp"] >= cutoff) & (df["Time Stamp"] <= cuton) & (df["Time Stamp"].dt.minute % 15 == 0)]

    filename = os.path.splitext(os.path.basename(file_path))[0]
    if filename.startswith("DeHaviland_"):
        filename = filename.replace("DeHaviland_", "")
    df["display_name"] = filename

    # Rename columns
    df.rename(columns={"Time Stamp": "timeStamp", "Total kW": "import", "Peak kW": "export"}, inplace=True)

    # Add new columns
    df["meterUid"] = None
    df["import"] = df["import"] * 10
    df["isEstimate"] = False
    df["energyUnit"] = "Wh"
    df["interval"] = 15

    # Format the timeStamp column with timezone +08:00
    df["timeStamp"] = df["timeStamp"].dt.tz_localize('Asia/Shanghai').dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Ensure column order
    df = df[['meterUid', 'interval', 'timeStamp', 'import', 'export', 'isEstimate', 'energyUnit', 'display_name']]

    return df


def process_all_files(input_folder, output_file):
    all_files = glob(os.path.join(input_folder, "*.csv"))
    if not all_files:
        print(f"No CSV files found in folder {input_folder}.")
        return

    df_list = []
    for f in all_files:
        try:
            processed_df = process_one_file(f)
            if not processed_df.empty:
                df_list.append(processed_df)
                print(f"Processed file: {f}")
        except Exception as e:
            print(f"Error processing file {f}: {e}")

    if not df_list:
        print("No files were successfully processed.")
        return

    final_df = pd.concat(df_list, ignore_index=True)
    final_df.to_csv(output_file, index=False)
    print(f"All files processed and merged into {output_file}")


if __name__ == "__main__":
    dfmain = process_one_file(r"Standard/Testing_v2/Data/Grid_meter.csv")
    dfmain.to_csv("main.csv", index=False)
