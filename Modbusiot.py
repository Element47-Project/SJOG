import os
import pandas as pd
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# SQL Server credentials
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')

# SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}'
                       f'?driver=ODBC+Driver+18+for+SQL+Server')

# File paths
input_file_path = "Apollo/Data/dataout"


def process_json_data(file_path):
    """Process JSON data and return DataFrames for two table structures."""
    data_list = []
    with open(file_path, "r") as file:
        lines = file.readlines()

    for line in lines:
        line = line.strip()
        if line:
            try:
                data = json.loads(line)
                data_list.extend(data["Apollo"])
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {line}")
                print(f"JSONDecodeError: {e}")

    df = pd.DataFrame(data_list)
    df["Val"] = pd.to_numeric(df["Val"], errors="coerce")
    df["RecOn"] = pd.to_datetime(df["RecOn"], errors="coerce")

    # Pivot the data to create a common structure
    pivoted_df = df.pivot(index=["RecOn", "Meter"], columns="Key", values="Val").reset_index()
    pivoted_df.rename(columns={"RecOn": "DateTime"}, inplace=True)

    # Prepare the DataFrame for table1 (ApolloTesting)
    table1_columns = [
        "DateTime", "kWh_IMP", "kWh_EXP", "kvarh_IMP", "kvarh_EXP",
        "kVAh", "V", "I", "kW", "I_THD", "Meter"
    ]
    for col in table1_columns:
        if col not in pivoted_df.columns:
            pivoted_df[col] = None
    table1_df = pivoted_df[table1_columns].copy()

    # Prepare the DataFrame for table2 (Apollo_Main_Check)
    table2_columns = [
        "DateTime", "Meter", "kWh_Import_Total", "kWh_Export_Total",
        "kvarh_Import_Total", "kvarh_Export_Total", "kVAh_Total", "kVAh_Import_Total", "kVAh_Export_Total", "V12",
        "V23", "V13", "I1", "I2", "I3", "KW1", "KW2", "KW3", "I1_Current_THD", "I2_Current_THD", "I3_Current_THD"
    ]
    for col in table2_columns:
        if col not in pivoted_df.columns:
            pivoted_df[col] = None
    table2_df = pivoted_df[table2_columns].copy()

    # Add intervals and aggregation for table1
    table1_df["Interval"] = table1_df["DateTime"].dt.floor("15T")
    table1_aggregated = (
        table1_df.groupby(["Meter", "Interval"], as_index=False)
        .agg({
            "DateTime": "max",
            "kWh_IMP": "max", "kWh_EXP": "max",
            "kvarh_IMP": "max", "kvarh_EXP": "max",
            "kVAh": "max", "V": "max", "I": "max",
            "kW": "max", "I_THD": "max"
        })
    )
    table1_aggregated.drop(columns=["DateTime"], inplace=True)
    table1_aggregated.rename(columns={"Interval": "DateTime"}, inplace=True)

    # Add intervals and aggregation for table2
    table2_df["Interval"] = table2_df["DateTime"].dt.floor("15T")
    table2_aggregated = (
        table2_df.groupby(["Meter", "Interval"], as_index=False)
        .agg({
            "DateTime": "max",
            "kWh_Import_Total": "max",
            "kWh_Export_Total": "max",
            "kvarh_Import_Total": "max",
            "kvarh_Export_Total": "max",
            "kVAh_Total": "max",
            "kVAh_Import_Total": "max",
            "kVAh_Export_Total": "max",
            "V12": "max", "V23": "max", "V13": "max",
            "I1": "max", "I2": "max", "I3": "max",
            "KW1": "max", "KW2": "max", "KW3": "max",
            "I1_Current_THD": "max",
            "I2_Current_THD": "max",
            "I3_Current_THD": "max"
        })
    )
    table2_aggregated.drop(columns=["DateTime"], inplace=True)
    table2_aggregated.rename(columns={"Interval": "DateTime"}, inplace=True)

    return table1_aggregated, table2_aggregated


def fetch_existing_primary_keys(engine, table_name):
    """Fetch existing primary keys (DateTime and Meter) from the SQL table."""
    query = f"SELECT [DateTime], [Meter] FROM {table_name}"
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def upload_new_rows(df, table_name, engine):
    """Upload new rows to the SQL table."""
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print("New rows successfully uploaded.")
    except Exception as e:
        print(f"Error uploading new rows: {e}")


def fetch_existing_primary_keys_check(engine, table_name):
    """Fetch existing primary keys (DateTime and Meter) from Apollo_Main_Check."""
    query = f"SELECT [DateTime], [Meter] FROM {table_name}"
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def process_main_check_data(df, meters):
    """
    Process data for Apollo_Main_Check without renaming columns.
    """
    filtered_df = df[df["Meter"].isin(meters)].copy()

    if not filtered_df.empty:

        # Ensure the DataFrame is sorted by Meter and DateTime
        filtered_df.sort_values(by=["Meter", "DateTime"], inplace=True)

        # Calculate previous and difference columns for kWh_Import_Total
        filtered_df['kWh_Import_Previous'] = filtered_df.groupby('Meter')['kWh_Import_Total'].shift(1)
        filtered_df['KWh_Import_Diff'] = filtered_df['kWh_Import_Total'] - filtered_df['kWh_Import_Previous']

        # Similarly for kvarh_Import_Total
        filtered_df['kvarh_Import_Previous'] = filtered_df.groupby('Meter')['kvarh_Import_Total'].shift(1)
        filtered_df['kvarh_Import_Diff'] = filtered_df['kvarh_Import_Total'] - filtered_df['kvarh_Import_Previous']

        # Fill NaN values resulting from the shift operation
        filtered_df[['kWh_Import_Previous', 'KWh_Import_Diff', 'kvarh_Import_Previous', 'kvarh_Import_Diff']] = \
            (filtered_df[['kWh_Import_Previous', 'KWh_Import_Diff', 'kvarh_Import_Previous', 'kvarh_Import_Diff']]
             .fillna(0))

        # Select required columns
        required_columns = [
            "DateTime", "Meter", "kWh_Import_Total", "kWh_Import_Previous", "KWh_Import_Diff",
            "kWh_Export_Total", "kvarh_Import_Total", "kvarh_Import_Previous", "kvarh_Import_Diff",
            "kvarh_Export_Total", "kVAh_Total", "kVAh_Import_Total", "kVAh_Export_Total",
            "V12", "V23", "V13", "I1", "I2", "I3", "KW1", "KW2", "KW3",
            "I1_Current_THD", "I2_Current_THD", "I3_Current_THD"
        ]

        # Add any missing required columns with default values
        for col in required_columns:
            if col not in filtered_df.columns:
                filtered_df[col] = None

        # Reorder columns to match the required structure
        filtered_df = filtered_df[required_columns]

        return filtered_df


def upload_to_apollo_main_check(df, table_name, engine):
    """Upload data to Apollo_Main_Check table."""
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print("Data successfully uploaded to Apollo_Main_Check.")
    except Exception as e:
        print(f"Error uploading data to Apollo_Main_Check: {e}")


def main():
    table_name_1 = "ApolloTesting"
    table_name_2 = "Apollo_Main_Check"
    meters = [
        "RMT-APL-01-MSB-CMON-01-75000040-DL1",
        "RMT-APL-01-MSB-MSB-01-40002624-DL1",
        "RMT-APL-01-MSB-UMS-01-75000029-DL1",
        "RMT-APL-01-MDB2-3-MDB2-3-01-75000043-DL1",
        "RMT-APL-01-MDB4-5-MDB4-5-01-75000038-DL1",
    ]

    # Step 1: Process JSON data
    print("Processing JSON data...")
    table1_df, table2_df = process_json_data(input_file_path)

    # Ensure DateTime is in the correct format
    table1_df['DateTime'] = pd.to_datetime(table1_df['DateTime'], format='%Y-%m-%d %H:%M')
    table2_df['DateTime'] = pd.to_datetime(table2_df['DateTime'], format='%Y-%m-%d %H:%M')
    table1_df = table1_df[~table1_df['Meter'].isin(meters)].copy()

    # Step 2: Upload to ApolloTesting
    print("Fetching existing primary keys from ApolloTesting...")
    existing_keys_1 = fetch_existing_primary_keys(engine, table_name_1)
    existing_keys_set_1 = set(zip(existing_keys_1['DateTime'], existing_keys_1['Meter']))
    new_rows_1 = table1_df[~table1_df.apply(lambda row: (row['DateTime'], row['Meter']) in existing_keys_set_1, axis=1)]

    if not new_rows_1.empty:
        print(f"Uploading {len(new_rows_1)} new rows to ApolloTesting...")
        upload_new_rows(new_rows_1, table_name_1, engine)
    else:
        print("No new rows to upload to ApolloTesting.")

    # Step 3: Process data for Apollo_Main_Check
    print("Processing data for Apollo_Main_Check...")
    main_check_df = process_main_check_data(table2_df, meters)

    if main_check_df is None or main_check_df.empty:
        print("No data available for Apollo_Main_Check. Skipping upload.")
        return

    print("Fetching existing primary keys from Apollo_Main_Check...")
    existing_keys_2 = fetch_existing_primary_keys(engine, table_name_2)
    existing_keys_set_2 = set(zip(existing_keys_2['DateTime'], existing_keys_2['Meter']))
    new_rows_2 = main_check_df[
        ~main_check_df.apply(lambda row: (row['DateTime'], row['Meter']) in existing_keys_set_2, axis=1)
    ]

    if not new_rows_2.empty:
        print(f"Uploading {len(new_rows_2)} new rows to Apollo_Main_Check...")
        upload_to_apollo_main_check(new_rows_2, table_name_2, engine)
    else:
        print("No new rows to upload to Apollo_Main_Check.")


if __name__ == "__main__":
    main()
