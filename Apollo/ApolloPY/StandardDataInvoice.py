import pandas as pd
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import warnings
from sqlalchemy import text

process_date = ''  # "%Y-%m-%d"
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
warnings.simplefilter(action='ignore', category=FutureWarning)


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


def fetch_data(start, meter):
    try:
        start = pd.to_datetime(start)
        end = start + timedelta(days=1)

        meter = "(" + ",".join(f"'{m}'" for m in meter) + ")"

        query = f"""
            SELECT 
                [DateTime], [Meter], [kWh_IMP], [kWh_EXP], [kvarh_IMP], [kvarh_EXP],
                [kVAh], [V1], [V2], [V3], [I1], [I2], [I3], [kW1], [kW2], [kW3] 
            FROM [dbo].[Meter_Output_RAW]
            WHERE [DateTime] >= '{start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND [DateTime] < '{end.strftime('%Y-%m-%d %H:%M:%S')}'
              AND [Meter] IN {meter}

            UNION ALL 

            SELECT                 
                [DateTime], [Meter], [kWh_IMP], [kWh_EXP], [kvarh_IMP], [kvarh_EXP],
                [kVAh], [V1], [V2], [V3], [I1], [I2], [I3], [kW1], [kW2], [kW3]  
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY [Meter] ORDER BY [DateTime]) AS rn
                FROM [dbo].[Meter_Output_RAW]
                WHERE [DateTime] >= '{end.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND [Meter] IN {meter}
            ) t
            WHERE rn = 1
        """

        df = pd.read_sql(query, engine)
        return df.sort_values(by=['Meter', 'DateTime']).reset_index(drop=True)

    except Exception as e:
        logging.error(f"Error fetching all meter data with next point: {e}")
        return None


def clean_data(df):
    try:
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        required_cols = ['kWh_IMP', 'kWh_EXP']

        meters = df['Meter'].dropna().unique()
        all_results = []
        next_day_points = df.iloc[0:0].copy()
        start_time = pd.to_datetime(df['DateTime'].min().date())
        next_day_start = start_time + pd.Timedelta(days=1)
        for meter in meters:
            meter_df = df[df['Meter'] == meter].copy()
            end_time = meter_df['DateTime'].max()

            full_time_range = pd.date_range(start=start_time, end=end_time, freq='15min')
            meter_df = meter_df.set_index('DateTime').reindex(full_time_range)
            meter_df['Meter'] = meter  # 补回 meter

            for col in required_cols:
                meter_df[col] = meter_df[col].ffill().bfill()

            if next_day_start in meter_df.index:
                point = meter_df.loc[[next_day_start]].copy()
                point["DateTime"] = next_day_start
                point["Meter"] = meter
                next_day_points = pd.concat([next_day_points, point], ignore_index=True)

            for col in required_cols:
                meter_df[col] = meter_df[col].fillna(0).round().astype(int)

            # 生成 Prev 和 Diff 列
            meter_df['Prev_kWh_IMP'] = meter_df['kWh_IMP'].shift(1)
            meter_df['Prev_kWh_EXP'] = meter_df['kWh_EXP'].shift(1)

            meter_df['Diff_kWh_IMP'] = meter_df['kWh_IMP'] - meter_df['Prev_kWh_IMP']
            meter_df['Diff_kWh_EXP'] = meter_df['kWh_EXP'] - meter_df['Prev_kWh_EXP']

            meter_df = meter_df.reset_index().rename(columns={'index': 'DateTime'})
            all_results.append(meter_df)

        if not all_results:
            logging.warning("⚠️ No valid meter data after filtering.")
            return pd.DataFrame()

        result_df = pd.concat(all_results, ignore_index=True)
        result_df = result_df.sort_values(by=['Meter', 'DateTime']).reset_index(drop=True)
        result_df = result_df[result_df["DateTime"] <= next_day_start]

        logging.info(f"✅ Strict clean completed for {len(all_results)} meters.")
        return result_df, next_day_points

    except Exception as e:
        logging.error(f"❌ Error in clean_meter_data_with_diff_and_prev_strict: {e}")
        return df


def get_time_ranges(tariff_type):
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


def fetch_all_tariff_data(projectname, date):
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
                  AND Bundled_Tariff = ?
        """
        params = (date, date, projectname)
        tariff_data = pd.read_sql(query, engine, params=params)

        if tariff_data.empty:
            logging.warning(f"No tariff data found for date '{date}'.")
        else:
            logging.info(f"Successfully fetched all tariff data for date '{date}'")

        return tariff_data

    except Exception as e:
        logging.error(f"Error fetching tariff data: {e}")
        return None


def calculate_imp_exp_consumption(df_clean, date):
    try:
        result_rows = []

        for meter in df_clean['Meter'].unique():
            meter_df = df_clean[df_clean['Meter'] == meter].copy()

            total_imp = meter_df['Diff_kWh_IMP'].sum()
            result_rows.append({
                'Date': date,
                'Meter': meter,
                'EXP': 0,
                'Consumption': total_imp,
                'Anytime': total_imp,
                'On_Peak': None,
                'Shoulder': None,
                'Off_Peak': None,
                'Overnight': None,
                'Super_Off_Peak': None,
                'Demand': None
            })

            total_exp = meter_df['Diff_kWh_EXP'].sum()
            if total_exp > 0:
                result_rows.append({
                    'Date': date,
                    'Meter': meter,
                    'EXP': 1,
                    'Consumption': total_exp,
                    'Anytime': total_exp,
                    'On_Peak': None,
                    'Shoulder': None,
                    'Off_Peak': None,
                    'Overnight': None,
                    'Super_Off_Peak': None,
                    'Demand': None
                })

        return pd.DataFrame(result_rows)

    except Exception as e:
        logging.error(f"❌ Error in calculate_imp_exp_consumption: {e}")
        return pd.DataFrame()


def apply_tariff_rates(df_consumption, tariff_row):
    rate_columns = {
        'Anytime': 'Anytime_Rate',
        'On_Peak': 'On_Peak_Rate',
        'Shoulder': 'Shoulder_Rate',
        'Off_Peak': 'Off_Peak_Rate',
        'Overnight': 'Overnight_Rate',
        'Super_Off_Peak': 'Super_Off_Peak_Rate'
    }
    # 直接乘费率，覆盖原值
    for usage_col, rate_col in rate_columns.items():
        rate = tariff_row.get(rate_col)
        if rate is not None and usage_col in df_consumption.columns:
            df_consumption[usage_col] = df_consumption[usage_col] * rate

    # 固定费用列
    fixed_price = tariff_row.get("Fixed_Price", 0)
    df_consumption["Fixed_Daily"] = fixed_price

    # 所有费用字段 + fixed price，求和再乘以1.1
    price_cols = list(rate_columns.keys()) + ["Fixed_Daily"]
    df_consumption["Total"] = df_consumption[price_cols].fillna(0).sum(axis=1) * 1.1

    return df_consumption


def upload_to_sql(df, table_name):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ Uploaded to SQL table: {table_name}")
    except Exception as e:
        logging.error(f"❌ Error uploading to {table_name}: {e}")
        print(f"❌ Error uploading to {table_name}: {e}")


def delete_and_insert_next_day_points(df_next, table_name="Meter_Output_RAW"):
    try:
        if df_next.empty:
            print("⚠️ df_next 为空，无需处理")
            return

        with engine.begin() as conn:
            unique_date = df_next["DateTime"].iloc[0]
            if isinstance(unique_date, pd.Timestamp):
                unique_date = unique_date.to_pydatetime()

            meters_to_delete = df_next["Meter"].unique().tolist()

            params = {"date": unique_date}
            placeholders = []

            for i, meter in enumerate(meters_to_delete):
                param_name = f"meter_{i}"
                params[param_name] = meter
                placeholders.append(f":{param_name}")

            delete_sql = text(f"""
                DELETE FROM {table_name} 
                WHERE DateTime = :date 
                AND Meter IN ({", ".join(placeholders)})
            """)

            conn.execute(delete_sql, params)

            df_next.to_sql(
                table_name,
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000
            )

            print(f"✅ upload {len(df_next)} records | Datetime: {unique_date}")

    except Exception as e:
        logging.exception("❌ Failed:")
        raise


def main(project_name, date):
    try:
        print(f"\n🚀 Running pipeline for {project_name} on {date}")

        meter_data = fetch_meter_data()
        meter_data = meter_data[meter_data["ProjectName"] == project_name]
        if meter_data.empty:
            print(f"⚠️ No meters found for project {project_name}")
            return

        meter_list = meter_data['Meter'].tolist()

        df_raw = fetch_data(date, meter_list)
        if df_raw.empty:
            print("⚠️ No raw meter data found.")
            return

        df_clean, next_day_points = clean_data(df_raw)

        tariff = fetch_all_tariff_data(project_name, date)
        if tariff.empty:
            print("⚠️ No tariff found.")
            return

        tariff_row = tariff.iloc[0]

        df_consumption = calculate_imp_exp_consumption(df_clean, date)
        df_with_costs = apply_tariff_rates(df_consumption, tariff_row)

        df_clean_trimmed = (
            df_clean.sort_values(["Meter", "DateTime"])
            .groupby("Meter", group_keys=False)
            .apply(lambda x: x.iloc[1:])
            .reset_index(drop=True)
        )
        delete_and_insert_next_day_points(next_day_points)
        upload_to_sql(df_clean_trimmed, "Meter_Output_Detail")
        upload_to_sql(df_with_costs, "Invoice_All")

    except Exception as e:
        logging.error(f"❌ Fatal error in main(): {e}")
        print(f"❌ Fatal error: {e}")


if __name__ == "__main__":
    project_name = "Apollo"

    if process_date is None:
        start_date = datetime.now().date() - timedelta(days=1)  # 默认只跑昨天
    else:
        start_date = pd.to_datetime(process_date).date()

    end_date = datetime.now().date() - timedelta(days=1)  # 只跑到昨天

    current_date = start_date
    while current_date <= end_date:
        main(project_name, current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
