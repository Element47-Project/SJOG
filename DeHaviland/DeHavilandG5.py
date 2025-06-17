import numpy as np
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from API import PowerLedgerUploader
import logging
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Initial
end_time = datetime.now().strftime('%Y-%m-%d 00:00:00')
start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
log_file = "process_log.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_file, filemode='w')
mapping = {
    'Grid meter': '142812',
    '101': '142813',
    '102': '142814',
    '103': '142815',
    '201': '142816',
    '202': '142825',
    '203': '142817',
    '301': '142818',
    '302': '142819',
    '303': '142820',
    'Commercial': '142821',
    'Common Area Lights': '142822',
    'Common Area': '142823',
    'SOLAR AND BATTERY DB': '142824'
}
# Load environment variables
load_dotenv()
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
SQL_DB_NAME = os.environ.get('AZURE_SQL_DB_NAME')
SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
EMAIL_USERNAME = os.environ.get('EMAIL_USERNAME')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')


class AzureConnector:
    def __init__(self):
        self.connection_string = (f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DB_NAME}"
                                  f"?driver=ODBC+Driver+17+for+SQL+Server")
        self.engine = create_engine(self.connection_string)

    def fetch_data(self, start=start_time, end=end_time):
        logging.info(f"Fetching data from {start} to {end}...")
        query = f"""
        SELECT 
            [DateTime], 
            kWh_IMP, 
            kWh_EXP, 
            d.Meter, 
            Unit AS display_name
        FROM 
            Meter_Output_RAW d
        INNER JOIN 
            Meter_Table m
        ON 
            d.Meter = m.Meter
        WHERE 
            m.ProjectName = 'Dehavilland Apartment'
        AND 
            [DateTime] BETWEEN '{start}' AND '{end}'
        ORDER BY 
            d.Meter;
        """
        try:
            data = pd.read_sql(query, self.engine)
            return data
        except Exception as E:
            logging.error("Error fetching data:", E)
            return pd.DataFrame()


def clean_data(data):
    # Ensure timeStamp is in datetime format
    data['timeStamp'] = pd.to_datetime(data['timeStamp'])

    # Sort data by meterUid and timeStamp
    data.sort_values(by=['meterUid', 'timeStamp'], inplace=True)

    # Initialize an empty DataFrame for cleaned data
    clean_data_initial = pd.DataFrame()

    # Process each meter independently
    for meter, group in data.groupby('meterUid'):
        # Calculate differences for import and export
        group['import_diff'] = group['import'].diff()
        group['export_diff'] = group['export'].diff()

        # Filter rows after calculating differences
        group = group[
            (group['import_diff'] >= 0) & (group['import_diff'] <= 200000) &
            (group['export_diff'] >= 0) & (group['export_diff'] <= 200000)
            ]

        # Append cleaned group to the result
        clean_data_initial = pd.concat([clean_data_initial, group])

    # Reset index for the cleaned data
    clean_data_initial.reset_index(drop=True, inplace=True)

    return clean_data_initial


class DataProcessor:
    def __init__(self, api_map):
        self.mapping = api_map

    def format_data(self, raw_data):
        # 转换为 datetime 格式
        raw_data['DateTime'] = pd.to_datetime(raw_data['DateTime'])

        # 按 Meter 分组
        result = []
        for meter, group in raw_data.groupby('Meter'):
            # 生成目标时间范围（20分钟间隔）
            start_time = group['DateTime'].min().floor('H')
            end_time = group['DateTime'].max().ceil('H')
            target_times = pd.date_range(start=start_time, end=end_time, freq='20T')

            # 以 DateTime 为索引
            group = group.set_index('DateTime')

            # 创建一个新的 DataFrame 按目标时间点对齐
            aligned_data = pd.DataFrame({'DateTime': target_times})
            aligned_data = aligned_data.set_index('DateTime')

            # 合并原始数据
            merged_data = aligned_data.join(group, how='left')

            # 填补缺失数据（最近时间点，时间差不超过6分钟）
            for idx in merged_data.index[merged_data['kWh_IMP'].isna()]:
                time_diffs = abs(group.index - idx)  # 计算时间差
                if not time_diffs.empty:  # 确保有数据点可以比较
                    closest_idx = time_diffs.argmin()  # 找到最小时间差的索引
                    if time_diffs[closest_idx].total_seconds() <= 360:  # 检查是否在6分钟以内
                        merged_data.loc[idx] = group.iloc[closest_idx]

            # 重置索引并追加到结果
            merged_data.reset_index(inplace=True)
            merged_data['Meter'] = meter
            result.append(merged_data)

        # 合并所有仪表数据
        final_data = pd.concat(result, ignore_index=True)

        # 填补剩余 NaN 数据
        final_data['kWh_IMP'] = final_data['kWh_IMP'].fillna(0).astype(float)
        final_data['kWh_EXP'] = final_data['kWh_EXP'].fillna(0).astype(float)

        # Prepare formatted data
        format_data = pd.DataFrame()
        format_data['timeStamp'] = final_data['DateTime']
        format_data['import'] = final_data['kWh_IMP']
        format_data['export'] = final_data['kWh_EXP']
        format_data['meterUid'] = final_data['display_name'].map(self.mapping)
        format_data['energyUnit'] = 'Wh'
        format_data['display_name'] = final_data['display_name']

        # 返回格式化后的数据
        return format_data


def generate_full_time_range(group):
    """
    Generate a complete range of timestamps at 20-minute intervals,
    starting from start_time + 20 minutes and ending at end_time.
    """
    # Adjust the start and end times for the group
    adjusted_start = pd.Timestamp(start_time) + pd.Timedelta(minutes=20)
    adjusted_end = pd.Timestamp(end_time)

    # Generate a range of timestamps at 20-minute intervals
    full_range = pd.date_range(start=adjusted_start, end=adjusted_end, freq='20T')
    full_df = pd.DataFrame({'timeStamp': full_range})

    # Assign the meterUid for the group
    full_df['meterUid'] = group['meterUid'].iloc[0]
    return full_df


def fill_missing_intervals(group):
    """
    Add missing rows for 20-minute intervals and merge with the original group.
    """
    full_df = generate_full_time_range(group)
    merged = pd.merge(full_df, group, on=['timeStamp', 'meterUid'], how='left')
    merged['import'] = merged['import'].replace(0, np.NaN)
    merged['export'] = merged['export'].replace(0, np.NaN)

    # Fill missing import and export values with interpolation
    merged['import'] = merged['import'].interpolate(method='linear', limit_direction='both')
    merged['export'] = merged['export'].interpolate(method='linear', limit_direction='both')

    # Convert interpolated values to integers
    merged['import'] = merged['import'].fillna(0).round().astype(int)
    merged['export'] = merged['export'].fillna(0).round().astype(int)
    # Fill other columns with forward and backward fill
    for col in ['energyUnit', 'display_name']:
        merged[col] = merged[col].ffill().bfill()

    # Recalculate intervals
    merged['interval'] = merged['timeStamp'].diff().dt.total_seconds() / 60
    merged['interval'] = merged['interval'].fillna(20)  # Fill the first interval with 20
    return merged


class DataFilling:
    def __init__(self, data):
        self.data = data
        self.data['timeStamp'] = pd.to_datetime(self.data['timeStamp'])
        self.data.sort_values(by=['meterUid', 'timeStamp'], inplace=True)
        self.data['import'].replace(pd.NA, 0)
        self.data['export'].replace(pd.NA, 0)

    def calculate_time_diff(self):
        self.data['time_diff'] = self.data.groupby('meterUid')['timeStamp'].diff().dt.total_seconds() / 60
        # Calculate time difference in minutes
        self.data['interval'] = self.data['time_diff']  # Create interval column

    def identify_gaps(self):
        self.calculate_time_diff()
        gap = self.data[self.data['time_diff'] > 60]  # Identify rows with gaps
        if not gap.empty:
            logging.info(gap[['meterUid', 'timeStamp', 'time_diff']])
        else:
            logging.info("No intervals greater than 60 minutes found.")
        return gap

    def process_by_meter(self):
        filled_data = pd.DataFrame()
        for meter_id, group in self.data.groupby('meterUid'):
            group = group.sort_index()
            filled_group = fill_missing_intervals(group)
            filled_data = pd.concat([filled_data, filled_group])

        return filled_data

    def clean_data(self):
        processedapi_data = self.process_by_meter()
        processedapi_data.sort_values(by=['meterUid', 'timeStamp'], inplace=True)
        processedapi_data['timeStamp'] = processedapi_data['timeStamp'].dt.strftime('%Y-%m-%dT%H:%M:%S+08:00')
        processedapi_data['isEstimate'] = False
        processedapi_data.reset_index(drop=True, inplace=True)
        return processedapi_data


def send_email_with_attachment(file_paths, recipient_email, sender_email, sender_password):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "DeHaviland Process Log Report"

    body = "Please find the attached process log report."
    msg.attach(MIMEText(body, 'plain'))

    if isinstance(file_paths, str):
        file_paths = [file_paths]

    for file_path in file_paths:
        if os.path.exists(file_path):
            with open(file_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
                msg.attach(part)
        else:
            logging.warning(f"File not found: {file_path}")

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)
    server.sendmail(sender_email, recipient_email, msg.as_string())
    server.quit()
    logging.info("Log file emailed successfully.")


if __name__ == "__main__":
    azure_connector = AzureConnector()
    fetched_data = azure_connector.fetch_data(start_time, end_time)
    fetched_data.drop_duplicates(subset=['DateTime', 'Meter'], inplace=True)
    unique_meters = fetched_data['Meter'].unique()

    # 123 Processing
    data_processor = DataProcessor(mapping)
    formatted_data = data_processor.format_data(fetched_data)
    cleaned_data = clean_data(formatted_data)

    # 123 Filling
    data_filling = DataFilling(cleaned_data)
    gaps = data_filling.identify_gaps()
    processed_data = data_filling.clean_data()

    # Ensure column order for the output file
    final_columns = ['meterUid', 'interval', 'timeStamp', 'import',
                     'export', 'isEstimate', 'energyUnit', 'display_name']
    processed_data = processed_data[final_columns]

    # Save processed data to file
    processed_file = f"DHdata_processed_{pd.Timestamp('now').strftime('%Y%m%d')}.csv"
    processed_data.to_csv(processed_file, index=False)

    uploader = PowerLedgerUploader(file_path=f"DHdata_processed_{pd.Timestamp('now').strftime('%Y%m%d')}.csv")
    try:
        # 认证并上传数据
        uploader.authenticate()
        uploader.process_and_upload()
        # os.remove(processed_file)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    send_email_with_attachment(log_file, "zhengliangqiu50@gmail.com",
                               EMAIL_USERNAME, EMAIL_PASSWORD)
