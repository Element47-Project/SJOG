import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import numpy as np

log_file = os.path.join("logs", "process_log.log")
end_time = datetime.now().strftime('%Y-%m-%d 00:00:00')
start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_file, filemode='w')


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


def clean_data(data):
    # Convert timestamp and sort
    data['timeStamp'] = pd.to_datetime(data['timeStamp'])
    data.sort_values(by=['meterUid', 'timeStamp'], inplace=True)

    cleaned_data = pd.DataFrame()

    for meter, group in data.groupby('meterUid'):
        # Calculate differences
        group['import_diff'] = group['import'].diff()
        group['export_diff'] = group['export'].diff()
        group['time_diff'] = group['timeStamp'].diff().dt.total_seconds() / 60

        # Validate data
        valid_mask = (
                (group['import_diff'] >= 0) &
                (group['import_diff'] <= 200000) &
                (group['export_diff'] >= 0) &
                (group['export_diff'] <= 200000) &
                ((group['time_diff'] >= 20) | (group['time_diff'].isna()))  # Allow ~20±5 min intervals
        )

        cleaned_data = pd.concat([cleaned_data, group[valid_mask]])

    return cleaned_data.reset_index(drop=True)


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


def generate_full_time_range(group):
    """Generate complete 20-minute intervals for the group's time range"""
    group_start = group['timeStamp'].min().floor('20T')
    group_end = group['timeStamp'].max().ceil('20T')
    full_range = pd.date_range(start=group_start, end=group_end, freq='20T')
    full_df = pd.DataFrame({'timeStamp': full_range})
    full_df['meterUid'] = group['meterUid'].iloc[0]
    return full_df


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
