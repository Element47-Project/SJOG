import os
import pandas as pd
import re
import logging
from datetime import datetime
import shutil

INPUT_DATA_DIR = 'data'
OUTPUT_DATA_DIR = 'format_data'
FINISHED_DATA_DIR = 'data_finished'

# Set up logging
logging.basicConfig(
    filename='meter_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_unit_name(row):
    # Skip SMSB rows for now
    if 'SMSB' in str(row['unit_name']):
        return None, None
        
    # Extract cluster and lot number using regex
    match = re.search(r'(\d+[A-Za-z])\s*(?:LOT|Lot|lot)?\s*(\d+)', row['unit_name'])
    if match:
        cluster = match.group(1).upper()  # Convert to uppercase
        lot_no = f"{int(match.group(2)):02d}"  # Format lot number with leading zero
        return cluster, lot_no
    return None, None

def get_meter_readings(row):
    # Check each possible meter column pair
    if row['Power Meter Import'] != 0 or row['Power Meter Export'] != 0:
        return row['Power Meter Import'], row['Power Meter Export']
    elif row['Power Meter 1 Import'] != 0 or row['Power Meter 1 Export'] != 0:
        return row['Power Meter 1 Import'], row['Power Meter 1 Export']
    elif row['CETA Meter Import'] != 0 or row['CETA Meter Export'] != 0:
        return row['CETA Meter Import'], row['CETA Meter Export']
    return 0, 0

def process_smsb_data(df):
    smsb_records = []
    
    # Process SMSB rows
    smsb_df = df[df['unit_name'].str.contains('SMSB', na=False)]
    
    for _, row in smsb_df.iterrows():
        cluster = row['unit_name'].split()[1]  # Get cluster number
        
        # Create EV record if exists
        if row['EV Import'] != 0 or row['EV Export'] != 0:
            smsb_records.append({
                'Cluster': cluster,
                'Point': 'COMMON_EV',
                'Reading Time': row['Period End'],
                'Import': row['EV Import'],
                'Export': row['EV Export']
            })
            
        # Create CS (House Services) record if exists
        if row['House Services Import'] != 0 or row['House Services Export'] != 0:
            smsb_records.append({
                'Cluster': cluster,
                'Point': 'COMMON_SERVICES',
                'Reading Time': row['Period End'],
                'Import': row['House Services Import'],
                'Export': row['House Services Export']
            })
            
        # Create BESS record if exists
        if (row['BESS Import'] != 0 or row['BESS Export'] != 0 or 
            row['BESS Meter Import'] != 0 or row['BESS Meter Export'] != 0):
            # 获取所有可能的BESS值
            bess_values = [
                row['BESS Import'] or 0,
                row['BESS Export'] or 0,
                row['BESS Meter Import'] or 0,
                row['BESS Meter Export'] or 0
            ]
            # 过滤掉0值
            bess_values = [v for v in bess_values if v != 0]
            
            if bess_values:  # 如果有非零值
                max_value = max(bess_values)
                min_value = min(bess_values)
                
                smsb_records.append({
                    'Cluster': cluster,
                    'Point': 'COMMON_BESS',
                    'Reading Time': row['Period End'],
                    'Import': max_value,  # 较大值作为import
                    'Export': min_value   # 较小值作为export
                })
    
    return pd.DataFrame(smsb_records)

def get_most_common_start_time(df):
    """Get the most common start time from the dataset"""
    return df['Period Start'].mode().iloc[0]

def process_meter_data():
    # Create necessary directories if they don't exist
    for directory in [OUTPUT_DATA_DIR, FINISHED_DATA_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
    
    # Check input directory
    if not os.path.exists(INPUT_DATA_DIR):
        raise FileNotFoundError(f"Data directory not found: {INPUT_DATA_DIR}")
    
    # Get all CSV files from input directory
    csv_files = [f for f in os.listdir(INPUT_DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("No CSV files found in data directory")
    
    # 创建所有可能的住户列表
    expected_units = {
        '1A': [f"{i:02d}" for i in range(1, 25)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2'],
        '1B': [f"{i:02d}" for i in range(1, 22)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2'],
        '1C': [f"{i:02d}" for i in range(1, 20)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2'],
        '2A': [f"{i:02d}" for i in range(1, 24)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2'],
        '2B': [f"{i:02d}" for i in range(1, 23)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2'],
        '3A': [f"{i:02d}" for i in range(1, 28)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2'],
        '3B': [f"{i:02d}" for i in range(1, 20)] + ['COMMON_BESS', 'COMMON_EV', 'COMMON_SERVICES', 'COMMON_SERVICES_DB1', 'COMMON_SERVICES_DB2']
    }
    
    # Process each CSV file
    for input_filename in csv_files:
        input_path = os.path.join(INPUT_DATA_DIR, input_filename)
        logging.info(f"Processing file: {input_path}")
        
        # Read the CSV file
        df = pd.read_csv(input_path)
        
        # Convert datetime columns
        df['Period Start'] = pd.to_datetime(df['Period Start'])
        df['Period End'] = pd.to_datetime(df['Period End'])
        
        # Track points with adjusted dates
        adjusted_points = []
        
        # Process regular lots
        processed_records = []
        regular_df = df[~df['unit_name'].str.contains('SMSB', na=False)].copy()
        
        # 添加标准化的cluster和lot_no列
        regular_df[['cluster', 'lot_no']] = regular_df.apply(
            lambda row: pd.Series(process_unit_name(row)), 
            axis=1
        )
        
        # 按cluster和lot_no分组
        for (cluster, lot_no), group in regular_df.groupby(['cluster', 'lot_no']):
            if pd.notna(cluster) and pd.notna(lot_no):  # 确保cluster和lot_no不是None
                if len(group) > 1:
                    logging.warning(
                        f"Multiple records found for unit {cluster}-{lot_no}:\n"
                        f"  - Original unit names: {group['unit_name'].tolist()}\n"
                        f"  - Number of records: {len(group)}\n"
                        f"  - Reading times: {group['Period End'].tolist()}"
                    )
                
                # 获取最新的读表记录
                latest_record = group.loc[group['Period End'].idxmax()]
                import_value, export_value = get_meter_readings(latest_record)
                
                processed_records.append({
                    'Cluster': cluster,
                    'Point': lot_no,
                    'Reading Time': latest_record['Period End'],
                    'Import': import_value,
                    'Export': export_value
                })
        
        # Log all date adjustments for this file
        if adjusted_points:
            logging.info(f"Date adjustments in {input_filename}:")
            for adj in adjusted_points:
                logging.info(f"  {adj['point']}: {adj['original_start']} -> {adj['adjusted_to']}")
        
        # Process SMSB data
        smsb_records = []
        smsb_df = process_smsb_data(df)
        
        # Create final DataFrame
        processed_df = pd.DataFrame(processed_records)
        
        # 只有在smsb_records不为空时才创建和合并smsb_df
        if len(smsb_df) > 0:
            final_df = pd.concat([processed_df, smsb_df], ignore_index=True)
        else:
            final_df = processed_df
            
        final_df = final_df.sort_values(['Cluster', 'Point'])
        
        # 在生成final_df之后，补充缺失的住户记录
        all_records = []
        reading_time = final_df['Reading Time'].max()  # 使用当前文件的最新时间
        
        # 遍历所有预期的住户
        for cluster, lots in expected_units.items():
            for lot in lots:
                # 检查是否已存在记录
                existing = final_df[
                    (final_df['Cluster'] == cluster) & 
                    (final_df['Point'] == lot)
                ]
                
                if len(existing) == 0:
                    # 添加空值记录
                    all_records.append({
                        'Cluster': cluster,
                        'Point': lot,
                        'Reading Time': reading_time,
                        'Import': None,  # 或者用0，取决于业务需求
                        'Export': None   # 或者用0，取决于业务需求
                    })
        
        # 合并现有记录和空值记录
        if all_records:
            empty_df = pd.DataFrame(all_records)
            # 确保empty_df的数据类型与final_df匹配
            empty_df = empty_df.astype({
                'Cluster': final_df['Cluster'].dtype,
                'Point': final_df['Point'].dtype,
                'Reading Time': final_df['Reading Time'].dtype,
                'Import': final_df['Import'].dtype,
                'Export': final_df['Export'].dtype
            })
            final_df = pd.concat([final_df, empty_df], ignore_index=True)
            final_df = final_df.sort_values(['Cluster', 'Point'])
        
        # Create output filename based on reading time
        end_date = final_df['Reading Time'].max().strftime('%d%m%y')
        output_file = f"wev_{end_date}.csv"
        output_path = os.path.join(OUTPUT_DATA_DIR, output_file)
        
        # Save processed data
        final_df.to_csv(output_path, index=False)
        logging.info(f"Processed data saved to {output_path}")
        
        # Move processed file to finished directory
        finished_path = os.path.join(FINISHED_DATA_DIR, input_filename)
        shutil.move(input_path, finished_path)
        logging.info(f"Moved {input_filename} to {FINISHED_DATA_DIR}")
        
        print(f"Processed {input_filename}")
    
    print(f"Processing complete. Check {OUTPUT_DATA_DIR} for results and meter_processing.log for details")

if __name__ == "__main__":
    process_meter_data() 