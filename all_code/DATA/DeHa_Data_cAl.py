import pandas as pd
import os
from datetime import datetime


def strict_ddmmyyyy_parser(date_str):
    """只解析 dd/mm/yyyy 格式的时间字符串"""
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(date_str.strip(), "%m/%d/%Y %H:%M")
        except:
            return pd.NaT


# 主处理流程
folder_path = r"C:\Users\Shane\PycharmProjects\pythonProject\element 47\SJOG\all_code\DATA"
output_excel = r"C:\Users\Shane\PycharmProjects\pythonProject\element 47\SJOG\all_code\DATA\filtered_30mins_data.xlsx"

all_data = []

with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            meter = os.path.splitext(filename)[0]
            print(f"\n📂 Processing: {filename}")

            try:
                # 读取CSV并清理数据
                df = pd.read_csv(file_path)
                df['Timestamp'] = df['Timestamp'].astype(str).str.strip()

                # 严格按照 dd/mm/yyyy 格式解析时间
                df['Parsed_Time'] = df['Timestamp'].apply(strict_ddmmyyyy_parser)
                df = df[df['Parsed_Time'].notna()].copy()

                # 保留整点或半点数据
                df_filtered = df[df['Parsed_Time'].dt.minute.isin([0, 30])].copy()

                # 计算用电差值
                if "Imported Total reading (kWh)" in df.columns:
                    df_filtered["Imported (kWh)"] = (
                        df_filtered["Imported Total reading (kWh)"]
                        .diff()
                        .clip(lower=0)
                    )

                # 使用格式化后的时间，列名叫 Timestamp
                df_filtered["Timestamp"] = df_filtered["Parsed_Time"].dt.strftime("%d/%m/%Y %H:%M:%S")
                df_filtered.drop(columns=["Parsed_Time"], inplace=True)

                # 存储汇总数据
                if "Imported (kWh)" in df_filtered.columns:
                    temp_df = df_filtered[["Timestamp", "Imported (kWh)"]].copy()
                    # 恢复 datetime 格式用于 groupby
                    temp_df["Timestamp"] = pd.to_datetime(temp_df["Timestamp"], format="%d/%m/%Y %H:%M:%S")
                    all_data.append(temp_df)

                # 写入 Excel（保留 Timestamp 列）
                df_filtered.to_excel(writer, sheet_name=meter[:31], index=False)

            except Exception as e:
                print(f"❌ 处理失败: {filename}\n错误: {e}")

    # # 汇总生成 Total Building Load Pivot 表
    # if all_data:
    #     combined = pd.concat(all_data)
    #     combined["Date"] = combined["Timestamp"].dt.strftime("%d/%m/%Y")
    #     combined["Time"] = combined["Timestamp"].dt.strftime("%H:%M:%S")
    #
    #     total_load = combined.groupby(["Date", "Time"])["Imported (kWh)"].sum().reset_index()
    #     pivot_load = total_load.pivot(index="Date", columns="Time", values="Imported (kWh)")
    #     pivot_load.to_excel(writer, sheet_name="Total Building Load (Pivot)")
