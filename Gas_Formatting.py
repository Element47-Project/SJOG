import pandas as pd
from datetime import datetime


# 日期转换函数
def convert_date_format(value):
    if pd.isnull(value):
        return value
    try:
        value_str = str(value).strip().replace(".", "").lower()
        dt = datetime.strptime(value_str, "%d/%m/%Y %I:%M %p")
        return dt.strftime("%Y-%m-%d %H:%M:%S.000")
    except Exception as e:
        print(f"Error：{value} → {e}")
        return value


# gas 数据处理函数
def gas_consumption(df, site):
    if site == "Midland":
        fixed_columns = {
            'ACCOUNT NUMBER': 605628,
            'ACNAME': 'SJG Midland Hospital',
            'NMI': 56009523942,
            'METER': 'M1600IR003',
            'SITE ADDRESS': '1 Clayton Street Midland, WA, 6156',
        }

        # 重命名列
        df_renamed = df.rename(columns={
            'Date': 'END INTERVAL',
            'Carbon Neutral Charge': 'GAS (GJ)'
        })

        # 转换日期格式
        df_renamed['END INTERVAL'] = df_renamed['END INTERVAL'].astype(str).apply(convert_date_format)

        # 添加固定列
        for col_name, col_value in fixed_columns.items():
            df_renamed[col_name] = col_value

        # 重排序列
        final_columns = list(fixed_columns.keys()) + ['END INTERVAL', 'GAS (GJ)']
        df_final = df_renamed[final_columns]
        df_final['Site'] = site

        return df_final
