import pandas as pd
from datetime import datetime

input_excel = r"C:\Users\Shane\PycharmProjects\pythonProject\element 47\SJOG\all_code\DATA\filtered_30mins_data.xlsx"
output_excel = r"C:\Users\Shane\PycharmProjects\pythonProject\element 47\SJOG\all_code\DATA\output_filled_file.xlsx"

# 打开多 sheet
sheets = pd.read_excel(input_excel, sheet_name=None)

filled_sheets = {}

for sheet_name, df in sheets.items():
    if 'Timestamp' not in df.columns:
        print(f"⚠️ Sheet '{sheet_name}' skipped (no Timestamp column)")
        continue

    try:
        # 时间列转为 datetime
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        df = df.dropna(subset=['Timestamp']).copy()
        df = df.drop_duplicates(subset='Timestamp')
        df = df.sort_values('Timestamp').set_index('Timestamp')

        # 构建完整时间轴（30 分钟间隔）
        full_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='30T')

        # 重建数据框，自动补上缺失时间
        df_full = df.reindex(full_index)

        # 线性插值（只对数值列）
        df_full_interp = df_full.interpolate(method='linear')

        # 重置索引并格式化时间
        df_full_interp = df_full_interp.reset_index().rename(columns={'index': 'Timestamp'})
        df_full_interp['Timestamp'] = df_full_interp['Timestamp'].dt.strftime('%d/%m/%Y %H:%M:%S')

        # 存到字典
        filled_sheets[sheet_name] = df_full_interp

        print(f"✅ Sheet '{sheet_name}' filled and processed.")

    except Exception as e:
        print(f"❌ Error processing sheet '{sheet_name}': {e}")

# 写入新的 Excel
with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
    for name, filled_df in filled_sheets.items():
        filled_df.to_excel(writer, sheet_name=name, index=False)

print(f"\n🎉 所有 Sheet 已完成处理，输出文件: {output_excel}")

