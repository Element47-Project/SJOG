import pandas as pd

# 读取数据
file_path = 'Apollo/Data/MSB2.csv'
df = pd.read_csv(file_path, parse_dates=['DateTime'], dayfirst=True)

# 确保按时间排序
df = df.sort_values(by='DateTime')

# 创建一个完整的时间序列（30分钟间隔）
full_range_30 = pd.date_range(start=df['DateTime'].min(), end=df['DateTime'].max(), freq='30T')
df_full_30 = pd.DataFrame({'DateTime': full_range_30})

# 合并原始数据，保留15分钟的原始数据
df = pd.merge(df_full_30, df, on='DateTime', how='outer')

# 填补 `kWh_IMP` 和 `kWh_EXP` 的空白值（线性插值）
df['kWh_IMP'] = df['kWh_IMP'].interpolate(method='linear', limit_direction='both').round(3)
df['kWh_EXP'] = df['kWh_EXP'].interpolate(method='linear', limit_direction='both').round(3)

# 确保数据按时间排序
df = df.sort_values(by='DateTime')

# 设置 `Prev_kWh_IMP` 和 `Prev_kWh_EXP` 为上一条记录的值
df['Prev_kWh_IMP'] = df['kWh_IMP'].shift(1).round(3)
df['Prev_kWh_EXP'] = df['kWh_EXP'].shift(1).round(3)

# 计算 `Diff_KWH_IMP` 和 `Diff_KWH_EXP`
df['Diff_KWH_IMP'] = (df['kWh_IMP'] - df['Prev_kWh_IMP']).round(3)
df['Diff_KWH_EXP'] = (df['kWh_EXP'] - df['Prev_kWh_EXP']).round(3)

# 填充空白的 `Meter` 列
df['Meter'] = 'RMT-APL-01-MSB-MSB-01-40002624-DL1'

# 确保列顺序一致
df = df[['DateTime', 'kWh_IMP', 'Prev_kWh_IMP', 'Diff_KWH_IMP', 'kWh_EXP', 'Prev_kWh_EXP', 'Diff_KWH_EXP', 'Meter']]

# 检查数据是否存在空缺
if df.isnull().sum().sum() == 0:
    print("所有空白值已填补完成！")
else:
    print("仍有空缺值存在，请检查！")

# 保存结果
output_file = 'Apollo/Data/SMB_30.csv'
df.to_csv(output_file, index=False)

print(f"数据填补完成并保存到 {output_file}！")
