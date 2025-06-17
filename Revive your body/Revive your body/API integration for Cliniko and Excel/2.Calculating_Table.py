import os
import pandas as pd
import tempfile

# region **获取临时文件 1 的路径**
temp_dir = tempfile.gettempdir()
file_path = os.path.join(temp_dir, "Cliniko_Appointments.xlsx")  # 调用临时文件 1

# **检查临时文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
else:
    # **读取 Excel 文件**
    df = pd.read_excel(file_path)

    # **确保 'Start Time' 是 datetime 类型**
    df['Start Time'] = pd.to_datetime(df['Start Time'])

    # **提取日期（yyyy-mm-dd）并重命名为 'This Week'**
    df['This Week'] = df['Start Time'].dt.date

    # **提取星期几**
    df['Weekday'] = df['Start Time'].dt.day_name()

    # **计算每天每位 Staff Member 的总价**
    daily_summary = df.groupby(['Practitioner', 'This Week']).agg(
        Estimated_Revenue=('Price', 'sum'),
        Total_Hour_Manual_Calculation=('Duration (Minutes)', 'sum')  # 直接使用已有的列
    ).reset_index()

    # **转换 Total Hour (Manual Calculation) 为小时，保留两位小数**
    daily_summary['Total_Hour_Manual_Calculation'] = (daily_summary['Total_Hour_Manual_Calculation'] / 60).round(2)

    # **计算包含 waxing 的时长**
    df['Contains_Waxing'] = df['Appointment Type Name'].str.contains('waxing', case=False, na=False)
    waxing_summary = df[df['Contains_Waxing']].groupby(['Practitioner', 'This Week']).agg(
        Total_BAU_Bookable_Hours=('Duration (Minutes)', 'sum')
    ).reset_index()

    # **转换 Total_BAU_Bookable_Hours 为小时，保留两位小数**
    waxing_summary['Total_BAU_Bookable_Hours'] = (waxing_summary['Total_BAU_Bookable_Hours'] / 60).round(2)

    # **合并 waxing 结果，若无 waxing 则赋值为 0**
    daily_summary = daily_summary.merge(
        waxing_summary, on=['Practitioner', 'This Week'], how='left'
    ).fillna({'Total_BAU_Bookable_Hours': 0})

    # **去重合并 Appointment Type Name**
    def aggregate_appointment_names(names):
        """
        统计相同类型的预约，并格式化为 '2x 60 Minute Deep Tissue Massage' 这种形式。
        """
        name_counts = names.value_counts()
        return "\n".join([f"{count}x {name}" for name, count in name_counts.items()])

    appointment_summary = df.groupby(['Practitioner', 'This Week'])['Appointment Type Name'].apply(aggregate_appointment_names).reset_index()
    daily_summary = daily_summary.merge(appointment_summary, on=['Practitioner', 'This Week'], how='left')

    # **追加星期几信息**
    daily_summary = daily_summary.merge(df[['This Week', 'Weekday']].drop_duplicates(), on='This Week', how='left')

    # **重命名列**
    daily_summary.rename(columns={
        'Practitioner': 'Staff Member',
        'Estimated_Revenue': 'Estimated Revenue',
        'Total_Hour_Manual_Calculation': 'Total Hour (Manual Calculation)'
    }, inplace=True)

    # **重新排序列**
    daily_summary = daily_summary[['Staff Member', 'This Week', 'Weekday', 'Estimated Revenue',
                                   'Total Hour (Manual Calculation)', 'Total_BAU_Bookable_Hours', 'Appointment Type Name']]

    # **保存处理结果到临时文件**
    output_file = os.path.join(temp_dir, "Processed_Cliniko_Appointments.xlsx")
    daily_summary.to_excel(output_file, index=False)

    print(f"✅ 处理完成，结果已保存至临时文件: {output_file}")
# endregion

#region **获取系统临时目录 2**
# **获取系统临时目录**
temp_dir = tempfile.gettempdir()
file_path = os.path.join(temp_dir, "Cliniko_Appointments_2.xlsx")  # 读取临时文件

# **检查文件是否存在**
if not os.path.exists(file_path):
    raise FileNotFoundError(f"❌ 临时文件未找到: {file_path}")

# **读取 Excel 数据**
df = pd.read_excel(file_path, engine="openpyxl")

# **去除列名的空格**
df.columns = df.columns.str.strip()

# **检查 'Start Time' 和 'Total Amount' 是否存在**
required_columns = ["Start Time", "Practitioner", "Total Amount"]
for col in required_columns:
    if col not in df.columns:
        raise KeyError(f"❌ 未找到 '{col}' 列，请检查 Excel 文件。")

# **提取所需列**
df_filtered = df[["Start Time", "Practitioner", "Total Amount"]].copy()

# **转换 'Start Time' 格式**
df_filtered.rename(columns={"Start Time": "Date", "Total Amount": "Actual Revenue"}, inplace=True)  # 重命名
df_filtered["Date"] = pd.to_datetime(df_filtered["Date"]).dt.date  # 仅保留 YYYY-MM-DD

# **覆盖原始文件**
df_filtered.to_excel(file_path, index=False, engine="openpyxl")

print(f"✅ 处理完成，数据已覆盖至: {file_path}")

#endregion

# region **获取临时文件 2 的路径**
# **获取临时文件路径**
temp_dir = tempfile.gettempdir()
file_path = os.path.join(temp_dir, "Cliniko_Appointments_2.xlsx")  # 调用临时文件 2

# **检查临时文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
else:
    # **读取 Excel 文件**
    df = pd.read_excel(file_path)

    # **打印原始列名，确保 'Date' 存在**
    print("📊 原始列名:", df.columns.tolist())

    # **去除列名的前后空格**
    df.columns = df.columns.str.strip()

    # **检查并重命名 Date → Last Week**
    if 'Date' in df.columns:
        df.rename(columns={'Date': 'Last Week'}, inplace=True)
    elif 'date' in df.columns:
        df.rename(columns={'date': 'Last Week'}, inplace=True)
    else:
        raise KeyError("❌ 没有找到 'Date' 列，请检查 Excel 文件的列名！")

    # **确保 'Last Week' 是 datetime 格式**
    df['Last Week'] = pd.to_datetime(df['Last Week'], errors='coerce')

    # **确保 'Actual Revenue' 是数值格式**
    df['Actual Revenue'] = pd.to_numeric(df['Actual Revenue'], errors='coerce').fillna(0)

    # **按天计算总价（按 Last Week + Practitioner 分组求和）**
    df_grouped = df.groupby(['Last Week', 'Practitioner'], as_index=False).agg({
        'Actual Revenue': 'sum'  # 按天求和
    })

    # **添加 Weekday2（星期几）**
    df_grouped['Weekday2'] = df_grouped['Last Week'].dt.day_name()

    # **打印修改后的列名，确保 'Last Week' 存在**
    print("📊 修改后列名:", df_grouped.columns.tolist())

    # **打印转换后的数据类型**
    print("🔍 转换后数据类型:\n", df_grouped.dtypes)

    # **保存处理结果到新的临时文件**
    output_file = os.path.join(temp_dir, "Updated_Staff_Total_Amount.xlsx")
    df_grouped.to_excel(output_file, index=False)

    print(f"✅ 处理完成，结果已保存至临时文件: {output_file}")

#endregion

# region 读取两个 Excel 文件

# **获取系统临时目录**
temp_dir = tempfile.gettempdir()

# **设置临时文件路径**
file_this_week = os.path.join(temp_dir, "Processed_Cliniko_Appointments.xlsx")  # 临时文件 1
file_last_week = os.path.join(temp_dir, "Updated_Staff_Total_Amount.xlsx")  # 临时文件 2

# **检查临时文件是否存在**
if not os.path.exists(file_this_week):
    print(f"❌ 临时文件 1 未找到: {file_this_week}")
if not os.path.exists(file_last_week):
    print(f"❌ 临时文件 2 未找到: {file_last_week}")

if os.path.exists(file_this_week) and os.path.exists(file_last_week):
    # **读取两个 Excel 文件**
    df_this_week = pd.read_excel(file_this_week)  # 本周数据
    df_last_week = pd.read_excel(file_last_week)  # 上周数据

    # **去除所有列名的空格**
    df_this_week.columns = df_this_week.columns.str.strip()
    df_last_week.columns = df_last_week.columns.str.strip()

    # **重命名列以匹配**
    df_last_week.rename(columns={'Practitioner': 'Staff Member'}, inplace=True)

    # **确保日期列是 datetime 格式，并转换为 yyyy-mm-dd**
    df_this_week['This Week'] = pd.to_datetime(df_this_week['This Week']).dt.date
    df_last_week['Last Week'] = pd.to_datetime(df_last_week['Last Week']).dt.date

    # **保留 Weekday2 作为 Last Week 的 Weekday**
    df_last_week.rename(columns={'Weekday2': 'Last Weekday'}, inplace=True)

    # **筛选出需要的列**
    df_last_week = df_last_week[['Staff Member', 'Last Weekday', 'Last Week', 'Actual Revenue']]
    df_this_week = df_this_week[['Staff Member', 'This Week', 'Weekday', 'Estimated Revenue',
                                 'Total Hour (Manual Calculation)', 'Total_BAU_Bookable_Hours', 'Appointment Type Name']]

    # **生成完整的一周日期范围**
    all_weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # **获取所有 Staff Member**
    all_staff = set(df_this_week['Staff Member']).union(set(df_last_week['Staff Member']))

    # **创建完整的 DataFrame 结构**
    full_week_data = pd.DataFrame([(staff, day) for staff in all_staff for day in all_weekdays], columns=['Staff Member', 'Weekday'])

    # **合并数据**
    df_merged = full_week_data.merge(df_this_week, on=['Staff Member', 'Weekday'], how='left') \
                              .merge(df_last_week, left_on=['Staff Member', 'Weekday'], right_on=['Staff Member', 'Last Weekday'], how='left')

    # **填充空缺数据**
    df_merged.fillna({'This Week': 'Not work', 'Last Week': 'Not work', 'Estimated Revenue': 'Not work',
                      'Total Hour (Manual Calculation)': 'Not work', 'Total_BAU_Bookable_Hours': 'Not work',
                      'Appointment Type Name': 'Not work', 'Actual Revenue': 'Not work'}, inplace=True)

    # **调整列顺序**
    df_merged = df_merged[['Staff Member', 'This Week', 'Weekday', 'Estimated Revenue', 'Total Hour (Manual Calculation)',
                           'Total_BAU_Bookable_Hours', 'Appointment Type Name', 'Last Week', 'Last Weekday', 'Actual Revenue']]

    # **保存处理结果到临时文件**
    output_file = os.path.join(temp_dir, "Complete_Staff_Total_Amount.xlsx")
    df_merged.to_excel(output_file, index=False)

    print(f"✅ 处理完成，完整一周数据已保存至临时文件: {output_file}")


#endregion¥

#region 统计周KPI **获取系统临时目录获取系统临时目录**
temp_dir = tempfile.gettempdir()

# **读取临时文件**
file_path = os.path.join(temp_dir, "Complete_Staff_Total_Amount.xlsx")
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
    exit()

# **读取数据**
df = pd.read_excel(file_path)
df.columns = df.columns.str.strip()  # 清理列名空格

# **打印原始列名**
print("📊 原始列名:", df.columns.tolist())

# **确保 These Columns 是数值类型**
numeric_cols = ["Estimated Revenue", "Total Hour (Manual Calculation)", "Total_BAU_Bookable_Hours", "Actual Revenue"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)  # 转换为数值并填充 NaN

# **打印转换后的数据类型**
print("📊 转换后数据类型:\n", df.dtypes)

# **确保 This Week 是日期**
df['This Week'] = pd.to_datetime(df['This Week'], format='%Y-%m-%d', errors='coerce')

# **计算 Start from 和 End at**
df['Start from'] = df['This Week'] - pd.to_timedelta(df['This Week'].dt.weekday, unit='D')
df['End at'] = df['Start from'] + pd.Timedelta(days=6)

# **按 Staff Member 计算合计**
summary = df.groupby('Staff Member').agg({
    'Estimated Revenue': 'sum',
    'Total Hour (Manual Calculation)': 'sum',
    'Total_BAU_Bookable_Hours': 'sum',
    'Actual Revenue': 'sum'
}).reset_index()

# **合并 Start from 和 End at**
date_summary = df.groupby('Staff Member').agg({'Start from': 'min', 'End at': 'min'}).reset_index()
summary = summary.merge(date_summary, on='Staff Member', how='left')

# **转换 `Start from` 和 `End at` 格式**
summary['Start from'] = summary['Start from'].dt.date
summary['End at'] = summary['End at'].dt.date

# **合并 Appointment Type Name**
def merge_appointments(appt_list):
    """合并相同类型的预约，计算次数"""
    counts = {}
    for appt in appt_list.dropna():
        parts = appt.split("\n")  # 按换行分割多个预约类型
        for p in parts:
            if p in counts:
                counts[p] += 1
            else:
                counts[p] = 1
    return "\n".join([f"{v}x {k}" if v > 1 else k for k, v in counts.items()])

appt_summary = df.groupby('Staff Member')['Appointment Type Name'].apply(merge_appointments).reset_index()
summary = summary.merge(appt_summary, on='Staff Member', how='left')

# **创建 Total Bookable Hours 和 Total BAU Bookable Hours**
bookable_hours = {
    "Alyssia Strauss (Director, $10 Extra Female therapist)": (32.5, 21),
    "Ana-maria Van den Aakster": (21, 13.5),
    "Andrew Haning (snr Male Therapist)": (32.5, 32.5),
    "Kelly Street (Snr Female Therapist)": (10, 10),
    "Lourdes Barros  (snr, female therapist)": (17, 17),
    "Martin Allen (snr Male Therapist)": (6.5, 6.5),
    "Rainer Puhmas (snr Male Therapist)": (23, 23),
    "Sayuri Hirakawa (snr Female Therapist)": (32.5, 22),
    "Sunny Perera (snr male therapist)": (32.5, 21),
    "Waka Iguchi (snr, female Therapist)": (32.5, 20.5),
}

# **填充 Total Bookable Hours 和 Total BAU Bookable Hours**
summary["Total Bookable Hours"] = summary["Staff Member"].map(lambda x: bookable_hours.get(x, (0, 0))[0])
summary["Total BAU Bookable Hours"] = summary["Staff Member"].map(lambda x: bookable_hours.get(x, (0, 0))[1])

# **调整列顺序**
summary = summary[['Staff Member', 'Start from', 'End at', 'Estimated Revenue', 'Total Hour (Manual Calculation)',
                   'Total_BAU_Bookable_Hours', 'Actual Revenue', 'Appointment Type Name',
                   'Total Bookable Hours', 'Total BAU Bookable Hours']]

# **保存处理结果到新的临时文件**
output_file = os.path.join(temp_dir, "Weekly_Staff_Summary.xlsx")
summary.to_excel(output_file, index=False)

print(f"✅ 处理完成，结果已保存至临时文件: {output_file}")


file_path = "/var/folders/6p/lnvklhvj1d97_cym59v2vx9r0000gn/T/Weekly_Staff_Summary.xlsx"
df = pd.read_excel(file_path)
print(df.head())
#endregion

# region 获取系统临时目录**
temp_dir = tempfile.gettempdir()

# **设置临时文件路径**
file_path = os.path.join(temp_dir, "Weekly_Staff_Summary.xlsx")  # 读取临时文件

# **检查临时文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
else:
    # **读取 Excel 文件**
    df = pd.read_excel(file_path)

    # **去除所有列名的空格**
    df.columns = df.columns.str.strip()

    # **转换 Start from 和 End at 为 YYYY-MM-DD**
    df['Start from'] = pd.to_datetime(df['Start from'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['End at'] = pd.to_datetime(df['End at'], errors='coerce').dt.strftime('%Y-%m-%d')

    # **计算 BAU Capacity**
    df["BAU Capacity"] = df.apply(
        lambda row: f"{int((row['Total Hour (Manual Calculation)'] / row['Total Bookable Hours']) * 100)}%"
        if row['Total Bookable Hours'] > 0 else "", axis=1
    )

    # **计算 Actual Capacity**
    df["Actual Capacity"] = df.apply(
        lambda row: f"{int((row['Total Hour (Manual Calculation)'] / row['Total BAU Bookable Hours']) * 100)}%"
        if row['Total BAU Bookable Hours'] > 0 else "", axis=1
    )

    # **调整列顺序，保持原始列顺序，仅在最后添加新列**
    df = df[['Staff Member', 'Start from', 'End at', 'Estimated Revenue', 'Total Hour (Manual Calculation)',
             'Total_BAU_Bookable_Hours', 'Actual Revenue', 'Appointment Type Name', 'Total Bookable Hours',
             'Total BAU Bookable Hours', 'BAU Capacity', 'Actual Capacity']]

    # **保存处理结果到新的临时文件**
    output_file = os.path.join(temp_dir, "Weekly_Staff_Summary_Updated.xlsx")
    df.to_excel(output_file, index=False)

    print(f"✅ 处理完成，已添加 BAU Capacity 和 Actual Capacity，日期格式已修正，结果已保存至临时文件: {output_file}")

#endregion

#region 获取系统临时目录**
temp_dir = tempfile.gettempdir()

# **设置临时文件路径**
file_path = os.path.join(temp_dir, "Weekly_Staff_Summary_Updated.xlsx")  # 读取临时文件

# **检查临时文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
else:
    # **读取 Excel 文件**
    summary = pd.read_excel(file_path)

    # **确保列名没有空格**
    summary.columns = summary.columns.str.strip()

    # **计算 TOTAL 行**
    total_row = pd.DataFrame([{
        "Staff Member": "TOTAL",
        "Estimated Revenue": summary["Estimated Revenue"].sum(),
        "Total Hour (Manual Calculation)": summary["Total Hour (Manual Calculation)"].sum(),
        "Total BAU Bookable Hours": summary["Total BAU Bookable Hours"].sum(),
        "Total Bookable Hours": summary["Total Bookable Hours"].sum(),
        "Actual Revenue": summary["Actual Revenue"].sum(),
        "BAU Capacity": "",  # 这个列不计算，留空
        "Actual Capacity": ""  # 这个列不计算，留空
    }])

    # **添加 TOTAL 行**
    summary = pd.concat([summary, total_row], ignore_index=True)

    # **计算 Total Percentage Booked of Available Hours**
    if summary.loc[summary["Staff Member"] == "TOTAL", "Total BAU Bookable Hours"].values[0] > 0:
        total_percentage = f"{int((summary.loc[summary['Staff Member'] == 'TOTAL', 'Total Hour (Manual Calculation)'].values[0] / summary.loc[summary['Staff Member'] == 'TOTAL', 'Total BAU Bookable Hours'].values[0]) * 100)}%"
    else:
        total_percentage = ""

    total_percentage_row = pd.DataFrame([{
        "Staff Member": "Total Percentage Booked of Available Hours",
        "Actual Capacity": total_percentage
    }])

    # **添加 Total Percentage Booked of Available Hours 行**
    summary = pd.concat([summary, total_percentage_row], ignore_index=True)

    # **保存回临时文件**
    output_file = os.path.join(temp_dir, "Weekly_Staff_Summary_Updated.xlsx")
    summary.to_excel(output_file, index=False)

    print(f"✅ 处理完成，已成功添加 TOTAL 和 Total Percentage Booked of Available Hours，结果已保存至临时文件: {output_file}")

#endregion

# region 读取 Excel 文件 *获取系统临时目录**
temp_dir = tempfile.gettempdir()

# **设置临时文件路径**
file_path = os.path.join(temp_dir, "Weekly_Staff_Summary_Updated.xlsx")  # 读取临时文件

# **检查临时文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
else:
    # **读取 Excel 文件**
    df = pd.read_excel(file_path)

    # **去除所有列名的空格**
    df.columns = df.columns.str.strip()

    # **计算 Last Week Actual Revenue Rate**
    df["Last Week Actual Revenue Rate"] = df.apply(
        lambda row: f"{int((row['Actual Revenue'] / (row['Total Bookable Hours'] * 115)) * 100)}%"
        if row["Total Bookable Hours"] > 0 and row["Actual Revenue"] > 0 else "", axis=1
    )

    # **调整列顺序，确保所有列都保持不变**
    df = df[['Staff Member', 'Start from', 'End at', 'Estimated Revenue', 'Total Hour (Manual Calculation)',
             'Total_BAU_Bookable_Hours', 'Actual Revenue', 'Appointment Type Name', 'Total Bookable Hours',
             'Total BAU Bookable Hours', 'BAU Capacity', 'Actual Capacity', 'Last Week Actual Revenue Rate']]

    # **保存回临时文件**
    output_file = os.path.join(temp_dir, "Weekly_Staff_KPI_Summary_Updated.xlsx")
    df.to_excel(output_file, index=False)

    print(f"✅ 处理完成，结果已保存至临时文件: {output_file}")
#endregion





