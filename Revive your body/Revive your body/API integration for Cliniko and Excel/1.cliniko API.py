import requests
import pandas as pd
import openpyxl
import base64
from datetime import datetime, timedelta
from datetime import datetime, timezone
import time  # ✅ 添加延迟
import pandas as pd
import tempfile
import os
import pytz
import re
import tempfile

#region Cliniko API 配置
API_KEY = "MS0xNjE5MzQzNjU3NzUwNTAxNDcyLTNFV3ZlQXRCdVZSVjhabHpWaVJtelZzcWlSUTZPOS8r-au2"  # 请确保 API Key 无误
BASE_URL = "https://api.au2.cliniko.com/v1/appointments"  # 使用正确的 API 端点
PER_PAGE = 100
MAX_PAGES = 50

# **生成 Basic 认证**
auth_string = f"{API_KEY}:"
base64_auth = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

headers = {
    "Authorization": f"Basic {base64_auth}",
    "User-Agent": "MyClinikoApp (p.luo@element47.com.au)",  # 符合 API 要求
    "Accept": "application/json"  # 确保 API 返回 JSON
}

#endregion

#region Table 1 获取 Cliniko 预约数据并保存 Excel

def fetch_appointments_for_date():
    """ 获取 Cliniko 预约数据，并保存 Excel 文件 """
    print("📤 发送请求到 Cliniko API 获取预约数据...")

    # **用户输入 Perth 时间**
    start_date_input = input("请输入开始日期 (YYYY-MM-DD): ")
    end_date_input = input("请输入结束日期 (YYYY-MM-DD): ")

    try:
        # **定义 Perth 时区**
        perth_tz = pytz.timezone("Australia/Perth")

        # **将输入的 Perth 时间转换为 UTC**
        start_date_perth = datetime.strptime(start_date_input, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
        start_date_utc = perth_tz.localize(start_date_perth).astimezone(pytz.utc).isoformat()

        end_date_perth = datetime.strptime(end_date_input, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        end_date_utc = perth_tz.localize(end_date_perth).astimezone(pytz.utc).isoformat()

    except ValueError:
        print("❌ 输入的日期格式不正确，请使用 YYYY-MM-DD")
        return

    print(f"🔎 查询时间范围 (Perth): {start_date_perth} ~ {end_date_perth}")
    print(f"🔎 查询时间范围 (UTC): {start_date_utc} ~ {end_date_utc}")

    all_appointments = []
    page = 1
    request_count = 0

    while True:
        # **使用转换后的 UTC 时间**
        url = f"{BASE_URL}?per_page={PER_PAGE}&page={page}&start_date={start_date_utc}&end_date={end_date_utc}"
        print(f"📥 获取第 {page} 页数据... 🌐 URL: {url}")

        response = requests.get(url, headers=headers)
        request_count += 1

        if response.status_code == 429:
            print("⚠️ 请求过多，等待 10 秒...")
            time.sleep(10)
            continue

        if response.status_code != 200:
            print(f"❌ 请求失败，状态码: {response.status_code}")
            print(f"📝 API 响应内容: {response.text}")
            return

        data = response.json()
        appointments = data.get("appointments", [])

        if not appointments:
            break

        all_appointments.extend(appointments)
        page += 1

        if request_count % 100 == 0:
            print("🕒 等待 1 秒防止 Cliniko 速率限制...")
            time.sleep(1)

    if not all_appointments:
        print(f"⚠️ Cliniko API 没有返回 {start_date_input} ~ {end_date_input} 的预约数据，请检查 API 过滤条件")
        return

    # **转换为 Pandas DataFrame**
    df = pd.json_normalize(all_appointments)

    # **字段映射**
    rename_map = {
        "appointment_start": "Start Time",
        "appointment_end": "End Time",
        "patient_arrived": "Patient Arrived",
        "appointment_type.links.self": "Appointment Type URL",
        "practitioner.links.self": "Practitioner API URL",
        "invoices.links.self": "Invoice URL"  # **新增 Invoice URL**
    }
    df.rename(columns=rename_map, inplace=True)

    # **选择保留的列**
    required_columns = ["Start Time", "End Time", "Appointment Type URL", "Patient Arrived", "Practitioner API URL",
                        "Invoice URL"]
    df = df[[col for col in required_columns if col in df.columns]]

    # **转换 API 返回的 UTC 时间**
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce", utc=True)
    df["End Time"] = pd.to_datetime(df["End Time"], errors="coerce", utc=True)

    # **转换 UTC → Perth 时间**
    df["Start Time"] = df["Start Time"].dt.tz_convert("Australia/Perth").dt.tz_localize(None)
    df["End Time"] = df["End Time"].dt.tz_convert("Australia/Perth").dt.tz_localize(None)

    # **筛选目标日期的预约**
    df_filtered = df[
        (df["Start Time"].dt.date >= start_date_perth.date()) &
        (df["Start Time"].dt.date <= end_date_perth.date())
    ]
    print("📥 过滤后数据行数:", len(df_filtered))

    if df_filtered.empty:
        print(f"⚠️ 过滤后没有 {start_date_input} ~ {end_date_input} 的数据，请检查 API 过滤条件。")
        return

    # **筛选 `Patient Arrived = True`**
    df_filtered = df_filtered[df_filtered["Patient Arrived"] == True]

    if df_filtered.empty:
        print(f"⚠️ 过滤后没有 `Patient Arrived = TRUE` 的数据")
        return

    # **获取系统临时目录**
    temp_dir = tempfile.gettempdir()

    # **强制使用固定临时文件名**
    temp_file_path = os.path.join(temp_dir, "Cliniko_Appointments.xlsx")

    # **如果文件已存在，先删除**
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

    # **保存 Excel 文件**
    df_filtered.to_excel(temp_file_path, index=False, engine="openpyxl")

    # **打印文件路径**
    print(f"✅ 预约数据已保存到临时文件: {temp_file_path}")

    # **后续代码可以直接使用 `temp_file_path` 访问 Excel 文件**
    df_later = pd.read_excel(temp_file_path, engine="openpyxl")
    print("📊 读取的数据示例：")
    print(df_later.head())

    # **可选：程序结束后，删除临时文件**
    # os.remove(temp_file_path)  # 取消注释可删除文件
# **运行代码**
fetch_appointments_for_date()

#endregion

# region 获取系统临时目录，增加Practitioner mapping**
temp_dir = tempfile.gettempdir()
file_path = os.path.join(temp_dir, "Cliniko_Appointments.xlsx")  # **临时文件路径**

# **检查文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 未找到临时文件: {file_path}，请确保已经运行数据获取脚本。")
else:
    # **加载 Excel 数据**
    df = pd.read_excel(file_path, sheet_name=0, engine="openpyxl")  # **默认读取第一个 sheet**

    # **Practitioner ID 映射表**
    practitioner_mapping = {
        "428712064291183103": "Alyssia Strauss (Director, $10 Extra Female therapist)",
        "1475690988780395188": "Ana-maria Van den Aakster",
        "743728674032851165": "Andrew Haning (snr Male Therapist)",
        "1565010732964125576": "Franco Daprotis (snr, Male therapist)",
        "1330772131448887335": "Kelly Street (Snr Female Therapist)",
        "1565011824238140297": "Lourdes Barros  (snr, female therapist)",
        "1331023810517801040": "Martin Allen (snr Male Therapist)",
        "1600536011799009554": "Rainer Puhmas (snr Male Therapist)",
        "1403989585238694524": "Reihanna Orlandi",
        "1095326174797633991": "Sayuri Hirakawa (snr Female Therapist)",
        "1109111631553501771": "Sunny Perera (snr male therapist)",
        "1563542280558615412": "Waka Iguchi (snr, female Therapist)"
    }

    # **检查并提取 Practitioner ID**
    if "Practitioner API URL" in df.columns:
        df["Practitioner ID"] = df["Practitioner API URL"].str.split("/").str[-1]  # **从 URL 提取 ID**
        df["Practitioner"] = df["Practitioner ID"].map(practitioner_mapping)  # **映射姓名**
        df.drop(columns=["Practitioner API URL"], inplace=True, errors="ignore")  # **删除 URL 列**

    # **覆盖原临时 Excel 文件**
    df.to_excel(file_path, index=False, engine="openpyxl")

    # **显示成功信息**
    print(f"✅ 处理完成！已覆盖临时文件: {file_path}")

#endregion

# region **Step 1: 读取 Excel 文件** 获取appointment type的信息
# **获取系统临时目录**
temp_dir = tempfile.gettempdir()
input_file = os.path.join(temp_dir, "Cliniko_Appointments.xlsx")  # **临时文件路径**

# **检查文件是否存在**
if not os.path.exists(input_file):
    print(f"❌ 未找到临时文件: {input_file}，请确保已经运行数据获取脚本。")
else:
    # **加载 Excel 数据**
    df = pd.read_excel(input_file, sheet_name=0, engine="openpyxl")

    # **Step 2: 获取 'Appointment Type URL' 列**
    appointment_urls = df["Appointment Type URL"].dropna().unique()

    API_KEY = "MS0xNjE5MzQzNjU3NzUwNTAxNDcyLTNFV3ZlQXRCdVZSVjhabHpWaVJtelZzcWlSUTZPOS8r-au2"  # 请确保 API Key 无误
    BASE_URL2 = "https://api.au2.cliniko.com/v1/appointment_types"
    PER_PAGE = 100
    MAX_PAGES = 50

    # **生成 Basic 认证**
    auth_string = f"{API_KEY}:"
    base64_auth = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {base64_auth}",
        "User-Agent": "MyClinikoApp (p.luo@element47.com.au)",
        "Accept": "application/json"
    }

    # **Step 3: 逐条请求 API 以获取 Appointment Type 数据**
    appointment_data = {}

    for url in appointment_urls:
        try:
            print(f"📤 请求数据: {url}")
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                appt = response.json()
                name = appt.get("name", "Unknown")
                duration = appt.get("duration_in_minutes", "Unknown")
                billable_item_url = appt.get("billable_item", {}).get("links", {}).get("self", None)
                category = appt.get("category", "Unknown")

                appointment_data[url] = {
                    "name": name,
                    "duration_in_minutes": duration,
                    "billable_item": billable_item_url,
                    "category": category
                }

            else:
                print(f"⚠️ 请求失败: {response.status_code}, URL: {url}")
                appointment_data[url] = {
                    "name": "Unknown",
                    "duration_in_minutes": "Unknown",
                    "billable_item": None,
                    "category": "Unknown"
                }

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ 发生错误: {e}, URL: {url}")
            appointment_data[url] = {
                "name": "Unknown",
                "duration_in_minutes": "Unknown",
                "billable_item": None,
                "category": "Unknown"
            }

    # **Step 4: 进行匹配并填充 DataFrame**
    df["Appointment Type Name"] = df["Appointment Type URL"].map(
        lambda x: appointment_data.get(x, {}).get("name", "Unknown"))
    df["Duration (Minutes)"] = df["Appointment Type URL"].map(
        lambda x: appointment_data.get(x, {}).get("duration_in_minutes", "Unknown"))
    df["Billable Item URL"] = df["Appointment Type URL"].map(
        lambda x: appointment_data.get(x, {}).get("billable_item", ""))
    df["Category"] = df["Appointment Type URL"].map(
        lambda x: appointment_data.get(x, {}).get("category", "Unknown"))

    # **Step 5: 直接覆盖原文件**
    df.to_excel(input_file, index=False, engine="openpyxl")

    print(f"✅ 处理完成！临时文件已更新: {input_file}")

#endregion

# region 读取 Excel 文件（覆盖原始文件）
# **使用临时文件路径**
temp_dir = tempfile.gettempdir()
file_path = os.path.join(temp_dir, "Cliniko_Appointments.xlsx")

# **检查临时文件是否存在**
# **🔹 读取临时文件**
temp_dir = tempfile.gettempdir()  # 获取系统临时目录
file_path = os.path.join(temp_dir, "Cliniko_Appointments.xlsx")  # **使用临时文件路径**

if not os.path.exists(file_path):
    print(f"❌ 未找到临时文件: {file_path}，请先运行数据获取脚本！")
    exit()

df = pd.read_excel(file_path)

# **🔹 提取 Billable Item ID**
def extract_id(url):
    match = re.search(r'billable_items/(\d+)', str(url))
    return match.group(1) if match else None

df["Billable Item ID"] = df["Billable Item URL"].apply(extract_id)

# **🔹 获取所有唯一 ID**
unique_ids = df["Billable Item ID"].dropna().unique()

# **🔹 API 认证信息**
API_KEY = "MS0xNjE5MzQzNjU3NzUwNTAxNDcyLTNFV3ZlQXRCdVZSVjhabHpWaVJtelZzcWlSUTZPOS8r-au2"  # 确保 API Key 正确
BASE_URL = "https://api.au2.cliniko.com/v1/billable_items"

# **生成 Basic 认证**
auth_string = f"{API_KEY}:"
base64_auth = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

headers = {
    "Authorization": f"Basic {base64_auth}",
    "User-Agent": "MyClinikoApp",
    "Accept": "application/json"
}

# **🔹 获取价格的映射表**
price_dict = {}

for item_id in unique_ids:
    api_url = f"{BASE_URL}/{item_id}"
    retries = 0  # 记录重试次数
    success = False

    while retries < 5:  # 最多重试 5 次
        response = requests.get(api_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            price_dict[item_id] = data.get("price", None)
            success = True
            break  # 请求成功，退出循环
        elif response.status_code == 429:  # 处理 API 速率超限
            wait_time = (2 ** retries) * 5  # 等待时间: 5s, 10s, 20s, 40s, 80s
            print(f"⚠️ 请求速率超限（429），等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)
        else:
            print(f"❌ 获取失败: {item_id}, 状态码: {response.status_code}")
            break  # 其他错误，跳过该 ID

        retries += 1  # 增加重试次数

    if not success:
        price_dict[item_id] = None  # 请求失败，存储 None

# **🔹 将价格匹配回 DataFrame**
df["Price"] = df["Billable Item ID"].map(price_dict)

# **🔹 覆盖临时文件**
df.to_excel(file_path, index=False, engine="openpyxl")

print(f"✅ 数据已更新并覆盖到临时文件: {file_path}")

#endregion

# region Table 2 **Step 1: 读取 Excel 文件**
from fetch_cliniko_data import fetch_cliniko_appointments

if __name__ == "__main__":
    start_date = input("请输入开始日期 (YYYY-MM-DD): ")
    end_date = input("请输入结束日期 (YYYY-MM-DD): ")

    fetch_cliniko_appointments(start_date, end_date)

#endregion

#region 获取Practitioner的名字

# **获取临时文件 2 的路径**
temp_dir = tempfile.gettempdir()
file_path = os.path.join(temp_dir, "Cliniko_Appointments_2.xlsx")  # 调用临时文件 2

# **检查临时文件是否存在**
if not os.path.exists(file_path):
    print(f"❌ 临时文件未找到: {file_path}")
else:
    # **加载 Excel 数据**
    df = pd.read_excel(file_path, sheet_name=0)  # 默认读取第一个 sheet

    # **Practitioner ID 映射表**
    practitioner_mapping = {
        "428712064291183103": "Alyssia Strauss (Director, $10 Extra Female therapist)",
        "1475690988780395188": "Ana-maria Van den Aakster",
        "743728674032851165": "Andrew Haning (snr Male Therapist)",
        "1565010732964125576": "Franco Daprotis (snr, Male therapist)",
        "1330772131448887335": "Kelly Street (Snr Female Therapist)",
        "1565011824238140297": "Lourdes Barros  (snr, female therapist)",
        "1331023810517801040": "Martin Allen (snr Male Therapist)",
        "1600536011799009554": "Rainer Puhmas (snr Male Therapist)",
        "1403989585238694524": "Reihanna Orlandi",
        "1095326174797633991": "Sayuri Hirakawa (snr Female Therapist)",
        "1109111631553501771": "Sunny Perera (snr male therapist)",
        "1563542280558615412": "Waka Iguchi (snr, female Therapist)"
    }

    # **检查并提取 Practitioner ID**
    if "Practitioner API URL" in df.columns:
        # 提取 Practitioner ID（从 URL 提取最后一部分）
        df["Practitioner ID"] = df["Practitioner API URL"].str.split("/").str[-1]

        # 映射 Practitioner Name
        df["Practitioner"] = df["Practitioner ID"].map(practitioner_mapping)

        # **删除原始的 URL 列**
        df.drop(columns=["Practitioner API URL"], inplace=True, errors="ignore")

    # **覆盖临时文件 2**
    df.to_excel(file_path, index=False, engine="openpyxl")  # **确保 `openpyxl` 用于 Excel 写入**

    # **显示成功信息**
    print(f"✅ 处理完成！已覆盖临时文件: {file_path}")

#endregion

# region **Cliniko API 配置**
API_KEY = "MS0xNjE5MzQzNjU3NzUwNTAxNDcyLTNFV3ZlQXRCdVZSVjhabHpWaVJtelZzcWlSUTZPOS8r-au2"  # 请替换为你的 API Key

# **获取临时文件 2 的路径**
temp_dir = tempfile.gettempdir()
FILE_PATH = os.path.join(temp_dir, "Cliniko_Appointments_2.xlsx")  # 调用临时文件 2

# **生成 Basic 认证**
auth_string = f"{API_KEY}:"
base64_auth = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
headers = {
    "Authorization": f"Basic {base64_auth}",
    "User-Agent": "MyClinikoApp",
    "Accept": "application/json"
}

# **检查临时文件是否存在**
if not os.path.exists(FILE_PATH):
    print(f"❌ 临时文件未找到: {FILE_PATH}")
else:
    # **读取 Excel 数据**
    df = pd.read_excel(FILE_PATH)

    # **检查 Invoice URL 列是否存在**
    if "Invoice URL" not in df.columns:
        print("❌ 没有找到 'Invoice URL' 列！请检查文件格式。")
    else:
        df["Total Amount"] = "Unknown"  # 预设空值

        for index, row in df.iterrows():
            invoice_url = row["Invoice URL"]
            if pd.isna(invoice_url):
                continue

            print(f"📤 处理: {invoice_url}")

            try:
                response = requests.get(invoice_url, headers=headers)
                if response.status_code == 200:
                    invoices = response.json().get("invoices", [])
                    if invoices:
                        total_amount = invoices[0].get("total_amount", "Unknown")
                        df.at[index, "Total Amount"] = total_amount
                        print(f"✅ 提取成功: {total_amount}")
                    else:
                        print(f"⚠️ 没有找到 Invoice 数据: {invoice_url}")
                else:
                    print(f"❌ 请求失败，状态码: {response.status_code}")

            except Exception as e:
                print(f"❌ 发生错误: {e}")

            time.sleep(0.5)  # 避免 API 速率限制

        # **直接覆盖临时文件 2**
        df.to_excel(FILE_PATH, index=False, engine="openpyxl")
        print(f"✅ 处理完成！数据已覆盖至: {FILE_PATH}")

#endregion