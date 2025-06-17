import os
import requests
import pandas as pd
import time
import pytz
import tempfile
from datetime import datetime
import base64

# Cliniko API 配置
API_KEY = "MS0xNjE5MzQzNjU3NzUwNTAxNDcyLTNFV3ZlQXRCdVZSVjhabHpWaVJtelZzcWlSUTZPOS8r-au2"
BASE_URL = "https://api.au2.cliniko.com/v1/appointments"
PER_PAGE = 100
MAX_PAGES = 50

# 生成 Basic 认证
auth_string = f"{API_KEY}:"
base64_auth = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
headers = {
    "Authorization": f"Basic {base64_auth}",
    "User-Agent": "MyClinikoApp (p.luo@element47.com.au)",
    "Accept": "application/json"
}

def fetch_cliniko_appointments(start_date, end_date, save_to_file=True):
    """ 获取 Cliniko 预约数据，并保存到另一个临时文件（覆盖原有临时文件 2）"""
    try:
        perth_tz = pytz.timezone("Australia/Perth")
        start_date_perth = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
        start_date_utc = perth_tz.localize(start_date_perth).astimezone(pytz.utc).isoformat()

        end_date_perth = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        end_date_utc = perth_tz.localize(end_date_perth).astimezone(pytz.utc).isoformat()

    except ValueError:
        print("❌ 输入的日期格式不正确，请使用 YYYY-MM-DD")
        return None, None

    print(f"🔎 查询时间范围 (Perth): {start_date_perth} ~ {end_date_perth}")
    print(f"🔎 查询时间范围 (UTC): {start_date_utc} ~ {end_date_utc}")

    all_appointments = []
    page = 1
    request_count = 0

    while True:
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
            return None, None

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
        print(f"⚠️ Cliniko API 没有返回 {start_date} ~ {end_date} 的预约数据，请检查 API 过滤条件")
        return None, None

    df = pd.json_normalize(all_appointments)

    rename_map = {
        "appointment_start": "Start Time",
        "appointment_end": "End Time",
        "patient_arrived": "Patient Arrived",
        "appointment_type.links.self": "Appointment Type URL",
        "practitioner.links.self": "Practitioner API URL",
        "invoices.links.self": "Invoice URL"
    }
    df.rename(columns=rename_map, inplace=True)

    required_columns = ["Start Time", "End Time", "Appointment Type URL", "Patient Arrived", "Practitioner API URL", "Invoice URL"]
    df = df[[col for col in required_columns if col in df.columns]]

    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce", utc=True)
    df["End Time"] = pd.to_datetime(df["End Time"], errors="coerce", utc=True)

    df["Start Time"] = df["Start Time"].dt.tz_convert("Australia/Perth").dt.tz_localize(None)
    df["End Time"] = df["End Time"].dt.tz_convert("Australia/Perth").dt.tz_localize(None)

    df_filtered = df[
        (df["Start Time"].dt.date >= start_date_perth.date()) &
        (df["Start Time"].dt.date <= end_date_perth.date())
    ]
    print("📥 过滤后数据行数:", len(df_filtered))

    if df_filtered.empty:
        print(f"⚠️ 过滤后没有 {start_date} ~ {end_date} 的数据，请检查 API 过滤条件。")
        return None, None

    df_filtered = df_filtered[df_filtered["Patient Arrived"] == True]

    if df_filtered.empty:
        print(f"⚠️ 过滤后没有 `Patient Arrived = TRUE` 的数据")
        return None, None

    if save_to_file:
        # **获取系统临时目录，并创建另一个固定文件路径**
        temp_dir = tempfile.gettempdir()  # 获取系统临时目录
        temp_file_path = os.path.join(temp_dir, "Cliniko_Appointments_2.xlsx")  # **创建另一个固定临时文件路径**

        # **如果文件已存在，先删除**
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        # **保存到临时文件**
        df_filtered.to_excel(temp_file_path, index=False, engine="openpyxl")
        print(f"✅ 预约数据已保存到另一个临时文件: {temp_file_path}")

        return df_filtered, temp_file_path  # 返回 DataFrame 和临时文件路径

    return df_filtered, None  # 如果不保存文件，只返回数据





