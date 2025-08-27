import requests
import json
from datetime import datetime, timedelta

# ✅ 你的 Access Token
access_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjFDQUY4RTY2NzcyRDZEQzAyOEQ2NzI2RkQwMjYxNTgxNTcwRUZDMTkiLCJ0eXAiOiJKV1QiLCJ4NXQiOiJISy1PWm5jdGJjQW8xbkp2MENZVmdWY09fQmsifQ.eyJuYmYiOjE3NDIzODAxMzMsImV4cCI6MTc0MjM4MTkzMywiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS54ZXJvLmNvbSIsImF1ZCI6Imh0dHBzOi8vaWRlbnRpdHkueGVyby5jb20vcmVzb3VyY2VzIiwiY2xpZW50X2lkIjoiNTc4OERDNTE5RDY3NDJFQ0I5OTIwRkFCNEQ1MjExRTciLCJzdWIiOiIwMGFkMzYzNDE1OTg1NTQ0ODdlODJhNzJiY2Y2ZDQwNiIsImF1dGhfdGltZSI6MTc0MjM3OTY5NiwieGVyb191c2VyaWQiOiI5NjM5Y2ZiOS1iNGFmLTQ2ZGEtOTA5My1hNmZlNjk0ZmYxNmUiLCJnbG9iYWxfc2Vzc2lvbl9pZCI6IjM3MjczOGU3ZWQ5ODQ2YTViOWEzZjE1M2Y3YzBlNDI4Iiwic2lkIjoiMzcyNzM4ZTdlZDk4NDZhNWI5YTNmMTUzZjdjMGU0MjgiLCJqdGkiOiIwNUM5NTdEN0NBMEYwRjQ2MTg1NzA0Njg1NEI2QzA2MyIsImF1dGhlbnRpY2F0aW9uX2V2ZW50X2lkIjoiMjJiOGM1MzEtNDM5Ni00Y2Q2LWFjM2EtZGVmMGNjYWUyZWQxIiwic2NvcGUiOlsiZW1haWwiLCJwcm9maWxlIiwib3BlbmlkIiwicGF5cm9sbC5lbXBsb3llZXMiLCJwYXlyb2xsLmVtcGxveWVlcy5yZWFkIiwicGF5cm9sbC50aW1lc2hlZXRzIiwicGF5cm9sbC50aW1lc2hlZXRzLnJlYWQiLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsicHdkIiwibWZhIiwia2JhIl19.D052BUY-LVaOyJu_zm3K5OGfNx2kMw9JMIbTacrK_97-yniFs92qCpjoiFaHzj_6m-6YOO0qNg_6iO6ofhqYwNQgt6HUu5t63zC0Gh9KNnIsbJzD_l2SuUciuwWvPu5inqeX4A7Uev3j08nRsk6WTzRLKuA-ozAFg3o6-wsDMozR3f3oN43KmehjDPUQvSzx7n_NGR1NYeaeWDnpUFNjuHg9hTLy72lyYFGGvqcBX6OQLSOLjFpKPqxt-TGWI-8vkByfqa9o8RUrN1waVZ4X1PnKhIKlz-tsDhGaiNcD_VQ9HiIXBrw6aBL-7Ms_i8p82BPkIYsI0AnPHz0dYylLzw"

# ✅ 你的 Xero Tenant ID
tenant_id = "af61e7e6-15d3-456d-8423-2ad18e87da9c"

# ✅ API Headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Xero-Tenant-Id": tenant_id,
    "Accept": "application/json"
}

# ✅ 1. 时间转换工具
def perth_to_utc(perth_time_str):
    perth_time = datetime.strptime(perth_time_str, "%Y-%m-%d")
    return perth_time - timedelta(hours=8)  # Perth 是 UTC+8

def utc_to_xero_timestamp(utc_time):
    timestamp_ms = int(utc_time.timestamp() * 1000)
    return f"/Date({timestamp_ms})/"

def xero_timestamp_to_perth(xero_date_str):
    timestamp_ms = int(xero_date_str[6:-2])  # 提取毫秒级时间戳
    utc_time = datetime.utcfromtimestamp(timestamp_ms / 1000)
    perth_time = utc_time + timedelta(hours=8)  # Perth 是 UTC+8
    return perth_time.strftime('%Y-%m-%d')

# ✅ 2. 获取所有员工信息
employee_url = "https://api.xero.com/payroll.xro/1.0/Employees"
response_employee = requests.get(employee_url, headers=headers)

if response_employee.status_code == 200:
    employees = response_employee.json()["Employees"]
    print(f"\n✅ 获取到 {len(employees)} 名员工数据\n")
else:
    print("❌ 获取员工数据失败:", response_employee.status_code, response_employee.json())
    exit()

# ✅ 3. 生成 EmployeeID -> 员工姓名 的映射字典
employee_dict = {emp["EmployeeID"]: f"{emp['FirstName']} {emp['LastName']}" for emp in employees}

# ✅ 4. 输入时间范围（Perth +8:00）
start_date_input = input("请输入开始日期 (YYYY-MM-DD, Perth +8:00): ")
end_date_input = input("请输入结束日期 (YYYY-MM-DD, Perth +8:00): ")

# 验证日期格式
try:
    start_date_perth = datetime.strptime(start_date_input, "%Y-%m-%d")
    end_date_perth = datetime.strptime(end_date_input, "%Y-%m-%d")
except ValueError:
    print("❌ 日期格式不正确，请使用 YYYY-MM-DD 格式")
    exit()

# 转换时间格式
start_date_utc = perth_to_utc(start_date_input)
end_date_utc = perth_to_utc(end_date_input)

start_date_xero = utc_to_xero_timestamp(start_date_utc)
end_date_xero = utc_to_xero_timestamp(end_date_utc)

# ✅ 5. 获取所有 Timesheets
timesheet_url = "https://api.xero.com/payroll.xro/1.0/Timesheets"
response_timesheets = requests.get(timesheet_url, headers=headers)

if response_timesheets.status_code == 200:
    timesheets = response_timesheets.json()["Timesheets"]
    print(f"\n✅ 获取到 {len(timesheets)} 条 Timesheets 数据\n")
else:
    print("❌ 获取 Timesheets 失败:", response_timesheets.status_code, response_timesheets.json())
    exit()

# ✅ 6. 筛选符合时间范围的 Timesheets
filtered_timesheets = []

for ts in timesheets:
    ts_start_perth = xero_timestamp_to_perth(ts['StartDate'])
    ts_end_perth = xero_timestamp_to_perth(ts['EndDate'])

    # 判断 Timesheet 是否在输入的时间范围内
    if (ts_start_perth <= end_date_input) and (ts_end_perth >= start_date_input):
        employee_name = employee_dict.get(ts["EmployeeID"], "Unknown Employee")  # 获取员工姓名
        filtered_timesheets.append({
            "EmployeeName": employee_name,
            "EmployeeID": ts["EmployeeID"],
            "StartDate": ts_start_perth,
            "EndDate": ts_end_perth,
            "Status": ts["Status"],
            "WorkHours": [line["NumberOfUnits"] for line in ts["TimesheetLines"]]
        })

# ✅ 7. 打印筛选后的 Timesheets
if filtered_timesheets:
    print(f"\n✅ **筛选后的 Timesheets 记录** ({start_date_input} 到 {end_date_input}) ✅\n")
    for ts in filtered_timesheets:
        print(f"✅ {ts['EmployeeName']} ({ts['EmployeeID']}) 的 Timesheet 数据：")
        print(f"📅 起始日期: {ts['StartDate']} - 结束日期: {ts['EndDate']}")
        print(f"📌 状态: {ts['Status']}")

        for i, work_hours in enumerate(ts["WorkHours"]):
            day_of_week = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][i]
            print(f"🕒 {day_of_week} 工时: {work_hours}")
        print("-" * 40)
else:
    print(f"\n⚠️ 在 {start_date_input} 到 {end_date_input} 之间 **没有 Timesheet 记录**")