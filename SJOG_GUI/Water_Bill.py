import fitz
import pandas as pd
import re
import datetime

file_path = '9013558645-B0109-01-638828329055457551.pdf'


def get_text_by_position(blocks, target_top, target_left=None, tol_top=1.0, tol_left=5.0, multiline=False):
    for block in blocks:
        x0, y0, _, _, text, *_ = block
        if abs(y0 - target_top) <= tol_top:
            if target_left is None or abs(x0 - target_left) <= tol_left:
                return text.strip() if not multiline else text
    return None


def water(pdf_path, page_number=1):
    with fitz.open(pdf_path) as doc:
        page = doc[page_number]
        blocks = page.get_text("blocks")
        blocks = [b for b in blocks if b[1] < 400]
        for block in blocks:
            x0, y0, x1, y1, text, *_ = block

        read_date_str = get_text_by_position(blocks, target_top=88.4, target_left=53.5, multiline=True).splitlines()[3]
        usage_str = get_text_by_position(blocks, 88.4, 53.5, multiline=True).splitlines()[5]
        usage = int(usage_str.replace(",", ""))
        last_consumption = 0
        # 尝试从字符串中提取月份
        try:
            read_month = read_date_str.strip().split()[1]
            read_year = int(read_date_str.strip().split()[2])
        except ValueError:
            read_month = 0  # 或其他处理方式

        if read_month in ["Jan", "Feb"]:
            term = "6th"
            read_year = f"{read_year - 1}/{read_year}"
            consumption = usage + last_consumption
        elif read_month in ["Mar", "Apr"]:
            term = "1st"
            read_year = f"{read_year}/{read_year + 1}"
            consumption = usage
        elif read_month in ["May", "Jun"]:
            term = "2nd"
            read_year = f"{read_year}/{read_year + 1}"
            consumption = usage + last_consumption
        elif read_month in ["Jul", "Aug"]:
            term = "3rd"
            read_year = f"{read_year}/{read_year + 1}"
            consumption = usage + last_consumption
        elif read_month in ["Sep", "Oct"]:
            term = "4th"
            read_year = f"{read_year}/{read_year + 1}"
            consumption = usage + last_consumption
        elif read_month in ["Nov", "Dec"]:
            term = "5th"
            read_year = f"{read_year}/{read_year + 1}"
            consumption = usage + last_consumption

        info = {
            "Water Use Year": read_year,
            "Read Date": read_date_str,
            "Reading": term,
            "Dial Reading": get_text_by_position(blocks, 88.4, 53.5, multiline=True).splitlines()[4],
            "Consumption Year to Date": usage,
            "Consumption": consumption
        }

        return pd.DataFrame([info])


def bill(pdf_path, page_number=0):
    with fitz.open(pdf_path) as doc:
        page = doc[page_number]
        blocks = page.get_text("blocks")
        for block in blocks:
            x0, y0, x1, y1, text, *_ = block

        account = get_text_by_position(blocks, 110.4, 402.2, multiline=True).splitlines()[1]
        account = account.replace(" ", "")

        property_street_match = get_text_by_position(blocks, 263.1, 53.9, multiline=True)
        match = re.search(r"at (.+)", property_street_match)
        property_street = match.group(1).strip(".") if match else ""
        billID = int(get_text_by_position(blocks, 140.9, 402.2, multiline=True).splitlines()[1])
        issue_date = get_text_by_position(blocks, 156.7, 402.2, multiline=True).splitlines()[1]
        due_date = get_text_by_position(blocks, 297.4, 426.3, multiline=True).splitlines()[1]
        amount_str = get_text_by_position(blocks, 242.6, 439.0, multiline=True).splitlines()[1]
        amount_str = amount_str.replace(",", "")
        amount = amount_str.replace("$", "")

        info = {
            "Property Account Number": account,
            "Property Address": property_street,
            "Bill Issue Date": datetime.datetime.strptime(issue_date.strip(), "%d %b %Y"),
            "Bill Due Date": datetime.datetime.strptime(due_date.strip(), "%d %b %Y"),
            "Bill ID": billID,
            "Bill Amount": amount
        }

        return pd.DataFrame([info])


print(water(file_path).to_string())