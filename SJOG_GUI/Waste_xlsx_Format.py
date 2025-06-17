import pandas as pd
import warnings
from datetime import datetime, timedelta


def excel_date_to_datetime(serial):
    if pd.isna(serial):
        return None
    if isinstance(serial, (int, float)):
        return datetime(1899, 12, 30) + timedelta(days=int(serial))
    return None


def waste(file_path):
    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet.header_footer")
    df = pd.read_excel(file_path, sheet_name="DATA_REPORT")
    df["Cal Month"] = pd.to_datetime(df["Cal Month"], format="%Y-%m", errors="coerce").dt.to_period("M").astype(str)
    df["Posting Month"] = pd.to_datetime(df["Posting Month"], format="%Y-%m", errors="coerce").dt.to_period("M").astype(str)
    df["Invoice Date (YYYY-MM-DD)"] = pd.to_datetime(df["Invoice Date (YYYY-MM-DD)"], format="%Y-%m-%d",
                                                     errors="coerce")
    df["Service Request Date (YYYY-MM-DD)"] = pd.to_datetime(df["Service Request Date (YYYY-MM-DD)"], format="%Y-%m-%d",
                                                             errors="coerce")
    df["Date of the service provided (YYYY-MM-DD)"] = pd.to_datetime(df["Date of the service provided (YYYY-MM-DD)"],
                                                                     format="%Y-%m-%d", errors="coerce")
    df["Invoice Number"] = pd.to_numeric(df["Invoice Number"], errors="coerce")
    df["Invoice Number"] = df["Invoice Number"].fillna(0).astype('Int64')
    df["Document Date"] = df["Document Date"].apply(excel_date_to_datetime)
    df["Posting Date"] = df["Posting Date"].apply(excel_date_to_datetime)

    return df

