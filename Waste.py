import pandas as pd
import warnings


def waste(file_path):
    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet.header_footer")
    df = pd.read_excel(file_path, sheet_name="DATA_REPORT")
    df = df[:-1]
    df["Cal Month"] = pd.to_datetime(df["Cal Month"], format="%Y-%m", errors="coerce").dt.to_period("M").astype(str)
    df["Posting Month"] = pd.to_datetime(df["Posting Month"], format="%Y-%m", errors="coerce").dt.to_period("M").astype(str)
    df["Invoice Date (YYYY-MM-DD)"] = pd.to_datetime(df["Invoice Date (YYYY-MM-DD)"], format="%Y-%m-%d",
                                                     errors="coerce")
    df["Service Request Date (YYYY-MM-DD)"] = pd.to_datetime(df["Service Request Date (YYYY-MM-DD)"], format="%Y-%m-%d",
                                                             errors="coerce")
    df["Date of the service provided (YYYY-MM-DD)"] = pd.to_datetime(df["Date of the service provided (YYYY-MM-DD)"],
                                                                     format="%Y-%m-%d", errors="coerce")
    df['Quantity Invoiced (Units)'] = df['Quantity Invoiced (Units)'].astype(float).astype(int)
    df['Quantity (Units)'] = df['Quantity (Units)'].astype(float).astype(int)
    df['Invoice Number'] = df['Invoice Number'].astype(float).astype(int)
    df['Document Date'] = pd.to_datetime(df['Document Date'].astype(float), unit='D', origin='1899-12-30')
    df['Posting Date'] = pd.to_datetime(df['Posting Date'].astype(float), unit='D', origin='1899-12-30')

    return df



