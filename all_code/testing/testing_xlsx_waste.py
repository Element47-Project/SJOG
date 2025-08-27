import pandas as pd


def process_veolia_xlsx(file_path, site):
    # Define row ranges for each table section (based on fixed Excel layout)
    cost_df = pd.read_excel(file_path, skiprows=7, nrows=22).iloc[:, [0, -3]]
    volume_df = pd.read_excel(file_path, skiprows=54, nrows=21).iloc[:, [0, -3]]
    weight_df = pd.read_excel(file_path, skiprows=100, nrows=21).iloc[:, [0, -3]]

    # Rename columns
    cost_df.columns = ["WasteDiversion", "Cost"]
    volume_df.columns = ["WasteDiversion", "Volume"]
    weight_df.columns = ["WasteDiversion", "Weight"]

    # Extract report date from first row, then drop it
    report_date = cost_df["Cost"].iloc[0]
    cost_df = cost_df.iloc[1:].reset_index(drop=True)

    # Combine all value tables by row index
    df = pd.concat([cost_df, volume_df["Volume"], weight_df["Weight"]], axis=1)

    # Add metadata columns
    df["Date"] = pd.to_datetime(report_date)
    df["Site"] = site

    # Hardcoded group mapping
    group_map = {
        "BATTERIES": "Diversion",
        "CARDBOARD RECYCLING": "Diversion",
        "COMINGLE CONTAINERS": "Diversion",
        "COPPER": "Diversion",
        "ELECTRONIC WASTE": "Diversion",
        "FLUORO RECYCLING": "Diversion",
        "FOOD WASTE /ORGANICS RECYCLING": "Diversion",
        "GREASE TRAP WASTE": "Diversion",
        "KIMGUARD": "Diversion",
        "METAL RECYCLING": "Diversion",
        "OPERATING ROOM PLASTICS": "Diversion",
        "PLASTIC": "Diversion",
        "STAINLESS STEEL": "Diversion",
        "SURGICAL DEVICES": "Diversion",
        "TIMBER RECYCLING": "Diversion",
        "BUILDING WASTE": "Landfill",
        "GENERAL WASTE DRY": "Landfill",
        "NAPPY": "Landfill",
        "SANITARY WASTE": "Landfill"
    }

    # Normalize and map WasteGroup values
    df["WasteDiversion"] = df["WasteDiversion"].str.strip()
    df["WasteGroup"] = df["WasteDiversion"].map(group_map)

    # Remove header/category rows (i.e., group labels like "DIVERSION")
    df = df[df["WasteGroup"].notna()]

    # Reorder columns
    df = df[["WasteGroup", "WasteDiversion", "Date", "Cost", "Volume", "Weight", "Site"]]

    return df
