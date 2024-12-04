import pandas as pd


def gas_consumption(df):
    # Define the fixed metadata columns
    fixed_columns = {
        'ACCOUNT NUMBER': 605628,
        'ACNAME': 'SJG Midland Hospital',
        'NMI': 56009523942,
        'METER': 'M1600IR003',
        'SITE ADDRESS': '1 Clayton Street Midland, WA, 6156',
    }

    # Rename the columns for 'END INTERVAL' and 'GAS (GJ)'
    df_renamed = df.rename(columns={
        'Date': 'END INTERVAL',
        'Carbon Neutral Charge': 'GAS (GJ)'
    })

    df_renamed['END INTERVAL'] = pd.to_datetime(df_renamed['END INTERVAL'], format='%d/%m/%Y %H:%M')

    # Add the fixed columns to the DataFrame
    for col_name, col_value in fixed_columns.items():
        df_renamed[col_name] = col_value

    # Reorder the columns to match the desired structure
    final_columns = list(fixed_columns.keys()) + ['END INTERVAL', 'GAS (GJ)']
    df_final = df_renamed[final_columns]

    return df_final


