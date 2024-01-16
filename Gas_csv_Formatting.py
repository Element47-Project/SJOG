import pandas as pd

def consumption(df):
    rename_dict = {
        'CONSUMPTION_HR01': '00', 'CONSUMPTION_HR02': '01', 'CONSUMPTION_HR03': '02',
        'CONSUMPTION_HR04': '03', 'CONSUMPTION_HR05': '04', 'CONSUMPTION_HR06': '05',
        'CONSUMPTION_HR07': '06', 'CONSUMPTION_HR08': '07', 'CONSUMPTION_HR09': '08',
        'CONSUMPTION_HR10': '09', 'CONSUMPTION_HR11': '10', 'CONSUMPTION_HR12': '11',
        'CONSUMPTION_HR13': '12', 'CONSUMPTION_HR14': '13', 'CONSUMPTION_HR15': '14',
        'CONSUMPTION_HR16': '15', 'CONSUMPTION_HR17': '16', 'CONSUMPTION_HR18': '17',
        'CONSUMPTION_HR19': '18', 'CONSUMPTION_HR20': '19', 'CONSUMPTION_HR21': '20',
        'CONSUMPTION_HR22': '21', 'CONSUMPTION_HR23': '22', 'CONSUMPTION_HR24': '23'
    }

    # Rename the columns
    columns_to_drop = [df.columns[i] for i in range(4) if i != 2]
    df.drop(columns=columns_to_drop, inplace=True)
    df.drop(columns=df.columns[[-1]], inplace=True)
    df.rename(columns=rename_dict, inplace=True)

    df.drop(columns=['TOTAL_DAILY_CONSUMPTION', 'DAILY_HEAT_VALUE'], inplace=True)
    df_melted = df.melt(id_vars=['GASDAY'], var_name='Time', value_name='Value')
    df_melted['GASDAY'] = pd.to_datetime(df_melted['GASDAY'])

    # Check if any 'Time' value is '24' and adjust
    # This creates a boolean mask where 'Time' is '24'
    mask = df_melted['Time'] == '24'

    # For rows where 'Time' is '24', add one day to 'GASDAY' and set 'Time' to '00'
    df_melted.loc[mask, 'GASDAY'] += pd.Timedelta(days=1)
    df_melted.loc[mask, 'Time'] = '00'

    # Now convert Time to string and pad with zeros to ensure two digits
    df_melted['Time'] = df_melted['Time'].astype(str).str.zfill(2)
    # Combine GASDAY and Time into a single datetime column
    df_melted['END INTERVAL'] = pd.to_datetime(
        df_melted['GASDAY'].dt.strftime('%d-%m-%Y') + ' ' + df_melted['Time'] + ':00:00',
        format='%d-%m-%Y %H:%M:%S'  # Correct format string
    )
    df_melted.sort_values(by='END INTERVAL', inplace=True)

    df_melted.drop(columns=['GASDAY', 'Time'], inplace=True)
    df_melted['GAS (GJ)'] = round(df_melted['Value']/1000, 2)
    df_melted.drop(columns=['Value'], inplace=True)
    df_melted['ACCOUNT NUMBER'] = '605628'
    df_melted['ACNAME'] = 'SJG Midland Hospital'
    df_melted['NMI'] = '56009523942'
    df_melted['METER'] = 'M1600IR003'
    df_melted['SITE ADDRESS'] = '1 Clayton Street Midland, WA, 6156'
    print("Formatting Process Done")
    return df_melted





