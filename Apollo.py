import pandas as pd


def upload_apollo(df, filename):
    df['Meter'] = filename
    new_columns = {
        col: col.replace(' L1', '').replace('1', '')
                  .replace(' L2', '').replace('2', '')
                  .replace(' L3', '').replace('3', '')
                  .replace('31', '').replace('TDD', 'THD')
                  .replace('I THD', 'I_THD')
                  .replace('kWh IMP', 'kWh_IMP').replace('kWh EXP', 'kWh_EXP')
                  .replace('kvarh IMP', 'kvarh_IMP').replace('kvarh EXP', 'kvarh_EXP')
        for col in df.columns
    }
    df.rename(columns=new_columns, inplace=True)
    drop_columns = ['IsHeader', 'DeviceID', 'DataLogNumber', 'SetPoint', 'LogRecNum', 'EventNum']
    df.drop(columns=drop_columns, index=0, inplace=True, errors='ignore')
    df.dropna(axis=1, how='all', inplace=True)

    # Truncate DateTime to 'YYYY-MM-DD HH:MM:SS'
    df['DateTime'] = pd.to_datetime(df['DateTime']).dt.floor('S')

    df.sort_values(by=['DateTime', 'Meter'], inplace=True)
    df.drop_duplicates(subset=['DateTime', 'Meter'], keep='first', inplace=True)

    # Adding previous value and difference columns
    metrics = ['kWh_IMP', 'kvarh_IMP', 'kvarh_EXP', 'kVAh']

    for metric in metrics:
        prev_col = 'Prev_' + metric
        diff_col = metric + '_Diff'
        df[prev_col] = df.groupby('Meter')[metric].shift(1)
        df[diff_col] = round(df[metric] - df[prev_col], 3)

    return df
