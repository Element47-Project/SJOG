import numpy as np
import logging


def detect_outliers_by_difference(df, columns, group_col, threshold=200):
    df_copy = df.copy()
    for col in columns:
        df_copy = df_copy.sort_values([group_col, 'DateTime'])
        df_copy[f'{col}_diff'] = df_copy.groupby(group_col)[col].diff()
        outliers = (df_copy[f'{col}_diff'].abs() > threshold) | (df_copy[f'{col}_diff'].abs().shift(-1) > threshold)
        df_copy.loc[outliers, col] = np.nan
        df_copy = df_copy.drop(f'{col}_diff', axis=1)
    return df_copy


def clean_meter_data(df, pre_data):
    try:
        process_cols = ['kWh_IMP', 'kWh_EXP']
        df[process_cols] = df[process_cols].replace(0, np.nan)
        df = detect_outliers_by_difference(df, process_cols, group_col='Meter', threshold=200)
        df[process_cols] = df.groupby('Meter')[process_cols].ffill().bfill()

        for meter in df['Meter'].unique():
            prev_row = pre_data[pre_data['Meter'] == meter]
            if not prev_row.empty:
                mask = (df['Meter'] == meter) & (df['DateTime'] == df[df['Meter'] == meter]['DateTime'].min())
                for col in ['kWh_IMP', 'kWh_EXP']:
                    df.loc[mask, f'Prev_{col}'] = prev_row[col].values[0]

        df['Prev_kWh_IMP'] = df.groupby('Meter')['kWh_IMP'].shift(1).fillna(df['Prev_kWh_IMP'])
        df['Prev_kWh_EXP'] = df.groupby('Meter')['kWh_EXP'].shift(1).fillna(df['Prev_kWh_EXP'])

        for col in process_cols:
            df[f'Diff_{col}'] = df[col] - df[f'Prev_{col}']
            df[f'Diff_{col}'] = df[f'Diff_{col}'].fillna(0)

        return df
    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        return None
