import pandas as pd


def determine_combined(row):
    charge_description = ['Ancillary Charge', 'Market Fee', 'Off Peak Energy',
                          'Peak Energy', 'Capacity Charge', 'Discount']
    charge_group = ['Environmental', 'Other', 'Network Transmission', 'Network Distribution', 'Network Other']
    if row['Charge Description'] in charge_description:
        return row['Charge Description']
    elif row['Charge Group'] in charge_group:
        return row['Charge Group']
    else:
        return "Error"


def e_formatting(df):
    df_formatting = df
    drop_columns = ['Name', 'Supplier', 'Fuel', 'Consumer', 'Unit Of Measure', 'Charge Description', 'Charge Group',
                    'Charge Rate', 'Uplifted Rate', 'Information Only', 'GST ($)', 'Total Charge ($)']
    # Apply the function to each row
    df_formatting['Combined'] = df_formatting.apply(determine_combined, axis=1)
    # Rename specific values in the 'Combined' column
    rename_values = {
        'Ancillary Charge': 'AEMO Market Charges',
        'Market Fee': 'AEMO Market Charges',
        'Environmental': 'REC FEE'
    }
    # Use the replace method to update the 'Combined' column
    df_formatting['Combined'] = df_formatting['Combined'].replace(rename_values)
    df_formatting['Net Charge ($)'] = df_formatting['Net Charge ($)'].astype(str).str.replace(',', '').astype(float)
    df_formatting.drop(columns=drop_columns, inplace=True)

    unchanged_columns = ['Account', 'Bill Date', 'Due Date', 'NMI', 'Site Address',
                         'From Date', 'To Date']
    grouped_columns = unchanged_columns + ['Combined']
    aggregated_data = df_formatting.groupby(grouped_columns).agg({
        'Net Charge ($)': 'sum',
        'Quantity': 'sum'
    }).reset_index()

    pivot_charges = aggregated_data.pivot_table(index=unchanged_columns, columns='Combined',
                                                values='Net Charge ($)', aggfunc='sum', fill_value=0)
    pivot_quantity = aggregated_data.pivot_table(index=unchanged_columns, columns='Combined',
                                                 values='Quantity', aggfunc='sum', fill_value=0)

    pivot_charges.reset_index(inplace=True)
    pivot_quantity.reset_index(inplace=True)

    # Merge the pivoted data frames
    pivot_df = pd.merge(pivot_charges, pivot_quantity, on=unchanged_columns, suffixes=('', '_quantity'))
    # Merge additional columns like 'DLF' and 'TLF' from the original df
    pivot_df = pivot_df.merge(df[['From Date', 'NMI', 'DLF', 'TLF']].drop_duplicates(),
                              on=['From Date', 'NMI'],
                              how='left').dropna(subset=['DLF'])
    # GST AND Total calculation
    # pivot_df['GST ($)'] = round(pivot_df['Net Charge ($)'] * 0.1, 2)
    # pivot_df['Total ($)'] = pivot_df['Net Charge ($)'] + pivot_df['GST ($)']

    columns = ['ACCTNO', 'ACCOUNT', 'SITE ADDRESS', 'INVOICE #', 'NMI',
               'BILLING PERIOD START DATE', 'BILLING PERIOD END DATE',
               'BILLING PERIOD NUMBER OF DAYS', 'TOTAL PEAK CONSUMPTION (KWH)',
               'TOTAL OFF-PEAK CONSUMPTION (KWH)', 'TOTAL WEEKEND CONSUMPTION (KWH)',
               'TOTAL PEAK SPEND ($)', 'TOTAL OFF-PEAK SPEND ($)',
               'TOTAL WEEKEND SPEND ($)', 'TOTAL ENERGY SPEND $',
               'CAPACITY CHARGE ($)', 'NETWORK CHARGE ($)', 'IMO FEE ($)',
               'REC CHARGE ($)', 'DAILY SUPPLY CHARGE ($)', 'OTHER CHARGES',
               'INVOICE TOTAL ($)', 'DLF', 'TLF']
    df_data = pd.DataFrame(index=pivot_df.index, columns=columns)
    df_data['ACCTNO'] = 'ALINTA ENERGY'
    df_data['ACCOUNT'] = pivot_df['Account'].astype(str)
    df_data['SITE ADDRESS'] = pivot_df['Site Address'].astype(str)
    df_data['NMI'] = pivot_df['NMI'].astype(str)
    pivot_df['From Date'] = pd.to_datetime(pivot_df['From Date'], errors='coerce', dayfirst=True)
    pivot_df['To Date'] = pd.to_datetime(pivot_df['To Date'], errors='coerce', dayfirst=True)
    df_data['BILLING PERIOD START DATE'] = pd.to_datetime(
        pivot_df['From Date'].dt.strftime('%Y-%m-%d') + ' 00:00:00',
        format='%Y-%m-%d %H:%M:%S').astype(str)
    df_data['BILLING PERIOD END DATE'] = pd.to_datetime(
        pivot_df['To Date'].dt.strftime('%Y-%m-%d') + ' 00:00:00',
        format='%Y-%m-%d %H:%M:%S').astype(str)
    df_data['TOTAL PEAK CONSUMPTION (KWH)'] = pivot_df['Peak Energy_quantity']
    df_data['TOTAL OFF-PEAK CONSUMPTION (KWH)'] = pivot_df['Off Peak Energy_quantity']
    df_data['TOTAL PEAK SPEND ($)'] = pivot_df['Peak Energy']
    df_data['TOTAL OFF-PEAK SPEND ($)'] = pivot_df['Off Peak Energy']
    df_data['CAPACITY CHARGE ($)'] = pivot_df['Capacity Charge']
    df_data['NETWORK CHARGE ($)'] = (pivot_df['Network Distribution'] + pivot_df['Network Transmission'] +
                                     pivot_df['Network Other'])
    df_data['IMO FEE ($)'] = pivot_df['AEMO Market Charges']
    df_data['REC CHARGE ($)'] = pivot_df['REC FEE']
    df_data['OTHER CHARGES'] = pivot_df['Other']
    df_data['INVOICE TOTAL ($)'] = pivot_df[['Capacity Charge', 'Network Distribution', 'Network Transmission',
                                             'Network Other', 'AEMO Market Charges', 'REC FEE', 'Other',
                                             'Discount']].sum(axis=1)
    df_data['DLF'] = pivot_df['DLF']
    df_data['TLF'] = pivot_df['TLF']
    df_data['INVOICE #'] = 'Upload later'
    df_data['TOTAL ENERGY SPEND $'] = df_data['TOTAL PEAK SPEND ($)'] + df_data['TOTAL OFF-PEAK SPEND ($)']
    return df_data
