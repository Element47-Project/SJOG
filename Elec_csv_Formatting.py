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


def e_formatting(filename):
    df = pd.read_csv(filename)
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

    # Pivot 'Net Charge ($)' and 'Quantity' separately
    pivot_charges = aggregated_data.pivot_table(index=unchanged_columns, columns='Combined',
                                                values='Net Charge ($)', aggfunc='sum', fill_value=0)
    pivot_quantity = aggregated_data.pivot_table(index=unchanged_columns, columns='Combined',
                                                 values='Quantity', aggfunc='sum', fill_value=0)

    # Reset index to turn multi-index into columns
    pivot_charges.reset_index(inplace=True)
    pivot_quantity.reset_index(inplace=True)

    # Merge the pivoted data frames
    pivot_df = pd.merge(pivot_charges, pivot_quantity, on=unchanged_columns, suffixes=('', '_quantity'))

    # Merge additional columns like 'DLF' and 'TLF' from the original df if needed
    pivot_df = pivot_df.merge(df[['Due Date', 'NMI', 'DLF', 'TLF']].drop_duplicates(),
                              on=['Due Date', 'NMI'],
                              how='left').dropna(subset=['DLF'])

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
    df_data = pd.DataFrame(columns=columns)

    for col in columns:
        if col == 'ACCOUNT':
            df_data[col] = pivot_df['Account']  # Assuming 'Account' is an existing column you want to copy
        elif col == 'SITE ADDRESS':
            df_data[col] = pivot_df['Site Address']  # Assuming 'Site Address' is an existing column you want to copy
        elif col == 'NMI':
            df_data[col] = pivot_df['NMI']  # Assuming 'NMI' is an existing column you want to copy
        elif col == 'BILLING PERIOD START DATE':
            df_data[col] = pivot_df['From Date']
        elif col == 'BILLING PERIOD END DATE':
            df_data[col] = pivot_df['To Date']
        elif col == 'TOTAL PEAK CONSUMPTION (KWH)':
            df_data[col] = pivot_df['Peak Energy_quantity']
        elif col == 'TOTAL OFF-PEAK CONSUMPTION (KWH)':
            df_data[col] = pivot_df['Off Peak Energy_quantity']
        elif col == 'TOTAL PEAK SPEND ($)':
            df_data[col] = pivot_df['Peak Energy']
        elif col == 'TOTAL OFF-PEAK SPEND ($)':
            df_data[col] = pivot_df['Off Peak Energy']
        elif col == 'CAPACITY CHARGE ($)':
            df_data[col] = pivot_df['Capacity Charge']
        elif col == 'NETWORK CHARGE ($)':
            df_data[col] = (pivot_df['Network Distribution'] + pivot_df['Network Transmission'] +
                            pivot_df['Network Other'])
        elif col == 'IMO FEE ($)':
            df_data[col] = pivot_df['AEMO Market Charges']
        elif col == 'REC CHARGE ($)':
            df_data[col] = pivot_df['REC FEE']
        elif col == 'OTHER CHARGES':
            df_data[col] = pivot_df['Other']
        elif col == 'INVOICE TOTAL ($)':
            df_data[col] = pivot_df[['Capacity Charge', 'Network Distribution', 'Network Transmission',
                                    'Network Other', 'AEMO Market Charges', 'REC FEE', 'Other', 'Discount']].sum(axis=1)
        elif col == 'DLF':
            df_data[col] = pivot_df['DLF']
        elif col == 'TLF':
            df_data[col] = pivot_df['TLF']
        else:
            df_data[col] = pd.NA
    df_data['TOTAL ENERGY SPEND $'] = df_data['TOTAL PEAK SPEND ($)'] + df_data['TOTAL OFF-PEAK SPEND ($)']
    return df_data


