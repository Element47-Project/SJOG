import pandas as pd

def gb_formatting(df):

    # Error Handdling 
    if df.empty:
        raise ValueError("The input file contains no data.")
    
    # Cleaning data, remove null values 
    df.dropna(subset=['NMI_NUMBER'], inplace=True)

    # Calculate each NMI's gas consumption, gas charge and network charge
    summary_df = df.groupby("NMI_NUMBER").agg(
        TOTAL_GAS_CONSUMPTION=pd.NamedAgg(column="QUANTITY", 
                                          aggfunc=lambda x: x[df.loc[x.index, "CHARGE_GROUP"] == "ENERGY"].sum()),
        GAS_SPENT=pd.NamedAgg(column="NET_CHARGE", 
                              aggfunc=lambda x: x[df.loc[x.index, "CHARGE_GROUP"] == "ENERGY"].sum()),
        NW_SPENT=pd.NamedAgg(column="NET_CHARGE", 
                             aggfunc=lambda x: x[df.loc[x.index, "CHARGE_GROUP"] == "NETWORK"].sum())
        ).reset_index()
    
    # Drop unuseful columns
    drop_columns = ['ACEXTERNALNAME','SUPPLIER','FUEL','STATEMENT','ISSUE_DATE','DUE_DATE',
                    'CONSUMER','CHARGE_GROUP','CHARGE_DESCRIPTION','UNIT_OF_MEASURE',
                    'CHARGE_RATE','QUANTITY','GST','NET_CHARGE', 'TOTAL_CHARGE']
    df_dropped = df.drop(drop_columns, axis=1)

    # Combine results to dataset
    final_df = pd.merge(df_dropped, summary_df, 'inner', on='NMI_NUMBER').drop_duplicates()


    # Formatting the dataset as azure dataset

    columns = ['ACCTNO', 'ACCOUNT', 'SITE ADDRESS', 'INVOICE #', 'NMI',
                'BILLING PERIOD START DATE', 'BILLING PERIOD END DATE',
                'BILLING PERIOD NUMBER OF DAYS', 'TOTAL GAS CONSUMPTION (GJ)',
                'TOTAL ENERGY SPEND $', 'NETWORK CHARGE ($)', 
                'DAILY SUPPLY CHARGE ($)', 'OTHER CHARGES', 'INVOICE TOTAL ($)'
                ]

    fm_data = pd.DataFrame(index=final_df.index, columns=columns)
    fm_data['ACCTNO'] = final_df['ACCT_NO'].astype(str)
    fm_data['ACCOUNT'] = 'SJG Midland Hospital'
    fm_data['SITE ADDRESS'] = final_df['SITE_ADDRESS'].astype(str)
    fm_data['INVOICE #'] = final_df['INVOICE'].astype(str)
    fm_data['NMI'] = final_df['NMI_NUMBER'].astype(int) 

    fm_data['BILLING PERIOD START DATE'] = pd.to_datetime(
        final_df['FROM_DATE'], format='%d/%m/%Y %I:%M:%S %p').dt.strftime('%Y-%m-%d') + ' 00:00:00'
    fm_data['BILLING PERIOD END DATE'] = pd.to_datetime(
        final_df['TO_DATE'], format='%d/%m/%Y %I:%M:%S %p').dt.strftime('%Y-%m-%d') + ' 00:00:00'
    
    fm_data['BILLING PERIOD NUMBER OF DAYS'] = (
        pd.to_datetime(final_df['TO_DATE'], format='%d/%m/%Y %I:%M:%S %p') - 
        pd.to_datetime(final_df['FROM_DATE'], format='%d/%m/%Y %I:%M:%S %p')
        ).dt.days
    fm_data['TOTAL GAS CONSUMPTION (GJ)'] = final_df['TOTAL_GAS_CONSUMPTION']
    fm_data['TOTAL ENERGY SPEND $'] = final_df['NW_SPENT']
    fm_data['NETWORK CHARGE ($)'] = final_df['GAS_SPENT']
    fm_data['DAILY SUPPLY CHARGE ($)'] = 0
    fm_data['OTHER CHARGES'] = 0
    fm_data['INVOICE TOTAL ($)'] = fm_data['TOTAL ENERGY SPEND $'] + fm_data['NETWORK CHARGE ($)']


    return fm_data



