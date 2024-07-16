import pandas as pd
file = 'RMT-APL-01-MSB-APR73-01-50002728-DL3.csv'
df = pd.read_csv(file)
df.dropna(axis=1, how='all', inplace=True)
df.drop_duplicates(subset=['DateTime'], keep='first', inplace=True)
df.to_csv(file, index=False)
