import pandas as pd

# Load the dataset
file_path = 'RMT-APL-01-MDB1-APR01-01-50002745-DL1.csv'
df = pd.read_csv(file_path)

# Ensure Meter column is included
df['Meter'] = 'RMT-APL-01-MDB1-APR01-01-50002745-DL1'

# Check for duplicates based on Meter and DateTime
duplicates = df[df.duplicated(subset=['Meter', 'DateTime'], keep=False)]

# Display the duplicates
# Display all columns for the identified duplicates
print(duplicates.loc[36505:36604, :])
