import pandas as pd

data = pd.read_csv('763K_plus_IOS_Apps_Info.csv')

for column in data.select_dtypes(['object']).columns:
    if column not in ['Original_Release_Date', 'Current_Version_Release_Date']:
        data[column] = data[column].fillna('-')

for column in data[data.columns.difference(data.select_dtypes(['object']).columns)].columns:
    data[column] = data[column].fillna(-999)

data['Original_Release_Date'] = data['Original_Release_Date'].fillna('1000-01-01T23:00:00Z')
data['Current_Version_Release_Date'] = data['Current_Version_Release_Date'].fillna('1000-01-01T23:00:00Z')

data.to_csv('parse.csv', index=False)