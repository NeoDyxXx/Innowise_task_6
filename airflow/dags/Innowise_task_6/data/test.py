import pandas as pd

data = pd.read_csv('763K_plus_IOS_Apps_Info.csv')
data['All_Genres'] = data['All_Genres'].str.replace('\'', '')
data['Languages'] = data['All_Genres'].str.replace('\'', '')

print(data.iloc[1])