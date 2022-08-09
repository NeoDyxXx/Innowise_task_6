from airflow.models.baseoperator import BaseOperator
import pandas as pd

class CSVParser(BaseOperator):
    def __init__(self, input_file_name: str, output_file_name: str, **kwargs):
        super().__init__(**kwargs)
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def execute(self, context):
        data = pd.read_csv(self.input_file_name)

        for column in data.select_dtypes(['object']).columns:
            if column not in ['Original_Release_Date', 'Current_Version_Release_Date']:
                data[column] = data[column].fillna('-')

        for column in data[data.columns.difference(data.select_dtypes(['object']).columns)].columns:
            data[column] = data[column].fillna(-999)

        data['Original_Release_Date'] = data['Original_Release_Date'].fillna('1000-01-01T23:00:00Z')
        data['Current_Version_Release_Date'] = data['Current_Version_Release_Date'].fillna('1000-01-01T23:00:00Z')
        data[data['Original_Release_Date'] == '0000-00-00T00:00:00Z'] = '1000-01-01T01:00:00Z'
        data[data['Current_Version_Release_Date'] == '0000-00-00T00:00:00Z'] = '1000-01-01T01:00:00Z'

        data['All_Genres'] = data['All_Genres'].str.replace('\'', '')
        data['Languages'] = data['All_Genres'].str.replace('\'', '')

        data.to_csv(self.output_file_name, index=False)

        # TODO: Make writing files in one directory with file's counter