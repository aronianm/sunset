from sunset.sunrise import SunRise
import pandas as pd


connection = "mysql://root:FLGA--@localhost/pandora"


class Foo(SunRise):
    def __init__(self, connection):
        super().__init__(connection)
    
    @property
    def table_name(self):
        return 'cities'

    def load_data(self, cx):
        df = pd.read_csv('cities.csv')
        return df

    def selected_columns(self):
        return ['id', 'master_name', 'state_id']
    
    def db_columns(self):
        return ['master_name']
    
    @property
    def rename_columns(self):
        return {'master_name': 'name'}

    @property
    def uniq_keys(self):
        return ['name']
    
    def format_dataframe(self, df):
        return df.drop(columns=['id', 'state_id'])



Foo(connection).purport()