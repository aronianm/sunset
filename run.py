from sunset.sunset import Sunset
import pandas as pd


connection = "mysql://root:FLGA--@localhost/pandora"


class Foo(Sunset):
    def __init__(self, connection):
        super().__init__(connection)
    
    @property
    def table_name(self):
        return 'cities'

    def load_data(self, cx):
        sql = 'select * from cities'
        df = pd.read_sql(sql, cx)
        return df

    def selected_columns(self):
        return ['id', 'name', 'state_id']
    
    def db_columns(self):
        return ['master_name']
    
    @property
    def rename_columns(self):
        return {'name': 'master_name'}

    
    def format_dataframe(self, df):
        return df.drop(columns=['id', 'state_id'])



Foo(connection).purport()