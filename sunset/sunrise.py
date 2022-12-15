from sunset.connector import DBConnection
import pandas as pd
from abc import ABC, abstractmethod
import logging
from sunset.sunset import Sunset
from sunset.mysql_helpers import TwentyThree
from sunset.bright_moon import BrightMoon


class SunRise(ABC):

    def __init__(self, db_connection):
        self.db_connection = db_connection
    

    def purport(self):
        with DBConnection(self.db_connection) as connection:
            self.__build_sqltable__(connection[1])
            self.df = self.load_data(connection[1])
            self._assert_dataframe_exists(self.df)
            cols = self.selected_columns()
            self._assert_columns_exist(cols)
            self.df = self.df.rename(
                columns=self.rename_columns)
            if len(self.df) > 0:
                self.df = self.format_dataframe(self.df)
                sr = Sunset(self.df, connection[1], self.table)
                sr.set_db_cols(self.db_columns())
                updates, appends = sr.create_boat(self.uniq_keys)
                BrightMoon(updates, appends, connection[0]).imports(self.table, self.uniq_keys)
                
            else:
                print("\N{winking face}")
                print('\nYour DataFrame is empty.\n')

    @abstractmethod
    def load_data(self):
        pass

    @abstractmethod
    def selected_columns(self):
        pass

    @abstractmethod
    def format_dataframe(self):
        pass

    @abstractmethod
    def db_columns(self):
        pass
    
    def rename_columns(self):
        return {}
    
    @abstractmethod
    def uniq_keys(self):
        pass
    
    def __build_sqltable__(self, cx):
        self.table = TwentyThree(self.table_name, cx).build()

    def _assert_columns_exist(self, cols):
        dataframe_cols = self.df.columns.to_list()
        col_error = ""
        for c in cols:
            if c not in dataframe_cols:
                col_error += f"\n Add columns '{c}' to your selected_columns or remove it from your dataframe"
        if len(col_error) > 0:
            raise EnvironmentError(col_error)
        else:
            self.df = self.df[cols]
    
    def _assert_dataframe_exists(self, obj):
        assert type(obj) == pd.DataFrame, 'Must use pandas \n pip install pandas >= 1.5.2'
                


        
