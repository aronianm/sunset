import pandas as pd
from sqlalchemy import Column, Integer, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
import re

SQL_TYPES  = [
    ('bigint', BigInteger),
    ('varchar\(\d+\)', String),
]

def lookup(tp, lookups):
    for pattern, value in lookups:
        if re.search(pattern, tp):
            if re.search('\d+', tp):
                return value(re.search('\d+', tp)[0])
            else:
                return value
    return None


class JEN23():
    def __init__(self, table_name, cx) -> None:
        self.table_name = table_name
        self.class_name = self.camel_name()
        self.cx = cx
        self.table_options = {}

    def camel_name(self):
        return ''.join(
            x for x in self.table_name.title() if not x.isspace())

    def build(self):
        describe_sql = f'describe {self.table_name}'

        table = pd.read_sql(describe_sql, self.cx)
        return self.constructor(table)

    # constructor
    def constructor(self,table):
        self.table_options['__tablename__'] = self.table_name
        for i, row in table.iterrows():
            self.table_options[row['Field']] = self._configure_field(row)
        return self._create()

    def _get_type(self, t):
        return lookup(t, SQL_TYPES)

    def _configure_field(self, column):
        typ = self._get_type(column['Type'])
        column_hash = self._format_args(column.to_dict())
        return Column(typ, **column_hash)

       
    def _format_args(self, h):
        del h['Field']
        del h['Type']
        if h.get('Null'):
            h['nullable'] = h.get('Null') != 'NO'
            del h['Null']
        if h.get('Key'):
            h['primary_key'] = h.get('Key') == 'PRI'
        del h['Extra']
        del h['Key']
        del h['Default']
        return h

    # class method
    @classmethod
    def classMethod(cls, arg):
        print(arg)
    
    # creating class dynamically
    def _create(self):
        return type(self.class_name, (Base, ), self.table_options)