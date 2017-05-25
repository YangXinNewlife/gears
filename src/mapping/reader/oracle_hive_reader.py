# -*- coding:utf-8 -*-
__author__ = 'yx'
from reader import Reader


class OracleHiveReader(Reader):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'varchar2' == data_type.lower():
            return 'string'
        elif 'varchar' == data_type.lower():
            return 'string'
        elif 'varchar(1)' == data_type.lower():
            return 'string'
        elif 'nchar' == data_type.lower():
            return 'string'
        elif 'nvarchar2' == data_type.lower():
            return 'string'
        elif 'char' == data_type.lower():
            return 'string'
        elif 'long' == data_type.lower():
            return 'bigint'
        elif 'raw' == data_type.lower():
            return 'bigint'
        elif 'blob' == data_type.lower():
            return 'string'
        elif 'clob' == data_type.lower():
            return 'string'
        elif 'nclob' == data_type.lower():
            return 'string'
        elif 'number' == data_type.lower():
            return 'int'
        elif 'integer' == data_type.lower():
            return 'int'
        elif 'real' == data_type.lower():
            return 'double'
        elif 'smallint' == data_type.lower():
            return 'smallint'
        elif 'int' == data_type.lower():
            return 'int'
        elif 'float' == data_type.lower():
            return 'float'
        elif 'decimal' == data_type.lower():
            return 'decimal'
        elif 'double' == data_type.lower():
            return 'double'
        elif 'datetime' == data_type.lower():
            return 'date'
        elif 'date' == data_type.lower():
            return 'timestamp'
        elif 'timestamp' == data_type.lower():
            return 'timestamp'
        elif 'timestamp(6)' == data_type.lower():
            return 'timestamp'
        else:
            return data_type.lower()
