# -*- coding:utf-8 -*-
__author__ = 'yx'
from reader import Reader

class SQLServerHiveReader(Reader):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'int' == data_type:
            return 'int'
        elif 'smallint' == data_type:
            return 'smallint'
        elif 'tinyint' == data_type:
            return 'tinyint'
        elif 'bigint' == data_type:
            return 'bitint'
        elif 'bit' == data_type:
            return 'int'
        elif 'float' == data_type:
            return 'float'
        elif 'double' == data_type:
            return 'double'
        elif 'real' == data_type:
            return 'decimal'
        elif 'decimal' == data_type:
            return 'decimal'
        elif 'char' == data_type:
            return 'string'
        elif 'nchar' == data_type:
            return 'string'
        elif 'varchar' == data_type:
            return 'string'
        elif 'nvarchar' == data_type:
            return 'string'
        elif 'string' == data_type:
            return 'string'
        elif 'ntext' == data_type:
            return 'string'
        elif 'smallmoney' == data_type:
            return 'string'
        elif 'money' == data_type:
            return 'string'
        elif 'text' == data_type:
            return 'string'
        elif 'image' == data_type:
            return 'string'
        elif 'boolean' == data_type:
            return 'boolean'
        elif 'timestamp' in data_type:
            return 'timestamp'
        elif 'date' in data_type:
            return 'date'
