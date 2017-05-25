# -*- coding:utf-8 -*-
__author__ = 'yx'
from writer import Writer

class SQLServerHiveHawqWriter(Writer):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'bigint' == data_type.lower():
            return 'bigint'
        elif 'int' == data_type.lower():
            return 'int'
        elif 'smallint' == data_type.lower():
            return 'smallint'
        elif 'tinyint' == data_type.lower():
            return 'tinyint'
        elif 'money' == data_type.lower():
            return 'string'
        elif 'smallmoney' == data_type.lower():
            return 'string'
        elif 'decimal' == data_type.lower():
            return 'double'
        elif 'numeric' == data_type.lower():
            return 'double'
        elif 'datetime' == data_type.lower():
            return 'date'
        elif 'timestamp' == data_type.lower():
            return 'timestamp'
        elif 'bit' == data_type.lower():
            return 'int'
        elif 'float' == data_type.lower():
            return 'float'
        elif 'real' == data_type.lower():
            return 'double'
        elif 'char' == data_type.lower():
            return 'string'
        elif 'nchar' == data_type.lower():
            return 'string'
        elif 'varchar' == data_type.lower():
            return 'string'
        elif 'nvarchar' == data_type.lower():
            return 'string'
        elif 'text' == data_type.lower():
            return 'string'
        elif 'ntext' == data_type.lower():
            return 'string'
        elif 'image' == data_type.lower():
            return 'string'
        elif 'binary' == data_type.lower():
            return 'binary'
        elif 'varbinary' == data_type.lower():
            return 'binary'
        elif 'sql_variant' == data_type.lower():
            return 'string'
        elif 'uniqueidentifier' == data_type.lower():
            return 'string'
        elif 'xml' == data_type.lower():
            return 'string'
        elif 'hierarchyid' == data_type.lower():
            return 'string'
        else:
            return data_type.lower()


