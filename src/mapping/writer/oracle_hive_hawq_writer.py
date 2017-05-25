# -*- coding:utf-8 -*-
__author__ = 'yx'
from writer import Writer


class OracleHiveHawqWriter(Writer):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'varchar2' == data_type.lower():
            return 'varchar'
        elif 'varchar' == data_type.lower():
            return 'varchar'
        elif 'varchar(1000)' == data_type.lower():
            return 'varchar'
        elif 'varchar(1)' == data_type.lower():
            return 'varchar'
        elif 'nchar' == data_type.lower():
            return 'varchar'
        elif 'nvarchar2' == data_type.lower():
            return 'varchar'
        elif 'char' == data_type.lower():
            return 'varchar'
        elif 'long' == data_type.lower():
            return 'bigint'
        elif 'raw' == data_type.lower():
            return 'bigint'
        elif 'blob' == data_type.lower():
            return 'varchar'
        elif 'clob' == data_type.lower():
            return 'varchar'
        elif 'nclob' == data_type.lower():
            return 'varchar'
        elif 'number' == data_type.lower():
            return 'int'
        elif 'integer' == data_type.lower():
            return 'int'
        elif 'real' == data_type.lower():
            return 'float8'
        elif 'smallint' == data_type.lower():
            return 'smallint'
        elif 'int' == data_type.lower():
            return 'int'
        elif 'float' == data_type.lower():
            return 'float4'
        elif 'decimal' == data_type.lower():
            return 'decimal'
        elif 'double' == data_type.lower():
            return 'float8'
        elif 'datetime' == data_type.lower():
            return 'timestamp'
        elif 'date' == data_type.lower():
            return 'timestamp'
        elif 'timestamp' == data_type.lower():
            return 'timestamp'
        elif 'timestamp(6)' == data_type.lower():
            return 'timestamp'
        elif 'timestamp(6)(11)' == data_type.lower():
            return 'timestamp'
        elif 'timestamp(11)' == data_type.lower():
            return 'timestamp'
        else:
            return data_type.lower()
