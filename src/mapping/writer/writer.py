# -*- coding:utf-8 -*-
__author__ = 'yx'


class Writer(object):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'int' == data_type:
            return 'int'
        elif 'tingint' == data_type:
            return 'tinyint'
        elif 'bigint' == data_type:
            return 'bigint'
        elif 'binary' == data_type:
            return 'binary'
        elif 'float' == data_type:
            return 'float4'
        elif 'double' == data_type:
            return 'float8'
        elif 'decimal' == data_type:
            return 'decimal'
        elif 'string' == data_type:
            return 'varchar'
        elif 'char' == data_type:
            return 'varchar'
        elif 'varchar' == data_type:
            return 'varchar'
        elif 'boolean' == data_type:
            return 'boolean'
        elif 'timestamp' in data_type:
            return 'timestamp'
        elif 'date' in data_type:
            return 'date'
