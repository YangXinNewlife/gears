# -*- coding:utf-8 -*-
__author__ = 'yx'
from src.mapping.reader.reader import Reader


class MysqlHiveReader(Reader):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'int' == data_type:
            return 'int'
        elif 'integer' == data_type:
            return 'int'
        elif 'smallint' == data_type:
            return 'int'
        elif 'mediumint' == data_type:
            return 'int'
        elif 'tinyint' == data_type:
            return 'int'
        elif 'bigint' == data_type:
            return 'bigint'
        elif 'binary' == data_type:
            return 'binary'
        elif 'varbinary' == data_type:
            return 'string'
        elif 'float' == data_type:
            return 'float'
        elif 'double' == data_type:
            return 'double'
        elif 'decimal' == data_type:
            return 'decimal'
        elif 'string' == data_type:
            return 'string'
        elif 'char' == data_type:
            return 'string'
        elif 'varchar' == data_type:
            return 'string'
        elif 'tinyblob' == data_type:
            return 'string'
        elif 'tinytext' == data_type:
            return 'string'
        elif 'blob' == data_type:
            return 'string'
        elif 'text' == data_type:
            return 'string'
        elif 'mediumblob' == data_type:
            return 'string'
        elif 'mediumtext' == data_type:
            return 'string'
        elif 'longblob' == data_type:
            return 'string'
        elif 'longtext' == data_type:
            return 'string'
        elif 'boolean' == data_type:
            return 'boolean'
        elif 'time' in data_type:
            return 'timestamp'
        elif 'year' in data_type:
            return 'timestamp'
        elif 'datetime' in data_type:
            return 'timestamp'
        elif 'timestamp' in data_type:
            return 'timestamp'
        elif 'date' in data_type:
            return 'date'
