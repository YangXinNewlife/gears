# -*- coding:utf-8 -*-
__author__ = 'yx'
from reader import Reader
class PostgresqlHiveReader(Reader):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'int' == data_type:
            return 'int'
        elif 'int2' == data_type:
            return 'int'
        elif 'int4' in data_type:
            return 'int'
        elif 'int8' in data_type:
            return 'int'
        elif 'bigint' in data_type:
            return 'bigint'
        elif 'bit' in data_type:
            return 'int'
        elif 'bit varying' in data_type:
            return 'int'
        elif 'boolean' in data_type:
            return 'boolean'
        elif 'box' in data_type:
            return 'string'
        elif 'bytea' in data_type:
            return 'binary'
        elif 'character varying' in data_type:
            return 'string'
        elif 'bigserial' in data_type:
            return 'binary'
        elif 'character' in data_type:
            return 'string'
        elif 'cidr' in data_type:
            return 'string'
        elif 'circle' in data_type:
            return 'string'
        elif 'float' in data_type:
            return 'float'
        elif 'double precision' in data_type:
            return 'double'
        elif 'float8' in data_type:
            return 'double'
        elif 'inet' in data_type:
            return 'string'
        elif 'integer' in data_type:
            return 'int'
        elif 'interval' in data_type:
            return 'string'
        elif 'line' in data_type:
            return 'string'
        elif 'lseg' in data_type:
            return 'string'
        elif 'macaddr' in data_type:
            return 'string'
        elif 'numeric' in data_type:
            return 'decimal'
        elif 'path' in data_type:
            return 'string'
        elif 'point' in data_type:
            return 'string'
        elif 'polygon' in data_type:
            return 'string'
        elif 'real' in data_type:
            return 'decimal'
        elif 'smallint' in data_type:
            return 'int'
        elif 'serial' in data_type:
            return 'int'
        elif 'text' in data_type:
            return 'string'
        elif 'uuid' in data_type:
            return 'string'
        elif 'timestamp' in data_type:
            return 'timestamp'
        elif 'date' in data_type:
            return 'date'