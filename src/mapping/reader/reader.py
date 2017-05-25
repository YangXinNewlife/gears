# -*- coding:utf-8 -*-
__author__ = 'yx'


class Reader(object):
    def __init__(self):
        pass

    def convert_data_type(self, data_type):
        if 'int' == data_type:
            return 'int'
        elif 'float' == data_type:
            return 'float'
        elif 'double' == data_type:
            return 'double'
        elif 'string' == data_type:
            return 'string'
        elif 'boolean' == data_type:
            return 'boolean'
        elif 'timestamp' in data_type:
            return 'timestamp'
        elif 'date' in data_type:
            return 'date'

