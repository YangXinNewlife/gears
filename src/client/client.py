# -*- coding:utf-8 -*-
__author__ = 'yangxin'

from abc import ABCMeta
from abc import abstractmethod
import src.logger

class Client:
    __metaclass__ = ABCMeta

    def __init__(self, data_source):
        self.params = data_source

    def execute_sql(self, sql):
        self.conn = self.get_connection()
        self.execute_sql_inner(sql)
        self.close_connection()

    def query_sql(self, sql):
        pass

    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass

    @abstractmethod
    def execute_sql_inner(self, sql):
        pass

    @abstractmethod
    def query_sql_inner(self, sql):
        pass

    @staticmethod
    def get_client(self, options):
        '''
        if (options.client_type == 'mysql'):
            return OracleClient(option)
        '''

