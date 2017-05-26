# -*- coding:utf-8 -*-
__author__ = 'yangxin'

from src.client.client import Client
import pyhs2

class Hive(Client):

    def get_connection(self):
        conn = pyhs2.connect(
            host=self.params.get('host'),
            port=self.params.get('port'),
            authMechanism=self.params.get('authMechanism'),
            user=self.params.get('user'),
            password=self.params.get('password'),
            database=self.params.get('db'))
        return conn

    def close_connection(self):
        self.conn.close()

    def query_sql_inner(self, sql):
        cursor = self.conn.cursor()
        rows = cursor.execute(sql)
        return rows

    def execute_sql_inner(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)