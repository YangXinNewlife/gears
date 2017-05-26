# -*- coding:utf-8 -*-
__author__ = 'yangxin'
import MySQLdb
from src.logger import logger
from src.client.client import Client

class MysqlClient(Client):

    def get_connection(self):
        conn = MySQLdb.connection(
            host=self.params.get("host"),
            port = self.params.get("port"),
            user=self.params.get("user"),
            password=self.params.get("password"),
            database=self.params.get("db"))
        return conn

    def query_sql_inner(self, sql):
        cursor = self.conn.cursor()
        rows = cursor.execute(sql)
        return rows

    def execute_sql_inner(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)

    def close_connection(self):
        self.conn.close()