# -*- coding:utf-8 -*-
__author__ = 'yx'
from router import Router
import happybase


class HBaseRouter(Router):

    def __init__(self, host):
        self.connection = happybase.Connection(host)

    def get_all_tables(self):
        alltables = self.connection.tables()
        return alltables

    def create_hbase_table(self, tablename, columns):
        self.connection.create_table('%s'%(tablename),{})

    def get_table(self, tablename):
        table = self.connection.table('%s'%(tablename))
        return table

    def get_row(self, table, row_key):
        row = self.connection.table(table).row(b'%s'%(row_key))
        return row

    def close(self):
        self.close()

hclient = HBaseRouter('192.168.1.51')
hclient.getalltable()

