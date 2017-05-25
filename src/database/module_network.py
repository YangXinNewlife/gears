#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys

reload(sys)


class ModuleNetwork(ModuleBase):

    def __init__(self, table_name="\"ironhide_schema\".\"t_network\""):
        self.table = table_name

    def update_status(self, network_id, status):
        sql = "UPDATE %s SET status = '%s' WHERE \"autoKey\" = %s" %(self.table, status, network_id)
        client = PostgresClient()
        client.execute_sql(sql)
        client.close()


    def get(self, network_id):
        #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
        client = PostgresClient()
        row = client.fetch_data(self.table, "WHERE \"autoKey\" = %s" % network_id)
        client.close()
        return row if not row else row[0]

    def get_network_by_userid(self, user_id):
        #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
        client = PostgresClient()
        rows = client.fetch_data(self.table, "WHERE \"user_id\" = '%s' AND \"status\"!='ceased'" % user_id)
        client.close()
        return rows

    def add(self, user_id, info, status="running"):
        sql = "INSERT INTO %s (user_id, info, status) VALUES ('%s', '%s', '%s') returning *;" \
              % (self.table, user_id, info, status)
        client = PostgresClient()
        ret = client.insert_sql(sql)
        client.close()
        return ret

