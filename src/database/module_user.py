#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys

reload(sys)


class ModuleUser(ModuleBase):
    def __init__(self, table="t_user"):
        self.schema = "ehc"
        self.table = table
        self.table_name = "\"%s\".\"%s\"" % (self.schema, self.table)

    # def __init__(self, table_name="\"ehc\".\"t_user\""):
    #     self.table = table_name

    # def get(self, user_id=None, user_name=None):
    #     client = PostgresClient()
    #     row = client.fetch_data(self.table, "WHERE \"autoKey\" = %s" % user_id)
    #     client.close()
    #     return row if not row else row[0]
    #
    # def get_by_partner_id(self, partner_userid):
    #     #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
    #     client = PostgresClient()
    #     row = client.fetch_data(self.table, "WHERE partner_user_id = '%s'" % partner_userid)
    #     client.close()
    #     return row

    # def add(self, name, partnerRawdata, partner_user_id, email="", phone=""):
    #     sql = "INSERT INTO %s (name, email, phone, partnerRawdata, partner_user_id) VALUES ('%s', '%s', '%s', '%s', '%s') returning *;" \
    #           % (self.table, name, email, phone, partnerRawdata, partner_user_id)
    #     print sql
    #     client = PostgresClient()
    #     ret = client.insert_sql(sql)
    #     client.close()
    #     return ret

    def update_access_info(self, access, user_id):
        sql = "UPDATE %s SET access_info = '%s' WHERE \"autoKey\" = %s" % (self.table, access, user_id)
        client = PostgresClient()
        client.execute_sql(sql)
        client.close()

    def update_status(self, status, user_id):
        sql = "UPDATE %s SET status = '%s' WHERE \"autoKey\" = %s" % (self.table, status, user_id)
        client = PostgresClient()
        client.execute_sql(sql)
        client.close()

    def get_all(self):
        client = PostgresClient()
        rows = client.fetch_data(self.table)
        client.close()
        return rows
