#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys

reload(sys)


class ModuleJob(ModuleBase):

    def __init__(self, table_name="\"ironhide_schema\".\"t_job\""):
        self.table = table_name

    def update_status(self, job_id, status):
        sql = "UPDATE %s SET status = '%s' WHERE \"autoKey\" = %s" %(self.table, status, job_id)
        client = PostgresClient()
        client.execute_sql(sql)
        client.close()

    def get(self, job_id):
        #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
        client = PostgresClient()
        row = client.fetch_data(self.table, "WHERE \"autoKey\" = %s" % job_id)
        client.close()
        return row if not row else row[0]

    def get_user_latest(self, user_id):
        client = PostgresClient()
        row = client.fetch_data(self.table, "WHERE \"user_id\" = %s ORDER BY create_time DESC" % user_id)
        client.close()
        return row if not row else row[0]

    # def get_envs_by_userid(self, user_id):
    #     #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
    #     client = PostgresClient()
    #     rows = client.fetch_data(self.table, "WHERE \"user_id\" = '%s'" % user_id)
    #     client.close()
    #     return rows

    def add(self, user_id, type, raw_data="{}"):
        sql = "INSERT INTO %s (user_id, type, raw_data) VALUES ('%s', '%s', '%s') returning *;" % (self.table, user_id, type, raw_data)
        client = PostgresClient()
        ret = client.insert_sql(sql)
        client.close()
        return ret
