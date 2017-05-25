#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys
import time
reload(sys)


class ModuleImportJob(ModuleBase):

    def __init__(self, table_name="\"public\".\"gears\""):
        self.table = table_name

    def update_status(self, job_id, status):
        sql = "UPDATE %s SET status = '%s' WHERE \"job_id\" = '%s'" %(self.table, status, job_id)
        client = PostgresClient()
        client.execute_sql(sql)
        client.close()

    def update_status_time(self, job_id, status):
        sql = "UPDATE %s SET status = '%s', finish_time = '%d' WHERE \"job_id\" = '%s'" %(self.table, status, (time.time()*1000), job_id)
        client = PostgresClient()
        client.execute_sql(sql)
        client.close()

    def get(self, job_id):
        #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
        client = PostgresClient()
        row = client.fetch_data("public", "gears", job_id=job_id)
        client.close()
        return row if not row else row[0]

    # def get_user_latest(self, user_id):
    #     client = PostgresClient()
    #     row = client.fetch_data(self.table, "WHERE \"user_id\" = %s ORDER BY create_time DESC" % user_id)
    #     client.close()
    #     return row if not row else row[0]

    # def get_envs_by_userid(self, user_id):
    #     #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
    #     client = PostgresClient()
    #     rows = client.fetch_data(self.table, "WHERE \"user_id\" = '%s'" % user_id)
    #     client.close()
    #     return rows

    def add(self, job_id, status):
        sql = "INSERT INTO %s (job_id, status) VALUES ('%s', '%s') returning *;" % (self.table, job_id, status)
        client = PostgresClient()
        ret = client.insert_sql(sql)
        client.close()
        return ret
