#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys

reload(sys)

class ModuleBilling(ModuleBase):

    def __init__(self, table_name="\"ironhide_schema\".\"t_billing\""):
        self.table = table_name


    def get(self, billing_id):
        #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
        client = PostgresClient()
        row = client.fetch_data(self.table, "WHERE \"autoKey\" = %s" % billing_id)
        client.close()
        return row if not row else row[0]

    def get_by_user_id(self, userid):
        #sql = "SELECT * FROM %s WHERE autoKey = %s" % (self.table, env_id)
        client = PostgresClient()
        rows = client.fetch_data(self.table, "WHERE user_id = '%s'" % userid)
        client.close()
        return rows

    def add(self, user_id, env_id, env_type, service_id, type):
        sql = "INSERT INTO %s (user_id, env_id, env_type, service_id, type) VALUES ('%s', '%s', '%s', '%s', '%s') returning *;" \
              % (self.table, user_id, env_id, env_type, service_id, type)
        print sql
        client = PostgresClient()
        ret = client.insert_sql(sql)
        client.close()
        return ret

#     def get_user_bill_env_during(self, user_id, start, end):
#         """SELECT env_id from ironhide_schema.t_billing where create_time<now() and user_id=20 GROUP BY (env_id) HAVING "count"(env_id)=2
# UNION
# (SELECT env_id from ironhide_schema.t_billing where ("type"='stop' and create_time at time zone 'CCT'>DATE(now() + '8 hours')))
# INNER JOIN (SELECT env_id from ironhide_schema.t_billing where ("type"='start' and create_time at time zone 'CCT'< DATE(now() + '8 hours')+1) as table2) on env_id=table2.env_id
#
# """
#         sql = """SELECT env_id from %s where create_time at time zone 'CCT'>%s and user_id=%s GROUP BY (env_id) HAVING "count"(env_id)=1
# UNION
# SELECT env_id from %s where (("type"='stop' and create_time at time zone 'CCT'> %s)
# or ("type"='start' and create_time at time zone 'CCT'< %s))
# and user_id =%s
# """%(self.table, start, user_id, self.table, start, end, user_id)
#         client = PostgresClient()
#         cur = client.execute_sql(sql)
#         rows = cur.fetchall()
#         client.close()
#         return rows
