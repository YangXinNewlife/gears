#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys

reload(sys)

class ModuleEHC(ModuleBase):

    def __init__(self, table="t_ehc"):
        self.schema = "ehc"
        self.table = table
        self.table_name = "\"%s\".\"%s\"" % (self.schema, self.table)

    # def add(self, **columns):
    #     #id, user_id, ehc_type, ehc_package, vpc_id, ehc_setting
    #     table_columns = self.get_columns()
    #     keys = []
    #     values = []
    #     for column in columns.items():
    #         if column not in table_columns:
    #             pass
    #         keys.append(column[0])
    #         values.append("'%s'" % column[1])
    #     key_str = ','.join(keys)
    #     value_str = ','.join(values)
    #     sql = "INSERT INTO %s " % self.table + " (%s)" % key_str + " VALUES " + "(%s)" % value_str + " returning *;"
    #     client = PostgresClient()
    #     ret = client.insert_sql(sql)
    #     client.close()
    #     return ret

    # def update_status(self, ehc_id, status):
    #     sql = "UPDATE %s SET status = '%s' WHERE \"autoKey\" = %s" %(self.table, status, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def update_err_info(self, ehc_id, errinfo):
    #     sql = "UPDATE %s SET err_info = '%s' WHERE \"autoKey\" = %s" %(self.table, errinfo, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def update_billing(self, ehc_id, billing_id):
    #     sql = "UPDATE %s SET billing = '%s' WHERE \"autoKey\" = %s" %(self.table, billing_id, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def update_billing_ret(self, ehc_id, billing_ret):
    #     sql = "UPDATE %s SET billing_ret = '%s' WHERE \"autoKey\" = %s" %(self.table, billing_ret, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def update_network(self, ehc_id, network_id):
    #     sql = "UPDATE %s SET network_id = '%s' WHERE \"autoKey\" = %s" %(self.table, network_id, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def update_info(self, ehc_id, info):
    #     sql = "UPDATE %s SET info = '%s' WHERE \"autoKey\" = %s" %(self.table, info, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def update_name_description(self, ehc_id, name, description):
    #     sql = "UPDATE %s SET name = '%s',description = '%s'  WHERE \"autoKey\" = %s" %(self.table, name, description, ehc_id)
    #     client = PostgresClient()
    #     client.execute_sql(sql)
    #     client.close()
    #
    # def get(self, ehc_id):
    #     client = PostgresClient()
    #     row = client.fetch_data(self.table, "WHERE \"autoKey\" = %s" % ehc_id)
    #     client.close()
    #     return row if not row else row[0]
    #
    # def get_ehcs_by_userid(self, user_id):
    #     client = PostgresClient()
    #     rows = client.fetch_data(self.table, "WHERE \"user_id\" = '%s' AND \"status\"!='ceased'" % user_id)
    #     client.close()
    #     return rows
    #
    # def get_count_of_running(self):
    #     sql = "select user_id,count(*) from ironhide_schema.t_ehc where status!='terminated' and status!='ceased' GROUP BY user_id"
    #     client = PostgresClient()
    #     cur = client.execute_sql(sql)
    #     rows = cur.fetchall()
    #     client.close()
    #     return rows
    #
    # def get_recent(self, days):
    #     sql = "select user_id,count(*) from ironhide_schema.t_ehc where create_time > (now() - interval '%s day') GROUP BY user_id" % days
    #     client = PostgresClient()
    #     cur = client.execute_sql(sql)
    #     rows = cur.fetchall()
    #     client.close()
    #     return rows
