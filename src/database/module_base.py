#-*- coding:utf-8 -*-
__author__ = 'yx'

from postgres_client import *
import sys
reload(sys)

class ModuleBase:
    def __init__(self, table=None):
        self.schema = "public"
        self.table = table
        self.table_name = "\"%s\".\"%s\"" % (self.schema, self.table)

    def get_columns(self):
        client = PostgresClient()
        ret = client.get_schema(self.schema, self.table)
        client.close()
        return ret

    def get_mycolumns(self,host,port, user,password, database,schema, table):
        client = PostgresClient(conf=None, db_host=host,port=port, user=user, password=password, database=database)
        ret = client.get_schema_type(schema, table)
        client.close()
        return ret

    def add(self, **columns):
        #id, user_id, ehc_type, ehc_package, vpc_id, ehc_setting
        table_columns = self.get_columns()
        keys = []
        values = []
        for column in columns.items():
            if column[0] not in table_columns:
                pass
            if column[1]:
                keys.append(column[0])
                values.append("%s" % str(adapt(column[1].decode('unicode_escape'))))
        key_str = ','.join(keys)
        value_str = ','.join(values)
        sql = "INSERT INTO %s " % self.table_name + " (%s)" % key_str + " VALUES " + "(%s)" % value_str + " returning *;"
        client = PostgresClient()
        ret = client.insert_sql(sql)
        client.close()
        return ret

    def get(self, ids=None, **filters):
        client = PostgresClient()
        rows = client.fetch_data(self.schema, self.table, ids, **filters)
        client.close()
        return rows


    def update(self, id, **pairs):
        table_columns = self.get_columns()
        key_values = []
        for column in pairs.items():
            if column[0] not in table_columns:
                pass
            value = column[1] if not column[1] else str(adapt(column[1].decode('unicode_escape')))
            key_values.append("%s = %s" % (column[0], value))
        key_value_str = ','.join(key_values)
        #sql = "UPDATE %s SET name = '%s',description = '%s'  WHERE \"autoKey\" = %s" %(self.table, name, description, ehc_id)

        sql = "UPDATE %s SET %s WHERE \"id\" = '%s';" % (self.table_name, key_value_str, id)
        client = PostgresClient()
        ret = client.execute_sql(sql)
        client.close()
        return ret
