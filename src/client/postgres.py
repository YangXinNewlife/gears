# -*- coding:utf-8 -*-
__author__ = 'yx'
import psycopg2
import sys
from src.logger import logger
from src.job_attributes import JobStatus
from src.database import db_conns


class PostgresqlClient(object):
    def __init__(self, db_host = None, user = None, password = None, database = None, port = None, jobid = None):
        loger = logger.Logger("Method:init")
        try:
            self.conn = psycopg2.connect(host=db_host, port=port, user=user, password=password, database=database)
            self.jobid = jobid
            self.conn.set_client_encoding('utf-8')
        except Exception as error_message:
            loger.print_error("postgresql db connection error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def postgresql_table_attributes(self, table):
        loger = logger.Logger("Method:postgresql_table_attributes")
        try:
            primary_key_sql = "select pg_attribute.attname as colname from pg_constraint  inner join pg_class on pg_constraint.conrelid = pg_class.oid inner join pg_attribute on pg_attribute.attrelid = pg_class.oid  and  pg_attribute.attnum = pg_constraint.conkey[1]inner join pg_type on pg_type.oid = pg_attribute.atttypid where pg_class.relname = '%s' and pg_constraint.contype='p' and pg_table_is_visible(pg_class.oid)"%(table)
            primary_keys = self.query_one(primary_key_sql)
            columns = self.getTable_columns(table)
            return primary_keys, columns
        except Exception as error_message:
            loger.print_error("get postgresql_table_attributes error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

 
    def postgresql_external_table_hbase_attributes(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:postgresql_external_table_hbase_attributes")
        try:
            primary_key_sql = "select pg_attribute.attname as colname from pg_constraint  inner join pg_class on pg_constraint.conrelid = pg_class.oid inner join pg_attribute on pg_attribute.attrelid = pg_class.oid  and  pg_attribute.attnum = pg_constraint.conkey[1]inner join pg_type on pg_type.oid = pg_attribute.atttypid where pg_class.relname = '%s' and pg_constraint.contype='p' and pg_table_is_visible(pg_class.oid)"%(table)
            primary_keys = self.query_one(primary_key_sql)
            columns, columns_new = self.getTable_hbase_columns(table, column_family, statement_dt, statement)
            return primary_keys, columns, columns_new
        except Exception as error_message:
            loger.print_error("get postgresql_external_table_hbase_attributes error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_columns(self, table):
        loger = logger.Logger("Method:getTable_columns")
        try:
            sql = "select column_name, data_type from information_schema.columns where table_name = '%s' ORDER BY ordinal_position"%(table)
            fieldNames, fieldType = self.getTableInfo(sql)
            columns = ""
            count = 0
            for i in range(len(fieldNames)):
                columns += fieldNames[i]
                columns += ' '
                columns += fieldType[i]
                if count != (len(fieldNames) - 1):
                    columns += ', '
                count += 1
            return columns
        except Exception as error_message:
            loger.print_error("get getTable_columns error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_hbase_columns(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:getTable_hbase_columns")
        try:
            table_sql = "select column_name, data_type from information_schema.columns where table_name = '%s' ORDER BY ordinal_position"%(table)
            columns_name, data_type = self.getTableInfo(table_sql)
            columns = "recordkey %s, "%(statement_dt)
            count = 0
            for i in range(len(columns_name)):
                columns = str(columns) + '"'+ str(column_family) + ':'+ str(columns_name[i]) + '"'
                columns += ' '
                columns += data_type[i]
                if count != (len(columns_name) - 1):
                    columns += ', '
                count += 1
            columns_new = ''
            count1 = 0
            for j in range(len(columns_name)):
                if columns_name[j] == statement:
                    columns_new += 'recordkey'
                else:
                    columns_new += '"'+ str(column_family) + ':'+ str(columns_name[j]) + '"'
                if count1 != (len(columns_name) - 1):
                    columns_new += ', '
                count1 += 1
            return columns,columns_new
        except Exception as error_message:
            loger.print_error("get getTable_hbase_columns error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTableInfo(self, strReslut):
        loger = logger.Logger("Method:getTableInfo")
        try:
            fieldsName = []
            fieldsValue = []
            rt = self.execute_sql(strReslut)
            for i in rt:
                fieldsName.append(i[0])
                fieldsValue.append(i[1])
            return fieldsName, fieldsValue
        except Exception as error_message:
            loger.print_error("get getTableInfo error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def execute_sql(self, sql):
        loger = logger.Logger("Method:execute_sql")
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            return cur
        except Exception as error_message:
            loger.print_error("Execute SQL engine error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def query_one(self, sql):
        loger = logger.Logger("Method:query_one")
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rt = cursor.fetchall()
            return rt
        except Exception as error_message:
            loger.print_error("Execute SQL engine error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def close(self):
        self.conn.close()




