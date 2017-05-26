# -*- coding:utf-8 -*-
__author__ = 'jiuzhang'
import pymssql
from src.logger import logger
from src.database import db_conns
from src.job_attributes import JobStatus


class SQLServerClient(object):
    user = None
    password = None
    host = None
    port = None
    database = None

    def __init__(self, host, user, password, database, jobid):
        loger = logger.Logger("Method:init")
        try:
            self.host = host
            self.user = user
            self.password = password
            self.database = database
            self.conn = pymssql.connect(host = self.host, user = self.user, password = self.password, database = self.database, charset = "utf8")
            cur = self.conn.cursor()
            self.jobid = jobid
        except Exception as error_message:
            loger.print_error("SQLServer connection error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def sqlserver_table_attributes(self,table):
        loger = logger.Logger("Method:sqlserver_table_attributes")
        try:
            primary_key_sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE  WHERE TABLE_NAME='%s'"%(table)
            primary_keys = self.query_one(primary_key_sql)
            columns = self.getTable_columns(table)
            return primary_keys, columns
        except Exception as error_message:
            loger.print_error("sqlserver_table_attributes error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def sqlserver_external_table_hbase_attributes(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:sqlserver_external_table_hbase_attributes")
        try:
            primary_key_sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE  WHERE TABLE_NAME='%s'"%(table)
            primary_keys = self.query_one(primary_key_sql)
            columns, columns_new = self.getTable_hbase_columns(table, column_family, statement_dt, statement)
            return primary_keys, columns, columns_new
        except Exception as error_message:
            loger.print_error("sqlserver_external_table_hbase_attributes error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_columns(self, table):
        loger = logger.Logger("Method:getTable_columns")
        try:
            table_sql = "select column_name,data_type from information_schema.columns where table_name = '%s'"%(table.upper())
            columns_name, data_type = self.getTableInfo(table_sql)
            columns = ''
            count = 0
            for i in range(len(columns_name)):
                columns += columns_name[i]
                columns += ' '
                columns += data_type[i]
                if count != (len(columns_name) - 1):
                    columns += ', '
                count += 1
            return columns
        except Exception as error_message:
            loger.print_error("sqlserver_external_table_hbase_attributes error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_hbase_columns(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:getTable_hbase_columns")
        try:
            table_sql = "select column_name,data_type from information_schema.columns where table_name = '%s'"%(table)
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
            loger.print_error("getTable_hbase_columns" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTableInfo(self, strReslut):
        loger = logger.Logger("Method:getTableInfo")
        try:
            fieldsName = []
            fieldsValue = []
            rt = self.query_one(strReslut)
            for i in rt:
                fieldsName.append(i[0])
                fieldsValue.append(i[1])
            return fieldsName, fieldsValue
        except Exception as error_message:
            loger.print_error("getTableInfo" + str(error_message))
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

    def execute_meod(self, sql):
        loger = logger.Logger("Method:query_one")
        try:
            loger.print_info(sql)
            cursor = self.conn.cursor()
            rt = cursor.execute(sql)
            return rt
        except Exception as error_message:
            loger.print_error("Execute SQL engine error, reason : " + str(error_message))
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

    def close(self):
        self.conn.close()




