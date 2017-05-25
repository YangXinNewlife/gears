# -*- coding:utf-8 -*-
__author__ = 'yx'
import MySQLdb
from src.logger import logger
from src.database import db_conns
from src.job_attributes import JobStatus
from src.mapping.writer import mysql_hive_hawq_writer

class MysqlClient(object):
    user = None
    password = None
    host = None
    port = None
    db = None

    def __init__(self, json_params):
        loger = logger.Logger("Method:init")
        try:
            self.jobid = json_params["jobid"]
            self.host = json_params["src_conn"]["hostname"]
            self.port = int(json_params["src_conn"]["port"])
            self.user = json_params["src_conn"]["username"]
            self.password = json_params["src_conn"]["password"]
            self.database = json_params["src_conn"]["database"]
            self.conn = MySQLdb.connect(host = self.host, port = self.port, user = self.user, passwd = self.password, db = self.database)
        except Exception as error_message:
            loger.print_error("Mysql DB connection error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTableInfo(self, strReslut):
        loger = logger.Logger("Method:getTableInfo")
        try:
            loger.print_info(strReslut)
            loger.print_info("get table schema information")
            fieldsName = []
            fieldsValue = []
            rt = self.execute_sql(strReslut)
            for i in rt:
                fieldsName.append(i[0])
                fieldsValue.append(i[1])
            return fieldsName, fieldsValue
        except Exception as error_message:
            loger.print_error("getTableInfo method error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def mysql_table_attributes(self, table):
        loger = logger.Logger("Method:mysql_table_attributes")
        try:
            loger.print_info(table)
            primary_key_sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='%s' AND COLUMN_KEY='PRI'"%(table)
            primary_keys = self.query_one(primary_key_sql)
            columns = self.getTable_columns(table)
            return primary_keys, columns
        except Exception as error_message:
            loger.print_error("get table attributes error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def mysql_external_table_hbase_attributes(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:mysql_external_table_hbase_attributes")
        try:
            loger.print_info("get hbase external table attributes")
            primary_key_sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='%s' AND COLUMN_KEY='PRI'"%(table)
            loger.print_info(primary_key_sql)
            primary_keys = self.query_one(primary_key_sql)
            columns, columns_new = self.getTable_hbase_columns(table, column_family, statement_dt, statement)
            return primary_keys, columns, columns_new
        except Exception as error_message:
            loger.print_error("get external table attributes error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_columns(self, table):
        loger = logger.Logger("Method:getTable_columns")
        try:
            table_sql = "select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where table_name = '%s'"%(table)
            loger.print_info(table_sql)
            columns_name, data_type = self.getTableInfo(table_sql)
            columns = ""
            count = 0
            for i in range(len(columns_name)):
                columns += columns_name[i]
                columns += ' '
                data_type_obj = mysql_hive_hawq_writer.MysqlHiveHawqWriter()
                data_type[i] = data_type_obj.convert_data_type(data_type[i])
                columns += data_type[i]
                if count != (len(columns_name) - 1):
                    columns += ', '
                count += 1
            return columns
        except Exception as error_message:
            loger.print_error("get table column attributes error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_hbase_columns(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("getTable_hbase_columns")
        try:
            table_sql = "select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where table_name = '%s'"%(table)
            loger.print_info(table_sql)
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
            loger.print_error("get hbase table column attributes error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def execute_meod(self, sql):
        loger = logger.Logger("Method:execute_meod")
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
            loger.print_info(sql)
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
            loger.print_info(sql)
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
