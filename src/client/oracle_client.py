# -*- coding:utf-8 -*-
__author__ = 'jiuzhang'
import cx_Oracle
from src.mapping.writer import oracle_hive_hawq_writer
from src.database import db_conns
from src.logger import logger
from src.job_attributes import JobStatus


class OracleClient(object):
    user = None
    password = None
    host = None
    port = None
    db = None

    def __init__(self, user, password, host, port, db, jobid):
        loger = logger.Logger("Method:init")
        try:
            self.conn = cx_Oracle.connect('%s/%s@%s:%s/%s' %(user, password, host, port, db))
            self.user = user
            self.password = password
            self.host = host
            self.port = port
            self.db = db
            self.jobid = jobid
        except Exception as error_message:
            loger.print_error("oracle connection error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def oracle_table_attributes(self, table, table_owner):
        loger = logger.Logger("Method:oracle_table_attributes")
        try:
            primary_key_sql = "select cu.COLUMN_NAME from user_cons_columns cu, user_constraints au where cu.constraint_name = au.constraint_name and au.constraint_type = 'P' and au.table_name = '%s'"%(table)
            serial_key_sql = "select sequence_name from user_sequences"
            maxval_sql = "select max_value from user_sequences"
            serial_key = self.execute_meod(serial_key_sql)
            maxval = self.execute_meod(maxval_sql)
            primary_keys = self.execute_meod(primary_key_sql)
            columns = self.getTable_columns(table, table_owner)
            length = len(columns)
            str2 = ''
            count = 0
            for ii in columns:
                ohw = oracle_hive_hawq_writer.OracleHiveHawqWriter()
                fieldsValue = ohw.convert_data_type(ii[1])
                if fieldsValue != 'int' and fieldsValue != 'integer':
                    str2 += ii[0] + " " + fieldsValue + "(" + str(ii[2]) + ")"
                else:
                    str2 += ii[0] + " " + fieldsValue
                if count != length - 1:
                    str2+= ', '
                count += 1
            if serial_key == None:
                serial_key = None
            else:
                serial_key = serial_key
            return primary_keys, serial_key, maxval, str2
        except Exception as error_message:
            loger.print_error("oracle_table_attributes error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def oracle_external_table_hbase_attributes(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:oracle_external_table_hbase_attributes")
        try:
            primary_key_sql = "select cu.COLUMN_NAME from user_cons_columns cu, user_constraints au where cu.constraint_name = au.constraint_name and au.constraint_type = 'P' and au.table_name = '%s'"%(table)
            primary_keys = self.execute_meod(primary_key_sql)
            columns, columns_new = self.getTable_hbase_columns(table, column_family, statement_dt, statement)
            return primary_keys, columns, columns_new
        except Exception as error_message:
            loger.print_error("oracle_external_table_hbase_attributes error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_hbase_columns(self, table, column_family, statement_dt, statement):
        loger = logger.Logger("Method:getTable_hbase_columns")
        try:
            table_sql = "SELECT COLUMN_NAME, DATA_TYPE FROM user_tab_columns WHERE table_name = '%s'"%(table)
            columns_name, data_type = self.getTableInfo(table_sql)
            columns = "recordkey %s, "%(statement_dt)
            count = 0
            for i in range(len(columns_name)):
                ohw = oracle_hive_hawq_writer.OracleHiveHawqWriter()
                fieldsValue = ohw.convert_data_type(data_type[i])
                columns = str(columns) + '"'+ str(column_family) + ':'+ str(columns_name[i]) + '"'
                columns += ' '
                columns += fieldsValue
                if count != (len(columns_name) - 1):
                    columns += ', '
                count += 1
            columns_new = ''
            count1 = 0
            for j in range(len(columns_name)):
                if columns_name[j].lower() == statement:
                    columns_new += 'recordkey'
                else:
                    columns_new += '"'+ str(column_family) + ':'+ str(columns_name[j]) + '"'
                if count1 != (len(columns_name) - 1):
                    columns_new += ', '
                count1 += 1
            return columns,columns_new
        except Exception as error_message:
            loger.print_error("getTable_hbase_columns error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def createTable(self, sql):
        loger = logger.Logger("Method:createTable")
        try:
            cursor = self.conn.cursor()
            rt = cursor.execute(sql)
            cursor.commit()
        except Exception as error_message:
            loger.print_error("createTable error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def execute_meod(self, sql):
        loger = logger.Logger("Method:execute_meod")
        try:
            cursor = self.conn.cursor()
            rt = cursor.execute(sql)
            return rt.fetchall()
        except Exception as error_message:
            loger.print_error("Execute SQL error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def execute_meod1(self, sql):
        loger = logger.Logger("Method:execute_meod1")
        try:
            cursor = self.conn.cursor()
            rt = cursor.execute(sql)
            return rt.fetchone()
        except Exception as error_message:
            loger.print_error("Execute SQL error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTable_columns(self, table, table_owner):
        loger = logger.Logger("Method:getTable_columns")
        try:
            table_sql = "select column_name,data_type, data_length From dba_tab_columns where table_name='%s' and owner = '%s' ORDER BY COLUMN_ID"%(table, table_owner)
            columns = self.query(table_sql)
            return columns
        except Exception as error_message:
            loger.print_error("getTable error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def getTableInfo(self, strReslut):
        loger = logger.Logger("Method:getTableInfo")
        try:
            fieldsName = []
            fieldsValue = []
            fieldsLength = []
            cursor = self.conn.cursor()
            rt = cursor.execute(strReslut)
            for i in rt:
                fieldsName.append(i[0])
                fieldsValue.append(i[1])
                fieldsLength.append(i[2])
            return fieldsName, fieldsValue, fieldsLength
        except Exception as error_message:
            loger.print_error("getTableInfo error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def query(self, sql):
        loger = logger.Logger("Method:query")
        try:
            cursor = self.conn.cursor()
            rt = cursor.execute(sql)
            return rt.fetchall()
        except Exception as error_message:
            loger.print_error("Execute SQL engine error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def close(self):
        self.conn.close()
