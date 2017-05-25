# -*- coding:utf-8 -*-
__author__ = 'yx'
from router import Router
from src.mapping.reader import mysql_hive_reader
from src.mapping.reader import csv_hive_reader
from src.mapping.reader import postgres_hive_reader
from src.mapping.reader import sqlserver_hive_reader
from src.mapping.reader import oracle_hive_reader
from src.job_attributes import JobStatus
from src.logger import logger
from src.database import db_conns
from src.config import config
import pyhs2

class HiveRouter(Router):
    host = None
    user = None
    password = None
    database = None
    port = None
    authMechanism = None

    def __init__(self, jobid):
        loger = logger.Logger("Method:init")
        try:
            self.jobid = jobid
            self.host = config.hive_host
            self.port = int(config.hive_port)
            self.user = config.hive_user
            self.password = config.hive_passwd
            self.database = config.hive_database
            self.authMechanism = config.hive_authMechanism
            self.conn = pyhs2.connect(host = self.host, port = self.port, authMechanism = self.authMechanism, user = self.user, password = self.password, database = self.database)
        except Exception as error_message:
            loger.print_error("Hive DB connection error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def mysql_create_hiveTable(self, database, dst_table, src_table, obj, jobid):
        loger = logger.Logger("Method:mysql_create_hiveTable")
        try:
            sqls_list = []
            sql1 = "create database if not exists `%s`"%(database)
            sql2 = "use `%s`"%(database)
            struTable = "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.Columns where table_name= '%s'" %(src_table)
            fieldsName, fieldsValue = obj.getTableInfo(struTable)
            createtable1 = "create table if not exists `%s`(" %(dst_table)
            createtable2 = ''
            for i in xrange(0, len(fieldsName)):
                data_type_obj = mysql_hive_reader.MysqlHiveReader()
                fieldsValue[i] = data_type_obj.convert_data_type(fieldsValue[i])
                createtable2 += "`" + bytes(fieldsName[i]) + "`" + " " + bytes(fieldsValue[i])
                if i != len(fieldsName) - 1:
                    createtable2 += ', '
                else:
                    createtable2 += ')'
            sql3 = createtable1 + createtable2
            sqls_list.append(sql1)
            sqls_list.append(sql2)
            sqls_list.append(sql3)
            return sqls_list
        except Exception as error_message:
            loger.print_error("create hive table by mysql schema error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def postgresql_create_hiveTable(self, database, dst_table, src_table, pg, jobid):
        loger = logger.Logger("Method:postgresql_create_hiveTable")
        try:
            sqls_list = []
            sql1 = "create database if not exists `%s`"%(database)
            sql2 = "use `%s`"%(database)
            struTable = "select column_name, data_type from information_schema.columns where table_name = '%s' ORDER BY ordinal_position"%(src_table)
            fieldsName, fieldsValue = pg.getTableInfo(struTable)
            createtable1 = 'create table if not exists `%s`(' %(dst_table)
            createtable2 = ''
            for i in xrange(0, len(fieldsName)):
                data_type_obj = postgres_hive_reader.PostgresqlHiveReader()
                fieldsValue[i] = data_type_obj.convert_data_type(fieldsValue[i])
                createtable2 += bytes(fieldsName[i]) + " " + bytes(fieldsValue[i])
                if i != len(fieldsName) - 1:
                    createtable2 += ', '
                else:
                    createtable2 += ')'
            sql3 = createtable1 + createtable2
            sqls_list.append(sql1)
            sqls_list.append(sql2)
            sqls_list.append(sql3)
            return sqls_list
        except Exception as error_message:
            loger.print_error("create hive table by postgresql schema error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def csv_create_hiveTable(self, table, database, columns, separatorsign, jobid):
        loger = logger.Logger("Method:csv_create_hiveTable")
        try:
            sqls_list = []
            sql1 = "create database if not exists %s"%(database)
            sql2 = "use %s"%(database)
            create_table_sql1 = "create table if not exists %s ("%(table)
            create_table_sql2 = ''
            count = 0
            for i in columns:
                count += 1
                create_table_sql2 += i.get("col_name")
                create_table_sql2 += ' '
                data_type_obj = csv_hive_reader.CsvHiveReader()
                create_table_sql2 += data_type_obj.convert_data_type(i.get("col_type"))
                create_table_sql2 += ' '
                if i.get("col_def_value") != '':
                    create_table_sql2 += 'default '
                    create_table_sql2 += i.get("col_def_value")
                if count != len(columns):
                    create_table_sql2 += ', '
            create_table_sql2 += ')'
            if separatorsign == 't':
                create_table_sql2 += " row format delimited fields terminated by '\%s'"%(separatorsign)
            elif separatorsign == ',':
                create_table_sql2 += " row format delimited fields terminated by '%s'"%(separatorsign)
            elif separatorsign == '^':
                create_table_sql2 += " row format delimited fields terminated by '%s'"%(separatorsign)
            sql3 = create_table_sql1 + create_table_sql2
            sqls_list.append(sql1)
            sqls_list.append(sql2)
            sqls_list.append(sql3)
            return sqls_list
        except Exception as error_message:
            loger.print_error("create hive table by file columns error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def sqlserver_create_hiveTable(self, dst_database, dst_table, table, sqc, jobid):
        loger = logger.Logger("Method:sqlserver_create_hiveTable")
        try:
            sqls_list = []
            sql1 = "create database if not exists `%s`"%(dst_database)
            sql2 = "use `%s`"%(dst_database)
            struTable = "select column_name,data_type from information_schema.columns where table_name = '%s'" %(table)
            fieldsName, fieldsValue  = sqc.getTableInfo(struTable)
            createtable1 = 'create table `%s`(' %(dst_table)
            createtable2 = ''
            for i in xrange(0, len(fieldsName)):
                data_type_obj = sqlserver_hive_reader.SQLServerHiveReader()
                fieldsValue[i] = data_type_obj.convert_data_type(fieldsValue[i])
                createtable2 += bytes(fieldsName[i]) + " " + bytes(fieldsValue[i])
                if i != len(fieldsName) - 1:
                    createtable2 += ', '
                else:
                    createtable2 += ')'
            sql3 = createtable1 + createtable2
            sqls_list.append(sql1)
            sqls_list.append(sql2)
            sqls_list.append(sql3)
            return sqls_list
        except Exception as error_message:
            loger.print_error("create hive table by sqlserver schema error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def oracle_create_hiveTable(self, dst_database, dst_table, table, oc, jobid, table_owner):
        loger = logger.Logger("Method:oracle_create_hiveTable")
        try:
            sqls_list = []
            sql1 = "create database if not exists `%s`"%(dst_database)
            sql2 = "use %s"%(dst_database)
            struTable = "select column_name,data_type, data_length From dba_tab_columns where table_name='%s' and owner = '%s' ORDER BY COLUMN_ID"%(table.upper(), table_owner)
            print struTable
            fieldsName, fieldsValue, field_Length = oc.getTableInfo(struTable)
            createtable1 = 'create table `%s`(' %(dst_table.lower())
            createtable2 = ''
            for i in xrange(0, len(fieldsName)):
                ohr = oracle_hive_reader.OracleHiveReader()
                fieldsValue[i] = ohr.convert_data_type(fieldsValue[i])
                if fieldsValue[i] != 'int' and fieldsValue[i] != 'integer':
                    createtable2 += "`" + bytes(fieldsName[i]) + "`" + " " + bytes(fieldsValue[i])
                else:
                    createtable2 += "`" + bytes(fieldsName[i]) + "`" + " " + bytes(fieldsValue[i])
                if i != len(fieldsName) - 1:
                    createtable2 += ', '
                else:
                    createtable2 += ')'
            sql3 = createtable1 + createtable2
            sqls_list.append(sql1)
            sqls_list.append(sql2)
            sqls_list.append(sql3)
            return sqls_list
        except Exception as error_message:
            loger.print_error("create hive table by oracle schema error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def upload_data(self, tablename, database ,filename,Ttype, jobid):
        loger = logger.Logger("Method:upload_data")
        try:
            if Ttype == 2:
                loger.print_info("load file append operation")
                sqls_list = []
                sql1 = "use %s"%(database)
                sql2 = "LOAD DATA INPATH '%s' INTO TABLE %s"%(filename, tablename)
                sqls_list.append(sql1)
                sqls_list.append(sql2)
                return sqls_list
            elif Ttype == 3:
                loger.print_info("load file overwrite operation")
                sqls_list = []
                sql3 = "use %s"%(database)
                sql4 = "LOAD DATA INPATH '%s' OVERWRITE INTO TABLE %s"%(filename, tablename)
                sqls_list.append(sql3)
                sqls_list.append(sql4)
                return sqls_list
        except Exception as error_message:
            loger.print_error("load data file into hive table error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def execute_meod(self, sql):
        loger = logger.Logger("Method:execute_meod")
        try:
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
