# -*- coding:utf-8 -*-
__author__ = 'yx'
from courier import Courier
from src.logger import logger
import os
from src.database import db_conns
class PostgresCourier(Courier):
    def __init__(self):
        loger = logger.Logger("Method:init")

    @staticmethod
    def sqoop_data(host, port, db, username, password, jobid,  hive_database, dst_table, src_table, ml):
        loger = logger.Logger("Method:sqoop_data")
        try:
            primary_key, columns = ml.postgresql_table_attributes(src_table)
            if len(primary_key) != 0:
                primary_key = primary_key[0][0]
                sqoop_str = "sqoop import --hive-import --connect 'jdbc:postgresql://%s:%s/%s' --username '%s' --password '%s' --table '%s' --hive-table %s.%s --null-string ' ' --null-non-string ' ' --hive-drop-import-delims --split-by '%s' -m 3;"%(host, port, db, username, password, src_table, hive_database, dst_table, primary_key)
            else:
                sqoop_str = "sqoop import --hive-import --connect 'jdbc:postgresql://%s:%s/%s' --username '%s' --password '%s' --table '%s' --hive-table %s.%s --null-string ' ' --null-non-string ' ' --hive-drop-import-delims -m 1;"%(host, port, db, username, password, src_table, hive_database, dst_table)
            loger.print_info(sqoop_str)
            os.system(sqoop_str)
        except Exception as error_message:
            loger.print_error("postgres sqoop error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def sqoop_data_hbase(host, port, db, username, password, jobid, hbase_database, dst_table, src_table, statement, statement_value):
        loger = logger.Logger("Method:soop_data_hbase")
        try:
            sqoop_str = "sqoop import --connect jdbc:postgresql://%s:%s/%s --table %s --hbase-table %s --column-family %s --hbase-create-table -username %s -password %s --incremental append --check-column %s --last-value %s --null-string ' ' --null-non-string ' ' --hive-drop-import-delims"%(host, port, db, src_table, dst_table, dst_table, username, password, statement, statement_value)
            loger.print_info(sqoop_str)
            os.system(sqoop_str)
        except Exception as error_message:
            loger.print_error("postgresql sqoop into hbase failed" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def postgresql_pxf_write_table(obj, dst_table, src_table, host, database, operation, jobid):
        loger = logger.Logger("Method:postgresql_pxf_write_table")
        try:
            primary_keys1, columns1 = obj.postgresql_table_attributes(src_table)
            table_sql = []
            port = 51200
            create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s.%s?PROFILE=Hive\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (dst_table,columns1,host, port, database, dst_table)
            loger.print_info(create_ext_table_sql)
            if operation == 2 or operation == '2':
                table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext";' % (dst_table, dst_table))
                return table_sql
            else:
                table_sql.append(create_ext_table_sql)
                table_sql.append('CREATE TABLE "%s" (\n%s\n)\nWITHOUT OIDS;' % (dst_table, columns1))
                table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext";' % (dst_table, dst_table))
            loger.print_info(table_sql)
            return table_sql
        except Exception as error_message:
            loger.print_error("postgres pxf_service error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def postgresql_hbase_pxf_write_table(obj, dst_table, src_table, host, database, jobid, statement_dt, statement):
        loger = logger.Logger("Method:postgresql_hbase_pxf_write_table")
        try:
            columns_family = '%s'%(dst_table)
            primary_keys1, columns1, columns_new = obj.postgresql_external_table_hbase_attributes(src_table, columns_family, statement_dt, statement)
            primary_keys1, columns2 = obj.postgresql_table_attributes(src_table)
            table_sql = []
            port = 51200
            create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s?PROFILE=HBase\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (dst_table,columns1,host, port, dst_table)
            table_sql.append(create_ext_table_sql)
            table_sql.append('CREATE TABLE "%s" (\n%s\n)\nWITHOUT OIDS;' % (dst_table, columns2))
            table_sql.append('INSERT INTO "%s" SELECT %s FROM "%s_ext";' % (dst_table, columns_new, dst_table))
            loger.print_info(table_sql)
            return table_sql
        except Exception as error_message:
            loger.print_error("postgresql hbase pxf_service error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message
