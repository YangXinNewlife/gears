# -*- coding:utf-8 -*-
__author__ = 'yx'
from courier import Courier
import os
from src.logger import logger
from src.database import db_conns

class OracleCourier(Courier):
    def __init__(self):
        pass

    @staticmethod
    def sqoop_data(host, port, sid, db, username, password, jobid, dst_database, dst_table, table, oc, table_owner):
        loger = logger.Logger("Method:sqoop_data")
        try:
            primary_key, serial_key, maxval, str2 = oc.oracle_table_attributes(table, table_owner)
            print primary_key, serial_key, maxval, str2
            if len(primary_key) != 0:
                sqoop_str = "sqoop import --hive-import --connect 'jdbc:oracle:thin:@%s:%s:%s' --username '%s' --password '%s' --table '%s.%s' --hive-table '%s.%s' --null-string ' ' --null-non-string ' ' --hive-drop-import-delims --split-by '%s' -m 3;"%(host, port, sid, username, password, db, table.upper(), dst_database, dst_table, primary_key[0][0])
            else:
                sqoop_str = "sqoop import --hive-import --connect 'jdbc:oracle:thin:@%s:%s:%s' --username '%s' --password '%s' --table '%s.%s' --hive-table '%s.%s' --null-string ' ' --null-non-string ' ' --hive-drop-import-delims -m 1;"%(host, port, sid, username, password, db, table.upper(), dst_database, dst_table)
            loger.print_info(sqoop_str)
            os.system(sqoop_str)
        except Exception as error_message:
            loger.print_error("Oracle sqoop into hive error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message


    @staticmethod
    def sqoop_data_hbase(host, port, sid, db,username, password, jobid, hbase_database, dst_table, src_table, statement, statement_value):
        loger = logger.Logger("Method:sqoop_data_hbase")
        try:
            sqoop_str = "sqoop import --connect jdbc:oracle:thin:@%s:%s:%s --table %s.%s --hbase-table %s --column-family %s --hbase-create-table -username %s -password %s --incremental append --check-column %s --last-value %s --null-string ' ' --null-non-string ' ' --hive-drop-import-delims"%(host, port, sid, db, src_table, dst_table, dst_table, username, password, statement, statement_value)
            loger.print_info(sqoop_str)
            os.system(sqoop_str)
        except Exception as error_message:
            loger.print_error("Oracle sqoop into hbase failed" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def oracle_pxf_write_table(obj, dst_table, src_table, host, database, operation, jobid, table_owner):
        loger = logger.Logger("Method:oracle_pxf_write_table")
        try:
            primary_keys1,serial_key, maxval, columns1 = obj.oracle_table_attributes(src_table.upper(), table_owner)
            table_sql = []
            port = 51200
            create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s.%s?PROFILE=Hive\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (dst_table.lower(),columns1,host, port, database, dst_table.lower())
            loger.print_info(create_ext_table_sql)
            loger.print_info("generate the pxf's sql language")
            if operation == 2 or operation == '2':
                table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext";' % (dst_table.lower(), dst_table.lower()))
                return table_sql
            else:
                table_sql.append(create_ext_table_sql)
                table_sql.append('CREATE TABLE "%s" (\n%s\n)\nWITHOUT OIDS;' % (dst_table.lower(), columns1))
                table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext";' % (dst_table.lower(), dst_table.lower()))
            loger.print_info(table_sql)
            return table_sql
        except Exception as error_message:
            loger.print_error("oracle pxf_service error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def oracle_hbase_pxf_write_table(obj, dst_table, src_table, host, database, jobid, statement_dt, statement):
        loger = logger.Logger("Method:oracle_hbase_pxf_write_table")
        try:
            columns_family = '%s'%(dst_table)
            primary_keys1, columns1, columns_new = obj.oracle_external_table_hbase_attributes(src_table.upper(), columns_family, statement_dt, statement)
            primary_keys1,serial_key, maxval, columns2 = obj.oracle_table_attributes(src_table.upper())
            table_sql = []
            port = 51200
            create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s?PROFILE=HBase\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (dst_table,columns1,host, port, dst_table)
            table_sql.append(create_ext_table_sql)
            table_sql.append('CREATE TABLE "%s" (\n%s\n)\nWITHOUT OIDS;' % (dst_table.lower(), columns2))
            table_sql.append('INSERT INTO "%s" SELECT %s FROM "%s_ext";' % (dst_table.lower(), columns_new, dst_table.lower()))
            loger.print_info(table_sql)
            return table_sql
        except Exception as error_message:
            loger.print_error("Oracle hbase pxf_service error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message
