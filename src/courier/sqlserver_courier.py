# -*- coding:utf-8 -*-
__author__ = 'yx'
from courier import Courier
import os
from src.logger import logger
from src.database import db_conns


class SQLServerCourier(Courier):
    def __init__(self):
        pass

    @staticmethod
    def sqoop_data(src_host, src_database, src_username, src_password, jobid, dst_database, dst_table, table, sqc):
        loger = logger.Logger("SQLServerCourier.sqoop_data")
        try:
            primary_key, columns = sqc.sqlserver_table_attributes(table)
            if len(primary_key) != 0:
                primary_key = primary_key[0][0]
                sqoop_str = "sqoop import --hive-import --connect 'jdbc:sqlserver://%s;username=%s;password=%s;database=%s' --table '%s' --hive-table '%s.%s' --null-string ' ' --null-non-string ' ' --hive-drop-import-delims --split-by '%s' -m 3;"%(src_host, src_username, src_password, src_database, table, dst_database, dst_table, primary_key)
            else:
                sqoop_str = "sqoop import --hive-import --connect 'jdbc:sqlserver://%s;username=%s;password=%s;database=%s' --table '%s' --hive-table '%s.%s' --null-string ' ' --null-non-string ' ' --hive-drop-import-delims -m 1;"%(src_host, src_username, src_password, src_database, table, dst_database, dst_table)
            loger.print_info(sqoop_str)
            os.system(sqoop_str)
        except Exception as e:
            loger.print_error("sqlserver sqoop error" + str(e))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')

    @staticmethod
    def sqoop_data_hbase(host, db, username, password, jobid, hbase_database, dst_table, src_table, statement, statement_value):
        loger = logger.Logger("SQLServerCourier.SqoopDataHBase")
        try:
            sqoop_str = "sqoop import --connect 'jdbc:sqlserver://%s;username=%s;password=%s;database=%s' --table %s --hbase-table %s --column-family %s --hbase-create-table --incremental append --check-column %s --last-value %s --null-string ' ' --null-non-string ' ' --hive-drop-import-delims"%(host, username, password, db, src_table, dst_table, dst_table,  statement, statement_value)
            loger.print_info(sqoop_str)
            os.system(sqoop_str)
        except Exception as e:
            loger.print_error("sqlserver sqoop into hbase failed" + str(e))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')


    @staticmethod
    def pxf_write_table(sqc, dst_table, table, dst_hostname, dst_database, operation, jobid):
        loger = logger.Logger("SQLServerCourier.pxf_write_table")
        try:
            primary_keys1, columns1 = sqc.sqlserver_table_attributes(table)
            table_sql = []
            port = 51200
            create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s.%s?PROFILE=Hive\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (dst_table,columns1,dst_hostname, port, dst_database, dst_table)
            loger.print_info(create_ext_table_sql)
            print "generate the pxf's sql language"
            if operation == 2 or operation == '2':
                table_sql.append('INSERT INTO %s SELECT * FROM %s_ext;' % (dst_table, dst_table))
                return table_sql
            else:
                table_sql.append(create_ext_table_sql)
                table_sql.append('CREATE TABLE %s (\n%s\n)\nWITHOUT OIDS;' % (dst_table, columns1))
                table_sql.append('INSERT INTO %s SELECT * FROM %s_ext;' % (dst_table, dst_table))
            loger.print_info(table_sql)
            return table_sql
        except Exception as e:
            loger.print_error("sqlserver pxf_service error" + str(e))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')

    @staticmethod
    def sqlserver_hbase_pxf_write_table(obj, dst_table, src_table, host, database, jobid, statement_dt, statement):
        loger = logger.Logger("SQLServerCourier.sqlserver_hbase_pxf")
        try:
            columns_family = '%s'%(dst_table)
            primary_keys1, columns1, columns_new = obj.sqlserver_external_table_hbase_attributes(src_table, columns_family, statement_dt, statement)
            primary_keys1, columns2 = obj.sqlserver_table_attributes(src_table)
            table_sql = []
            port = 51200
            create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s?PROFILE=HBase\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (dst_table, columns1, host, port, dst_table)
            table_sql.append(create_ext_table_sql)
            table_sql.append('CREATE TABLE %s (\n%s\n)\nWITHOUT OIDS' % (dst_table, columns2))
            table_sql.append('INSERT INTO %s SELECT %s FROM %s_ext' % (dst_table, columns_new, dst_table))
            loger.print_info(table_sql)
            return table_sql
        except Exception as e:
            loger.print_error("sqlserver hbase pxf_service error" + str(e))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
