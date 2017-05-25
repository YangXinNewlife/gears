# -*- coding:utf-8 -*-
__author__ = 'yx'
from courier import Courier
from src.mapping.writer import csv_hive_hawq_writer
from src.logger import logger
from src.database import db_conns

class CsvCourier(Courier):
    def __init__(self):
        pass

    def sqoop_data(self):
        pass

    @staticmethod
    def csv_pxf_write_table(table, host, database, t_type, columns_param, datacolumns_last, jobid):
        loger = logger.Logger()
        str2 = ''
        count = 0
        print columns_param
        print type(columns_param)
        print len(columns_param)
        if t_type == 1 or t_type == '1':
            try:
                for i in columns_param:
                    cw = csv_hive_hawq_writer.CsvHiveHawqWriter()
                    fieldsValue = cw.convert_data_type(i.get('col_type'))
                    str2 += i.get('col_name') + " " + fieldsValue
                    if count != len(columns_param) - 1:
                        str2+= ', '
                    count += 1
                columns1 = str2
                #this is a list store
                table_sql = []
                port = 51200
                create_ext_table_sql = "CREATE EXTERNAL TABLE %s_ext (%s)\nLOCATION (\'pxf://%s:%s/%s.%s?PROFILE=Hive\') FORMAT \'custom\' (formatter=\'pxfwritable_import\');" % (table, columns1, host, port, database, table)
                table_sql.append(create_ext_table_sql)
                table_sql.append('CREATE TABLE "%s" (\n%s\n)\nWITHOUT OIDS;' % (table, columns1))
                table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext";' % (table, table))
                return table_sql
            except Exception as e:
                loger.print_error("pxf service failed")
                importjob = db_conns.importjob_conn.add(jobid, "exception")
        elif t_type == 2 or t_type == 3:
            try:
                table_sql = []
                if datacolumns_last == None:
                    table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext"' % (table, table))
                elif str(datacolumns_last) > 0:
                    table_sql.append('INSERT INTO "%s" SELECT * FROM "%s_ext" limit %s;' % (table, table, datacolumns_last))
                return table_sql
            except Exception as e:
                loger.print_error("pxf service failed")
                importjob = db_conns.importjob_conn.add(jobid, "exception")

