# -*- coding:utf-8 -*-
__author__ = 'yx'
import psycopg2
from src.config import config
from psycopg2.extras import RealDictCursor
import db_conns
import sys
from src.config import config
from src.logger import logger
from src.job_attributes import JobStatus
NUM_COMMIT = 20
reload(sys)

class PostgresClient(object):
    def __init__(self, conf=config, db_host=None, user=None, password=None, database=None, port=None, jobid = None):
        try:
            loger = logger.Logger("Method:init")
            if conf:
                self.conn = psycopg2.connect(host=conf.gears_host,
                                     port=conf.gears_port,
                                     user=conf.gears_user,
                                     password=conf.gears_passwd,
                                     database=conf.gears_db, )
            else:
                self.test = db_host,port,user,password,database
                self.conn = psycopg2.connect(host=db_host,
                                 port=port,
                                 user=user,
                                 password=password,
                                 database=database)
            self.conn.set_client_encoding('utf-8')
            self.jobid = jobid
        except Exception as error_message:
            loger.print_error("HDB connection error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message

    def fetch_data(self, db_schema, table, ids=None, **filters):
        table_columns = self.get_schema(db_schema, table)
        table_full_name = "\"%s\".\"%s\"" % (db_schema, table)
        if not ids:
            l = []
            for filter in filters.items():
                if filter[0] not in table_columns:
                    pass
                l.append("\"%s\"='%s'" % (filter[0], filter[1]))
            where_str = 'and'.join(l)
            sql = "SELECT * FROM " + table_full_name + " WHERE " + where_str + ";"
        else:
            ids_l = ["'%s'" % id for id in ids]
            instr = ",".join(ids_l)
            sql = "SELECT * FROM " + table_full_name + " WHERE id in (%s);" % instr
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

    def create_db(self, db):
        cur = self.conn.cursor()
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur.execute("create database %s" %db)
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def get_schema(self, table_schema, table_name):
        with self.conn.cursor() as cursor:
            cursor.execute("select column_name from information_schema.columns where table_schema = '%s' and table_name='%s' ORDER BY \"ordinal_position\"" % (table_schema, table_name))
            column_names = [row[0] for row in cursor]
        return column_names
    def get_schema_type(self, table_schema, table_name):
        with self.conn.cursor() as cursor:
            cursor.execute("select column_name,data_type from information_schema.columns where table_schema = '%s' and table_name='%s' ORDER BY \"ordinal_position\"" % (table_schema, table_name))
            column_names = [(row[0],row[1]) for row in cursor]
        return column_names    

    def getTableColumns(self, tables):
        for table in tables:
            sql = "select column_name, data_type from information_schema.columns where table_name = '%s' ORDER BY ordinal_position"%(table)
            fieldNames, fieldType = self.execute_sql(sql)
            return fieldNames, fieldType

    def insert_sql(self, sql):
        try:
            loger = logger.Logger("Method:insert_sql")
            loger.print_info(sql)
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql)
            insert_record = cur.fetchone()
            self.conn.commit()
            return insert_record
        except Exception as error_message:
            loger.print_error("Execute SQL engine error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message


    def execute_sql(self, sql):
        loger = logger.Logger("Method:execute_sql")
        try:
            loger.print_info(sql)
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
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

