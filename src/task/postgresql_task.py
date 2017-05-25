# -*- coding:utf-8 -*-
__author__ = 'yx'
from src.logger import logger
from src.database import db_conns
from src.courier.postgres_courier import PostgresCourier
from src.database.postgres_client import PostgresClient
from src.router.hive_router import HiveRouter
from src.client.postgres_client import PostgresqlClient
from src.job_attributes import JobOperate, JobStatus, JobLevel
from src.metastore.ddl_client import DdlClient
from src.task.task_basc import TaskBasc
from src.tracker.hive_tracker import HiveTracker
from src.tracker.hawq_tracker import HAWQTracker
from src.config import config
import datetime
import unicodedata

class postgresql_jparser(object):
    def __init__(self, json_params):
        try:
            self.json_params = json_params
            loger = logger.Logger("Method:init")
            loger.print_info("Start Paraser")
            self.jobid = json_params["jobid"]
            self.token = json_params["token"]
            self.task_name = json_params["name"]
            self.description = json_params["description"]
            self.uid = json_params['uid']
            self.src_hostname = json_params["src_conn"]["hostname"]
            self.src_port= json_params["src_conn"]["port"]
            self.src_username = json_params["src_conn"]["username"]
            self.src_password = json_params["src_conn"]["password"]
            self.src_database = json_params["src_conn"]["database"]
            self.src_tables = json_params["src_conn"]["tables"]
            if type(self.src_tables) == list:
                pass
            else:
                src_tables = unicodedata.normalize('NFKD', self.src_tables).encode('utf-8', 'ignore')
                src_tables = src_tables.replace('[', '').replace(']', '')
                src_tables = src_tables.split(',')
                tables_list = []
                for i in range(len(src_tables)):
                    src_tables[i] = src_tables[i].replace("'", '')
                    tables_list.append(src_tables[i])
                self.src_tables = tables_list
            self.datasource = ''
            self.dst_hostname = json_params["dst_conn"]["hostname"]
            self.dst_database = json_params["dst_conn"]["database"]
            self.dst_clustername = json_params["dst_conn"]["clustername"]
            self.prefix = json_params["prefix"]
            self.operation_flag = json_params["operation_flag"]
            self.increment_flag = json_params["increment_flag"]
            self.determine_field = json_params["determine_field"]
            self.determine_type = json_params["determine_type"]
            self.determine_value = json_params["determine_value"]
            loger.print_info("Finish Paraser")
        except Exception as error_message:
            loger.print_error("Parser Postgres Json Error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message


class PostgresqlTask(TaskBasc):
    def __init__(self, jobid, jparams):
        loger = logger.Logger("Method:init")
        self.params = postgresql_jparser(jparams)
        self.jobid = jobid

    def run(self):
        importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.RUNNING)
        loger = logger.Logger("Method:run")
        hawq_obj = PostgresClient(None, self.params.dst_hostname, config.hawq_user, config.hawq_passwd, self.params.dst_database, config.hawq_port)
        loger.print_info("HDB Connection Successfully")
        hive_obj = HiveRouter(self.params.jobid)
        loger.print_info("Hive DB Connection Successfully")
        pg_obj = PostgresqlClient(self.params.src_hostname, self.params.src_username, self.params.src_password, self.params.src_database, self.params.src_port, self.params.jobid)
        loger.print_info("Postgresql DB Connection Successfuly")
        for table in self.params.src_tables:
            self.params.datasource = str(table).lower()
            loger.print_info("Table Is %s"%(table))
            if self.params.operation_flag == 1 or self.params.operation_flag == '1':
                loger.print_info("Operate Is : %s"%(JobOperate.CRETAE))
                loger.print_info("Start Hive pre-treatment")
                pretreate_sqls = HiveTracker.hive_pretreate(self.params.dst_database, self.params.prefix + table, self.params.jobid)
                for i in range(len(pretreate_sqls)):
                    loger.print_info("Hive Pretreatment is :%s"%(pretreate_sqls[i]))
                    hive_obj.execute_meod(pretreate_sqls[i])
                loger.print_info("Finish Hive pre-treatment")
                loger.print_info("Start HDB pre-treatment")
                hawq_sqls = HAWQTracker.hawq_pretreate(self.params.prefix + table, self.params.jobid)
                for i in range(len(hawq_sqls)):
                    hawq_obj.execute_sql(hawq_sqls[i])
                loger.print_info("Finish HDB pre-treatment")
                loger.print_info("Start create hive table")
                sqls = hive_obj.postgresql_create_hiveTable(self.params.dst_database, self.params.prefix + table, table, pg_obj, self.jobid)
                for i in range(len(sqls)):
                    hive_obj.execute_meod(sqls[i])
                loger.print_info("Finish create hive table")
                loger.print_info("Start sqoop import data into hive db")
                PostgresCourier.sqoop_data(self.params.src_hostname, self.params.src_port, self.params.src_database, self.params.src_username, self.params.src_password, self.params.jobid, self.params.dst_database, self.params.prefix + table,  table, pg_obj)
                loger.print_info("Finish sqoop import data into hive db")
                loger.print_info("Start PXF-Service import into HDB")
                table_sql = PostgresCourier.postgresql_pxf_write_table(pg_obj, self.params.prefix + table, table, self.params.dst_hostname,self.params.dst_database, self.params.operation_flag, self.jobid)
                for j in range(len(table_sql)):
                    loger.print_info(str(table_sql[j]))
                    hawq_obj.execute_sql(table_sql[j])
                loger.print_info("Finish PXF-Service import into HDB")
                loger.print_info("Start write meta data into pg db")
                client = DdlClient(config.alphatrion_host, 7080)
                client.create_table(self.params.dst_database, self.params.prefix + table, self.params.token)
                loger.print_info("Finish write meta data into pg db")
            elif self.params.operation_flag == 2 or self.params.operation_flag == '2':
                loger.print_info("Operate Is : %s"%(JobOperate.APPEND_ONLY))
                loger.print_info("Start empty hive db")
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Finish empty hive db")
                loger.print_info("Start sqoop import data into hive db")
                PostgresCourier.sqoop_data(self.params.src_hostname, self.params.src_port, self.params.src_database, self.params.src_username, self.params.src_password, self.params.jobid, self.params.dst_database, self.params.prefix + table,  table, pg_obj)
                loger.print_info("Finish sqoop import data into hive db")
                loger.print_info("Start PXF-Service import into HDB")
                table_sql = PostgresCourier.postgresql_pxf_write_table(pg_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.params.operation_flag, self.jobid)
                for j in range(len(table_sql)):
                    loger.print_info(str(table_sql[j]))
                    hawq_obj.execute_sql(table_sql[j])
                loger.print_info("Finish PXF-Service import into HDB")
                loger.print_info("Start empty hive db")
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Finish empty hive db")
            elif self.params.operation_flag == 3 or self.params.operation_flag == '3':
                loger.print_info("Operate Is : %s"%(JobOperate.OVERRID))
                loger.print_info("Start empty hive db")
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Finish empty hive db")
                loger.print_info("Start sqoop import data into hive db")
                PostgresCourier.sqoop_data(self.params.src_hostname, self.params.src_port, self.params.src_database, self.params.src_username, self.params.src_password, self.params.jobid, self.params.dst_database, self.params.prefix + table, table, pg_obj)
                loger.print_info("Finish sqoop import data into hive db")
                loger.print_info("Start PXF-Service import into HDB")
                table_sql = PostgresCourier.postgresql_pxf_write_table(pg_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.params.operation_flag, self.params.jobid)
                empty_external_sql = 'drop external table %s_ext'%(self.params.prefix + table)
                loger.print_info("drop table sql is %s:"%(empty_external_sql))
                hawq_obj.execute_sql(empty_external_sql)
                empty_sql = 'drop table %s'%(self.params.prefix + table)
                loger.print_info("drop table sql is %s"%(empty_sql))
                hawq_obj.execute_sql(empty_sql)
                for j in range(len(table_sql)):
                    loger.print_info(str(table_sql[j]))
                    hawq_obj.execute_sql(table_sql[j])
                loger.print_info("Finish PXF-Service import into HDB")
                loger.print_info("Start empty hive db")
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Finish empty hive db")
            elif self.params.operation_flag == 4 or self.params.operation_flag == '4':
                if self.params.increment_flag == 1 or self.params.increment_flag == '1':
                    loger.print_info("operate is :%s"%(JobOperate.UPDATE))
                    loger.print_info("Start sqoop import data into hbase db")
                    PostgresCourier.sqoop_data_hbase(self.params.src_hostname, self.params.src_port, self.params.src_database, self.params.src_username, self.params.src_password, self.jobid, self.params.dst_database, self.params.prefix + table, table, self.params.determine_field, self.params.determine_value)
                    loger.print_info("Finish sqoop import data into hbase db")
                    loger.print_info("Start PXF-Service import into HDB")
                    table_sql = PostgresCourier.postgresql_hbase_pxf_write_table(pg_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.jobid, self.params.determine_type, self.params.determine_field)
                    empty_external_sql = 'drop external table %s_ext'%(self.params.prefix + table)
                    loger.print_info("drop table sql is %s:"%(empty_external_sql))
                    hawq_obj.execute_sql(empty_external_sql)
                    empty_sql = 'drop table %s'%(self.params.prefix + table)
                    loger.print_info("drop table sql is %s"%(empty_sql))
                    hawq_obj.execute_sql(empty_sql)
                    for j in range(len(table_sql)):
                        hawq_obj.execute_sql(table_sql[j])
                    loger.print_info("Start PXF-Service import into HDB")
        hawq_obj.close()
        hive_obj.close()
        pg_obj.close()


