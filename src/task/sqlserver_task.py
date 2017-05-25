# -*- coding:utf-8 -*-
__author__ = 'yx'
from src.logger import logger
from src.database import db_conns
from src.database.postgres_client import PostgresClient
from src.router.hive_router import  HiveRouter
from src.client.sqlserver_client import SQLServerClient
from src.courier.sqlserver_courier import SQLServerCourier
from src.tracker.hive_tracker import HiveTracker
from src.tracker.hawq_tracker import HAWQTracker
from src.job_attributes import JobOperate, JobStatus
from src.task.task_basc import TaskBasc
from src.config import config
from src.metastore.ddl_client import DdlClient
import unicodedata


class sqlserver_jparser(object):
    def __init__(self, json_params):
        try:
            self.json_params = json_params
            loger = logger.Logger("Method:init")
            loger.print_info("Start Paraser")
            self.jobid = json_params["jobid"]
            self.token = json_params["token"]
            self.uid = json_params["uid"]
            self.task_name = json_params["name"]
            self.description = json_params["description"]
            self.src_hostname = json_params["src_conn"]["hostname"]
            self.src_port = json_params["src_conn"]["port"]
            self.src_port = int(self.src_port)
            self.src_username = json_params["src_conn"]["username"]
            self.src_password = json_params["src_conn"]["password"]
            self.src_database = json_params["src_conn"]["database"]
            self.src_tables = json_params["src_conn"]["tables"]
            if type(self.src_tables) == list:
                    loger.print_info("src_table type is list")
            else:
                src_tables = unicodedata.normalize('NFKD', self.src_tables).encode('utf-8', 'ignore')
                src_tables = src_tables.replace('[', '').replace(']', '')
                src_tables = src_tables.split(',')
                tables_list = []
                for i in range(len(src_tables)):
                    src_tables[i] = src_tables[i].replace("'", '')
                    tables_list.append(src_tables[i])
                loger.print_info("The import data client is : " + str(tables_list))
                self.src_tables = tables_list
            self.datasource = ''
            self.dst_hostname = json_params["dst_conn"]["hostname"]
            self.dst_database = json_params["dst_conn"]["database"]
            self.dst_clustername = json_params["dst_conn"]["clustername"]
            self.prefix = json_params["prefix"]
            self.operation_flag = json_params["operation_flag"]
            self.increment_flag = json_params["increment_flag"]
            self.determine_flag = json_params["determine_field"]
            self.determine_type = json_params["determine_type"]
            self.determine_value = json_params["determine_value"]
        except Exception as error_message:
            loger.print_error("Parser the paramas error!" + str(e))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message


class SQLServerTask(TaskBasc):
    def __init__(self, jobid, jparams):
        loger = logger.Logger("Method:init")
        self.params = sqlserver_jparser(jparams)
        self.jobid = jobid

    def run(self):
        importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.RUNNING)
        loger = logger.Logger("Method:run")
        hawq_obj = PostgresClient(None, self.params.dst_hostname, config.hawq_user, config.hawq_passwd, self.params.dst_database, config.hawq_port)
        loger.print_info("HDB Connection Successfully")
        hive_obj = HiveRouter(self.params.jobid)
        loger.print_info("Hive DB Connection Successfully")
        server_obj = SQLServerClient(self.params.src_hostname, self.params.src_username, self.params.src_password, self.params.src_database)
        loger.print_info("Sqlserver DB Connection Successfully")
        #loop traversal tables import data into hawq
        for table in self.params.src_tables:
            table = str(table).lower()
            self.params.datasource = table
            loger.print_info("Table is :%s"%(table))
            #create table
            if self.params.operation_flag == 1 or self.params.operation_flag == '1':
                loger.print_info("operate is :%s"%(JobOperate.CRETAE))
                #Hive Pretreatment
                pretreate_sqls = HiveTracker.hive_pretreate(self.params.dst_database, self.params.prefix + table)
                for i in range(len(pretreate_sqls)):
                    loger.print_info("Hive Pretreatment is :%s"%(pretreate_sqls[i]))
                    hive_obj.execute_meod(pretreate_sqls[i])
                #HAWQ Pretreatment
                hawq_sqls = HAWQTracker.hawq_pretreate(self.params.prefix + table)
                for i in range(len(hawq_sqls)):
                    hawq_obj.execute_sql(hawq_sqls[i])
                loger.print_info("Pretreatement Finish")
                #Create Hive Table Use SQLServer Schema
                sqls = hive_obj.sqlserver_create_hiveTable(self.params.dst_database, self.params.prefix + table, table, server_obj, self.jobid)
                for i in range(len(sqls)):
                    loger.print_info(str(sqls[i]))
                    hive_obj.execute_meod(sqls[i])
                loger.print_info("Create Hive Table Successfully")
                #sqoop import data
                SQLServerCourier.sqoop_data(self.params.src_hostname, self.params.src_database, self.params.src_username, self.params.src_password, self.jobid, self.params.dst_database, self.params.prefix + table, table, server_obj)
                loger.print_info("Sqoop Operation Successfully")
                #pxf-service import data
                table_sql = SQLServerCourier.pxf_write_table(server_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.params.operation_flag, self.jobid)
                loger.print_info("Gain the pxf service sql successfully")
                loger.print_info("Import Hive Into Hawq Part")
                #gain running time
                for j in range(len(table_sql)):
                    hawq_obj.execute_sql(table_sql[j])
                #write into metastore
                client = DdlClient(config.alphatrion_host, 7080)
                client.create_table(self.params.dst_database, self.params.prefix + table)
                loger.print_info("write database, table, schema into metastore successful")
                #empty the hive table
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Execute Empty Hive Database")
            #append only table
            elif self.params.operation_flag == 2 or self.params.operation_flag == '2':
                loger.print_info("operate is :%s!"%(JobOperate.APPEND_ONLY))
                #empty hive table data
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                #sqoop import data
                SQLServerCourier.sqoop_data(self.params.src_hostname, self.params.src_database, self.params.src_username, self.params.src_password, self.jobid, self.params.dst_database, self.params.prefix + table, table, server_obj)
                loger.print_info("sqoop operation successfully!")
                #use pxf-service import data into hawq from hive
                table_sql = SQLServerCourier.pxf_write_table(server_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.params.operation_flag, self.params.jobid)
                loger.print_info("gain the pxf service sql successfully!")
                loger.print_info("Import Hive To Hawq Part")
                #gain running time
                for j in range(len(table_sql)):
                    hawq_obj.execute_sql(table_sql[j])
                #empty the hive table
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Execute Empty Hive Database")
            #overwrite table
            elif self.params.operation_flag == 3 or self.params.operation_flag == '3':
                loger.print_info("operate is %s"%(JobOperate.OVERRID))
                #empty hive table
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                #sqoop import data
                SQLServerCourier.sqoop_data(self.params.src_hostname, self.params.src_database, self.params.src_username, self.params.src_password, self.jobid, self.params.dst_database, self.params.prefix + table, table, server_obj)
                loger.print_info("sqoop operation successfully")
                #use pxf-service import data into hawq from hive
                table_sql = SQLServerCourier.pxf_write_table(server_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.params.operation_flag, self.jobid)
                loger.print_info("gain the pxf service sql successfully")
                #drop external table and inner table
                empty_external_sql = 'drop external table %s_ext'%(self.params.prefix + table)
                loger.print_info("drop table sql is %s:"%(empty_external_sql))
                hawq_obj.execute_sql(empty_external_sql)
                empty_sql = 'drop table %s'%(self.params.prefix + table)
                loger.print_info("drop table sql is %s"%(empty_sql))
                hawq_obj.execute_sql(empty_sql)
                loger.print_info("Import Hive Into Hawq Part")
                #gain running time
                for j in range(len(table_sql)):
                    hawq_obj.execute_sql(table_sql[j])
                #empty the hive table
                empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.prefix + table)]
                for k in range(len(empty_hive_sql)):
                    hive_obj.execute_meod(empty_hive_sql[k])
                loger.print_info("Execute Empty Hive Database")
            #update
            elif self.params.operation_flag == 4 or self.params.operation_flag == '4':
                if self.params.increment_flag == 1 or self.params.increment_flag == '1':
                    loger.print_info("operate is %s"%(JobOperate.UPDATE))
                    #sqoop sqlserver data into hbase
                    SQLServerCourier.sqoop_data_hbase(self.params.src_hostname, self.params.src_database, self.params.src_username, self.params.src_password, self.jobid, self.params.dst_database, self.params.prefix + table, table, self.params.determine_flag, self.params.determine_value)
                    loger.print_info("sqoop operation successfully!")
                    #use pxf-service import data into hawq from hbase
                    table_sql = SQLServerCourier.sqlserver_hbase_pxf_write_table(server_obj, self.params.prefix + table, table, self.params.dst_hostname, self.params.dst_database, self.params.jobid, self.params.determine_type, self.params.determine_flag)
                    loger.print_info("gain the pxf service sql successfully!")
                    #drop external table and inner table
                    empty_external_sql = 'drop external table %s_ext'%(self.params.prefix + table)
                    loger.print_info("drop table sql is %s:"%(empty_external_sql))
                    hawq_obj.execute_sql(empty_external_sql)
                    empty_sql = 'drop table %s'%(self.params.prefix + table)
                    loger.print_info("drop table sql is %s"%(empty_sql))
                    hawq_obj.execute_sql(empty_sql)
                    loger.print_info("Execute HBase To Hawq Part")
                    #gain running time
                    for j in range(len(table_sql)):
                        hawq_obj.execute_sql(table_sql[j])
            #close connection
            hawq_obj.close()
            hive_obj.close()
            server_obj.close()

