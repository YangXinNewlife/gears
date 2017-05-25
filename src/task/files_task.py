# -*- coding:utf -*-
__author__ = 'yx'
from src.logger import logger
from src.database import db_conns
from src.job_attributes import JobOperate, JobStatus
from src.database.postgres_client import PostgresClient
from src.router.hive_router import HiveRouter
from src.courier.csv_courier import CsvCourier
from src.metastore.ddl_client import DdlClient
import unicodedata
from src.tools.excel_local_csv import ExcelLocalCsv
from src.tools.compress_local_csv import CompressLocalCsv
from src.tools.text_file_local_csv import TextFileLocalCsv
from src.task.task_basc import TaskBasc
from src.config import config
import os


class files_jparser(object):
    def __init__(self, json_params):
        try:
            loger = logger.Logger("Method:init")
            loger.print_info("Start Paraser")
            self.jobid = json_params["jobid"]
            self.token = json_params["token"]
            self.task_name = json_params["name"]
            self.description = json_params["description"]
            self.files = json_params["params"]["src_conn"]["files"]
            if type(self.files) == list:
                pass
            else:
                loger.print_info("convert filename format to list")
                files = unicodedata.normalize('NFKD', self.files).encode('utf-8', 'ignore')
                files = files.replace('[', '').replace(']', '')
                files = files.split(',')
                files_list = []
                for i in range(len(files)):
                    files[i] = files[i].replace("'", '')
                    files_list.append(files[i])
                self.files = files_list
            self.files_encoding = json_params["params"]["src_conn"]["files_encoding"]
            self.columns_name = json_params["params"]["src_conn"]["columns_name"]
            self.datacolumn_first = json_params["params"]["src_conn"]["DataColumn_first"]
            if self.datacolumn_first < 1:
                db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            self.datacolumn_last = json_params["params"]["src_conn"]["DataColumn_last"]
            self.dst_database = json_params["params"]["dst_conn"]["database"]
            self.dst_hostname = json_params["params"]["dst_conn"]["hostname"]
            if self.dst_database == None:
                db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            self.tableinfo = json_params["params"]["dst_conn"]["tableinfo"]
            self.t_type = json_params["params"]["dst_conn"]["tableinfo"]["Ttype"]
            self.separatorsign = json_params["params"]["dst_conn"]["tableinfo"]["separatorsign"]
            self.continue_with_error = json_params["params"]["dst_conn"]["tableinfo"]["continue_with_error"]
            self.tablename = json_params["params"]["dst_conn"]["tableinfo"]["tablename"]
            self.tablename = str(self.tablename).lower()
            if self.t_type == 1 or self.t_type == '1':
                self.columns = self.tableinfo["columns"]
            self.dst_ehc_id = json_params["params"]["dst_conn"]["ehc_id"]
            self.dst_clustername = json_params["clustername"]
            loger.print_info("Start Paraser")
        except Exception as error_message:
            loger.print_error("file parser the params error!" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(self.jobid, JobStatus.EXCEPTION)
            raise error_message


class FilesTask(TaskBasc):
    def __init__(self, jobid, jparams):
        loger = logger.Logger("Method:init")
        self.params = files_jparser(jparams)
        self.jobid = jobid

    def run(self):
        loger = logger.Logger("Method:run")
        importjob = db_conns.importjob_conn.update_status(self.jobid, 'running')
        hawq_obj = PostgresClient(None, self.params.dst_hostname, config.hawq_user, config.hawq_passwd, self.params.dst_database, config.hawq_port)
        loger.print_info("HDB Connection Successfully")
        hive_obj = HiveRouter(self.params.jobid)
        loger.print_info("Hive DB Connection Successfully")
        temp_type = self.params.t_type
        for i in range(len(self.params.files)):
            loger.print_info("Start Catalog pre-treatment")
            hadoop_rm_str = 'hdfs dfs -rm -r /HoneyWellDataStore/load/catalog/*'
            loger.print_info(hadoop_rm_str)
            os.system(hadoop_rm_str)
            delete_local_catalog = 'rm -rf %s'%(config.local_cache) + '*'
            loger.print_info(delete_local_catalog)
            os.system(delete_local_catalog)
            hadoop_cp_str = 'hdfs dfs -cp %s %s'%(self.params.files[i], config.load_catalog)
            loger.print_info(hadoop_cp_str)
            os.system(hadoop_cp_str)
            self.params.files[i] = config.load_catalog + self.params.files[i].split('/')[-1]
            loger.print_info("Finish Catalog pre-treatment")
            if str(self.params.files[i]).split("/")[-1].split(".")[-1] == 'xlsx' or str(self.params.files[i]).split("/")[-1].split(".")[-1] == 'xls':
                loger.print_info("Start excel convert to csv")
                temp_file = ExcelLocalCsv.download_excel(self.params.files[i], self.params.jobid)
                loger.print_info("download excel from hdfs is %s"%(temp_file))
                temp_file1 = ExcelLocalCsv.excel_convert_csv(temp_file, self.params.jobid)
                loger.print_info("excel to csv file is %s"%(temp_file1))
                if self.params.columns_name == '1':
                    ExcelLocalCsv.remove_csv_header(temp_file1, self.params.jobid)
                else:
                    loger.print_info("the excel file is no colunm_name")
                ExcelLocalCsv.upload_csv(temp_file1, self.params.jobid)
                self.params.files[i] = self.params.files[i].split('.')[0] + '.csv'
                loger.print_info("Finish excel convert to csv")
            elif str(self.params.files[i]).split("/")[-1].split(".")[-1] == 'del':
                loger.print_info("Start del convert to csv")
                self.params.files[i] = TextFileLocalCsv.rename(self.params.files[i])
                if self.params.columns_name == 1:
                    TextFileLocalCsv.remove_header(self.params.files[i])
                loger.print_info("Finish del convert to csv")
            elif str(self.params.files[i]).split("/")[-1].split(".")[-1] == 'txt':
                loger.print_info("Start txt convert to csv")
                self.params.files[i] = TextFileLocalCsv.rename(self.params.files[i])
                if self.params.columns_name == 1:
                    TextFileLocalCsv.remove_header(self.params.files[i])
                loger.print_info("Finish txt convert to csv")
            elif str(self.params.files[i]).split("/")[-1].split(".")[-1] == 'dat':
                loger.print_info("Start dat convert to csv")
                self.params.files[i] = TextFileLocalCsv.rename(self.params.files[i])
                if self.params.columns_name == 1:
                    TextFileLocalCsv.remove_header(self.params.files[i])
                loger.print_info("Finish dat convert to csv")
            elif str(self.params.files[i]).split(".")[-1] == 'gz':
                loger.print_info("Start compress convert csv")
                filename = CompressLocalCsv.download_compress(self.params.files[i])
                filename = CompressLocalCsv.uncompress_file(filename)
                CompressLocalCsv.remove_header(filename)
                filename = CompressLocalCsv.rename_file(filename)
                filename = CompressLocalCsv.upload_file(filename)
                self.params.files[i] = filename
                loger.print_info("Finish compress convert csv")
            elif str(self.params.files[i]).split("/")[-1].split(".")[-1] == 'csv':
                pass
            if temp_type == 1 or temp_type == '1':
                loger.print_info("operate is :" + JobOperate.CRETAE)
                loger.print_info("Start create hive table")
                sql = hive_obj.csv_create_hiveTable(self.params.tablename, self.params.dst_database, self.params.columns, self.params.separatorsign, self.params.jobid)
                for k in range(len(sql)):
                    hive_obj.execute_meod(sql[k])
                loger.print_info("Finish create hive table")
                temp_type = 2
                loger.print_info("Start Load data into hive")
                sql1 = hive_obj.upload_data(self.params.tablename, self.params.dst_database, self.params.files[i], temp_type, self.params.jobid)
                for i in range(len(sql1)):
                    hive_obj.execute_meod(sql1[i])
                loger.print_info("Finish Load data into hive")
            else:
                loger.print_info("operate is :" + JobOperate.APPEND_ONLY)
                loger.print_info("Start Load data into hive")
                sql2 = hive_obj.upload_data(self.params.tablename, self.params.dst_database, self.params.files[i], temp_type, self.params.jobid)
                for i in range(len(sql2)):
                    hive_obj.execute_meod(sql2[i])
                loger.print_info("Finish Load data into hive")
        loger.print_info("Start PXF-Service import into HDB")
        table_sql = CsvCourier.csv_pxf_write_table(self.params.tablename, self.params.dst_hostname, self.params.dst_database, self.params.t_type, self.params.columns, self.params.datacolumn_last, self.params.jobid)
        for j in range(len(table_sql)):
            hawq_obj.execute_sql(table_sql[j])
        loger.print_info("Start PXF-Service import into HDB")
        loger.print_info("Start empty hive db")
        empty_hive_sql = ["use %s"%(self.params.dst_database), "truncate table %s"%(self.params.tablename)]
        for k in range(len(empty_hive_sql)):
            hive_obj.execute_meod(empty_hive_sql[k])
        loger.print_info("Finish empty hive db")
        loger.print_info("Start write meta data into pg db")
        client = DdlClient(config.alphatrion_host, 7080)
        client.create_table(self.params.dst_database, self.params.tablename, self.params.token)
        loger.print_info("Start write meta data into pg db")
        hawq_obj.close()
        hive_obj.close()
