# -*- coding:utf-8 -*-
__author__ = 'yx'
from tools import Tools
from src.logger import logger
from openpyxl import load_workbook
from src.config import config
from src.database import db_conns
import os
import csv
import pandas as pd


class ExcelLocalCsv(Tools):
    def __init__(self):
        loger = logger.Logger("Method:init")

    @staticmethod
    def remove_csv_header(filename, jobid):
        loger = logger.Logger("Method:remove_csv_header")
        try:
            loger.print_info("Start csv header")
            remove_header_cmd = "sed -i '1d' %s"%(filename)
            os.system(remove_header_cmd)
            loger.print_info("Finish csv header")
        except Exception as error_message:
            loger.print_error("remove_csv_header error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message


    @staticmethod
    def download_excel(filename, jobid):
        loger = logger.Logger("Method:download_excel")
        try:
            loger.print_info("Start download excel file")
            download_cmd = 'hdfs dfs -get %s %s'%(filename, config.local_cache)
            os.system(download_cmd)
            temp_name = config.local_cache + filename.split('/')[-1]
            loger.print_info("Finish download excel file")
            return temp_name
        except Exception as error_message:
            loger.print_error("download_excel error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def upload_csv(filename, jobid):
        loger = logger.Logger("Method:upload_csv")
        try:
            loger.print_info("Start upload csv file")
            upload_cmd = 'hdfs dfs -put %s %s'%(filename, config.load_catalog)
            loger.print_info("upload command is :%s"%(upload_cmd))
            os.system(upload_cmd)
            loger.print_info("Finish upload csv file")
        except Exception as error_message:
            loger.print_error("upload_csv error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message

    @staticmethod
    def excel_convert_csv(filename, jobid):
        loger = logger.Logger("Method:excel_convert_csv")
        try:
            loger.print_info("file name is :" + str(filename))
            if str(filename) == '':
                loger.print_error("file is None!")
                raise Exception
            data_xls = pd.read_excel(filename, 'Sheet1', index_col = 0)
            csv_filename = filename.split('.')[0] + '.csv'
            data_xls.to_csv(csv_filename, encoding = 'utf-8')
            return csv_filename
        except Exception as error_message:
            loger.print_error("excel_convert_csv error" + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, 'exception')
            raise error_message
