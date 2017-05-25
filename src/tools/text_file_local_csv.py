__author__ = 'yx'
from src.logger import logger
import os

class TextFileLocalCsv(object):
    def __init__(self):
        loger = logger.Logger("Method:init")

    @staticmethod
    def rename(filename):
        loger = logger.Logger("Method:rename")
        loger.print_info("Start rename")
        file_new = str(filename).split(".")[0] + '.csv'
        rename_cmd = "hdfs dfs -mv %s %s"%(filename, file_new)
        os.system(rename_cmd)
        loger.print_info("Finish rename")
        return file_new

    @staticmethod
    def remove_header(filename):
        loger = logger.Logger("Method:remove_header")
        loger.print_info("Start remove_header")
        remove_header_cmd = "sed -i '1d' %s"%(filename)
        os.system(remove_header_cmd)
        loger.print_info("Finish remove_header")




