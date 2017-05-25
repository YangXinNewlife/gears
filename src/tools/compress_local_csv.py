__author__ = 'yx'

from src.logger import logger
from src.config import config
import os

class CompressLocalCsv(object):
    def __init__(self):
        loger = logger.Logger("Method:init")

    @staticmethod
    def download_compress(filename):
        loger = logger.Logger("Method:download_compress")
        loger.print_info("Start downlaod compress file")
        download_cmd = "hdfs dfs -get %s %s"%(filename, config.local_cache)
        loger.print_info(download_cmd)
        os.system(download_cmd)
        filename = config.local_cache + str(filename).split("/")[-1]
        loger.print_info("Finish downlaod compress file")
        return filename

    @staticmethod
    def upload_file(filename):
        loger = logger.Logger("Method:upload_file")
        loger.print_info("Start uplaod file")
        upload_cmd = "hdfs dfs -put %s %s"%(filename, config.load_catalog)
        os.system(upload_cmd)
        loger.print_info("Finish uplaod file")
        file_new = config.load_catalog + str(filename).split("/")[-1]
        return file_new

    @staticmethod
    def remove_header(filename):
        loger = logger.Logger("Method:remove_header")
        loger.print_info("Start remove header")
        remove_header_cmd = "sed -i '1d' %s"%(filename)
        os.system(remove_header_cmd)
        loger.print_info("Finish remove header")

    @staticmethod
    def rename_file(filename):
        loger = logger.Logger("Method:rename_file")
        loger.print_info("Start rename file")
        file_new = str(filename).split(".")[0] + '.csv'
        rename_cmd = "mv %s %s"%(filename, file_new)
        os.system(rename_cmd)
        loger.print_info("Finish rename file")
        return file_new

    @staticmethod
    def uncompress_file(filename):
        loger = logger.Logger("Method:uncompress")
        loger.print_info("Start uncompress")
        uncompress_cmd = "tar -zxvf %s"%(filename)
        os.system(uncompress_cmd)
        file_new = str(filename).split(".tar")[0] + '.del'
        loger.print_info("FInish uncompress")
        return file_new





