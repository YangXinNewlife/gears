# -*- coding:utf-8 -*-
__author__ = 'yangxin'
import logging
from src.config import config

class Logger(object):

    def __init__(self,loggerName="Default"):
       logFormat = "%(asctime)s %(levelname)s %(name)s - %(message)s"
       dateFortmat = "%Y-%m-%d %H:%M:%S"
       logging.basicConfig(level=logging.DEBUG,
                    format=logFormat,
                    datefmt = dateFortmat,
                    filename =config.gears_logInfo,
                    filemode ='a')

       if loggerName == "Default":
           pass

       self.loggerName = loggerName
       self.logger = logging.getLogger(loggerName)
       # output log to console
       if len(self.logger.handlers) == 0:
           shFormatter = logging.Formatter(fmt = logFormat,
                                           datefmt = dateFortmat)
           sh = logging.StreamHandler()
           sh.setLevel(logging.INFO)
           sh.setFormatter(shFormatter)
           self.logger.addHandler(sh)

    #print debug information
    def print_debug(self, info):
        self.logger.debug('%s'%(info))

    #print info information
    def print_info(self, info):
        self.logger.info('%s'%(info))

    #print warning information
    def print_waring(self, info):
        self.logger.warning('%s'%(info))

    #print error information
    def print_error(self, info):
        self.logger.error('%s'%(info))

    #print critical information
    def print_critical(self, info):
        self.logger.critical("%s"%(info))
