# -*- coding:utf-8 -*-
__author__ = 'yx'
from tracker import Tracker
from src.logger import logger
from src.database import db_conns
from src.job_attributes import JobStatus

class HiveTracker(Tracker):
    def __init__(self):
        loger = logger.Logger("Method:init")
        loger.print_info("This is Hivetracker")

    @staticmethod
    def hive_pretreate(database, table, jobid):
        loger = logger.Logger("Method:hive_pretreate")
        try:
            loger.print_info(table)
            hive_pretreate = []
            pretreate1 = "use " + "`%s`"%(database)
            pretreate2 = 'drop table if exists `%s`'%(table)
            hive_pretreate.append(pretreate1)
            hive_pretreate.append(pretreate2)
            loger.print_info(hive_pretreate)
            return hive_pretreate
        except Exception as error_message:
            loger.print_error("hive db pre treatment error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, JobStatus.EXCEPTION)
            raise error_message



