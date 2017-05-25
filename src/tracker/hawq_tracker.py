# -*- coding:utf-8 -*-
__author__ = 'yx'
from src.tracker.tracker import Tracker
from src.logger import logger
from src.database import db_conns
from src.job_attributes import JobStatus

class HAWQTracker(Tracker):
    def __init__(self):
        loger = logger.Logger("Method:init")
        loger.print_info("This is HAWQTracker")

    @staticmethod
    def hawq_pretreate(table, jobid):
        loger = logger.Logger("Method:hawq_pretreate")
        try:
            loger.print_info(table)
            hawq_pretreate = []
            pretreate1 = 'drop external table if exists "%s"'%(table + '_ext')
            pretreate2 = 'drop table if EXISTS "%s"'%(table)
            hawq_pretreate.append(pretreate1)
            hawq_pretreate.append(pretreate2)
            loger.print_info(hawq_pretreate)
            return hawq_pretreate
        except Exception as error_message:
            loger.print_error("HDB pre treatment error, reason : " + str(error_message))
            importjob = db_conns.importjob_conn.update_status(jobid, JobStatus.EXCEPTION)
            raise error_message


