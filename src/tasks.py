# -*- coding:utf-8 -*-
__author__ = 'yx'
import sys
sys.path.append("/usr/softwares/gears/")
from celery import Celery, platforms
from src.config import config
from src.database import db_conns
from src.task.mysql_task import MysqlTask
from src.task.postgresql_task import PostgresqlTask
from src.task.sqlserver_task import SQLServerTask
from src.task.oracle_task import OracleTask
from src.task.files_task import FilesTask
from src.job_attributes import JobStatus
from src.logger import logger
import json
celery_app = Celery('tasks', broker = 'sqla+postgresql://%s:%s@%s:%s/%s' % (config.celery_user,
                                                             config.celery_passwd,
                                                             config.celery_host,
                                                             config.celery_port,
                                                             config.celery_db))
celery_app.conf.update(
    CELERY_RESULT_BACKEND = 'db+postgresql://%s:%s@%s:%s/%s' % (config.celery_user,
                                                             config.celery_passwd,
                                                             config.celery_host,
                                                             config.celery_port,
                                                             config.celery_db),
    CELERY_TASK_SERIALIZER = 'json',
    CELERY_IGNORE_RESULT = False,
)

platforms.C_FORCE_ROOT = True

@celery_app.task
def submit_task(option):
    o = json.loads(option)
    import_type = int(o.get("type"))
    loger = logger.Logger("Method:submit")
    if import_type == 1:
        loger.print_info("Mysql Source")
        jobid = o.get("jobid")
        db_conns.importjob_conn.update_status(jobid, JobStatus.QUEUED)
        MysqlTask(jobid, o).run()
        loger.print_info("Mysql Finished")
        db_conns.importjob_conn.update_status(jobid, JobStatus.FINISHED)
    elif import_type == 2:
        jobid = o.get("jobid")
        db_conns.importjob_conn.update_status(jobid, JobStatus.QUEUED)
        PostgresqlTask(jobid, o).run()
        loger.print_info("Postgresql Finished")
        db_conns.importjob_conn.update_status(jobid, JobStatus.FINISHED)
    elif import_type == 3:
        jobid = o.get("jobid")
        db_conns.importjob_conn.update_status(jobid, JobStatus.QUEUED)
        FilesTask(jobid, o).run()
        loger.print_info("Files Finished")
        db_conns.importjob_conn.update_status(jobid, JobStatus.FINISHED)
    elif import_type == 5:
        jobid = o.get("jobid")
        db_conns.importjob_conn.update_status(jobid, JobStatus.QUEUED)
        SQLServerTask(jobid, o).run()
        loger.print_info("SQlServer Finished")
        db_conns.importjob_conn.update_status(jobid, JobStatus.FINISHED)
    elif import_type == 4:
        jobid = o.get("jobid")
        db_conns.importjob_conn.update_status(jobid, JobStatus.QUEUED)
        OracleTask(jobid, o).run()
        loger.print_info("Oracle Finished")
        db_conns.importjob_conn.update_status(jobid, JobStatus.FINISHED)
    else:
        pass
