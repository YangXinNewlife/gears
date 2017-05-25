# -*- coding:utf-8 -*-
__author__ = 'yx'
class JobStatus(object):
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    EXCEPTION = "exception"


class JobOperate(object):
    CRETAE = 'CREATE'
    APPEND_ONLY = 'APPEND_ONLY'
    OVERRID = 'OVERRID'
    UPDATE = 'UPDATE'

class JobLevel(object):
    INFO = 'INFO'
    WARING = 'WARNING'
    ERROR = 'ERROR'

joboperate_dict = {
    JobOperate.CRETAE:"Create table in hawq by parase the frontend post parameters, attention please the dst_database must be not exists the table, otherwise please choosing append, override or update  modes of operatiopn!",
    JobOperate.APPEND_ONLY:"Add data into hawq's tables, attention please the dst_database must be exists, otherwise please choosing create modes of operatiopn!",
    JobOperate.OVERRID:"Overwrite the hawq's tables, add newest data into tables, attention please the dst_database must be exists, otherwise please choosing create modes of operatiopn!",
    JobOperate.UPDATE:"Update the hawq's original table' structure or increment add data, otherwise please choosing create modes of operatiopn!"
}

jobstatus_dir = {
    JobStatus.QUEUED:"The job is waiting the celery handle the submit task!",
    JobStatus.RUNNING:"The job is running!",
    JobStatus.FINISHED:"The job is finish!",
    JobStatus.EXCEPTION:"The job exception occurred!"
}

jobstatus_level = {
    JobLevel.INFO:"normal import result",
    JobLevel.WARING:"include a little import error, less than 3 times",
    JobLevel.ERROR:"import error"
}