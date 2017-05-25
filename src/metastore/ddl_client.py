# -*- coding:utf-8 -*-
__author__ = 'yx'
import sys
import json
import requests
from src.logger import logger
from src.database.module_base import *

'''
definition a database which stored tablename, columns information
'''
class DdlClient(object):
    def __init__(self, host, port, version="v1"):
        loger = logger.Logger("Method:init")
        try:
            self.webapp = ""
            self.baseUri = "http://%s:%s/%s/metatable/create"%(host, port, version)
            loger.print_info("ddl client init: " + self.baseUri)
        except Exception as error_message:
            loger.print_error(error_message)
            raise error_message


    def create_table(self, db, table, token):
        loger = logger.Logger("create_table")
        url = self.baseUri + "/%s/%s"%(db, table)
        headers = {'Accept-Charset': 'utf-8',
                   'Content-Type': 'application/json',
                   'X-AUTH-TOKEN': '%s'%(token)
        }
        loger.print_info(config)
        try:
            columns = ModuleBase().get_mycolumns('base1.zetyun.com', config.hawq_port, config.hawq_user, config.hawq_passwd, db, "public", table)
        except Exception as error_message:
            loger.print_error(error_message)
            raise error_message
        loger.print_info(columns)
        ccc = [{"name":c[0],"col_type":c[1]} for c in columns]
        requestJson = {
            "tbl_type": "csvTable",
            "columns": ccc,
            "metas": {}
            }
        resp = requests.post(url, headers = headers, json = requestJson)
        if resp.status_code != 200:
            loger.print_info(resp.content)
            return None
        else:
            connResp = json.loads(resp.content)
            loger.print_info(connResp)
            loger.print_info(connResp["status"])
            if not connResp["status"]:
                return None
        return True
