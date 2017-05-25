# -*- coding:utf- 8 -*-
__author__ = 'yangxin'
import ConfigParser
import sys
import os
reload(sys)

'''
parser the config files
'''
class LocalConfig(object):
    def __init__(self):
        cf = ConfigParser.ConfigParser()
        #parser config.conf
        conf_path = os.path.join(os.path.dirname(__file__), '../config/config.conf')
        cf.read(conf_path)
        #gears
        self.gears_host = self.resolveEnv(cf.get("gears", "host"))
        self.gears_port = self.resolveEnv(cf.get("gears", "port"))
        self.gears_user = self.resolveEnv(cf.get("gears", "user"))
        self.gears_passwd = self.resolveEnv(cf.get("gears", "passwd"))
        self.gears_db = self.resolveEnv(cf.get("gears", "db"))
        #hdfs
        self.load_catalog = self.resolveEnv(cf.get("gears", "load_catalog"))
        #PT_Mind
        self.PT_catalog = self.resolveEnv(cf.get("gears", "PT_catalog"))
        #local_cache
        self.local_cache = self.resolveEnv(cf.get("gears","local_cache"))
        #thrift
        self.thrift_host = self.resolveEnv(cf.get("gears", "thrift_host"))
        self.thrift_port = self.resolveEnv(cf.get("gears", "thrift_port"))
        #loginfo
        self.gears_logInfo = self.resolveEnv(cf.get("gears", "gears_logInfo"))
        #celery
        self.celery_host = self.resolveEnv(cf.get("celery", "host"))
        self.celery_port = self.resolveEnv(cf.get("celery", "port"))
        self.celery_user = self.resolveEnv(cf.get("celery", "user"))
        self.celery_passwd = self.resolveEnv(cf.get("celery", "passwd"))
        self.celery_db = self.resolveEnv(cf.get("celery", "db"))
        #hawq
        self.hawq_port = self.resolveEnv(cf.get("hawq","port"))
        self.hawq_user = self.resolveEnv(cf.get("hawq", "user"))
        self.hawq_passwd = self.resolveEnv(cf.get("hawq", "passwd"))
        #hive
        self.hive_host = self.resolveEnv(cf.get("hive", "host"))
        self.hive_port = self.resolveEnv(cf.get("hive", "port"))
        self.hive_user = self.resolveEnv(cf.get("hive", "user"))
        self.hive_database = self.resolveEnv(cf.get("hive", "database"))
        self.hive_passwd = self.resolveEnv(cf.get("hive", "passwd"))
        self.hive_authMechanism = self.resolveEnv(cf.get("hive", "authMechanism"))
        #alphatrion
        self.alphatrion_host = self.resolveEnv(cf.get("alphatrion", "host"))
        self.alphatrion_port = self.resolveEnv(cf.get("alphatrion", "port"))
        #aps_user
        self.aps_user_host = self.resolveEnv(cf.get("aps_user", "host"))
        self.aps_user_port = self.resolveEnv(cf.get("aps_user", "port"))
        self.aps_user_user = self.resolveEnv(cf.get("aps_user", "user"))
        self.aps_user_password = self.resolveEnv(cf.get("aps_user", "password"))
        self.aps_user_db = self.resolveEnv(cf.get("aps_user", "db"))
        self.aps_user_schema = self.resolveEnv(cf.get("aps_user", "schema"))

    def reload(self):
        cf = ConfigParser.ConfigParser()
        #parser config.conf
        conf_path = os.path.join(os.path.dirname(__file__), '../config/config.conf')
        cf.read(conf_path)
        #gears
        self.gears_host = self.resolveEnv(cf.get("gears", "host"))
        self.gears_port = self.resolveEnv(cf.get("gears", "port"))
        self.gears_user = self.resolveEnv(cf.get("gears", "user"))
        self.gears_passwd = self.resolveEnv(cf.get("gears", "passwd"))
        self.gears_db = self.resolveEnv(cf.get("gears", "db"))
        #hdfs
        self.load_catalog = self.resolveEnv(cf.get("gears", "load_catalog"))
        #PT_Mind
        self.PT_catalog = self.resolveEnv(cf.get("gears", "PT_catalog"))
        #local_cache
        self.local_cache = self.resolveEnv(cf.get("gears","local_cache"))
        #thrift
        self.thrift_host = self.resolveEnv(cf.get("gears", "thrift_host"))
        self.thrift_port = self.resolveEnv(cf.get("gears", "thrift_port"))
        #loginfo
        self.gears_logInfo = self.resolveEnv(cf.get("gears", "gears_logInfo"))
        #celery
        self.celery_host = self.resolveEnv(cf.get("celery", "host"))
        self.celery_port = self.resolveEnv(cf.get("celery", "port"))
        self.celery_user = self.resolveEnv(cf.get("celery", "user"))
        self.celery_passwd = self.resolveEnv(cf.get("celery", "passwd"))
        self.celery_db = self.resolveEnv(cf.get("celery", "db"))
        #hawq
        self.hawq_port = self.resolveEnv(cf.get("hawq","port"))
        self.hawq_user = self.resolveEnv(cf.get("hawq", "user"))
        self.hawq_passwd = self.resolveEnv(cf.get("hawq", "passwd"))
        #hive
        self.hive_host = self.resolveEnv(cf.get("hive", "host"))
        self.hive_port = self.resolveEnv(cf.get("hive", "port"))
        self.hive_user = self.resolveEnv(cf.get("hive", "user"))
        self.hive_passwd = self.resolveEnv(cf.get("hive", "passwd"))
        self.hive_database = self.resolveEnv(cf.get("hive", "database"))
        self.hive_authMechanism = self.resolveEnv(cf.get("hive", "authMechanism"))
        #alphatrion
        self.alphatrion_host = self.resolveEnv(cf.get("alphatrion", "host"))
        self.alphatrion_port = self.resolveEnv(cf.get("alphatrion", "port"))
        #aps_user
        self.aps_user_host = self.resolveEnv(cf.get("aps_user", "host"))
        self.aps_user_port = self.resolveEnv(cf.get("aps_user", "port"))
        self.aps_user_user = self.resolveEnv(cf.get("aps_user", "user"))
        self.aps_user_password = self.resolveEnv(cf.get("aps_user", "password"))
        self.aps_user_db = self.resolveEnv(cf.get("aps_user", "db"))
        self.aps_user_schema = self.resolveEnv(cf.get("aps_user", "schema"))

    def resolveEnv(self, con):
        if con.startswith('ENV_'):
            return os.environ.get(con)
        return con

config = LocalConfig()
