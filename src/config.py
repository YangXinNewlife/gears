# -*- coding:utf-8 -*-
__author__ = 'YangXin_Ryan'
import ConfigParser
import os
class LocalConfig(object):
    def __init__(self):
        confParser = ConfigParser.ConfigParser()
        confPath = os.path.join(os.path.dirname(__file__), '../conf/config.conf')
        confParser.read(confPath)


