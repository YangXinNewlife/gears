# -*- coding:utf-8 -*-
__author__ = 'yx'
from tracker import Tracker
import os

class HDFSVerifier(Tracker):
    def __init__(self):
        pass

    def file_exist(self, file):
        hadoop_str = "hadoop fs -test -e %s"%(file)
        result = os.system(hadoop_str)
        if result == 0 or result == '0':
            drop_hadoop_file = "hadoop fs -rm %s"%(file)
            os.system(drop_hadoop_file)

