#-*- coding:utf-8 -*-
__author__ = 'yx'

from module_base import *
import sys

reload(sys)

class ModuleCommand(ModuleBase):
    def __init__(self, table="t_command"):
        self.schema = "ehc"
        self.table = table
        self.table_name = "\"%s\".\"%s\"" % (self.schema, self.table)
