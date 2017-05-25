__author__ = 'yx'
import os
from src.logger import logger
class PXFServerTracjer():
    def __init__(self):
        loger = logger.Logger("PXFServerTracker")
        loger.print_info("Restart Loading ... ... ...")

    @staticmethod
    def restart_pxf_server():
        loger = logger.Logger("PXFServerTracker")
        sys_command1 = '/etc/init.d/pxf-server restart'
        sys_command2 = 'ssh base2.zetyun.com'
        sys_command3 = 'ssh base3.zetyun.com'
        sys_command4 = 'exit'
        os.system(sys_command1)
        os.system(sys_command2)
        os.system(sys_command1)
        os.system(sys_command3)
        os.system(sys_command1)
        os.system(sys_command4)
        os.system(sys_command4)
        loger.print_info("Restart Successfully")






