#-*- coding: UTF-8 -*-
import os
import time
import datetime
import logging
import cx_Oracle
import traceback
from Mtools import *

import sys
sys.path.append('Balance')
from Balance_Leshua import *
'''
本文件主要负责调度层与业务入口之间的桥接，统一管理工作流接口
当支撑业务多时可从该程序定位业务过程位置
'''


def leshua_mancheck(i_date):
    try:
        logging.info('---------------------------------------BEGIN------------------------------------------------')
        if is_work_date(i_date) != returns.return_sucess:
            logging.info('非工作日不进行对帐')
            return returns.return_notretry
        filedate = i_date
        cbl = c_balance_leshua(filedate)
        o_ret = cbl.run()
        logging('result is '+o_ret[0])
        return o_ret[0]
    except Exception ,e:
        logging.error(e.message)
        return returns.return_notretry
    finally:
        logging.info('---------------------------------------END------------------------------------------------')

def database_history():
    try:
        logging.info('---------------------------------------BEGIN------------------------------------------------')
        connects = cx_Oracle.connect(Mtools_database_user)
        cursors = connects.cursor()
        parm = (0, '0000')
        parm = cursors.callproc('pkg_history.section_all', (parm[0], parm[1]))
        if(returns[1].find('sucess') == 0):
            logging.debug('history data backup sucess!!')
        else:
            logging.debug('history backup failed:[%d][%s] ' % (parm[0],parm[1]))
            returns.return_notretry
        cursors.close()
        connects.close()
        return returns.return_sucess
    except Exception ,e:
        logging.error(e)
        return returns.return_notretry
    finally:
        logging.info('---------------------------------------END------------------------------------------------')

