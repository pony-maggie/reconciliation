#-*- coding: UTF-8 -*-
import time
import datetime
import logging
import ConfigParser
import string
import cx_Oracle
#configfile readed
# 配置文件
cf = ConfigParser.ConfigParser()
cf.read('/home/innpay/timework/conf.ini')
Mtools_leshua_ftp_save_path = cf.get("ftp","leshua_ftp_save_path")
Mtools_leshua_mancheck_path = cf.get("ftp","leshua_mancheck_path")
Mtools_database_user        = cf.get("ftp","database_user")
Mtools_guocai_ftppath       = cf.get("ftp","guocai_ftppath")
Mtools_guocai_weixin_checkfile=cf.get("ftp","guocai_weixin_checkfile")
Mtools_guocai_alipay_checkfile=cf.get("ftp","guocai_alipay_checkfile")
Mtools_leshua_file_name = cf.get("ftp","leshua_file_name")
Mtools_worklist1 = cf.get("worklist","worklist1")
Mtools_logfile_save_path  = cf.get("sys","logfile_save_path")
#返回码
#unwork:未开始 sucess:成功 retry:失败可重试 notretry:错误勿重试
class returns() :
    return_unwork = 'unwork'
    return_sucess = 'sucess'
    return_retry  = 'retry'
    return_notretry = 'notretry'
    return_timeout = 'timeout'
    return_exception = 'exception'
    day_end_status_check_sucess = 'sucess'
    day_end_status_nofile = 'nofile'
    day_end_status_noavlid = 'noavlid'
    day_end_status_error = 'error'

Mtools_get_work_day_SQL = '''select cdate from work_day t
where
t.cdate >=
(select max(cdate) from work_day t where t.work = 'Y' and t.cdate <
(select min(cdate) from work_day t where t.work ='Y' and t.cdate >= 'FILEDATE'))
and t.cdate < (select min(cdate) from work_day t where t.work ='Y' and t.cdate >= 'FILEDATE')
'''
#多次替换
def miltple_replace(text,oper):
    retext = text
    reoper = oper+oper
    while retext.find(reoper) != -1:
        retext = retext.replace(reoper,oper)
    return retext

def changeCode(strs,must_code = 'none'):
    if must_code == 'gbk':
        return strs.decode('gbk').encode('utf-8')

    if isinstance(strs,unicode):
        strs = strs.encode('utf-8')
    elif isinstance(strs,str):
        return strs
    else :
        strs = strs.decode('gbk').encode('utf-8')
    return strs

def is_work_date(in_work_date = '1999/99/99'):
    nowdate = datetime.datetime.now().strftime('%Y%m%d')
    if in_work_date != '1999/99/99':
        nowdate = in_work_date
    connects = cx_Oracle.connect(Mtools_database_user)
    cursors = connects.cursor()
    sqls = "select work from work_day t where t.cdate = 'NOWDATE'"
    using_sqls = sqls.replace('NOWDATE', nowdate)
    exec_sql = cursors.execute(using_sqls)
    is_work = exec_sql.fetchall()
    cursors.close()
    connects.close()

    if is_work[0][0] == 'N':
        logging.debug('非工作日')
        return returns.return_notretry
    logging.debug('是工作日')
    return returns.return_sucess

def change_chinese_channel_2_english(str):
    try:
        str1 = changeCode(str)
        result = dict_list(str1)
        if result != 'unknow':
            return result
        str1 = changeCode(str,'gbk')
        result = dict_list(str1)
        if result != 'unknow':
            return result
    except Exception, e:
        return str

def dict_list(str):
    #国采
    if str.find(changeCode(u'扫码支付')) == 0:
        return 'SWEP'
    elif str.find(changeCode(u'微信公众号支付')) == 0:
        return 'WXPUB'
    elif str.find(changeCode(u'微信支付')) == 0:
        return 'GC-WX'
    elif str.find(changeCode(u'支付宝')) == 0:
        return 'GC-ALI'
    #乐刷
    elif str.find(changeCode(u'乐刷')) == 0:
        return 'Leshua'
    elif str.find(changeCode(u'消费撤销')) == 0:
        return '105'
    elif str.find(changeCode(u'消费冲正')) == 0:
        return '103'
    elif str.find(changeCode(u'消费')) == 0:
        return '101'
    elif str.find(changeCode(u'退货'))==0:
        return '125'
    elif str.find(changeCode(u'刷卡支付'))==0:
        return 'card'
    elif str.find(changeCode(u'信用卡')) == 0:
        return 'C'
    elif str.find(changeCode(u'借记卡')) == 0:
        return 'D'
    elif str.find(changeCode(u'境外卡'))==0:
        return 'O'
    elif str.find(changeCode(u'标准类')) == 0:
        return 'S'
    elif str.find(changeCode(u'优惠类')) ==0:
        return 'D'
    else:
        return 'unknow'
