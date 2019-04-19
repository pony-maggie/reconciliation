#-*- coding: UTF-8 -*-
import logging
import time
import datetime
import os
from work_hold import *
from Mtools import *
import cx_Oracle

'''
本文件为定时调用入口文件，
主要负责解析worklist并定时调度
'''
logging.basicConfig(level=logging.DEBUG,
   format='%(asctime)s %(filename)s [line:%(lineno)d] [%(levelname)s] %(message)s',
   datefmt='%Y/%m/%d %H:%M:%S',
   filename='today.log',
   filemode='w')
  
def hold_list(lines,worklist):
    logging.info( '------------------------------')
    logging.info(lines)
    if len(lines) == 0:
        logging.info('this is a empty line ')
        return 
    if lines.find('#') == 0:
        logging.info('this is a comment line')
        return  
    if lines.find('---') == 0 :
        logging.info('this is should exec it current')
        worklist.append(['99:99:99',lines[4:],0])
        return  
    worklist.append([lines[0:8],lines[9:],0])
    
    
    
def reccover_job(work_list):
    for need_recover in worklist:
        need_recover[2] = 9
    nowdate = datetime.datetime.now()
    shell_1 = 'cp today.log ' + Mtools_logfile_save_path +'/pytimeserver.%s.log' %(nowdate.strftime('%Y-%m-%d')  ,)
    os.system(shell_1)
    os.system('echo \'01\' > today.log' )
    #在非工作日直接放弃结算业务，只开启备份
    o_ret = is_work_date()
    if o_ret != returns.return_sucess:
        logging.info('非工作日放弃全部任务')
        for need_recover in worklist:
            need_recover[2] = 1
            if need_recover[0].find('historyback') == 0:
                need_recover[2] = 0
                logging.info('非工作日启用历史备份任务')
    
def parser_and_run_jobs_unitl_finissh(jobs,olddate):
    #针对每一个作业，时间未到情况下一直等待，时间超过情况下执行，执行失败最多等两小时
    #超过两小时则放弃，遇到日切则放弃
    #定时任务不在在支持依赖，依赖情在下成完成，上层只负责按时间调用
    if jobs[2] == returns.return_sucess or jobs[2] == returns.return_notretry:
        return
    now_time = datetime.datetime.now()
    jobs_time = datetime.datetime.strptime('%s %s' %(now_time.strftime('%Y-%m-%d'),jobs[0]), '%Y-%m-%d %H:%M:%S')
    logging.info('the work useing date is %s' %(jobs_time.strftime('%Y-%m-%d %H:%M:%S') ,))
    while 1:
        #if the job util 24:00 unsucess ,giveup it
        if now_time.day - olddate.day:
            return
        #wait jobs
        if now_time < jobs_time:
            logging.debug('wait for job :%s' %(jobs[1] ,) )
            time.sleep(120)
            now_time = datetime.datetime.now()
            continue
        if (now_time.hour - jobs_time.hour) == 2:
            jobs[2] = returns.return_timeout
            return
        if now_time >= jobs_time:    
            logging.debug('should execuate jobs now:%s' %(jobs[1] ,))
            o_ret = do_job(jobs)
            if o_ret == returns.return_sucess or o_ret == returns.return_notretry:
                return
            else:
                logging.debug('should execuate jobs now:%s,but run it failed' % (jobs[1],))
                time.sleep(120)
                continue
#unwork:未开始 sucess:成功 retry:失败可重试 notretry:错误勿重试
def do_job(jobs):
    now_time = datetime.datetime.now().strftime('%Y%m%d')
    if jobs[1].find('leshua_mancheck') == 0:
        jobs[2]=leshua_mancheck(now_time)
        logging.debug('乐刷对帐结果:%s' % (jobs[2],))
        return jobs[2]
    if jobs[1].find('historyback') == 0:
        jobs[2] = database_history()
        logging.debug('数据库备份结果:%s' % (jobs[2],))
        return jobs[2]
    if jobs[1].find('guocai_mancheck') == 0:
        jobs[2] = guocai_mancheck(now_time)
        logging.debug('国采对帐结果:%s' % (jobs[2],))
        return jobs[2]
    if jobs[1].find('guocai_draw') == 0:
        jobs[2] = guocai_draw(now_time)
        logging.debug('tianfubao alipay guocai_draw over')
        return jobs[2]
    return returns.return_exception

            
if __name__=="__main__":
    logging.info("启动定时任务进程")
    Mtools_worklist1=Mtools_worklist1.replace('\n','')
    list = Mtools_worklist1.split('|')
    worklist = []
    for lines in list:
        hold_list(lines,worklist)
    logging.info(worklist)
    olddate = datetime.datetime.now()
    while 1:
        #循环执行任务列表直至执行完
        for jobs in worklist:
            parser_and_run_jobs_unitl_finissh(jobs,olddate)
        while 1:#需保证本层阻塞至当天00:00:00
            newdate = datetime.datetime.now()
            # ervery day when 24:00 recover the jobs
            if newdate.day - olddate.day:
            #if abs(newdate.minues - olddate.minues) == 5:
                logging.info('时间到达凌晨，开始初始化任务')
                reccover_job(worklist)
                olddate = newdate
                break
            #时间未到则打印剩余失败信息，并加长等待时间
            else:
                the_failed_jobs = ' '
                for jobs in worklist:
                    if jobs[2] != returns.return_sucess:
                        the_failed_jobs +=jobs[1]
                        the_failed_jobs += ','
                if cmp(the_failed_jobs, ' ') == 0:
                    logging.info('本日跑批全部完成!!!')
                else:
                    logging.info('当日已跑批完成,但部分失败:%s' %(the_failed_jobs,))
                time.sleep(600)
