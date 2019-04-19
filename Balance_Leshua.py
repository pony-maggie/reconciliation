#-*- coding: UTF-8 -*-
import sys
sys.path.append('..')
from  Mtools import *
import os
import cx_Oracle
'''
此版本将乐刷整个对帐流程使用本模块进行
'''
class c_balance_leshua:
    m_date = ' '
    __check_file_name = ' '
    __connects =  ' '
    __cursors = ' '
    __insert_db_sql = '''insert into yljg_check(serial_no,transmsn_date_time,order_no,merch_id,
    fee_rate,trans_amt,fee_amt,sett_amt,trans_type,pay_type,card_type,discount_type,retrivl_ref_num,batch_id,
    sys_trace_audit_num,card_id,terminal_id,check_date,proc_status,CHANNEL_CODE,resp_code) values(:serial_no,:transmsn_date_time,:order_no,'00000'||:merch_id,
    :fee_rate/10000,:trans_amt/100,:fee_amt/100,:sett_amt/100,:trans_type,:pay_type,:card_type,:discount_type,:retrivl_ref_num,:batch_id,
    :sys_trace_audit_num,:card_id,:terminal_id,:check_date,'00','Leshua','00')'''

    def __init__(self,i_date):
        self.m_date = i_date
        self.__connects = cx_Oracle.connect(Mtools_database_user)
        self.__cursors = self.__connects.cursor()
    def __del__(self):
        self.__connects.commit()
        self.__cursors.close()
        self.__connects.close()


    #仅包含日期循环和日终处理和文件入库等操作
    def run(self):
        o_ret = -1
        #1. 获取乐刷对帐文件
        o_ret = self.get_check_file_Leshua()
        if o_ret != returns.return_sucess :
            return o_ret
        o_ret = self.get_workdate_list()
        if o_ret == returns.return_sucess:
            return o_ret
        wrok_day_list = o_ret
        #2. q清除ylhg下的数据
        self.recover_yljg_check(wrok_day_list)
        #3. 解析文件入库
        o_ret = self.parse_file_into_db()
        if o_ret != returns.return_sucess:
            return o_ret
        for work_date in wrok_day_list:
            #2. 进入具体试算流程
            remarks = '  '
            o_ret  = self.run_work_in_that_day(work_date[0])
            update_sql  = '''update dayendprogress t set t.status = 'M_STATUS',t.update_time =sysdate,t.remarks = 'REMARK' where t.workdate = 'NOWDATE' and t.branch_code = 'Leshua' '''
            update_ql1 = update_sql.replace('NOWDATE',work_date[0]).replace('REMARK',o_ret[1])
            if o_ret[0] == returns.return_sucess:
                self.__cursors.execute(update_ql1.replace('M_STATUS','00'))
            else:
                self.__cursors.execute(update_ql1.replace('M_STATUS','01'))

        logging.info('乐刷结算成功!!!')
        return returns.return_sucess

    #日终存储过程调用
    def run_work_in_that_day(self,sett_date):
        job_return = [returns.return_retry,'failed']
        # 定义过程变量
        logging.info('开始循环处理该工作日内交易%s' % (sett_date))
        # 插入日终进展表
        self.__init_todays_db(sett_date)
        proc_in_date = sett_date
        proc_out_rst = self.__cursors.var(cx_Oracle.NUMBER)
        proc_out_msg = self.__cursors.var(cx_Oracle.STRING)
        # 3. 比对乐刷交易和本地交易之间是否能意义对应
        logging.info('开始乐刷交易比对')
        self.__cursors.callproc('pkg_balance.pro_leshua_check1', (proc_in_date, proc_out_rst, proc_out_msg))
        job_return[1] = proc_out_msg.getvalue()
        logging.info('乐刷交易比对完成%d:%s' % (proc_out_rst.getvalue(), changeCode(job_return[1])))
        if proc_out_rst.getvalue() != 0:
            return job_return
        # 4. 清算各级收益
        logging.info('开始清算各方收益')
        self.__cursors.callproc('pkg_balance.pro_leshua_check2', (proc_in_date, proc_out_rst, proc_out_msg))
        job_return[1] = proc_out_msg.getvalue()
        logging.info('乐刷清算各方收益%d:%s' % (proc_out_rst.getvalue(), changeCode(job_return[1])))
        if proc_out_rst.getvalue() != 0:
            job_return[0] = returns.return_notretry
            return job_return
        # 5. 计入统计表
        logging.info('开始结算统计')
        self.__cursors.callproc('pkg_balance.pro_leshua_sett_sum', (proc_in_date, proc_out_rst, proc_out_msg))
        job_return[1] = proc_out_msg.getvalue()
        logging.info('乐刷结算统计完成%d:%s' % (proc_out_rst.getvalue(), changeCode(job_return[1])))
        if proc_out_rst.getvalue() != 0:
            job_return[0] = returns.return_notretry
            return job_return
        job_return[0] = returns.return_sucess
        return job_return


    def get_check_file_Leshua(self):
        tmp_date = self.m_date[0:4] + '-' + self.m_date[4:6] + '-' + self.m_date[6:8]
        paramenter = Mtools_leshua_file_name.replace('NOWDATE',tmp_date)
        exec_sh = 'sh Balance/ftp_Leshua.sh %s' % (paramenter,)
        returned = os.system(exec_sh)
        if returned == 256:
            return returns.return_retry
        file_exist = os.path.exists(paramenter)
        if (file_exist == False):
            logging.debug('未能正常获取到乐刷对账文件 [%s]' % (exec_sh,))
            return returns.return_retry
        file_size = os.path.getsize(paramenter)
        if file_size == 0:
            logging.debug('获取到乐刷对账文件[%s] 大小是0!!!' % (paramenter,))
            os.system('rm %s' % (paramenter,))
            return returns.return_retry
        logging.debug('获取乐刷对账文件[%s] 成功!!!' % (paramenter,))
        logging.debug('移动乐刷对账文件至:%s' % (Mtools_leshua_ftp_save_path,))
        os.system('mv %s %s' % (paramenter, Mtools_leshua_ftp_save_path))
        self.__check_file_name = Mtools_leshua_ftp_save_path + '/' + paramenter
        return returns.return_sucess


    #获取工作日列表
    def get_workdate_list(self):
        sqls = '''select cdate from work_day t
            where
            t.cdate >=
            (select max(cdate) from work_day t where t.work = 'Y' and t.cdate <
            (select min(cdate) from work_day t where t.work ='Y' and t.cdate >= 'FILEDATE'))
            and t.cdate < (select min(cdate) from work_day t where t.work ='Y' and t.cdate >= 'FILEDATE')'''
        today_sql = sqls.replace('FILEDATE',self.m_date)
        cursors_result = self.__cursors.execute(today_sql)
        rows = cursors_result.fetchall()
        return rows

    def recover_yljg_check(self,work_list):
        sqls = "delete from yljg_check t where t.check_date ='HOLD_DATE' "
        for lines in work_list:
            self.__cursors.execute(sqls.replace('HOLD_DATE',lines[0]))
        self.__connects.commit()

    def parse_file_into_db(self):
        logging.debug('准备打开乐刷对帐文件[%s]并准备入库' % (self.__check_file_name,))
        file_data = open(self.__check_file_name, 'r')
        count = 0
        for line in file_data:
            sub_line = miltple_replace(line,' ')
            olist = sub_line.split(' ')
            if len(olist) != 18:
                if len(olist) == 1:
                    continue
                #只在前两行允许未填充满的情况
                if count > 1:
                    return returns.return_notretry
                continue
            trans_type = change_chinese_channel_2_english(olist[9])
            pay_type = change_chinese_channel_2_english(olist[10])
            discount_type = change_chinese_channel_2_english(olist[12])
            card_type = change_chinese_channel_2_english(olist[11])
            check_date = olist[1][0:4] + olist[1][5:7] + olist[1][8:10]
            bind_val ={'serial_no':olist[0],'transmsn_date_time':olist[1]+' '+olist[2],'order_no':olist[3],'merch_id':olist[4],
                'fee_rate':olist[5],'trans_amt':olist[6],'fee_amt':olist[7],'sett_amt':olist[8],'trans_type':trans_type,
                'pay_type':pay_type,'card_type':card_type,
                'discount_type':discount_type,'retrivl_ref_num':olist[13],'batch_id':olist[14][0:6],
                'sys_trace_audit_num':olist[14][6:12],'card_id':olist[15],'terminal_id':olist[16],
                'check_date':check_date}
            #logging.info(bind_val)
            self.__cursors.execute(self.__insert_db_sql,bind_val)
            count = count + 1
        logging.info('共计录入%d条乐刷交易记录' % (count,))
        file_data.close()
        self.__connects.commit()
        return returns.return_sucess

    def __init_todays_db(self,sett_date):
        #初始化日中进展
        sqls_init_dayendprogress = '''delete from dayendprogress t where t.workdate = 'NOWDATE' and t.branch_code = 'Leshua' '''
        sqls_insert_dayendprogress = '''insert into dayendprogress(workdate,itemcode,branch_code,status,update_by,update_time,insert_time,remarks)
            values( 'NOWDATE', '101','Leshua','09', 'ls_admin', NULL,sysdate,NULL)'''
        self.__cursors.execute(sqls_init_dayendprogress.replace('NOWDATE',sett_date))
        self.__cursors.execute(sqls_insert_dayendprogress.replace('NOWDATE',sett_date))
        self.__connects.commit()

