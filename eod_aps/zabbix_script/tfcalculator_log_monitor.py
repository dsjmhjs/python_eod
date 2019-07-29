#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from eod_aps.job.tradeplat_init_index_job import TFCalculatorInit

from eod_aps.zabbix_script import *
from eod_aps.tools.date_utils import DateUtils


def zabbix_log_monitor(server_list):
    if not DateUtils().is_trading_time():
        return 0
    subject = '[Zabbix_Problem] TFCalculator Error'
    context = []
    for server_name in server_list:
        tf = TFCalculatorInit(server_name, [], [])
        rst = tf.log_monitor()
        if rst:
            context.append('<li>%s:</li>' % server_name)
            context.extend(rst)

            print 1
        else:
            print 0
    if context:
        email_utils2.send_email_group_all(subject, '<br>'.join(context), 'html')


if __name__ == '__main__':
    server_list = ['huabao', ]
    zabbix_log_monitor(server_list)
