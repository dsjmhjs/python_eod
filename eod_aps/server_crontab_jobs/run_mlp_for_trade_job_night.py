# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime
from ysquant.utility.email_func import mysender_send
from ysquant.utility.trading_day import TradingDay


def server_crontab_job(run_cmd, time_flag=False, email_flag=False):
    """
    :param run_cmd:
    :param time_flag: True表示只交易日执行
    :param email_flag: True表示正常执行也发送邮件通知
    """
    try:
        date_str = int(datetime.now().strftime('%Y%m%d'))
        if time_flag:
            td = TradingDay()
            if not td.is_trading_day(date_str):
                print 'Not Trading Day, Job:%s Skip!' % run_cmd
                return

        output = os.popen(run_cmd)
        run_message = output.read()
        if email_flag:
            mysender_send(subject="Job:%s_%s Run Log" % (os.path.basename(run_cmd), date_str),
                          content=run_message,
                          receiver_list=["guowei", ])
    except Exception:
        mysender_send(subject="[Error]Job:%s_%s" % (os.path.basename(run_cmd), date_str),
                      content=run_message,
                      receiver_list=["guowei", ])


if __name__ == '__main__':
    python_file_path = '/home/strategy/intraday_stock_dp_strategy/Boost/run_mlp_for_trade_night.py'
    parameter_str = sys.argv[1]
    run_cmd = '%s %s' % (python_file_path, parameter_str)

    time_flag = True
    email_flag = True
    server_crontab_job(run_cmd, time_flag, email_flag)
