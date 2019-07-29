# -*- coding: utf-8 -*-
# 检查account_trade_restrictions表是否需要新增数据
import threading
import traceback

from eod_aps.job import *


def __account_trade_restrictions_update(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                           '/home/trader/anaconda2/bin/python update_account_trade_restrictions.py'
                           ]
        server_model.run_cmd_str(';'.join(update_cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__account_trade_restrictions_update:%s.' % server_name, error_msg)


def account_trade_restrictions_update_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__account_trade_restrictions_update, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    account_trade_restrictions_update_job(['guoxin', ])
