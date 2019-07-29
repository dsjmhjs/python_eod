# -*- coding: utf-8 -*-
# 更新strategy_parameter表数据（目前只修改guoxin服务器上CancelTotalTimes值）
import threading
import traceback

from eod_aps.job import *


def __strategy_parameter_update(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                           '/home/trader/anaconda2/bin/python strategy_parameter_update.py'
        ]
        server_model.run_cmd_str(';'.join(update_cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__strategy_parameter_update:%s.' % server_name, error_msg)


def strategy_parameter_update_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__strategy_parameter_update, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    strategy_parameter_update_job(('guoxin',))
