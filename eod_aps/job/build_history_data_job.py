# -*- coding: utf-8 -*-
# 生成历史数据，用于策略加载
import threading
import traceback

from eod_aps.job import *


def __build_history_data(server_name, ctp_file_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        if ctp_file_name is None:
            update_python_cmd = 'python build_history_data.py'
        else:
            update_python_cmd = 'python build_history_data.py ' + ctp_file_name

        update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                           update_python_cmd
                           ]
        server_model.run_cmd_str(';'.join(update_cmd_list))
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__build_history_data:%s.' % server_name, error_msg)


def build_history_data_job(server_name_tuple, ctp_file_name=None):
    if len(server_name_tuple) == 0:
        return

    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__build_history_data, args=(server_name, ctp_file_name))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    build_history_data_job(('nanhua',))
