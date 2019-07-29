# -*- coding: utf-8 -*-
# 新增下一个交易日pf_position表数据
import threading
import traceback

from eod_aps.job import *


def __pf_position_rebuild(server_name, filter_date_str):
    try:
        server_model = server_constant.get_server_model(server_name)
        if filter_date_str is None or filter_date_str == '':
            update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                               '/home/trader/anaconda2/bin/python pf_position_rebuild.py'
            ]
        else:
            update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                               '/home/trader/anaconda2/bin/python pf_position_rebuild.py -d %s' % filter_date_str
            ]
        server_model.run_cmd_str(';'.join(update_cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__pf_position_rebuild:%s.' % server_name, error_msg)


def pf_position_rebuild_job(server_name_tuple, filter_date_str=None):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__pf_position_rebuild, args=(server_name, filter_date_str))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    pf_position_rebuild_job(('zhongxin', ))
