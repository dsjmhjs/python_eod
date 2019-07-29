# -*- coding: utf-8 -*-
# 行情中心日志校验，查看是否正确接收各市场的数据
from eod_aps.job import *


def mkt_center_log_check_job(server_name):
    server_model = server_constant.get_server_model(server_name)
    log_files_path = server_model.server_path_dict['tradeplat_log_folder']

    read_cmd_list = ['cd %s' % log_files_path,
                     'ls *screenlog_MktDTCenter*.log'
                     ]
    log_file_info = server_model.run_cmd_str(';'.join(read_cmd_list))
    log_file_list = []
    for log_file_name in log_file_info.split('\n'):
        if len(log_file_name) > 0:
            log_file_list.append(log_file_name)
    log_file_list.sort()

    today_filter_str = date_utils.get_today_str('%Y%m%d')
    log_check_cmd_list = ['cd %s' % log_files_path,
                          "grep -n 'code date' %s" % log_file_list[-1]
                          ]
    log_file_result = server_model.run_cmd_str(';'.join(log_check_cmd_list))
    error_message = []
    if log_file_result == '':
        error_message.append('Can not find %s in log file!' % (today_filter_str,))
    else:
        if 'code date is invalid' in log_file_result:
            error_message.append('Market Error:%s' % log_file_result)

    if len(error_message) > 0:
        email_utils2.send_email_group_all('MktdtCtr Log Error_' + today_filter_str,
                                         'please check MktdtCtr server!\n' + '\n'.join(error_message))


if __name__ == '__main__':
    mkt_center_log_check_job('guoxin')
