# -*- coding: utf-8 -*-
# 压缩各服务器上的log日志文件
import threading
import traceback

from eod_aps.job import *


def __log_zip_file(server_name, date_filter_str):
    try:
        date_filter_str2 = date_filter_str.replace('_', '')
        server_model = server_constant.get_server_model(server_name)

        tar_file_name = 'log_%s.tar.gz' % date_filter_str2

        # 判断该log文件是否已经生成过
        log_file_list = server_model.list_dir(server_model.server_path_dict['tradeplat_log_folder'])
        for log_file_name in log_file_list:
            if log_file_name == tar_file_name:
                return

        cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                    'tar -czf %s *%s*.log *%s*.log' % (tar_file_name, date_filter_str2, date_filter_str),
                    'rm -rf *%s*.log *%s*.log' % (date_filter_str2, date_filter_str)
        ]
        server_model.run_cmd_str(';'.join(cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__log_zip_file:%s.' % server_name, error_msg)


def __log_unzip_file(server_name, date_filter_str):
    server_model = server_constant.get_server_model(server_name)
    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                'tar -zxf log_%s.tar.gz' % date_filter_str.replace('_', '')
    ]
    server_model.run_cmd_str(';'.join(cmd_list))


def zip_log_file_job(server_name_tuple, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_last_trading_day('%Y-%m-%d')

    date_filter_str = date_filter_str.replace('-', '_')
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__log_zip_file, args=(server_name, date_filter_str))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def tar_trade_file_log(server_name, date_filter_str):
    server_model = server_constant.get_server_model(server_name)
    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                'tar -czf log_%s.tar.gz *%s*.log' % (date_filter_str, date_filter_str),
                'rm -rf *%s*.log' % date_filter_str
    ]
    server_model.run_cmd_str(';'.join(cmd_list))


if __name__ == '__main__':
    trading_day_list = date_utils.get_trading_day_list(date_utils.string_toDatetime('2017-01-11'),
                                                       date_utils.string_toDatetime('2017-02-16'))
    for trading_day in trading_day_list:
        tar_trade_file_log('nanhua', trading_day.strftime("%Y%m%d"))
