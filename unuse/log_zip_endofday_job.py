# -*- coding: utf-8 -*-
# 服务器磁盘管理，打包下载每日生成的Log(未使用)

import os
import threading
import paramiko
import datetime
from eod_aps.job import *

log_download_base_path = 'Z:/temp/dailyjob/trading_log/'


def __zip_server_file(server_model, base_file_folder, date_str):
    validate_date_filter_str_1 = date_str
    validate_date_filter_str_2 = date_str.replace('-', '')
    special_date_format = validate_date_filter_str_2[0:4] + 'a' + validate_date_filter_str_2[4:6] + 'a' + validate_date_filter_str_2[6:]

    folder_file_list = server_model.list_dir(base_file_folder[1])
    date_filter_list = []
    file_exists_flag = False
    for file_name in folder_file_list:
        if '.tar.gz' not in file_name:
            if validate_date_filter_str_1 in file_name:
                date_filter_list.append('*%s*' % validate_date_filter_str_1)
                file_exists_flag = True
            elif validate_date_filter_str_2 in file_name:
                date_filter_list.append('*%s*' % validate_date_filter_str_2)
                file_exists_flag = True

    if not file_exists_flag:
        return

    time_str = datetime.datetime.strftime(datetime.datetime.now(), "%m-%d-%Y-%H%M%S")
    tar_file_name = 'log_%s_%s_%s_%s.tar.gz' % (server_model.name, base_file_folder[0], special_date_format, time_str)
    tar_cmd = 'tar -czf %s %s' % (tar_file_name, ' '.join(date_filter_list))
    log_cmd1 = 'cd %s;%s' % (base_file_folder[1], tar_cmd)
    server_model.run_cmd_str(log_cmd1)

    download_path = log_download_base_path + server_model.name
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    include_key_list = [special_date_format, 'tar.gz']
    download_flag = server_model.download_folder(base_file_folder[1], download_path, include_key_list=include_key_list)
    if download_flag:
        del_tar_cmd = 'rm -rf *%s*.log *%s*.log' % (validate_date_filter_str_1, validate_date_filter_str_2)
        log_cmd2 = 'cd %s;%s' % (base_file_folder[1], del_tar_cmd)
        server_model.run_cmd_str(log_cmd2)


def get_name(folder_list):
    folder_list_with_name = []
    for folder in folder_list:
        name_temp = folder.split('/')[-2:]
        name = '_'.join(name_temp)
        folder_list_with_name.append([name, folder])
    return folder_list_with_name

# date 格式'%Y-%m-%d'
def __log_zip_endofday(server_name, date_str=None):
    server_model = server_constant.get_server_model(server_name)
    folder_to_zip = server_model.log_zip_folder_list
    folder_to_zip = get_name(folder_to_zip)

    for folder in folder_to_zip:
        task_logger.info('start tar--server:%s, date:%s, folder:%s' % (server_model.name, date_str, folder))
        __zip_server_file(server_model, folder, date_str)
        

def log_zip_endofday_job(server_name_tuple, date_str=None):
    if date_str is None:
        date_str = date_utils.get_today_str('%Y-%m-%d')
    date_str_last_trading_day = date_utils.get_last_trading_day( '%Y-%m-%d', date_str)
    inter_date_list = date_utils.get_between_day_list(datetime.datetime.strptime(date_str_last_trading_day, '%Y-%m-%d'),\
                                                      datetime.datetime.strptime(date_str, '%Y-%m-%d'))
    inter_date_str_list = []
    for date in inter_date_list:
        inter_date_str_list.append(datetime.datetime.strftime(date, '%Y-%m-%d'))

    for inter_date_str in inter_date_str_list:
        threads = []
        for server_name in server_name_tuple:
            t = threading.Thread(target=__log_zip_endofday, args=(server_name, inter_date_str))
            threads.append(t)

        # 启动所有线程
        for t in threads:
            t.start()

        # 主线程中等待所有子线程退出
        for t in threads:
            t.join()


if __name__ == '__main__':
    log_zip_endofday_job(('local118', ), )
