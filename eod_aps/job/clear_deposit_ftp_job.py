# -*- coding: utf-8 -*-
# 删除托管服务器FTP文件
import threading
from eod_aps.job import *
import traceback


def __clear_deposit_ftp(server_name):
    check_date_str = date_utils.get_interval_trading_day(-7, format_str='%Y%m%d')
    server_model = server_constant.get_server_model(server_name)
    for folder_path in (server_model.ftp_upload_folder, server_model.ftp_download_folder):
        for date_folder_name in server_model.listdir(folder_path):
            if date_folder_name > check_date_str:
                continue

            check_folder_path = '%s/%s' % (folder_path, date_folder_name)
            __clear_ftp_folder(server_model, check_folder_path)


def __clear_ftp_folder(server_model, folder_path):
    try:
        if server_model.is_exist(folder_path):
            rmdir_flag = server_model.clear(folder_path)
            if rmdir_flag:
                custom_log.log_info_job('remove:%s Success.' % folder_path)
            else:
                custom_log.log_info_job('remove:%s Fail!' % folder_path)
    except:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__clear_ftp_folder:%s.' % server_model.name, error_msg)


# FTP服务器文件清理
def clear_deposit_ftp_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__clear_deposit_ftp, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    # deposit_servers = server_constant.get_deposit_servers()
    # __clear_deposit_ftp('citics_test')
    __clear_deposit_ftp('zhongtai')

