# -*- coding: utf-8 -*-
# 上传文件至托管服务器FTP
import os
import tarfile
import threading
import traceback
from xmlrpclib import ServerProxy
from eod_aps.job import *

thread_result_dict = dict()
dict_upload_key = 'upload_flag'


def __upload_price_check_file(server_model, upload_folder):
    price_check_file_name = 'price_check_%s.csv' % date_utils.get_today_str('%Y-%m-%d')
    local_file_path = '%s/%s' % (PRICE_FILES_BACKUP_FOLDER, date_utils.get_today_str('%Y%m%d'))

    source_file_path = '%s/%s' % (local_file_path, price_check_file_name)
    target_file_path = '%s/%s' % (upload_folder, price_check_file_name)
    upload_flag = server_model.upload_file(source_file_path, target_file_path)
    if upload_flag:
        custom_log.log_info_job('upload:%s Success.' % source_file_path)
    else:
        custom_log.log_info_job('upload:%s Fail!' % source_file_path)


def __upload_fair_price_file(ftp_server, upload_folder):
    fair_price_file_name = 'fair_price_%s.csv' % date_utils.get_today_str('%Y-%m-%d')
    local_file_path = '%s/%s' % (PRICE_FILES_BACKUP_FOLDER, date_utils.get_today_str('%Y%m%d'))

    source_file_path = '%s/%s' % (local_file_path, fair_price_file_name)
    target_file_path = '%s/%s' % (upload_folder, fair_price_file_name)
    upload_flag = ftp_server.upload_file(source_file_path, target_file_path)
    if upload_flag:
        custom_log.log_info_job('upload:%s Success.' % source_file_path)
    else:
        custom_log.log_info_job('upload:%s Fail!' % source_file_path)


def __upload_daily_file(ftp_server, upload_folder):
    daily_file_name = 'market_%s.tar.gz' % date_utils.get_today_str('%Y-%m-%d')
    local_file_path = '%s/%s' % (PRICE_FILES_BACKUP_FOLDER, date_utils.get_today_str('%Y%m%d'))

    source_file_path = '%s/%s' % (local_file_path, daily_file_name)
    target_file_path = '%s/%s' % (upload_folder, daily_file_name)
    upload_flag = ftp_server.upload_file(source_file_path, target_file_path)
    if upload_flag:
        custom_log.log_info_job('upload:%s Success.' % source_file_path)
    else:
        custom_log.log_info_job('upload:%s Fail!' % source_file_path)


def __upload_etf_file(ftp_server, upload_folder):
    etf_file_name = 'etf_%s.tar.gz' % date_utils.get_today_str('%Y-%m-%d')
    local_file_path = '%s/%s' % (ETF_FILE_BACKUP_FOLDER, date_utils.get_today_str('%Y%m%d'))

    source_file_path = '%s/%s' % (local_file_path, etf_file_name)
    target_file_path = '%s/%s' % (upload_folder, etf_file_name)
    upload_flag = ftp_server.upload_file(source_file_path, target_file_path)
    if upload_flag:
        custom_log.log_info_job('upload:%s Success.' % source_file_path)
    else:
        custom_log.log_info_job('upload:%s Fail!' % source_file_path)


def __upload_tradeplat_file(server_model, upload_folder):
    tradeplat_file_name = 'tradeplat_%s.tar.gz' % date_utils.get_today_str('%Y%m%d')
    tradeplat_local_path = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_model.name
    source_file_path = '%s/%s' % (tradeplat_local_path, tradeplat_file_name)
    target_file_path = '%s/%s' % (upload_folder, tradeplat_file_name)
    if not os.path.exists(source_file_path):
        return
    upload_flag = server_model.upload_file(source_file_path, target_file_path)
    if upload_flag:
        custom_log.log_info_job('Upload:%s Success.' % source_file_path)
    else:
        custom_log.log_info_job('Upload:%s Fail!' % source_file_path)


def __upload_volume_profile(server_model, upload_folder):
    volume_profile_name = 'volume_profile_%s.tar.gz' % date_utils.get_today_str('%Y%m%d')
    source_file_path = '%s/%s' % (VOLUME_PROFILE_FOLDER, volume_profile_name)
    target_file_path = '%s/%s' % (upload_folder, volume_profile_name)
    upload_flag = server_model.upload_file(source_file_path, target_file_path)
    if upload_flag:
        custom_log.log_info_job('Upload:%s Success.' % source_file_path)
    else:
        custom_log.log_info_job('Upload:%s Fail!' % source_file_path)


# 生成压缩文件，用于上传
def __zip_local_file(file_path, tar_file_name):
    tar = tarfile.open(os.path.join(file_path, tar_file_name), "w:gz")
    for root, dir_str, files in os.walk(os.path.join(file_path, 'cfg')):
        root_ = os.path.relpath(root, start=file_path)
        for file_name in files:
            full_path = os.path.join(root, file_name)
            tar.add(full_path, arcname=os.path.join(root_, file_name))

    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    for root, dir_str, files in os.walk(os.path.join(file_path, 'update_sql')):
        root_ = os.path.relpath(root, start=file_path)
        for file_name in files:
            # 只上传当天的文件
            if filter_date_str not in file_name:
                continue
            full_path = os.path.join(root, file_name)
            tar.add(full_path, arcname=os.path.join(root_, file_name))
    tar.close()


def upload_deposit_server_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__upload_deposit_ftp_job, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def __upload_deposit_ftp_job(server_name):
    try:
        thread_result_dict['%s|%s' % (dict_upload_key, server_name)] = False
        server_model = server_constant.get_server_model(server_name)
        upload_ftp_file_path = '%s/%s' % \
            (server_model.ftp_upload_folder, date_utils.get_today_str('%Y%m%d'))

        if not server_model.is_exist(upload_ftp_file_path):
            server_model.mkdir(upload_ftp_file_path)

        __upload_price_check_file(server_model, upload_ftp_file_path)
        __upload_tradeplat_file(server_model, upload_ftp_file_path)
        __upload_volume_profile(server_model, upload_ftp_file_path)
        thread_result_dict['%s|%s' % (dict_upload_key, server_name)] = True
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__upload_deposit_ftp_job:%s.' % server_name, error_msg)


def upload_deposit_server_pm_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__upload_deposit_ftp_pm_job, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(120)

    for (dict_key, dict_value) in thread_result_dict.items():
        if not dict_value:
            email_utils2.send_email_group_all('[Error]upload_deposit_server_job, key:%s' % dict_key, '')


def __upload_deposit_ftp_pm_job(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        upload_ftp_file_path = '%s/%s' % \
            (server_model.ftp_upload_folder, date_utils.get_today_str('%Y%m%d'))

        if not server_model.is_exist(upload_ftp_file_path):
            server_model.mkdir(upload_ftp_file_path)
        __upload_daily_file(server_model, upload_ftp_file_path)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__upload_deposit_ftp_pm_job:%s.' % server_name, error_msg)


if __name__ == '__main__':
    __upload_deposit_ftp_job('citics')
