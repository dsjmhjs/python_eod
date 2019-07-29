#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import threading
import tarfile
import os
import shutil
import traceback

from eod_aps.model.schema_common import Instrument
from eod_aps.job.download_server_file_job import download_etf_file_job
from eod_aps.job.download_web_etf_job import download_etf_web_job
from eod_aps.job import *

SERVER_PYTHON_FOLDER = '%s/eod_aps/server_python' % EOD_PROJECT_FOLDER


def __validate_instrument_log(server_model):
    filter_date_str = date_utils.get_today_str('%Y%m%d')
    log_base_path = '%s/log' % server_model.server_path_dict['datafetcher_project_folder']
    cmd_list = ['cd %s' % log_base_path,
                'ls *fetch_instrument_%s*.log' % filter_date_str
                ]
    cmd_return_str = server_model.run_cmd_str(';'.join(cmd_list))
    log_file_list = []
    for log_file_name in cmd_return_str.split('\n'):
        if len(log_file_name) > 0:
            log_file_list.append(log_file_name)
    log_file_list.sort()

    if len(log_file_list) == 0:
        return

    log_file_name = log_file_list[-1]
    cmd_list = ['cd %s' % log_base_path,
                'tail -100 %s' % log_file_name
                ]
    cmd_return_str = server_model.run_cmd_str(';'.join(cmd_list))
    if 'Login fail' in cmd_return_str:
        email_content = "<div style='background-color: #ee4c50'>%s</div>" % cmd_return_str.replace('\n', '<br>')
        email_utils2.send_email_group_all('[Error]fetch_instrument has error!', email_content, 'html')


# def __build_instrument_files(server_name):
#     server_model = server_constant.get_server_model(server_name)
#     if server_model.data_source_type != '':
#         instrument_cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_project_folder'],
#                                './build64_release/fetcher/fetch_instrument -a %s' % server_model.data_source_type
#                                ]
#         server_model.run_cmd_str(';'.join(instrument_cmd_list))
#         __validate_instrument_log(server_model)
#         __backup_files(server_model)


def get_instrument_files(server_name):
    server_model = server_constant.get_server_model(server_name)
    if server_model.data_source_type != '':
        instrument_cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_project_folder'],
                               './build64_release/fetcher/fetch_instrument -a %s' % server_model.data_source_type
                               ]
        server_model.run_cmd_str(';'.join(instrument_cmd_list))
        __validate_instrument_log(server_model)
        __backup_files(server_model)


def __backup_files(server_model):
    date_str_1 = date_utils.get_today_str('%Y-%m-%d')
    date_str_2 = date_utils.get_today_str('%Y%m%d%H%M')
    backup_file_path = '%s/%s' % (server_model.server_path_dict['datafetcher_messagefile_backup'], date_str_2)
    backup_cmd_list = ['mkdir %s' % backup_file_path,
                       'cd %s' % server_model.server_path_dict['datafetcher_messagefile'],
                       'cp *%s* %s' % (date_str_1, backup_file_path)
                       ]
    server_model.run_cmd_str(';'.join(backup_cmd_list))


def __tar_market_file(server_model, tar_file_name, date_filter_str):
    cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_messagefile'],
                'rm *tar.gz',
                'tar -zcf %s --exclude=*POSITION* *%s*' % (tar_file_name, date_filter_str)
                ]
    server_model.run_cmd_str(';'.join(cmd_list))


def __unzip_tar_file(tar_file_path):
    tar_folder_path = os.path.dirname(tar_file_path)
    tar = tarfile.open(tar_file_path)
    names = tar.getnames()
    for name in names:
        tar.extract(name, path=tar_folder_path)
    tar.close()

    # 解压后删除源文件
    os.remove(tar_file_path)


def __zip_local_file(file_path, file_title):
    today_filter_str = date_utils.get_today_str('%Y-%m-%d')

    tar_file_name = '%s_%s.tar.gz' % (file_title, today_filter_str)
    tar = tarfile.open(os.path.join(file_path, tar_file_name), "w:gz")
    for rt, dirs, files in os.walk(file_path):
        for file_name in files:
            full_path = os.path.join(rt, file_name)
            tar.add(full_path, arcname=file_name)
    tar.close()


def unzip_market_file(folder_path, date_filter_str=None):
    tar_file_list = []
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')
    for rt, dirs, files in os.walk(folder_path):
        for file_name in files:
            if ('tar.gz' in file_name) and (date_filter_str in file_name):
                tar_file_list.append(file_name)
                if len(file_name.split('_')) < 3:
                    continue
                server_name = file_name.split('_')[2]
                tar = tarfile.open(folder_path + '/' + file_name)
                names = tar.getnames()
                for name in names:
                    if (server_name == 'huabao') and (('HUABAO' in name) or ('Femas' in name)):
                        tar.extract(name, path=folder_path)
                    elif server_name == 'nanhua' and ('CTP' in name):
                        tar.extract(name, path=folder_path)
                    elif server_name not in ('huabao', 'nanhua'):
                        tar.extract(name, path=folder_path)
                tar.close()

    for tar_file_name in tar_file_list:
        os.remove('%s/%s' % (folder_path, tar_file_name))


def download_market_file(server_name, date_filter_str=None):
    try:
        if date_filter_str is None:
            date_filter_str = date_utils.get_today_str('%Y-%m-%d')
        tar_file_name = 'all_market_%s_%s.tar.gz' % (server_name, date_filter_str)

        server_model = server_constant.get_server_model(server_name)
        server_file_path = '%s/%s' % (server_model.server_path_dict['datafetcher_messagefile'], tar_file_name)
        __tar_market_file(server_model, tar_file_name, date_filter_str)

        local_file_path = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, tar_file_name.decode('gb2312'))
        server_model.download_file(server_file_path, local_file_path)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]download_market_file:%s.' % server_name, error_msg)


def download_market_file_job(server_name, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')

    # if os.path.exists(DATAFETCHER_MESSAGEFILE_FOLDER):
    #     shutil.rmtree(DATAFETCHER_MESSAGEFILE_FOLDER)
    # os.mkdir(DATAFETCHER_MESSAGEFILE_FOLDER)
    if os.path.exists(DATAFETCHER_MESSAGEFILE_FOLDER):
        for file_name in os.listdir(DATAFETCHER_MESSAGEFILE_FOLDER):
            if date_filter_str not in file_name:
                os.remove(os.path.join(DATAFETCHER_MESSAGEFILE_FOLDER, file_name))
    download_market_file(server_name, date_filter_str)
    unzip_market_file(DATAFETCHER_MESSAGEFILE_FOLDER, date_filter_str)


def add_future_instrument_local():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python ctp_price_analysis_add.py')
    market_add_message = output.read()
    custom_log.log_info_job(market_add_message)


def add_stock_instrument_local():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python Lts_price_analysis_add.py')
    market_add_message = output.read()
    custom_log.log_info_job(market_add_message)


def update_stock_instrument_local():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python Lts_price_analysis.py')
    custom_log.log_info_job(output.read())


def update_future_instrument_local():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python ctp_price_analysis.py')
    custom_log.log_info_job(output.read())


def update_etf_instrument_local():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python update_by_etf_file.py')
    custom_log.log_info_job(output.read())


def update_future_instrument_job():
    ctp_market_server = server_constant.get_future_market_server()
    get_instrument_files(ctp_market_server)
    download_market_file_job(ctp_market_server)

    add_future_instrument_local()
    update_future_instrument_local()


def update_stock_instrument_job():
    stock_market_server = server_constant.get_stock_market_server()
    get_instrument_files(stock_market_server)
    download_market_file_job(stock_market_server)
    # unzip_market_file(DATAFETCHER_MESSAGEFILE_FOLDER, )
    __zip_local_file(DATAFETCHER_MESSAGEFILE_FOLDER, 'market')
    add_stock_instrument_local()
    update_stock_instrument_local()


def update_etf_instrument_job():
    etf_base_server = server_constant.get_etf_base_server()
    download_etf_file_job(etf_base_server)
    download_etf_web_job()
    update_etf_instrument_local()


if __name__ == '__main__':
    # ctp_market_server = server_constant.get_future_market_server()
    # download_market_file_job([ctp_market_server, ])
    update_future_instrument_job()
    # update_stock_instrument_job()
