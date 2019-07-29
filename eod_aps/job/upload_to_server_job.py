# -*- coding: utf-8 -*-
# 上传文件至远程服务器
import os
import threading
import tarfile
import traceback

from eod_aps.job import *


# 生成压缩文件，用于上传
def __zip_local_file(file_path, file_title):
    today_filter_str = date_utils.get_today_str('%Y-%m-%d')

    tar_file_name = '%s_%s.tar.gz' % (file_title, today_filter_str)
    tar = tarfile.open(os.path.join(file_path, tar_file_name), "w:gz")
    for rt, dirs, files in os.walk(file_path):
        for file_name in files:
            full_path = os.path.join(rt, file_name)
            tar.add(full_path, arcname=file_name)
    tar.close()


# 上传服务器行情文件
def __upload_file_to_server(server_model, source_file_path, target_file_path):
    try:
        today_filter_str = date_utils.get_today_str('%Y-%m-%d')
        files = os.listdir(source_file_path)
        for f in files:
            if (today_filter_str in f) and ('tar.gz' in f):
                tar_file_name = f
                break
        download_flag = server_model.upload_file(source_file_path + '/' + tar_file_name,
                                                 target_file_path + '/' + tar_file_name.decode('gb2312'))
        if download_flag:
            unzip_server_market_file(server_model, target_file_path, tar_file_name)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__upload_file_to_server:%s.' % server_model.name, error_msg)


# 服务器下解压文件
def unzip_server_market_file(server_model, tar_file_path, tar_file_name):
    unzip_cmd_list = ['cd %s' % tar_file_path,
                      'tar -zxf %s' % tar_file_name,
                      'rm -rf %s' % tar_file_name
                      ]
    server_model.run_cmd_str(';'.join(unzip_cmd_list))


# 上传行情文件
def upload_market_file_job(server_name_tuple):
    __zip_local_file(DATAFETCHER_MESSAGEFILE_FOLDER, 'market')

    threads = []
    for server_name in server_name_tuple:
        server_model = server_constant.get_server_model(server_name)
        datafetcher_file_folder = server_model.server_path_dict['datafetcher_messagefile']
        t = threading.Thread(target=__upload_file_to_server,
                             args=(server_model, DATAFETCHER_MESSAGEFILE_FOLDER, datafetcher_file_folder))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def upload_instrument_file_job(server_name_tuple):
    __zip_local_file(UPDATE_PRICE_PICKLE, 'INSTRUMENT')

    threads = []
    for server_name in server_name_tuple:
        server_model = server_constant.get_server_model(server_name)
        datafetcher_file_folder = server_model.server_path_dict['datafetcher_messagefile']
        t = threading.Thread(target=__upload_file_to_server,
                             args=(server_model, UPDATE_PRICE_PICKLE, datafetcher_file_folder))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


# 上传etf文件
def upload_etf_file_job(server_name_tuple):
    __zip_local_file(ETF_FILE_PATH, 'etf')

    threads = []
    for server_name in server_name_tuple:
        server_model = server_constant.get_server_model(server_name)
        etf_file_server_path = server_model.server_path_dict['etf_upload_folder']

        remove_cmd_list = ['cd %s' % etf_file_server_path,
                           'rm -rf ../ETF/*'
                           ]
        server_model.run_cmd_str(';'.join(remove_cmd_list))

        t = threading.Thread(target=__upload_file_to_server,
                             args=(server_model, ETF_FILE_PATH, etf_file_server_path))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


# 上传mktdtCenter的配置文件至服务器
def upload_mkt_cfg_file_job(server_name_tuple):
    for server_name in server_name_tuple:
        server_model = server_constant.get_server_model(server_name)
        source_file_folder = MKTDTCTR_CFG_FOLDER
        target_file_folder = '%s/cfg' % server_model.server_path_dict['tradeplat_project_folder']

        if server_name == 'huabao':
            upload_filename_list = ['rb7_instruments.csv', 'rb8_instruments.csv', 'rb9_instruments.csv',
                                    'fh7_instruments.csv', 'mg1_pre_bind_map_file.csv']
        elif server_name == 'guoxin':
            upload_filename_list = ['rb3_instruments.csv', 'rb4_instruments.csv', 'rb5_instruments.csv',
                                    'rb6_instruments.csv', 'mg1_pre_bind_map_file.csv']
        else:
            continue

        for f in upload_filename_list:
            server_model.upload_file(source_file_folder + '/' + f, target_file_folder + '/' + f.decode('gb2312'))  # 上传
            custom_log.log_info_job('Upload file:%s, to:%s success' % (f, server_model.ip))


# 上传行情文件
def upload_ctp_market_file_job(server_name_tuple):
    ctp_market_file_list = []
    for file_name in os.listdir(CTP_DATA_BACKUP_PATH):
        if 'Market' in file_name:
            ctp_market_file_list.append(file_name)
        ctp_market_file_list.sort()
    ctp_file_name = ctp_market_file_list[len(ctp_market_file_list) - 1]

    for server_name in server_name_tuple:
        server_model = server_constant.get_server_model(server_name)
        source_file_folder = CTP_DATA_BACKUP_PATH
        target_file_folder = server_model.server_path_dict['datafetcher_marketfile']
        server_model.upload_file(source_file_folder + '/' + ctp_file_name,
                                 target_file_folder + '/' + ctp_file_name.decode('gb2312'))


if __name__ == '__main__':
    # upload_market_file_job(('huabao', 'guoxin', 'zhongxin'))
    # upload_etf_file_job(('guoxin',))
    __zip_local_file(ETF_FILE_PATH, 'etf')
