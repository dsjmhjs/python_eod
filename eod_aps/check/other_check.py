# -*- coding: utf-8 -*-
import os
from eod_aps.check import *
from eod_aps.model.server_constans import server_constant, SpecialTickers
from eod_aps.job import PRICE_FILES_BACKUP_FOLDER,VOLUME_PROFILE_FOLDER

def msci_file_check(job_name):
    check_file_list = ['SMD_CNE5_LOCALID_ID_%s.zip', 'SMD_CNE5S_100_%s.zip']
    ys_date = date_utils.get_last_trading_day("%Y%m%d")

    server_model = server_constant.get_server_model('host')
    msci_data_path = server_model.server_path_dict['msci_data_path'] % ys_date
    base_check_folder = '%s/bime' % msci_data_path

    error_file_list = []
    for filename in check_file_list:
        file_path = os.path.join(base_check_folder, filename % ys_date[2:])
        if not os.path.exists(file_path):
            error_file_list.append(filename % ys_date[2:])
    if len(error_file_list):
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, '</br>'.join(error_file_list), 'html')


def factordata_file_check(job_name):
    check_folder = '%s/product' % const.EOD_CONFIG_DICT['Strategy_IntradaySignal_Folder']

    ticker_list = [x for x in os.listdir(check_folder)]

    if len(ticker_list) <= 100:
        email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, 'Check Path:%s!' % check_folder)
    else:
        check_ticker = ticker_list[0]
        check_ticker_folder = os.path.join(check_folder, check_ticker)
        modify_date = date_utils.timestamp_tostring(os.stat(check_ticker_folder).st_mtime, '%Y%m%d')
        now_date = date_utils.get_today_str('%Y%m%d')
        if modify_date != now_date:
            email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name,
                                              'Modify Time Error.Path:%s!' % check_ticker_folder)

def volume_profile_upload_check(job_name):
    filter_date_str = date_utils.get_today_str('%Y%m%d')
    tar_file_name = 'volume_profile_%s.tar.gz' % filter_date_str
    tar_file_path = os.path.join(VOLUME_PROFILE_FOLDER, tar_file_name)
    if not os.path.exists(tar_file_path):
        email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name,
                                          'Volume Profile Missing.Path:%s!' % tar_file_path)

    mkt_center_servers = server_constant.get_mktcenter_servers()
    for server_name in mkt_center_servers:
        server_model = server_constant.get_server_model(server_name)
        if server_model.type != 'trade_server':
            continue
        check_cmd_str = 'ls %s/volume_profile_datas' % server_model.server_path_dict['tradeplat_project_folder']
        result_str = server_model.run_cmd_str(check_cmd_str)
        if filter_date_str not in result_str:
            email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name,
                                              'File Upload Error.Server:%s!' % server_name)


def special_tickers_init_check(job_name):
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    special_ticker_list = []
    for special_ticker in session_jobs.query(SpecialTickers).filter(SpecialTickers.date == filter_date_str):
        special_ticker_list.append(special_ticker)

    suspend_stock_list = [x.ticker for x in special_ticker_list if 'Suspend' in x.describe]
    st_stock_list = [x.ticker for x in special_ticker_list if 'ST' in x.describe]
    if len(suspend_stock_list) == 0 or len(st_stock_list) == 0:
        email_content = 'Suspend Stock Size:%s, ST Stock Size:%s' % (len(suspend_stock_list), len(st_stock_list))
        email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, email_content)


def clear_deposit_ftp_check(job_name):
    day_list = date_utils.get_last_week_days()
    server_model = server_constant.get_server_model('host')
    for day_str in day_list:
        check_folder1 = '%s%s' % (server_model.ftp_upload_folder, day_str)
        check_folder2 = '%s%s' % (server_model.ftp_download_folder, day_str)

        for check_folder in (check_folder1, check_folder2):
            if server_model.is_exist(check_folder):
                email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name,
                                                  'Clear Error.Path:%s Exist!' % check_folder)


