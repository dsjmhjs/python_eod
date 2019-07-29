# -*- coding: utf-8 -*-
# 从远程服务器下载文件
import pickle
import tarfile
import threading
import subprocess
import shutil
import traceback
from eod_aps.model.obj_to_sql import to_many_sql
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.md5_check_utils import *
from eod_aps.job import *
import os

cwrsync_base_path = 'C:\\Program Files (x86)\\cwRsync\\bin'


def __remove_folder_tar_file(server_model, tar_file_path):
    cmd_list = ['cd %s' % tar_file_path,
                'rm *tar.gz'
                ]
    server_model.run_cmd_str(';'.join(cmd_list))


def __remove_server_tar_file(server_model, server_file_path):
    cmd_str = 'rm %s' % server_file_path
    server_model.run_cmd_str(cmd_str)


# 压缩服务器行情文件
def __tar_market_file(server_model, tar_file_name, date_filter_str):
    cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_messagefile'],
                'rm *tar.gz',
                'tar -zcf %s --exclude=*POSITION* *%s*' % (tar_file_name, date_filter_str)
                ]
    server_model.run_cmd_str(';'.join(cmd_list))


# 压缩服务器文件
def __tar_etf_file(server_model, tar_file_name):
    tar_str = 'tar -zcf %s/%s --exclude=*.tar.gz* *' % (
        server_model.server_path_dict['etf_upload_folder'], tar_file_name)
    cmd_list = ['cd %s' % server_model.etf_base_folder,
                'rm *tar.gz',
                tar_str
                ]
    server_model.run_cmd_str(';'.join(cmd_list))


# 压缩服务器ctp行情文件
def __tar_ctp_market_file(server_model, ctp_market_file_name):
    tar_cmd = 'tar -zcf %s.tar.gz %s' % (ctp_market_file_name, ctp_market_file_name)

    cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_marketfile'],
                'rm *tar.gz',
                tar_cmd
                ]
    server_model.run_cmd_str(';'.join(cmd_list))


def __tar_mktcenter_file(server_name, date_filter_str):
    try:
        server_model = server_constant.get_server_model(server_name)
        cmd_list = []
        for file_name_template in server_model.market_file_template.split(','):
            file_name = file_name_template % date_filter_str
            temp_cmd_list = ['cd %s' % server_model.server_path_dict['mktdtctr_data_folder'],
                             'tar -zcf %s.tar.gz %s' % (file_name, file_name)
                             ]
            cmd_list.append(';'.join(temp_cmd_list))
        server_model.run_cmd_str(';'.join(cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__tar_mktcenter_file:%s.' % server_name, error_msg)


# 压缩服务器mktcenter数据文件


# 压缩服务器TradePlat的日志文件并删除
def __tar_tradeplat_file(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        tar_file_name = 'tradeplat_log_%s.tar.gz' % date_utils.get_today_str('%Y%m%d%H%M%S')
        cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                    'tar -zcf %s *.log' % tar_file_name,
                    'rm *.log'
                    ]
        server_model.run_cmd_str(';'.join(cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__tar_tradeplat_file:%s.' % server_name, error_msg)


def __unzip_tar_file(tar_file_path):
    tar_folder_path = os.path.dirname(tar_file_path)
    tar = tarfile.open(tar_file_path)
    names = tar.getnames()
    for name in names:
        tar.extract(name, path=tar_folder_path)
    tar.close()

    # 解压后删除源文件
    os.remove(tar_file_path)


# 解压缩行情文件
def __unzip_market_file(folder_path, date_filter_str):
    tar_file_list = []
    for rt, dirs, files in os.walk(folder_path):
        for file_name in files:
            if ('tar.gz' in file_name) and (date_filter_str in file_name):
                tar_file_list.append(file_name)

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


# 通过rsync的方式下载行情文件,例如:
# rsync -avzP --password-file=/cygdrive/e/rsync/rsync_db.ps --include=*.tar.gz
# --exclude=* yangzhoujie@192.168.1.120::mkt_center_rsync /cygdrive/H/data_backup/LTS_data
def __download_by_rsync(server_model, local_file_path, type_flag):
    custom_log.log_info_job('Download By Rsync From Server:%s Start.' % server_model.name)
    vpn_status = server_model.check_connect()
    if not vpn_status:
        subject = '[Error]VPN Error_' + server_model.name
        msg = 'ping %s fail (Error)' % server_model.ip
        email_utils2.send_email_group_all(subject, msg)
        return msg

    file_save_path = '/cygdrive/%s' % local_file_path.replace(':', '').replace('\\', '/')
    if type_flag == 'ctp_market':
        if server_model.reserve_flag:
            server_file_path = '%s/marketFile/' % server_model.server_path_dict['datafetcher_project_folder']
            cmd = "rsync -e 'ssh -i %s -o StrictHostKeyChecking=no -p%s' -rltDvP --include=*.tar.gz \
--exclude=* %s@%s:%s %s" % (SSH_ID_RSA_PATH, server_model.port, server_model.user,
                            server_model.ip, server_file_path, file_save_path)
        else:
            cmd = 'rsync -rltDvP --password-file=/cygdrive/d/rsync/rsync_db.ps --include=*.tar.gz \
--exclude=* yangzhoujie@%s::ctp_market_rsync %s' % (server_model.ip, file_save_path)
    elif type_flag == 'mkt_center':
        cmd = 'rsync -rltDvP --password-file=/cygdrive/d/rsync/rsync_db.ps --include=*.tar.gz \
        --exclude=* yangzhoujie@%s::mkt_center_rsync %s' % (server_model.ip, file_save_path)
    elif type_flag == 'tradeplat_log':
        if server_model.reserve_flag:
            server_file_path = '%s/log/' % server_model.server_path_dict['tradeplat_project_folder']
            cmd = "rsync -e 'ssh -i %s -o StrictHostKeyChecking=no -p%s' -rltDvP --include=*.tar.gz \
--exclude=*  %s@%s:%s %s" % (SSH_ID_RSA_PATH, server_model.port, server_model.user,
                             server_model.ip, server_file_path, file_save_path)
        else:
            cmd = 'rsync -rltDvP --password-file=/cygdrive/d/rsync/rsync_db.ps --include=*.tar.gz \
--exclude=* yangzhoujie@%s::tradeplat_log_rsync %s' % (server_model.ip, file_save_path)
    else:
        return

    os.chdir(cwrsync_base_path)
    custom_log.log_info_job(cmd)
    rst = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_list = rst.stdout.readlines()
    err_list = rst.stderr.readlines()
    if len(err_list) != 0:
        msg = '[Error]Server:%s %s Download Fail %s' % (server_model.name, type_flag, ''.join(err_list))
    else:
        msg = 'Server:%s %s Download (Success)' % (server_model.name, type_flag)
    custom_log.log_info_job(msg)
    return msg


def __download_by_ftp(server_model, local_folder_path, download_day_str):
    custom_log.log_info_job('Download By Ftp From Server:%s Start.' % server_model.name)
    source_folder_path = "%s/%s" % (server_model.ftp_download_folder, download_day_str)
    ftp_server = server_model
    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    download_flag = False
    for file_name in ftp_server.listdir(source_folder_path):
        if 'tradeplat_log_%s' % download_day_str in file_name:
            source_file_path = '%s/%s' % (source_folder_path, file_name)
            target_file_path = '%s/%s' % (local_folder_path, file_name)
            ftp_server.download_file(source_file_path, target_file_path)

            download_flag = True
            break
    return download_flag


# # 解压缩文件
# def __unzip_file_base(file_path, date_filter_str):
#     for rt, dirs, files in os.walk(file_path):
#         for file_name in files:
#             if ('tar.gz' in file_name) and (date_filter_str in file_name):
#                 tar = tarfile.open(file_path + '/' + file_name)
#                 names = tar.getnames()
#                 for name in names:
#                     tar.extract(name, path=file_path)
#                 tar.close()

# 下载服务器行情文件
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


def __clear_local_folder(folder_path):
    remove_file_list = []
    for rt, dirs, files in os.walk(folder_path):
        for file_name in files:
            remove_file_list.append(rt + '/' + file_name)

    for file_path in remove_file_list:
        os.remove(file_path)


# 下载服务器etf文件
def download_etf_file_job(server_name):
    try:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')
        tar_file_name = 'etf_%s.tar.gz' % date_filter_str

        server_model = server_constant.get_server_model(server_name)
        __tar_etf_file(server_model, tar_file_name)
        server_file_path = '%s/%s' % (server_model.server_path_dict['etf_upload_folder'], tar_file_name)

        local_folder_path = ETF_FILE_PATH
        if os.path.exists(local_folder_path):
            __clear_local_folder(local_folder_path)
        if not os.path.exists(local_folder_path):
            os.mkdir(local_folder_path)
        local_file_path = '%s/%s' % (local_folder_path, tar_file_name.decode('gb2312'))

        server_model.download_file(server_file_path, local_file_path)

        __unzip_tar_file(local_file_path)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[ERROR]ETF文件下载失败:%s!' % server_name, error_msg)


# 下载服务器ctp_market文件
def __download_ctp_market_file_job(server_name, ctp_market_file_name):
    try:
        server_model = server_constant.get_server_model(server_name)

        server_folder_path = server_model.server_path_dict['datafetcher_marketfile']
        ctp_file_path = '%s/%s' % (server_folder_path, ctp_market_file_name)
        if not server_model.is_exist(ctp_file_path):
            raise Exception("Miss File:%s" % ctp_file_path)
        # 压缩文件
        tar_file_name = '%s.tar.gz' % ctp_market_file_name
        __tar_ctp_market_file(server_model, ctp_market_file_name)

        server_file_path = server_folder_path + '/' + tar_file_name

        local_folder_path = '%s/%s' % (CTP_DATA_BACKUP_PATH, server_name)
        if not os.path.exists(local_folder_path):
            os.makedirs(local_folder_path)
        local_file_path = '%s/%s' % (local_folder_path, tar_file_name.decode('gb2312'))

        # 文件下载
        __download_by_rsync(server_model, local_folder_path, 'ctp_market')
        # 下载校验
        try_times = 0
        while not md5_check_download_file(server_model, server_file_path, local_file_path) and try_times < 5:
            custom_log.log_info_job('Try to Download Server:%s CTP File. ReTry:%s' % (server_name, try_times))
            __download_by_rsync(server_model, local_folder_path, 'ctp_market')
            try_times += 1

        __unzip_tar_file(local_file_path)
        __remove_server_tar_file(server_model, server_file_path)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[ERROR]CTP行情文件下载失败:%s!' % server_name, error_msg)


def download_ctp_market_file_job(server_name_list, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')

    if int(date_utils.get_today_str('%H%M%S')) < 90000:
        date_filter_str = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
        ctp_market_file_name = 'CTP_Market_%s_2.txt' % date_filter_str
    else:
        ctp_market_file_name = 'CTP_Market_%s_1.txt' % date_filter_str

    for server_name in server_name_list:
        __download_ctp_market_file_job(server_name, ctp_market_file_name)


# 下载mktcenter行情数据文件
def __download_mktcenter_file_job(server_name, date_filter_str):
    server_model = server_constant.get_server_model(server_name)
    try:
        __download_by_rsync(server_model, server_model.market_file_localpath, 'mkt_center')
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)

    check_file_name_template = server_model.market_file_template.split(',')[0]
    tar_file_name = '%s.tar.gz' % (check_file_name_template % date_filter_str)
    server_file_path = '%s/%s' % (server_model.server_path_dict['mktdtctr_data_folder'], tar_file_name)
    local_file_path = '%s/%s' % (server_model.market_file_localpath, tar_file_name)

    try_times = 0
    download_flag = md5_check_download_file(server_model, server_file_path, local_file_path)
    while not download_flag and try_times < 5:
        __download_by_rsync(server_model, server_model.market_file_localpath, 'mkt_center')
        download_flag = md5_check_download_file(server_model, server_file_path, local_file_path)
        try_times += 1

    if download_flag:
        local_file_size = get_file_size(local_file_path)
        html_title = 'Server_Name,File_Name,File_Size(G)'
        email_content_list = [[server_model.name, tar_file_name, local_file_size]]
        html_content_list = email_utils.list_to_html(html_title, email_content_list)
        email_utils.send_email_group_all('[%s]行情文件下载成功!' % server_model.name, ''.join(html_content_list), 'html')
        __remove_folder_tar_file(server_model, server_model.server_path_dict['mktdtctr_data_folder'])


# 下载mktcenter行情数据文件(托管服务器)
def __download_deposit_mktcenter_file(server_name, date_filter_str):
    server_model = server_constant.get_server_model(server_name)
    try:
        __download_by_rsync(server_model, server_model.market_file_localpath, 'mkt_center')
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)

    check_file_name_template = server_model.market_file_template.split(',')[0]
    tar_file_name = '%s.tar.gz' % (check_file_name_template % date_filter_str)
    server_file_path = '%s/%s' % (server_model.server_path_dict['mktdtctr_data_folder'], tar_file_name)
    local_file_path = '%s/%s' % (server_model.market_file_localpath, tar_file_name)

    try_times = 0
    download_flag = md5_check_download_file(server_model, server_file_path, local_file_path)
    while not download_flag and try_times < 5:
        __download_by_rsync(server_model, server_model.market_file_localpath, 'mkt_center')
        download_flag = md5_check_download_file(server_model, server_file_path, local_file_path)
        try_times += 1

    if download_flag:
        local_file_size = get_file_size(local_file_path)
        html_title = 'Server_Name,File_Name,File_Size(G)'
        email_content_list = [[server_model.name, tar_file_name, local_file_size]]
        html_content_list = email_utils.list_to_html(html_title, email_content_list)
        email_utils.send_email_group_all('[%s]行情文件下载成功!' % server_model.name, ''.join(html_content_list), 'html')
        __remove_folder_tar_file(server_model, server_model.server_path_dict['mktdtctr_data_folder'])


# 下载mktcenter行情数据文件
def download_mktcenter_file_job(server_name_tuple, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y%m%d')

    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__tar_mktcenter_file, args=(server_name, date_filter_str))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for server_name in server_name_tuple:
        __download_mktcenter_file_job(server_name, date_filter_str)


def __download_tradeplat_log_job(server_name):
    log_save_path = LOG_BACKUP_FOLDER_TEMPLATE % server_name
    server_model = server_constant.get_server_model(server_name)
    try:
        msg = __download_by_rsync(server_model, log_save_path, 'tradeplat_log')
        return msg
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)


def __download_depositplat_log_job(server_name, download_day_str):
    download_flag = False
    log_save_path = LOG_BACKUP_FOLDER_TEMPLATE % server_name
    server_model = server_constant.get_server_model(server_name)
    try:
        download_flag = __download_by_ftp(server_model, log_save_path, download_day_str)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
    return download_flag

# 打包TradePlat日志文件
def tar_tradeplat_log_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__tar_tradeplat_file, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def download_trade_server_log_job(server_name_tuple):
    """
        下载trade_server日志文件
    :param server_name_tuple:
    """
    download_status = {}
    download_day = date_utils.get_today()
    for server_name in server_name_tuple:
        msg = __download_tradeplat_log_job(server_name)
        download_status[server_name] = msg
    subject = "Download TradePlat Logs Result"
    email_list = []
    tr_list = [download_day, ]
    for server_name in server_name_tuple:
        tr_list.append(download_status[server_name])

    html_title = 'Time' + ',' + ','.join(server_name_tuple)
    tr_list = [tr_list]
    html_list = email_utils2.list_to_html(html_title, tr_list)
    email_list.append(''.join(html_list))
    email_utils2.send_email_group_all(subject, '\n'.join(email_list), 'html')


def download_deposit_server_log_job(server_name_tuple, download_day_str=None):
    """
        下载deposit_server日志文件
    :param server_name_tuple:
    :param download_day_str:
    """
    download_status = {}
    if not download_day_str:
        download_day_str = date_utils.get_today_str()
    tr_list = []
    for server_name in server_name_tuple:
        download_flag = __download_depositplat_log_job(server_name, download_day_str)
        download_status[server_name] = download_flag if download_flag else '%s(Error)' % download_flag
        tr_list.append([server_name, download_status[server_name]])
    subject = "Download TradePlat Logs Result"
    email_list = []
    html_title = 'server_name' + ',' + 'status'
    html_list = email_utils2.list_to_html(html_title, tr_list)
    email_list.append(''.join(html_list))
    email_utils2.send_email_group_all(subject, '\n'.join(email_list), 'html')


def download_market_file_job(server_name_tuple, date_filter_str=None):
    """
        下载行情文件
    :param server_name_tuple:
    :param date_filter_str:
    """
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')

    if os.path.exists(DATAFETCHER_MESSAGEFILE_FOLDER):
        shutil.rmtree(DATAFETCHER_MESSAGEFILE_FOLDER)
    os.mkdir(DATAFETCHER_MESSAGEFILE_FOLDER)

    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=download_market_file, args=(server_name, date_filter_str))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    __unzip_market_file(DATAFETCHER_MESSAGEFILE_FOLDER, date_filter_str)


# 备份文件至服务器
def backup_etf_file():
    # 压缩etf文件
    __zip_local_file(ETF_FILE_PATH, 'etf')

    date_str = date_utils.get_today_str('%Y-%m-%d')
    source_folder = ETF_FILE_PATH
    target_folder_list = [ETF_FILE_BACKUP_FOLDER, ]
    for file_name in os.listdir(source_folder):
        for target_folder in target_folder_list:
            if target_folder == ETF_FILE_BACKUP_FOLDER2:
                if not (file_name.endswith('tar.gz') or file_name.endswith('.csv')):
                    continue

            source_path = '%s/%s' % (source_folder, file_name)
            target_path_base = '%s/%s' % (target_folder, date_str.replace('-', ''))
            if not os.path.exists(target_path_base):
                os.mkdir(target_path_base)
            target_path = '%s/%s' % (target_path_base, file_name)
            shutil.copy(source_path, target_path)


def backup_market_file():
    date_str = date_utils.get_today_str('%Y-%m-%d')
    source_folder = DATAFETCHER_MESSAGEFILE_FOLDER
    target_folder_list = [PRICE_FILES_BACKUP_FOLDER, ]
    for file_name in os.listdir(source_folder):
        for target_folder in target_folder_list:
            source_path = '%s/%s' % (source_folder, file_name)
            # 过滤掉文件夹
            if not os.path.isfile(source_path):
                continue
            target_path_base = '%s/%s' % (target_folder, date_str.replace('-', ''))
            if not os.path.exists(target_path_base):
                os.mkdir(target_path_base)
            target_path = '%s/%s' % (target_path_base, file_name)
            shutil.copy(source_path, target_path)


def backup_instrument_pickle_file():
    date_str = date_utils.get_today_str('%Y-%m-%d')
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    query = session_common.query(Instrument)
    obj_list = []
    for future_db in query.filter(Instrument.del_flag == 0):
        obj_list.append(future_db)
    instrument_obj_list = to_many_sql(Instrument, obj_list, 'common.instrument')
    pickle_file_name = 'INSTRUMENT_' + date_str.replace('-', '') + '.pickle'

    target_folder_list = [PRICE_FILES_BACKUP_FOLDER, ]
    for target_folder in target_folder_list:
        target_path_base = '%s/%s' % (target_folder, date_str.replace('-', ''))
        if not os.path.exists(target_path_base):
            os.mkdir(target_path_base)
        target_path = '%s/%s' % (target_path_base, pickle_file_name)
        with open(target_path, 'wb') as f:
            pickle.dump(instrument_obj_list, f, True)


def instrument_files_backup():
    try:
        backup_market_file()
        backup_etf_file()
        backup_instrument_pickle_file()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils6.send_email_group_all(u'[Error]备份行情文件异常', error_msg, 'html')


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


if __name__ == '__main__':
    # download_tradeplat_log_job(('citics',))
    # __download_mktcenter_file_job('huabao', '20190318')
    download_deposit_server_log_job(('guosen',), '20190507')
