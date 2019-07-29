# -*- coding: utf-8 -*-
# 根据sql文件更新本地数据库
import os
import pickle
import tarfile
import threading
import traceback
from xmlrpclib import ServerProxy
from eod_aps.job import *


def update_deposit_server_db_job(server_name_tuple, sql_library_list, termination_seconds=90):
    db_update_dict = dict()
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__update_deposit_server_db, args=(server_name, sql_library_list, db_update_dict))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        # 执行超过90秒则认为任务执行超时被中断
        t.join(termination_seconds)

    for (dict_key, dict_value) in db_update_dict.items():
        if not dict_value:
            email_utils2.send_email_group_all('[Error]update_deposit_server_db_job, Server:%s' % dict_key, '')


def __update_deposit_server_db(server_name, sql_library_list, db_update_dict):
    try:
        db_update_dict[server_name] = False
        server_model = server_constant.get_server_model(server_name)
        today_str = date_utils.get_today_str('%Y-%m-%d')

        __download_sql_file(server_model)

        __update_by_sql_file(server_name, sql_library_list, today_str)
        __update_by_pickle_file(server_name, today_str)
        db_update_dict[server_name] = True
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__update_deposit_server_db:%s.' % server_name, error_msg)


def __download_sql_file(server_model):
    date_filter_str = date_utils.get_today_str('%Y-%m-%d')
    sql_tar_file_name = 'db_backup_%s.tar.gz' % date_filter_str
    source_file_path = '%s/%s/%s' % (server_model.ftp_download_folder,
                                     date_filter_str.replace('-', ''), sql_tar_file_name)

    target_folder_path = DEPOSIT_SERVER_SQL_FILE_FOLDER_TEMPLATE % server_model.name
    target_file_path = '%s/%s' % (target_folder_path, sql_tar_file_name)
    server_model.download_file(source_file_path, target_file_path)
    source_file_size, target_file_size = 0, 0
    for i in range(3):
        source_file_size = server_model.get_size(source_file_path)
        target_file_size = os.stat(target_file_path).st_size
        if long(source_file_size) != long(target_file_size):
            server_model.download_file(source_file_path, target_file_path)
            continue
        else:
            break
    else:
        error_message = 'File:%s Server Size:%s,Local Size:%s' % (sql_tar_file_name, source_file_size, target_file_size)
        email_utils4.send_email_group_all('Sql File Download Fail_%s' % server_model.name, error_message, 'html')
        raise Exception("Sql File Download Fail_%s" % server_model.name)

    tar = tarfile.open(target_file_path)
    for file_name in tar.getnames():
        tar.extract(file_name, path=target_folder_path)
    tar.close()


def __update_by_sql_file(server_name, sql_library_list, today_str):
    server_model = server_constant.get_server_model(server_name)
    sql_folder_path = DEPOSIT_SERVER_SQL_FILE_FOLDER_TEMPLATE % server_name

    clear_path_list = []
    for sql_library in sql_library_list:
        sql_file_name = '%s_%s.sql' % (sql_library, today_str)
        sql_file_path = '%s/%s' % (sql_folder_path, sql_file_name)
        if not os.path.exists(sql_file_path):
            continue
        cmd = "mysql -u%s -p%s -h %s %s < %s" % \
              (server_model.db_user, server_model.db_password, server_model.ip, sql_library, sql_file_path)
        custom_log.log_info_job(cmd)

        sub = os.system(cmd)
        custom_log.log_info_job(sub)
        clear_path_list.append(sql_file_path)

    for temp_file_path in clear_path_list:
        os.remove(temp_file_path)


def __update_by_pickle_file(server_name, today_str):
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    pickle_folder_path = DEPOSIT_SERVER_SQL_FILE_FOLDER_TEMPLATE % server_name
    clear_path_list = []
    for pickle_file_name in os.listdir(pickle_folder_path):
        if today_str in pickle_file_name and pickle_file_name.endswith('.pickle'):
            pickle_file_path = os.path.join(pickle_folder_path, pickle_file_name)
            with open(pickle_file_path, 'rb') as f:
                update_sql_list = pickle.load(f)
            for update_sql in update_sql_list:
                session_common.execute(update_sql)
            clear_path_list.append(pickle_file_path)
            session_common.commit()

    for temp_file_path in clear_path_list:
        os.remove(temp_file_path)


def upload_sql_file(server_name, sql_library):
    server_model = server_constant.get_server_model(server_name)
    ftp_wsdl_address = server_model.ftp_wsdl_address
    ftp_server = ServerProxy(ftp_wsdl_address)
    today_str = date_utils.get_today_str('%Y-%m-%d')

    __backup_sql_file(server_name, sql_library, today_str)
    __upload_sql_file(server_name, sql_library, ftp_server, today_str)


def __backup_sql_file(server_name, sql_library, today_str):
    server_model = server_constant.get_server_model(server_name)
    deposit_server_sql_file_folder = DEPOSIT_SERVER_SQL_FILE_FOLDER_TEMPLATE % server_name
    sql_folder_path = '%s/sql_backup' % deposit_server_sql_file_folder
    sql_file_name = '%s_%s.sql' % (sql_library, today_str)
    sql_file_path = '%s/%s' % (sql_folder_path, sql_file_name)
    cmd = "mysqldump -u%s -p%s -h %s %s > %s" % \
        (server_model.db_user, server_model.db_password, server_model.ip, sql_library, sql_file_path)
    custom_log.log_info_job(cmd)
    os.system(cmd)


def __upload_sql_file(server_name, sql_library, ftp_server, today_str):
    server_model = server_constant.get_server_model(server_name)
    deposit_server_sql_file_folder = DEPOSIT_SERVER_SQL_FILE_FOLDER_TEMPLATE % server_name
    sql_file_name = '%s_%s.sql' % (sql_library, today_str)
    source_file_path = '%s/sql_backup/%s' % (deposit_server_sql_file_folder, sql_file_name)
    target_file_path = '%s/%s/%s' % \
                    (server_model.ftp_upload_folder, today_str.replace('-', ''), sql_file_name)
    ftp_server.upload_file(source_file_path, target_file_path)


if __name__ == '__main__':
    # sql_library_list = ['common', 'om', 'portfolio']
    # __update_deposit_server_db('citics', sql_library_list)
    # upload_sql_file('zhongxin_ftp', 'om')
    sql_library_list = ['common', 'om', 'portfolio']
    update_deposit_server_db_job(('citics', ), sql_library_list, 120)
