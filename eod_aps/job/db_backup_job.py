# -*- coding: utf-8 -*-
# 实现数据库文件备份
import threading
import traceback

from eod_aps.job import *

dbNames = ['common', 'history', 'om', 'portfolio', 'strategy']


def __db_backup(server_name):
    try:
        last_trading_day_str = date_utils.get_last_trading_day('%Y%m%d')
        server_model = server_constant.get_server_model(server_name)
        db_file_save_path = '%s/%s' % (server_model.server_path_dict['db_backup_folder'], last_trading_day_str)
        mkdir_cmd = 'mkdir -p %s' % db_file_save_path
        server_model.run_cmd_str(mkdir_cmd)

        backup_cmd_list = []
        for dbName in dbNames:
            backup_cmd = '/usr/bin/mysqldump -u%s -p%s %s  --opt -Q -R >%s/%s.sql' % \
                         (server_model.db_user, server_model.db_password, dbName, db_file_save_path, dbName)
            backup_cmd_list.append(backup_cmd)
        server_model.run_cmd_str(';'.join(backup_cmd_list))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__db_backup:%s.' % server_name, error_msg)


def db_backup_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__db_backup, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    db_backup_job(('nanhua',))
