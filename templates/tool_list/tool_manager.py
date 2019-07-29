# -*- coding: utf-8 -*-

import os
import time
import re
import traceback
from datetime import datetime
from eod_aps.model.server_constans import server_constant
from eod_aps.job.update_server_db_job import update_position_job
from eod_aps.tools.file_utils import FileUtils
from eod_aps.job.db_backup_job import db_backup_job
from eod_aps.server_python.screen_tools import screen_manager
from eod_aps.tools.server_manage_tools import get_service_list
from eod_aps.model.eod_const import const


# -----------------------------------------------ftp server----------------------------------------------------------
# host_name = 'ftp_server'
# ftp_server = ServerConstant().get_server_model(host_name)
# ftp_input_folder = ftp_server.server_path_dict['input_folder']
# ftp_output_folder = ftp_server.server_path_dict['output_folder']
# -----------------------------------------------host server--------------------------------------------------------
host_servert = server_constant.get_server_model('host')
# datafetcher_folder = const.EOD_CONFIG_DICT['datafetcher_project_folder']
# ============================================== backup folder ======================================================
# eod_log_path
eod_log_path = os.path.join(const.EOD_CONFIG_DICT['eod_project_folder'], 'log')
# tradeplat_log_folder = os.path.join(host_server_constant.server_path_dict['tradeplat_project_folder'], 'log')
tradeplat_log_folder = ''
data_temp_path = os.path.join(const.EOD_CONFIG_DICT['eod_project_folder'], 'temp_data')
# datafetcher_messagefile_folder = host_server_constant.server_path_dict['datafetcher_messagefile_folder']
# datafetcher_messagebackup_folder = os.path.join(datafetcher_folder, 'messageFile_backup')
# db_backup_folder = host_server_constant.server_path_dict['db_backup_folder']
# etf_file_folder = host_server_constant.server_path_dict['etf_file_path']
datafetcher_messagefile_folder = ''
datafetcher_messagebackup_folder = ''
db_backup_folder = ''
etf_file_folder = ''

def insert_sql_file_by_mysql_cmd(sql_file_name):
    start = time.time()
    # db_name = sql_name.split('.')[0]
    # src_file_path = os.path.join(data_temp_path, sql_name)
    user = host_servert.db_user
    ip = host_servert.db_ip
    password = host_servert.db_password
    cmd = "mysql -u%s -p%s -h %s %s < %s" % (str(user), str(password), ip, 'common', sql_file_name)
    host_servert.run_cmd_str([cmd])
    end = time.time()
    print 'total time :', end - start


# this function error happens if there is some chinese code;
# this is will be used for whole sql not just executioncd
def insert_sql_file_by_execute_backup(db_name, sql_name):
    start = time.time()
    src_file_path = os.path.join(data_temp_path, sql_name)
    # db_name = sql_name.split('.')[0]

    server_model = server_constant.get_server_model('host')
    session_db = server_model.get_db_session(db_name)
    file_ = file(src_file_path, 'rb')
    sql = ''
    record_tag = True
    while True:
        line = file_.readline()
        line.replace('\n', '')
        line.replace('\r', '')
        if line:
            # print line
            if line.startswith('/*'):
                # print '==============='
                # print line
                record_tag = False
            elif line.startswith('-'):
                continue

            elif line == '':
                continue

            if record_tag:
                sql += line

            if '*/' in line:
                record_tag = True

            if ';' in line:
                session_db.execute(sql)

                sql = ''
        else:
            break
    end = time.time()
    session_db.commit()
    session_db.close()

    print 'total time :', end - start


def insert_sql_file_by_sql(sql_file_path):
    server_model = server_constant.get_server_model('host')
    session_db = server_model.get_db_session('common')

    file_ = file(sql_file_path, 'rb')
    lines = file_.readlines()

    for line in lines:
        line = line.strip()
        if line == '':
            continue
        session_db.execute(line)
    session_db.commit()
    session_db.close()


def download_sql_and_insert(sql_name):
    # download_file_from_input_folder(data_temp_path, sql_name)
    # sql_file_path = os.path.join(data_temp_path, sql_name)
    # # insert_sql_file_by_sql(sql_file_path)
    # insert_sql_file_by_mysql_cmd(sql_file_path)
    pass


def grep_error_msg_upload(key, target_files):
    filename = 'grep_%s.log' % (datetime.now().strftime('%Y%m%d_%H%M%S'))
    tar_gz_name = '%s.tar.gz' % filename
    grep_log_path = os.path.join(data_temp_path, filename)

    if len(key) != 0:
        grep_cmd = "cd %s;grep -i -r '%s' %s > %s" % (tradeplat_log_folder, key, target_files, grep_log_path)
    else:
        grep_cmd = "cd %s;grep -i -r %s %s > %s" % (tradeplat_log_folder, key, target_files, grep_log_path)
    tar_cmd = 'cd %s;tar zcf %s %s' % (data_temp_path, tar_gz_name, filename)
    result_msg = host_servert.run_shell_cmd([grep_cmd, tar_cmd])

    # src_tar_path = os.path.join(data_temp_path, tar_gz_name)
    # des_tar_path = os.path.join(ftp_output_folder, tar_gz_name)
    # with ftp_server as ftp_utils:
    #     ftp_utils.upload_file(src_tar_path, des_tar_path)

    return result_msg


def start_get_position():
    server_tuple = ('host',)
    update_position_job(server_tuple)
    datafetcher_folder = ''

    tar_name = 'position_%s.tar.gz' % (datetime.now().strftime('%Y%m%d_%H%M%S'))
    cmd = "cd %s;tar -zcf %s %s" % (datafetcher_folder, tar_name, 'messageFile')

    src_tar_path = os.path.join(data_temp_path, tar_name)
    mv_cmd = "mv %s %s" % (os.path.join(datafetcher_folder, tar_name), src_tar_path)
    result = host_servert.run_shell_cmd([cmd, mv_cmd])
    # des_tar_path = os.path.join(ftp_output_folder, tar_name)
    # with ftp_server as ftp_utils:
    #     ftp_utils.upload_file(src_tar_path, des_tar_path)
    return result


def check_if_match(pattern, content, group_id):
    re_data = re.match(pattern, content)
    if re_data is not None:
        date_str = re_data.group(group_id)
        date_str = ''.join(date_str.split('-'))
        return date_str
    return None


def get_del_file_list(path, date):
    date = ''.join(date.split('-'))
    file_list = os.listdir(path)
    del_file_list = []
    group_id = 'date_str'
    datetime_pattern_1 = '.*(?P<%s>[\d]{8})' % group_id
    datetime_pattern_2 = '.*(?P<%s>[\d]{4}-[\d]{2}-[\d]{2})' % group_id

    for file_name in file_list:
        file_date_str = check_if_match(datetime_pattern_1, file_name, group_id)
        if file_date_str is not None and file_date_str < date:
            del_file_list.append(file_name)

        file_date_str = check_if_match(datetime_pattern_2, file_name, group_id)
        if file_date_str is not None and file_date_str < date:
            del_file_list.append(file_name)
    return del_file_list


def get_select_file_by_folder_key(folder, key):
    date = datetime.now().strftime('%Y%m%d')
    date = ''.join(date.split('-'))
    date_tab = '-'.join([date[:4], date[4:6], date[6:8]])
    select_file_list = FileUtils(folder).filter_file(key, date)
    select_file_list_2 = FileUtils(folder).filter_file(key, date_tab)
    select_file_list.extend(select_file_list_2)
    select_file_list = map(lambda x: os.path.join(folder, x), select_file_list)
    return select_file_list


def upload_backup_file():
    try:
        key_folder_dict = {
            'tar.gz': [
                tradeplat_log_folder, datafetcher_messagefile_folder, etf_file_folder,
                datafetcher_messagebackup_folder
            ],
            'sql': [db_backup_folder],
        }

        total_upload_file_list = []
        for key in key_folder_dict:
            for folder in key_folder_dict[key]:
                target_file_list = get_select_file_by_folder_key(folder, key)
                total_upload_file_list.extend(target_file_list)

        # with ftp_server as ftp_utils:
        #     for src_file_path in total_upload_file_list:
        #         file_name = os.path.basename(src_file_path)
        #         des_file_path = os.path.join(ftp_output_folder, file_name)
        #         ftp_utils.upload_file(src_file_path, des_file_path)
        #
        # with ftp_server as ftp_utils:
        #     for log_file in os.listdir(eod_log_path):
        #         src_file_path = os.path.join(eod_log_path, log_file)
        #         des_file_path = os.path.join(ftp_output_folder, log_file)
        #         ftp_utils.upload_file(src_file_path, des_file_path)
    except:
        pass

def clear_all_folder():
    date_str = datetime.now().strftime('%Y%m%d')

    select_path_list = [
        tradeplat_log_folder, datafetcher_messagefile_folder, db_backup_folder,
        etf_file_folder
    ]

    clear_folder_list = []
    for chosen_path in [datafetcher_messagebackup_folder]:
        folders = os.listdir(chosen_path)
        folders = filter(lambda x: x[:8] < date_str or x.endswith('tar.gz'), folders)
        folders = map(lambda x: os.path.join(chosen_path, x), folders)
        del_cmd_list = map(lambda x: "rm -rf %s" % x, folders)
        clear_folder_list.extend(del_cmd_list)

    clear_cmd_list = []
    for chosen_path in select_path_list:
        del_file_list = get_del_file_list(chosen_path, date_str)
        del_cmd_list = map(lambda x: "rm %s" % os.path.join(chosen_path, x), del_file_list)
        clear_cmd_list.extend(del_cmd_list)

    rm_temp_data_list = map(lambda x: "rm %s" % os.path.join(data_temp_path, x), os.listdir(data_temp_path))
    rm_log_cmd_list = map(lambda x: "rm %s" % os.path.join(eod_log_path, x), os.listdir(eod_log_path))
    clear_cmd_list.extend(rm_log_cmd_list)
    clear_cmd_list.extend(rm_temp_data_list)
    clear_cmd_list.extend(clear_folder_list)

    result = host_servert.run_shell_cmd(clear_cmd_list)
    return result


def export_db_table(db_name, table_name):
    sql_name = '%s_%s.sql' % (db_name, table_name)
    output_sql_path = os.path.join(data_temp_path, sql_name)

    user = host_servert.db_user
    ip = host_servert.db_ip
    password = host_servert.db_password
    cmd = "mysqldump -u%s -p%s -h %s %s %s > %s" % (str(user), str(password), ip, db_name, table_name, output_sql_path)
    run_result = host_servert.run_shell_cmd([cmd])
    #
    # with ftp_server as ftp_utils:
    #     des_file_path = os.path.join(ftp_output_folder, sql_name)
    #     ftp_utils.upload_file(output_sql_path, des_file_path)
    return run_result


def db_backup_tools():
    db_backup_job(('host',))
    # with ftp_server as ftp_utils:
    #     for file_name in os.listdir(db_backup_folder):
    #         src_file_path = os.path.join(db_backup_folder, file_name)
    #         des_file_path = os.path.join(ftp_output_folder, file_name)
    #         ftp_utils.upload_file(src_file_path, des_file_path)


def run_common_screen_cmd(service, cmd):
    screen_manager(service, cmd)
    today = datetime.now().strftime('%Y%m%d')
    select_file_list = FileUtils(tradeplat_log_folder).filter_file(service, today)
    time.sleep(5)  # too fast will not record info
    read_mainframe_log = ['cd %s;tail -200 %s' % (tradeplat_log_folder, select_file_list[-1])]
    log_info = host_servert.run_cmd_str(read_mainframe_log)
    pattern = 'Receive Command=[%s]' % cmd
    log_info = log_info.split('\n')
    record_info = []
    record_or_not = False
    for line in log_info:
        line = line.replace('\n', '')
        line = line.replace('\r', '')
        if line == '':
            continue
        if line == pattern:
            record_or_not = True
        if record_or_not:
            record_info.append(line)
    result = '\n'.join(record_info)
    return result


def check_service_status(server_list):
    status_dict = {}
    # for server_name in server_list:
    #     status_dict[server_name] = 'inactive'
    #
    # cmd = 'screen -ls'
    # result = host_servert.run_cmd_str(cmd)
    # time.sleep(1)
    # if 'No Sockets found' in result:
    #     return status_dict
    #
    # result = result.split('\n')
    # filter(lambda x: x != '' and x != '\r', result)
    #
    # for line in result:
    #     line = line.decode('utf-8')
    #     for service_name in server_list:
    #         if service_name in line:
    #             if 'Detached' in line:
    #                 status_dict[service_name] = 'Detached'
    #             elif 'Attached' in line:
    #                 status_dict[service_name] = 'Attached'
    return status_dict


def show_log(select_log, show_num='20'):
    filename = ''
    file_folder = ''
    server_list = get_service_list('host')
    server_list = [x.app_name for x in server_list]

    if select_log in server_list:
        file_folder = tradeplat_log_folder
        date = datetime.now().strftime('%Y%m%d')
        file_list = FileUtils(file_folder).filter_file(select_log, date)
        if len(file_list) == 0:
            return '%s has no log' % select_log
        filename = file_list[-1]
        print file_folder, filename

    elif select_log in os.listdir(datafetcher_messagefile_folder):
        file_folder = datafetcher_messagefile_folder
        filename = select_log

    elif select_log == 'DataFetcher':
        return 'never work now'

    elif select_log == 'tool_log':
        file_folder = eod_log_path
        filename = 'tool.log'

    elif select_log == 'eod_log':
        file_folder = eod_log_path
        filename = 'eod.log'

    elif select_log == 'account_log':
        file_folder = eod_log_path
        filename = 'account.log'

    elif select_log == 'cmd_log':
        file_folder = eod_log_path
        filename = 'cmd.log'

    # show_log_cmd = 'cd %s;tail -%s %s' % (file_folder, show_num, filename)
    show_file_path = os.path.join(file_folder, filename)
    file_ = file(show_file_path, 'r')
    content = file_.readlines()
    content = content[-1 * int(show_num):]
    content = map(lambda x: x.replace('\n', ''), content)
    # content = host_server_constant.run_shell_cmd([show_log_cmd])
    print '======================='
    # content = content.split('\n')
    content = '<br>'.join(content)
    if content == '':
        content = '%s has no content' % filename
    return content


def upgrade_server_tradeplat(server_name, upgrade_file_path):
    upgrade_flag = False
    try:
        server_model = server_constant.get_server_model(server_name)
        upgrade_file_name = os.path.basename(upgrade_file_path)

        source_file_path = upgrade_file_path
        target_folder_path = '%s/bin' % server_model.server_path_dict['tradeplat_project_folder']
        server_model.upload_file(source_file_path, '%s/%s' % (target_folder_path, upgrade_file_name))

        cmd_list = ['cd %s' % target_folder_path,
                    'tar -zxvf %s' % upgrade_file_name,
                    'rm %s' % upgrade_file_name,
                    'cd ..',
                    'rm build64_release',
                    'ln -s bin/%s/build64_release' % upgrade_file_name.split('.')[0]
        ]
        server_model.run_cmd_str(';'.join(cmd_list))
        upgrade_flag = True
    except Exception:
        error_msg = traceback.format_exc()
        print error_msg
    return upgrade_flag


if __name__ == '__main__':
    keys = "HandleRequest:"
    targets = 'screenlog_CiticsServer_20170815*.log'
    test_file = 'test_cmd.txt'
    # grep_error_msg_upload(keys, targets)
    # download_sql_and_insert('test.sql')
    # start_get_position()
    # download_sql_and_insert()
    # clear_all_folder()
    # upload_all_file('20170818')
    # upload_all_file('20170818')
    # get_data_from_sql_cmd('', '')
    # export_db_table('common', 'user')
    # run_common_screen_cmd('MainFrame', 'update pf')
    # check_service_status('MainFrame')
    # download_sql_and_insert('portfolio_account_position.sql')
    # grep_error_msg_upload('', '')
