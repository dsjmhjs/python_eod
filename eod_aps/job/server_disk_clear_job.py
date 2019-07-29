# -*- coding: utf-8 -*-
# 服务器磁盘管理，删除无用文件
import re
import threading
import traceback

from eod_aps.job import *

interval_days = 7


# 从字符串中获取时间格式的部分
def __get_date_from_str(base_str):
    today_filter_str = date_utils.get_today_str('%Y%m%d')
    date_elem_1 = re.findall(r'20\d{2}-\d{2}-\d{2}', base_str)
    date_elem_2 = re.findall(r'20\d{2}\d{2}\d{2}', base_str)
    if len(date_elem_1) == 0 and len(date_elem_2) == 0:
        custom_log.log_error_job('regular error input:', base_str)
        return today_filter_str
    elif len(date_elem_1) != 0 and len(date_elem_2) != 0:
        if date_elem_1[0].replace('-', '') < date_elem_2[0]:
            return date_elem_1[0].replace('-', '')
        else:
            return date_elem_2[0]
    elif len(date_elem_1) != 0 and len(date_elem_2) == 0:
        return date_elem_1[0].replace('-', '')
    elif len(date_elem_1) == 0 and len(date_elem_2) != 0:
        return date_elem_2[0]


# 通过shell命令删除文件
def __remove_server_file(server_model, base_file_folder, del_file_list):
    if len(del_file_list) == 0:
        return

    remove_cmd_list = []
    for remove_file_name in del_file_list:
        remove_cmd = 'rm -rf %s' % remove_file_name
        remove_cmd_list.append(remove_cmd)

    while len(remove_cmd_list) > 50:
        remove_cmd_full = 'cd %s;%s' % (base_file_folder, ';'.join(remove_cmd_list[:49]))
        server_model.run_cmd_str(remove_cmd_full)
        remove_cmd_list = remove_cmd_list[49:]
    else:
        remove_cmd_full = 'cd %s;%s' % (base_file_folder, ';'.join(remove_cmd_list))
        server_model.run_cmd_str(remove_cmd_full)


def __clear_file_folder(server_model, base_file_folder):
    cmd = "find %s -mtime +%s" % (base_file_folder, interval_days)
    rst = server_model.run_cmd_str(cmd)
    custom_log.log_info_job('prepare del file:%s' % rst)

    if server_model.is_exist(base_file_folder):
        remove_cmd = "find %s -mtime +%s| xargs rm -rf" % (base_file_folder, interval_days)
        server_model.run_cmd_str(remove_cmd)
    else:
        custom_log.log_info_job('Miss path:%s' % base_file_folder)


def __server_disk_clear(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        clear_folder_list = [server_model.server_path_dict['tradeplat_log_folder'],
                             server_model.server_path_dict['datafetcher_messagefile'],
                             server_model.server_path_dict['datafetcher_messagefile_backup'],
                             server_model.server_path_dict['datafetcher_marketfile'],
                             server_model.server_path_dict['db_backup_folder'],
                             server_model.server_path_dict['mktdtctr_data_folder'],
                             server_model.server_path_dict['mktdtctr_check_folder'],
                             '%s/%s' % (server_model.server_path_dict['datafetcher_project_folder'], 'log')]

        for folder in clear_folder_list:
            __clear_file_folder(server_model, folder)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__server_disk_clear:%s.' % server_name, error_msg)


def server_disk_clear_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__server_disk_clear, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    server_disk_clear_job(trade_servers_list)
