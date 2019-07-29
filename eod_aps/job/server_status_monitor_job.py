# -*- coding: utf-8 -*-
import re
import traceback
from eod_aps.job import *

disk_usage_percentage_threshold = 50
email_index_list = ['compare_server_time', 'screen_ls_result', 'disk_usage', 'memeory_usage',
                    'mysql_threads', 'boot_time', 'login_user', 'history_fifty_cmd']


def convert_ssh_result_to_table(ssh_result):
    if ssh_result[-1:] == '\n':
        ssh_result = ssh_result[:-1]
    while '  ' in ssh_result:
        ssh_result = ssh_result.replace('  ', ' ')
    ssh_result = ssh_result.replace(' ', '</td><td>').replace('\n', '</td></tr><tr><td>')
    ssh_result_table = '<table border="0"><tr><td>'
    ssh_result_table += ssh_result
    ssh_result_table += '</td></tr></table>'
    return ssh_result_table


def check_disk_usage(ssh_result, threshold):
    usage_list = re.findall('<td>\d{1,2}%</td>', ssh_result)
    for usage in usage_list:
        usage_num_str = ''
        for element in usage:
            if element.isdigit():
                usage_num_str += element
        usage_num = float(usage_num_str)
        if usage_num > threshold:
            usage_num_str += '%'
            ssh_result = ssh_result.replace(usage, '<td style="color:red;">%s</td>' % usage_num_str)
    return ssh_result


def __get_disk_usage(server_model):
    disk_usage = []
    ssh_result = server_model.run_cmd_str('df -hl')
    ssh_result = ssh_result.replace('Mounted on', 'Mounted_on')
    ssh_result = convert_ssh_result_to_table(ssh_result)
    ssh_result = check_disk_usage(ssh_result, disk_usage_percentage_threshold)
    disk_usage.append(ssh_result.replace('\n','<br>'))
    disk_usage = '<br>'.join(disk_usage)
    return disk_usage


def __get_memeory_usage(server_model):
    memeory_usage = []
    ssh_result = server_model.run_cmd_str('free -g')
    ssh_result = convert_ssh_result_to_table(ssh_result)
    memeory_usage.append(ssh_result.replace('\n', '<br>'))
    memeory_usage = '<br>'.join(memeory_usage)
    return memeory_usage


def __get_boot_time(server_model):
    boot_time = ['']
    ssh_result = server_model.run_cmd_str('who -b')
    boot_time.append(ssh_result.replace('\n', '<br>'))
    boot_time.append('')
    boot_time = '<br>'.join(boot_time)
    return boot_time


def __get_login_user(server_model):
    login_user = []
    ssh_result = server_model.run_cmd_str('last -10')
    login_user.append(ssh_result.replace('\n', '<br>'))
    login_user = '<br>'.join(login_user)
    return login_user


def __get_screen_ls_result(server_model):
    screen_ls_result = []
    ssh_result = server_model.run_cmd_str('screen -ls')
    ssh_result = ssh_result.replace(unicode('年', 'utf-8'), '-').replace(unicode('月', 'utf-8'), '-')\
                           .replace(unicode('日', 'utf-8'), '')
    ssh_result = ssh_result.replace(unicode('时', 'utf-8'), ':').replace(unicode('分', 'utf-8'), ':')\
                           .replace(unicode('秒', 'utf-8'), '')
    screen_ls_result.append(ssh_result.replace('\n','<br>'))
    screen_ls_result = '<br>'.join(screen_ls_result)
    return screen_ls_result


def __get_mysql_status(server_model):
    mysql_threads = ['']
    session_strategy = server_model.get_db_session('strategy')
    query_sql = "show status"
    status_query = session_strategy.execute(query_sql)

    for status in status_query:
        if 'Threads_connected' in status[0]:
            mysql_threads.append('Mysql Threads connected: %s<br>'% (status[1]))
            break
    mysql_threads.append('')
    mysql_threads = '<br>'.join(mysql_threads)
    return mysql_threads


def __get_history_fifty_cmd(server_model):
    history_fifty_cmd = []
    ssh_result = server_model.run_cmd_str('tail  -50 ~/.bash_history')
    history_fifty_cmd.append(ssh_result.replace('\n','<br>').replace('<br>ls<br>','<br>').replace('<br>pwd<br>','<br>'))
    history_fifty_cmd = '<br>'.join(history_fifty_cmd)
    return history_fifty_cmd


def __compare_server_time_cmd(server_model):
    ssh_result = server_model.run_cmd_str('date "+%F %T.%N"')
    # 将Linux时间转换为datetime格式
    time_linux_str = ssh_result.split('.')[0]
    microseconds_linux_str = str(int(round(int(ssh_result.split('.')[1].replace('\n', '')) / 1000)))
    datetime_linux_str = time_linux_str + '.' + microseconds_linux_str

    # 将datetime_local转换为字符串格式
    datetime_local_str = date_utils.get_today_str("%Y-%m-%d %H:%M:%S")
    # 取datetime_local与Linux服务器时间作差，得到时间差
    timedelta_linux_local = date_utils.get_interval_seconds(datetime_local_str, datetime_linux_str)

    # 检查linux服务器的时间是否在本机两个时间戳之间
    check_flag = abs(timedelta_linux_local) <= 1

    if check_flag:
        server_time_cmd_table = '<table border="0">'
    else:
        server_time_cmd_table = '<table border="0" style = "color:red;">'

    server_time_cmd_table += '<tr><td>'
    server_time_cmd_table += 'Local time: %s' % datetime_local_str
    server_time_cmd_table += '</td></tr>'
    server_time_cmd_table += '<tr><td>'
    server_time_cmd_table += 'Server time: %s' % datetime_linux_str
    server_time_cmd_table += '</td></tr>'
    server_time_cmd_table += '<tr><td>'
    server_time_cmd_table += 'Time delta: %s s' % timedelta_linux_local
    server_time_cmd_table += '</td></tr>'
    server_time_cmd_table += '</table>'
    return server_time_cmd_table


def __server_status_monitor(server_name):
    email_content_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    for check_item_name in email_index_list:
        if check_item_name == 'compare_server_time':
            # 校对服务器与本地的时间
            email_content_dict[check_item_name] = __compare_server_time_cmd(server_model)
        elif check_item_name == 'screen_ls_result':
            # 获取screen - ls指令的内容
            email_content_dict[check_item_name] = __get_screen_ls_result(server_model)
        elif check_item_name == 'disk_usage':
            # 获取磁盘信息
            email_content_dict[check_item_name] = __get_disk_usage(server_model)
        elif check_item_name == 'memeory_usage':
            # 获取内存信息
            email_content_dict[check_item_name] = __get_memeory_usage(server_model)
        elif check_item_name == 'mysql_threads':
            # 获取Mysql数据库信息
            email_content_dict[check_item_name] = __get_mysql_status(server_model)
        elif check_item_name == 'boot_time':
            # 显示开机时间
            email_content_dict[check_item_name] = __get_boot_time(server_model)
        elif check_item_name == 'login_user':
            # 显示登陆用户
            email_content_dict[check_item_name] = __get_login_user(server_model)
        elif check_item_name == 'history_fifty_cmd':
            # 查看历史的50个指令
            email_content_dict[check_item_name] = __get_history_fifty_cmd(server_model)
    return email_content_dict


def build_table(server_name_tuple, email_index_list, email_list_dict):
    table_list = '<table border="1">'
    # 做表头
    table_header = '<tr><th align="center" font-size:12px; bgcolor="#70bbd9"><b>Status</b></th>'
    for server_name in server_name_tuple:
        table_header += '<th align="center" font-size:12px; bgcolor="#70bbd9"><b>%s</b></th>' % server_name
    table_header += '</tr>'
    table_list += table_header

    # 做表格主体
    for email_index in email_index_list:
        table_line = '<tr><td align="center" font-size:12px><b>%s</b></td>' % email_index
        for server_name in server_name_tuple:
            table_line += '<td align="left" font-size:8px;>%s</td>' % (email_list_dict[server_name][email_index])
        table_line += '</tr>'
        table_list += __filter_string(table_line)
    table_list += '</table>'
    return table_list


def __filter_string(input_str_array):
    filter_result = []
    for input_str in input_str_array.split('\n'):
        try:
            input_str.decode("utf-8").encode("gbk")
            filter_result.append(input_str)
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_job(error_msg)
    return '\n'.join(filter_result)


def query_server_status(server_name_tuple):
    server_status_dict = dict()
    for server_name in server_name_tuple:
        email_content_dict = __server_status_monitor(server_name)
        server_status_dict[server_name] = email_content_dict
    return server_status_dict, email_index_list


def server_status_monitor_job(server_name_tuple):
    email_list_dict = dict()
    for server_name in server_name_tuple:
        email_content_dict = __server_status_monitor(server_name)
        email_list_dict[server_name] = email_content_dict

    table_list = build_table(server_name_tuple, email_index_list, email_list_dict)
    email_utils2.send_email_group_all(unicode('服务器状态报告', 'utf-8'), table_list, 'html')


if __name__ == '__main__':
    # server_model = server_constant.get_server_model('guoxin')
    # __compare_server_time_cmd(server_model)
    # trade_servers = server_constant.get_trade_servers()
    server_status_monitor_job(['test1', ])
