# -*- coding: utf-8 -*-
"""检测服务器端口和服务是否存在异常"""
import socket
import threading
import subprocess
import re
import traceback
import urllib
from xmlrpclib import ServerProxy
from eod_aps.model.schema_common import AppInfo
from eod_aps.model.schema_jobs import HardWareInfo
from eod_aps.model.schema_portfolio import RealAccount
from eod_aps.model.schema_strategy import StrategyOnline
from eod_aps.tools.weChat_tools import WechatPush
from eod_aps.job import *

ERROR_INFO_LIST = []
FTP_FILES_DICT = dict()
SERVERS_DICT = dict()


# 检测服务状态
def __server_service_monitor(server_model):
    server_name = server_model.name
    service_array = []
    service_array.extend(SERVERS_DICT[server_name])
    if server_name in const.CUSTOMIZED_SERVICES_MAP:
        service_array.extend(const.CUSTOMIZED_SERVICES_MAP[server_name])

    screen_result_list = server_model.run_cmd_str2('screen -ls')
    for service_name in service_array:
        detached_flag = False

        for screen_item in screen_result_list:
            if service_name in screen_item and 'Detached' in screen_item:
                detached_flag = True
                break

        if not detached_flag:
            ERROR_INFO_LIST.append('Server:%s Service:%s Is Inactive' % (server_name, service_name))


# 检测数据端口状态
def __server_port_monitor(server_model):
    port_array = server_model.check_port_list.split(',')

    port_monitor_dict = dict()
    grep_cmd = 'netstat -an | grep tcp | grep -E "%s" ' % '|'.join(port_array)
    net_message_list = server_model.run_cmd_str2(grep_cmd)
    for net_message in net_message_list:
        for port_str in port_array:
            find_key = ':%s' % port_str
            if find_key in net_message and 'ESTABLISHED' in net_message:
                if port_str in port_monitor_dict:
                    port_monitor_dict[port_str] += 1
                else:
                    port_monitor_dict[port_str] = 1

    for port_str in port_array:
        if port_str not in port_monitor_dict:
            ERROR_INFO_LIST.append('Server:%s Port:%s is Disconnect' % (server_model.name, port_str))
        elif port_monitor_dict[port_str] >= 40:
            ERROR_INFO_LIST.append('Server:%s Port:%s Connection Number is:%s' %
                                   (server_model.name, port_str, port_monitor_dict[port_str]))

    # for port in port_array:
    #     connect_flag = True
    #     grep_cmd = 'netstat -an | grep tcp | grep %s' % port
    #     netstat_return = server_model.run_cmd_str(grep_cmd)
    #     if netstat_return is None:
    #         connect_flag = False
    #     else:
    #         for port_info in netstat_return.split('\n'):
    #             if port_info == '' or port_info is None:
    #                 continue
    #             if 'ESTABLISHED' not in port_info:
    #                 connect_flag = False
    #                 break
    #
    #     if not connect_flag:
    #         error_info_list.append('Server:%s Port:%s is Disconnect' % (server_model.name, port))


def __server_status_monitor(server_model):
    log_cmd_list = ["cd %s" % server_model.server_path_dict['tradeplat_log_folder'],
                    "grep 'PANIC' error*.log",
                    "grep 'NORMAL' screenlog_MainFrame*.log"
                    ]
    return_message_items = server_model.run_cmd_str2(";".join(log_cmd_list))
    if not return_message_items:
        return

    format_message_dict = dict()
    for return_message_item in return_message_items:
        date_str = re.findall(r"(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", return_message_item)[0]
        format_message_dict[date_str] = return_message_item

    max_time = max(format_message_dict.keys())
    check_message = format_message_dict[max_time]

    now_date_str = date_utils.get_today_str('%Y-%m-%d')
    if 'PANIC' in check_message:
        date_str = re.findall(r"(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", check_message)[0]
        title = '[Error]Server:%s Is PANIC.Time:%s ' % (server_model.name, date_str)

        if now_date_str not in date_str:
            return
        performance = title
        remark = title
        wechat_push = WechatPush()
        wechat_push.send_message(title, date_str, performance, remark)

        result_message_list = __server_error_message(server_model)
        email_utils2.send_email_group_all(title, result_message_list)


def __server_error_message(server_model):
    log_status_cmd = "cd %s;ls error*.log" % server_model.server_path_dict['tradeplat_log_folder']
    error_file_list = server_model.run_cmd_str2(log_status_cmd)
    if not error_file_list:
        return ''

    result_message_list = []
    for error_file_name in error_file_list:
        result_message_list.append('FileName:%s' % error_file_name)
        cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                    'tail %s' % error_file_name
                    ]
        error_message = server_model.run_cmd_str(';'.join(cmd_list))
        result_message_list.append(error_message)
    return '\n'.join(result_message_list)


# 监测ftp服务器
def __deposit_server_ftp_monitor(server_model):
    ftp_wsdl_address = server_model.ftp_wsdl_address
    ftp_server = ServerProxy(ftp_wsdl_address)

    ftp_download_folder = server_model.ftp_download_folder
    monitor_folder = '%s/%s' % (ftp_download_folder, date_utils.get_today_str('%Y%m%d'))
    ftp_file_list = ftp_server.listdir(monitor_folder)

    local_email_folder = '%s/email' % SERVER_DAILY_FILES_FOLDER_TEMPLATE % server_model.name

    if server_model.name in FTP_FILES_DICT:
        last_file_list = FTP_FILES_DICT[server_model.name]
    else:
        last_file_list = []

    for ftp_file_name in ftp_file_list:
        if ftp_file_name in last_file_list:
            continue
        if not ftp_file_name.startswith('email'):
            continue

        source_file_path = '%s/%s' % (monitor_folder, ftp_file_name)
        target_file_path = '%s/%s' % (local_email_folder, ftp_file_name)
        ftp_server.download_file(source_file_path, target_file_path)

        with open(target_file_path, 'rb') as fr:
            email_content_list = fr.readlines()
        email_title = 'Deposit_Server_Email[%s]' % server_model.name
        email_utils2.send_email_group_all(email_title, ''.join(email_content_list), 'html')
    FTP_FILES_DICT[server_model.name] = ftp_file_list


def __server_monitor(server_name):
    try:
        custom_log.log_info_job('Check Server[%s] Status Start.' % server_name)
        server_model = server_constant.get_server_model(server_name)
        __server_service_monitor(server_model)
        __server_port_monitor(server_model)
        __server_status_monitor(server_model)
        if server_model.type == 'deposit_server' and server_model.is_ftp_monitor:
            __deposit_server_ftp_monitor(server_model)
        server_model.close()
        custom_log.log_info_job('Check Server[%s] Status Stop!' % server_name)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__server_monitor:%s.' % server_name, error_msg)


# 夜盘监测项
def __server_monitor_night(server_name):
    try:
        custom_log.log_info_job('Check Server:%s Start.' % server_name)
        server_model = server_constant.get_server_model(server_name)
        __server_service_monitor(server_model)
        __server_status_monitor(server_model)
        server_model.close()
        custom_log.log_info_job('Check Server:%s Stop.' % server_name)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__server_monitor_night:%s.' % server_name, error_msg)


def __build_servers_dict():
    if SERVERS_DICT:
        return

    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    query = session_common.query(AppInfo)
    for sever_info_db in query:
        if sever_info_db.server_name in SERVERS_DICT:
            SERVERS_DICT[sever_info_db.server_name].append(sever_info_db.app_name)
        else:
            SERVERS_DICT[sever_info_db.server_name] = [sever_info_db.app_name]
    server_host.close()


def __server_connection_monitor(server_name):
    try:
        custom_log.log_info_job('-----------start check vpn:%s-------------' % server_name)
        server_model = server_constant.get_server_model(server_name)
        connect_flag = server_model.check_connect()
        if not connect_flag:
            subject = '[Error]VPN Check Result_' + server_name
            email_utils2.send_email_group_all(subject, 'ping %s fail' % server_model.ip)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__server_connection_monitor:%s.' % server_name, error_msg)


def server_monitor_index_job(server_name_tuple):
    """
    程序入口
    :param server_name_tuple:
    :return:
    """
    # if not date_utils.is_trading_time():
    #     return

    global ERROR_INFO_LIST
    ERROR_INFO_LIST = []

    night_market_flag = date_utils.is_night_market()

    __build_servers_dict()
    threads = []
    for server_name in server_name_tuple:
        if night_market_flag:
            thread = threading.Thread(target=__server_monitor_night, args=(server_name,))
            threads.append(thread)
        else:
            thread = threading.Thread(target=__server_monitor, args=(server_name,))
            threads.append(thread)

    # 启动所有线程
    for thread in threads:
        thread.start()

    # 主线程中等待所有子线程退出
    for thread in threads:
        thread.join()

    if ERROR_INFO_LIST:
        email_utils2.send_email_group_all('[Error]Server Monitor Error Info', '\n'.join(ERROR_INFO_LIST))


def server_connection_monitor_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        thread = threading.Thread(target=__server_connection_monitor, args=(server_name,))
        threads.append(thread)

    # 启动所有线程
    for thread in threads:
        thread.start()

    # 主线程中等待所有子线程退出
    for thread in threads:
        thread.join()


def __local_server_connection_monitor_job(hardware_info_db):
    check_flag = False
    if 'Win' in hardware_info_db.operating_system:
        check_port = 445
    elif 'Ubuntu' in hardware_info_db.operating_system or 'CenterOS' in hardware_info_db.operating_system:
        check_port = 22
    else:
        custom_log.log_error_job('IP:%s Operating_System UnClear!' % hardware_info_db.ip)
        return check_flag

    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.settimeout(1)
    try:
        sk.connect((hardware_info_db.ip, check_port))
        custom_log.log_debug_job('Server:%s Connected!' % hardware_info_db.ip)
        check_flag = True
    except Exception:
        custom_log.log_error_job('[Error]Server Connect Fail:%s!' % hardware_info_db.ip)
    finally:
        sk.close()
    return check_flag


def local_server_connection_monitor_job():
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    error_list = []
    for hardware_info_db in session_job.query(HardWareInfo).filter(HardWareInfo.enable == 1):
        if '.10.' not in hardware_info_db.ip and '.12.' not in hardware_info_db.ip:
            continue
        check_result = __local_server_connection_monitor_job(hardware_info_db)
        if not check_result:
            error_list.append('ip:%s can not connect!' % hardware_info_db.ip)
    if len(error_list) > 0:
        email_utils2.send_email_group_all('[Error]IP Check Error', '\n'.join(error_list))


def __analysis_strategy_start_message(line_str):
    reg = re.compile(
        '^.*\[(?P<date>.*)\] \[(?P<from>.*)\] \[(?P<message_type>.*)\] (?P<strategy_type>[^:]*): (?P<strategy_name>[^,]*), IsEnable=(?P<isenable>[^ ]*)')

    reg_match = reg.match(line_str)
    line_dict = reg_match.groupdict()
    return line_dict


def order_route_log_check(server_name_list):
    email_message_list = ['<br><br><h>Order Route check:</h>']
    for server_name in server_name_list:
        if server_name not in ORDER_ROUTE_LOG_DICT:
            continue

        server_model = server_constant.get_server_model(server_name)
        check_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder']]
        for check_message in ORDER_ROUTE_LOG_DICT[server_name].split('|'):
            check_cmd_list.append("grep '%s' *.log" % check_message)
        return_message = server_model.run_cmd_str(';'.join(check_cmd_list))
        if return_message == '':
            continue
        email_message_list.append(return_message)
    return email_message_list


# 策略启动检查
def strategy_start_check(server_name_list):
    strategy_online_list = []
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)
    for strategy_online in query.filter(StrategyOnline.enable == 1):
        strategy_online_list.append(strategy_online)
    server_host.close()

    server_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        check_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                          'grep -E "Start Strategy|Stop Strategy" screen*.log'
                          ]
        return_message_items = server_model.run_cmd_str2(";".join(check_cmd_list))
        if len(return_message_items) == 0:
            continue

        format_message_list = []
        for return_message_item in return_message_items:
            line_dict = __analysis_strategy_start_message(return_message_item)
            format_message_list.append((line_dict['strategy_name'], line_dict['date'], line_dict['isenable']))
        format_message_list.sort()

        strategy_state_dict = {x[0]: '%s|%s' % (x[1], x[2]) for x in format_message_list}
        server_dict[server_name] = strategy_state_dict

    email_message_list = ['<h>Strategy Start Check:</h><br>']
    html_title = 'Strategy Name,%s' % ','.join(server_name_list)
    table_list = []
    for strategy_online_db in strategy_online_list:
        tr_list = []
        strategy_name = strategy_online_db.name
        target_server = strategy_online_db.target_server
        tr_list.append(strategy_name)
        for server_name in server_name_list:
            if server_name not in server_dict:
                status = ''
                if server_name in target_server:
                    status = 'Inactive(Error)'
                tr_list.append(status)
                continue

            strategy_state_dict = server_dict[server_name]
            if strategy_name in strategy_state_dict:
                strategy_state_message = strategy_state_dict[strategy_name]
                if 'false' in strategy_state_message:
                    tr_list.append('Inactive %s(Error)' % strategy_state_dict[strategy_name])
                else:
                    strategy_start_time_str = strategy_state_message.split('.')[0]
                    time_diff = date_utils.get_interval_seconds(strategy_start_time_str,
                                                                date_utils.get_today_str("%Y-%m-%d %H:%M:%S"))
                    if time_diff > 3600:
                        tr_list.append('Active %s(Error)' % strategy_state_dict[strategy_name])
                    else:
                        tr_list.append('Active %s' % strategy_state_dict[strategy_name])
            else:
                status = ''
                if server_name in target_server:
                    status = 'Inactive(Error)'
                tr_list.append(status)
        table_list.append(tr_list)
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_message_list.append(''.join(html_list))
    return email_message_list


def check_ts_order_group(server_name_list):
    email_message_list = ['<h>TS OrdGROUP Connect Check:</h><br>']

    connect_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        today_filter_str = date_utils.get_today_str('%Y-%m-%d')
        monitor_cmd_list = ["cd %s" % server_model.server_path_dict['tradeplat_log_folder'],
                            "grep 'connect to ts' screenlog_OrdGROUP_%s-*" % today_filter_str.replace('-', '')
                            ]
        return_message_items = server_model.run_cmd_str2(";".join(monitor_cmd_list))
        if not return_message_items:
            continue

        for return_message_item in return_message_items:
            try:
                reg = re.compile(
                    '^.*\[(?P<date>.*)\] \[(?P<from>.*)\] \[(?P<message_type>.*)\] connect to ts (?P<connect_result>[^:]*): (?P<connect_ip>.*),')
                reg_match = reg.match(return_message_item)
                line_dict = reg_match.groupdict()
                connect_result = line_dict['connect_result']
                connect_ip = line_dict['connect_ip']
                connect_dict['%s|%s' % (server_name, connect_ip)] = connect_result
            except AttributeError:
                continue

    table_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        account_id_list = []
        query = session_portfolio.query(RealAccount)
        for account_db in query.filter(RealAccount.enable == 1, RealAccount.accounttype == 'TS'):
            account_id_list.append(account_db.accountid)
            account_config_dict = dict(
                [x.replace(' ', '').split('=', 1) for x in account_db.accountconfig.split('\r\n')])
            connect_ip_str = 'tcp://%s:%s' % (account_config_dict['Address'], account_config_dict['Port'])

            find_key = '%s|%s' % (server_name, connect_ip_str)
            if find_key in connect_dict:
                connect_value = connect_dict[find_key]

                if 'fail' == connect_value:
                    connect_value = '%s(Error)' % connect_value
            else:
                connect_value = '(Error)'

            port_status_check_cmd = 'netstat -an|grep %s' % account_config_dict['Port']
            port_check_rst = server_model.run_cmd_str2(port_status_check_cmd)
            if port_check_rst:
                port_status = "true"
            else:
                port_status = "false(Error)"
            tr_list = [server_name, account_db.accountname, connect_ip_str, connect_value, port_status]
            table_list.append(tr_list)
    html_title = 'server_name,account_name,connect_ip,connect_result,port_status'
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_message_list.append(''.join(html_list))
    return email_message_list


def query_ip_url_check():
    try:
        query_ip_url = const.EOD_CONFIG_DICT['query_ip_url']
        page = urllib.urlopen(query_ip_url)
        html = page.read()
    except Exception:
        subject = '[Error]Query Ip Url Miss'
        email_utils2.send_email_group_all(subject, 'please check query_ip_url:%s' % query_ip_url)


if __name__ == '__main__':
    # cta_server_list = server_constant.get_cta_servers()
    # server_connection_monitor_job(cta_server_list)
    local_server_connection_monitor_job()
