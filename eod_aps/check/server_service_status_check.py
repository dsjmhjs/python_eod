# -*- coding: utf-8 -*-
from eod_aps.check import *
from eod_aps.model.schema_common import AppInfo
from eod_aps.model.server_constans import server_constant

aggregator_server_name = 'aggregator'


def __query_aggregator_status():
    try:
        server_model = server_constant.get_server_model(aggregator_server_name)
        ssh_result = server_model.run_cmd_str('screen -ls')
    except Exception:
        ssh_result = ''

    if 'Aggregator' in ssh_result:
        aggregator_status = True
    else:
        aggregator_status = False
    return aggregator_status


def kill_aggregator_check(job_name):
    aggregator_status = __query_aggregator_status()
    if aggregator_status:
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, 'Aggregator Still Active!')


def start_aggregator_check(job_name):
    aggregator_status = __query_aggregator_status()
    if not aggregator_status:
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, 'Aggregator Still InActive!')


def __query_server_service_status(server_name, service_list):
    if server_name in const.CUSTOMIZED_SERVICES_MAP:
        service_list.extend(const.CUSTOMIZED_SERVICES_MAP[server_name])

    try:
        server_model = server_constant.get_server_model(server_name)
        ssh_result = server_model.run_cmd_str2('screen -ls')
    except Exception:
        ssh_result = []

    service_status_dict = dict()
    for service_name in service_list:
        detached_flag = False

        for screen_item in ssh_result:
            if service_name in screen_item and 'Detached' in screen_item:
                detached_flag = True
                break
        service_status_dict[service_name] = detached_flag
    return service_status_dict


def stop_service_check(job_name):
    if job_name == 'stop_service_am':
        server_list = server_constant.get_night_session_servers()
    elif job_name == 'stop_service_pm':
        server_list = server_constant.get_trade_servers()
    else:
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, 'Undefined Job Name:%s' % job_name)
        return

    server_service_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    query = session_common.query(AppInfo)
    for sever_info_db in query:
        server_service_dict.setdefault(sever_info_db.server_name, []).append(sever_info_db.app_name)
    server_host.close()

    error_message_list = []
    for server_name in server_list:
        service_status_dict = __query_server_service_status(server_name, server_service_dict[server_name])
        for service_name, detached_flag in service_status_dict.items():
            if detached_flag:
                error_message_list.append('Server:%s Service:%s Is Active' % (server_name, service_name))

    if error_message_list:
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, '\n'.join(error_message_list))


def start_service_check(job_name):
    if job_name == 'start_server_am':
        server_list = server_constant.get_trade_servers()
    elif job_name == 'start_server_pm':
        server_list = server_constant.get_night_session_servers()
    else:
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, 'Undefined Job Name:%s' % job_name)
        return

    server_service_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    query = session_common.query(AppInfo)
    for sever_info_db in query:
        server_service_dict.setdefault(sever_info_db.server_name, []).append(sever_info_db.app_name)
    server_host.close()

    error_message_list = []
    for server_name in server_list:
        service_status_dict = __query_server_service_status(server_name, server_service_dict[server_name])
        for service_name, detached_flag in service_status_dict.items():
            if not detached_flag:
                error_message_list.append('Server:%s Service:%s Is Inactive' % (server_name, service_name))

    if error_message_list:
        email_utils2.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, '\n'.join(error_message_list))
