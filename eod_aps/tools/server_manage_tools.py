# -*- coding: utf-8 -*-
# 对托管服务器的服务进行管理工具类
import time
import re
from eod_aps.model.schema_common import AppInfo
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.model.eod_const import const
from cfg import custom_log

date_utils = DateUtils()
email_utils = EmailUtils(const.EMAIL_DICT['group2'])
server_host = server_constant.get_server_model('host')
start_check_str = 'TradingFramework start command loop now.'


def get_service_dict():
    """
    从本地数据库中获取远程服务器的服务列表
    """
    sever_info_db_dict = dict()
    session = server_host.get_db_session('common')
    query = session.query(AppInfo)
    for sever_info_db in query:
        if sever_info_db.server_name in sever_info_db_dict:
            sever_info_db_dict[sever_info_db.server_name].append(sever_info_db)
        else:
            sever_info_db_dict[sever_info_db.server_name] = [sever_info_db]
    return sever_info_db_dict


def get_service_list(server_name):
    """
    从本地数据库中获取远程服务器的服务列表
    """
    service_db_dict = get_service_dict()
    return service_db_dict[server_name]


def start_tradeplat(server_name):
    """
    整体启动tradeplat服务
    """
    service_list = get_service_list(server_name)
    server_level1 = []
    server_level2 = []
    for server_info in service_list:
        if server_info.level == 1:
            server_level1.append('./script/%s' % server_info.start_file)
        elif server_info.level == 2:
            server_level2.append('./script/%s' % server_info.start_file)

    server_model = server_constant.get_server_model(server_name)
    mainframe_flag = start_server_mainframe(server_model)

    if mainframe_flag:
        start_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_project_folder'],
                          ';'.join(server_level2)
        ]
        server_model.run_cmd_str(';'.join(start_cmd_list))
    else:
        email_utils.send_email_group_all('[Error]MainFrame Start Error_%s' % server_name, '')


def check_after_start_tradeplat(server_name):
    """
    tradeplat启动后检查
    """
    filter_date_str = date_utils.get_today_str()
    server_model = server_constant.get_server_model(server_name)
    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                "grep -a '%s' screenlog*_%s*.log" % (start_check_str, filter_date_str)
                ]
    grep_result_str = server_model.run_cmd_str(';'.join(cmd_list))
    if grep_result_str == '':
        return []

    service_log_dict = dict()
    for grep_result_item in grep_result_str.split('\n'):
        reg = re.compile(
            'screenlog_(?P<service_name>[^_]*)_(?P<date_str>[^-]*)-(?P<start_time>[^ ]*).log:(?P<log_content>.*)')
        reg_match = reg.match(grep_result_item)
        line_dict = reg_match.groupdict()

        service_name = line_dict['service_name']
        start_time = line_dict['start_time']
        if service_name in service_log_dict and service_log_dict[service_name] > int(start_time):
            continue
        service_log_dict[service_name] = int(start_time)

    error_message_list = []
    max_start_time = max(service_log_dict.values())
    service_list = get_service_list(server_name)
    for service_db_item in service_list:
        if service_db_item.app_name == 'serving_proxy':
            continue

        if service_db_item.app_name in service_log_dict:
            diff_time = max_start_time - service_log_dict[service_db_item.app_name]
            if diff_time > 120:
                error_message_list.append('Server:%s, App:%s Start Time Error!diff:%s' % (server_name, service_db_item.app_name, diff_time))
        else:
            error_message_list.append('Server:%s, App:%s Start Error!' % (server_name, service_db_item.app_name))
    return error_message_list


def stop_tradeplat(server_name):
    """
    整体关闭tradeplat服务
    """
    server_list = get_service_list(server_name)
    server_level1 = []
    server_level2 = []
    for server_info in server_list:
        if server_info.level == 1:
            server_level1.append(server_info)
        elif server_info.level == 2:
            server_level2.append(server_info)

    stop_sever_cmd_template = 'screen -r %s -X quit'
    stop_sever_cmd_list = []
    for server_info in server_level2:
        stop_sever_cmd_list.append(stop_sever_cmd_template % server_info.app_name)
    for server_info in server_level1:
        stop_sever_cmd_list.append(stop_sever_cmd_template % server_info.app_name)

    server_model = server_constant.get_server_model(server_name)
    server_model.run_cmd_str(';'.join(stop_sever_cmd_list))


def quit_tradeplat(server_name):
    """
    根据数据库server_info表配置依次quit的方式关闭服务
    """
    server_list = get_service_list(server_name)
    server_level1 = []
    server_level2 = []
    for server_info in server_list:
        if server_info.level == 1:
            server_level1.append(server_info)
        elif server_info.level == 2:
            server_level2.append(server_info)

    for server_info in server_level2:
        quit_server_service(server_name, server_info.app_name)

    for server_info in server_level1:
        quit_server_service(server_name, server_info.app_name)


def stop_server_service(server_name, service_name):
    """
    以-x的方式关闭单个服务
    """
    server_model = server_constant.get_server_model(server_name)
    stop_sever_cmd = 'screen -r %s -X quit' % service_name
    server_model.run_cmd_str(stop_sever_cmd)


def quit_server_service(server_name, service_name):
    """
    以quit的方式关闭单个服务
    """
    server_model = server_constant.get_server_model(server_name)
    tmp_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                    '/home/trader/anaconda2/bin/python screen_tools.py -s %s -c "quit"' % service_name]
    server_model.run_cmd_str(';'.join(tmp_cmd_list))

    time.sleep(3)
    pkill_server_service(server_name, service_name)


def pkill_server_service(server_name, service_name):
    """
    以pkill的方式关闭单个服务
    """
    server_model = server_constant.get_server_model(server_name)
    pkill_sever_cmd = 'pkill %s' % service_name
    server_model.run_cmd_str(pkill_sever_cmd)


def start_server_service(server_name, service_name):
    """
    启动单个服务
    """
    server_model = server_constant.get_server_model(server_name)
    server_list = get_service_list(server_name)
    for server_info in server_list:
        if server_info.app_name != service_name:
            continue

        start_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_project_folder'],
                          './script/%s' % server_info.start_file
                         ]
    server_model.run_cmd_str(';'.join(start_cmd_list))


def start_service_omaproxy(server_name):
    """
    启动服务omaproxy
    """
    server_model = server_constant.get_server_model(server_name)
    start_cmd_list = ['cd %s' % server_model.server_path_dict['omaproxy_project_folder'],
                      './start.omaproxy.sh'
                      ]
    server_model.run_cmd_str(';'.join(start_cmd_list))


def restart_server_service(server_name, service_name):
    """
    重启单个服务，以-x的方式关闭
    """
    stop_server_service(server_name, service_name)
    time.sleep(1)
    start_server_service(server_name, service_name)


def start_server_mainframe(server_model):
    """
    启动MainFrame服务
    """
    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_project_folder'],
                './script/start.mainframe.sh'
    ]
    server_model.run_cmd_str(';'.join(cmd_list))
    time.sleep(10)

    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                'ls *MainFrame*.log'
    ]
    log_file_info = server_model.run_cmd_str(';'.join(cmd_list))
    log_file_list = []
    for log_file_name in log_file_info.split('\n'):
        if len(log_file_name) > 0:
            log_file_list.append(log_file_name)
    log_file_list.sort()

    log_file_name = log_file_list[-1]
    filter_date_str = date_utils.get_today_str()
    if filter_date_str not in log_file_name:
        custom_log.log_error_task("[Error]start_server_mainframe---today_str:%s, log file:%s" %
                          (filter_date_str, log_file_name))
        return False

    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                'tail -100 %s' % log_file_name
                ]
    log_info = server_model.run_cmd_str(';'.join(cmd_list))

    try_times_index = 1
    while 'TradingFramework start command loop now.' not in log_info and try_times_index <= 10:
        time.sleep(10)
        log_info = server_model.run_cmd_str(';'.join(cmd_list))
        try_times_index += 1

    if try_times_index > 10:
        return False
    else:
        return True


def save_pf_position(server_name):
    """
    保存策略仓位
    """
    strategy_service_list = ['MainFrame', 'StrategyLoader',  'FutStrategyLoader', 'StrategyAccountLoader', 'IndexArb',
                             'ETFStrategy', 'OptionStrategy', 'SFStrategy', 'CalendarMA', 'OMAServer', 'CalendarSpread',
                             'CloseLockedPosition']
    for strategy_service in strategy_service_list:
        server_service_rum_cmd(server_name, strategy_service, 'save pf_position')


def server_service_rum_cmd(server_name, service_name, cmd_str):
    """
    单个服务执行命令接口
    """
    server_model = server_constant.get_server_model(server_name)
    tmp_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                    '/home/trader/anaconda2/bin/python screen_tools.py -s %s -c "%s"' % (service_name, cmd_str)]
    server_model.run_cmd_str(';'.join(tmp_cmd_list))


if __name__ == '__main__':
    # server_model = server_constant.get_server_model('test_118')
    # start_server_mainframe(server_model)
    # save_pf_position('guoxin')
    # save_pf_position('zhongxin')
    print check_after_start_tradeplat('nanhua')
