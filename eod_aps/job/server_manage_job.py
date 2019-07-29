# -*- coding: utf-8 -*-
# 对托管服务器的服务进行管理，启动、停止、重启服务
import threading
import time
import traceback

from eod_aps.job import *
from eod_aps.tools.server_manage_tools import start_tradeplat, stop_tradeplat, check_after_start_tradeplat


# 启动ctp行情落地程序
def receive_future_market_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__receive_future_market_job, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def __receive_future_market_job(server_name):
    server_model = server_constant.get_server_model(server_name)
    run_cmd_list = ['screen -r DerivativesClient_market -X quit',
                    'cd %s' % server_model.derivatives_client_folder,
                    './script/start.market_ctp.sh'
                    ]
    server_model.run_cmd_str(';'.join(run_cmd_list))


def __start_servers_tradeplat(server_name):
    from eod_aps.job.account_position_check_job import account_position_check_job
    try:
        if account_position_check_job(server_name):
            stop_tradeplat(server_name)
            start_tradeplat(server_name)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__start_servers_tradeplat:%s.' % server_name, error_msg)


def start_servers_tradeplat(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__start_servers_tradeplat, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    time.sleep(60)
    # 通过日志校验各服务是否启动成功
    start_flag = True
    all_error_message_list = []
    for server_name in server_name_tuple:
        error_message_list = check_after_start_tradeplat(server_name)
        all_error_message_list.extend(error_message_list)
    if all_error_message_list:
        email_utils2.send_email_group_all('[Error]Service Start Error!', '\n'.join(all_error_message_list))
        start_flag = False
    return start_flag


def stop_servers_tradeplat(server_name_tuple):
    for server_name in server_name_tuple:
        stop_tradeplat(server_name)


if __name__ == '__main__':
    # server_model = server_constant.get_server_model('test_118')
    # start_server_mainframe(server_model)
    # stop_tradeplat('guoxin')
    # stop_tradeplat('nanhua')
    stop_tradeplat('huabao')
