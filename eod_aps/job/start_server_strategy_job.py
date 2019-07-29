# -*- coding: utf-8 -*-
import traceback
from eod_aps.model.schema_strategy import StrategyOnline
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.tradeplat_message_tools import socket_init, send_strategy_parameter_change_request_msg, \
send_strategy_account_change_request_msg, send_strategy_info_request_msg, send_login_msg,\
send_tradeserverinfo_request_msg, send_subscribetradeserverinfo_request_msg
from eod_aps.job import *


common_utils = CommonUtils()


def start_server_strategy_job(server_name_tuple):
    try:
        socket = socket_init('aggregator')
        send_login_msg(socket)
        trade_server_info_msg = send_tradeserverinfo_request_msg(socket)
        trade_server_info_list = []
        for trade_server_info in trade_server_info_msg.TradeServerInfo:
            trade_server_info_list.append(trade_server_info)
        send_subscribetradeserverinfo_request_msg(socket, trade_server_info_list)

        for server_name in server_name_tuple:
            __start_server_strategy(socket, server_name)
    except Exception:
        email_utils2.send_email_group_all('[Error]Server:aggregator ZMQ Error', '', 'html')
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)


def stop_server_strategy_job(server_name_tuple):
    try:
        socket = socket_init('aggregator')
        send_login_msg(socket)
        trade_server_info_msg = send_tradeserverinfo_request_msg(socket)
        trade_server_info_list = []
        for trade_server_info in trade_server_info_msg.TradeServerInfo:
            trade_server_info_list.append(trade_server_info)
        send_subscribetradeserverinfo_request_msg(socket, trade_server_info_list)

        for server_name in server_name_tuple:
            __stop_server_strategy(socket, server_name)
    except Exception:
        email_utils2.send_email_group_all('[Error]Server:aggregator ZMQ Error', '', 'html')
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)


def __start_server_strategy(socket, server_name):
    strategy_name_list = __query_strategy_list(server_name)
    __send_control_message(socket, server_name, strategy_name_list, True)


def __stop_server_strategy(socket, server_name):
    strategy_name_list = __query_strategy_list(server_name)
    __send_control_message(socket, server_name, strategy_name_list, False)


def __query_strategy_list(server_name):
    strategy_name_list = []
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.enable == 1,
                                           StrategyOnline.target_server.like('%' + server_name + '%')):
        strategy_name_list.append(strategy_online_db.name)
    server_host.close()
    return strategy_name_list


def __send_control_message(socket, server_name, strategy_name_list, control_flag):
    server_model = server_constant.get_server_model(server_name)
    location_str = server_model.connect_address.replace('tcp://', '')

    for strategy_name in strategy_name_list:
        recv_result = send_strategy_parameter_change_request_msg(socket, strategy_name, location_str, control_flag)
        if control_flag:
            custom_log.log_info_job(
                'Send Start Message.Server:%s,Strategy:%s,Recv Result:%s' % (server_name, strategy_name, recv_result))
        else:
            custom_log.log_info_job(
                'Send Stop Message.Server:%s,Strategy:%s,Recv Result:%s' % (server_name, strategy_name, recv_result))
    # 偶发情况个别策略没启动成功，增加间隔5秒后重发一次
    # time.sleep(10)
    # socket = socket_init(server_name)
    # for strategy_name in strategy_name_list:
    #     send_strategy_parameter_change_request_msg(socket, strategy_name, control_flag)


# 修改策略Account的工具
def change_strategy_account(server_name):
    strategy_name_list = __query_strategy_list(server_name)
    socket = socket_init(server_name)
    for strategy_name in strategy_name_list:
        send_strategy_account_change_request_msg(socket, strategy_name)


# 查询策略启动状态
def query_strategy_status():
    strategy_status_dict = dict()
    socket = socket_init('aggregator')
    send_login_msg(socket)
    trade_server_info_msg = send_tradeserverinfo_request_msg(socket)
    trade_server_info_list = []
    for trade_server_info in trade_server_info_msg.TradeServerInfo:
        trade_server_info_list.append(trade_server_info)
    send_subscribetradeserverinfo_request_msg(socket, trade_server_info_list)

    strategy_info_response_msg = send_strategy_info_request_msg(socket, False)
    for strats_info in strategy_info_response_msg.Strats:
        server_name = common_utils.get_server_name(strats_info.Location)
        strategy_status_dict['%s|%s' % (server_name, strats_info.Name)] = strats_info.IsEnabled
    return strategy_status_dict


if __name__ == '__main__':
    # query_strategy_status('aggregator')
    start_server_strategy_job(['nanhua', 'zhongxin', 'luzheng'])
