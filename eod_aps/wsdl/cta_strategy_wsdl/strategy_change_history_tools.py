# -*- coding: utf-8 -*-
from eod_aps.model.schema_strategy import StrategyParameter, StrategyChangeHistory, StrategyOnline
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.date_utils import DateUtils
from eod_aps.wsdl.cta_strategy_wsdl import *
from cfg import custom_log

server_constant = ServerConstant()
date_utils = DateUtils()


def __strategy_online(session, strategy_change_history):
    query = session.query(StrategyOnline)
    strategy_online_db = query.filter(StrategyOnline.name == strategy_change_history.name).first()
    if strategy_online_db is None:
        custom_log.log_error_task('unfind strategy:%s' % strategy_change_history.name)
        return

    target_server_list = strategy_online_db.target_server.split('|')
    parameter_server_list = strategy_online_db.parameter_server.split('|')
    for index in range(0, len(target_server_list)):
        server_name = target_server_list[index]
        parameter_value = parameter_server_list[index]

        server_model = server_constant.get_server_model(server_name)
        session_strategy = server_model.get_db_session('strategy')

        strategy_parameter = StrategyParameter()
        strategy_parameter.time = date_utils.get_now()
        strategy_parameter.name = strategy_online_db.name
        strategy_parameter.value = parameter_value
        session_strategy.add(strategy_parameter)
        session_strategy.commit()

    strategy_online_db.enable = 1
    session.merge(strategy_online_db)


def __strategy_offline(session, strategy_change_history):
    query = session.query(StrategyOnline)
    strategy_online_db = query.filter(StrategyOnline.name == strategy_change_history.name).first()
    if strategy_online_db is None:
        custom_log.log_error_task('unfind strategy:%s' % strategy_change_history.name)
        return

    strategy_online_db.enable = 0
    session.merge(strategy_online_db)
    strategy_change_history.parameter_server = strategy_online_db.parameter_server


def __strategy_update(session, strategy_change_history):
    query = session.query(StrategyOnline)
    strategy_online_db = query.filter(StrategyOnline.name == strategy_change_history.name).first()
    if strategy_online_db is None:
        custom_log.log_error_task('unfind strategy:%s' % strategy_change_history.name)
        return

    target_server_list = strategy_online_db.target_server.split('|')
    parameter_server_list = strategy_online_db.parameter_server.split('|')

    temp_parameter_server_list = []
    for index in range(0, len(target_server_list)):
        target_server_name = target_server_list[index]
        parameter_value = parameter_server_list[index]

        if target_server_name != strategy_change_history.change_server_name:
            temp_parameter_server_list.append(parameter_value)
            continue

        temp_parameter_server_list.append(strategy_change_history.parameter_server)

        server_model = server_constant.get_server_model(strategy_change_history.change_server_name)
        session_strategy = server_model.get_db_session('strategy')
        strategy_parameter = StrategyParameter()
        strategy_parameter.time = date_utils.get_now()
        strategy_parameter.name = strategy_online_db.name
        strategy_parameter.value = strategy_change_history.parameter_server
        session_strategy.add(strategy_parameter)
        session_strategy.commit()

    strategy_online_db.parameter_server = '|'.join(temp_parameter_server_list)
    session.merge(strategy_online_db)


def strategy_change_history_job():
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyChangeHistory)

    for strategy_change_history in query.filter(StrategyChangeHistory.enable == 1):
        if strategy_change_history.change_type == 'online':
            # 策略上线
            __strategy_online(session_strategy, strategy_change_history)
        elif strategy_change_history.change_type == 'offline':
            # 策略下线
            __strategy_offline(session_strategy, strategy_change_history)
        elif strategy_change_history.change_type == 'update':
            # 策略参数修改
            __strategy_update(session_strategy, strategy_change_history)

        strategy_change_history.enable = 0
        strategy_change_history.update_time = date_utils.get_now()
        session_strategy.merge(strategy_change_history)
    session_strategy.commit()

    # 更新服务器的配置文件
    for server_name in server_constant.get_cta_servers():
        __rebuild_strategy_loader_file(server_host, server_name)
    server_host.close()


def __rebuild_strategy_loader_file(server_host, server_name):
    strategy_online_list = []
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.enable == 1, StrategyOnline.strategy_type == 'CTA',
                                           StrategyOnline.target_server.like('%' + server_name + '%')):
        strategy_online_list.append(strategy_online_db)

    line_list = []
    with open('%s/config.strategyloader_%s.txt' % (STRATEGYLOADER_FILE_PATH, server_name), 'rb') as fr:
        for line in fr.readlines():
            line_list.append(line.replace('\n', ''))

    for strategy_online_db in strategy_online_list:
        line_list.append('[Strategy.lib%s.%s]' % (strategy_online_db.assembly_name, strategy_online_db.name))
        line_list.append('WatchList = %s' % strategy_online_db.instance_name)
        line_list.append('')

    file_path = '%s/%s/config.strategyloader.txt' % (STRATEGYLOADER_FILE_PATH, server_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(line_list))

    server_model = server_constant.get_server_model(server_name)
    server_model.upload_file\
        (file_path, '%s/config.strategyloader.txt' % server_model.server_path_dict['tradeplat_project_folder'])


if __name__ == '__main__':
    strategy_change_history_job()
