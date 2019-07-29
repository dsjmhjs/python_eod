# -*- coding: utf-8 -*-
import os
import json
from eod_aps.job import *


def get_strategy_name_list():
    custom_log.log_info_job('getting strategy name list...')
    if not os.path.exists(STRATEGY_NAME_LIST_FOLD_PATH):
        os.makedirs(STRATEGY_NAME_LIST_FOLD_PATH)
    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select `NAME` from strategy.strategy_online where `ENABLE` = 1 and strategy_type = 'CTA';"
    query_result = session_strategy.execute(query_sql)
    strategy_name_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        strategy_name_list.append(strategy_name)

    with open(STRATEGY_NAME_LIST_FOLD_PATH + 'strategy_name_list.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_name_list))
    return strategy_name_list


def build_backtest_strategy_group_dict():
    if not os.path.exists(STRATEGY_GROUP_STR_FOLD_PATH):
        os.makedirs(STRATEGY_GROUP_STR_FOLD_PATH)
    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select strategy_name, group_number from strategy.strategy_backtest_group;"
    query_result = session_strategy.execute(query_sql)
    backtest_strategy_group_list = ['%s,%s' % (x[0], x[1]) for x in query_result]

    with open(STRATEGY_GROUP_STR_FOLD_PATH + 'strategy_group_str.csv', 'w+') as fr:
        fr.write('\n'.join(backtest_strategy_group_list))


def get_strategy_backtest_parameter(strategy_name_list, server_name_list):
    custom_log.log_info_job('getting strategy backtest parameter...')
    if not os.path.exists(BACKTEST_PARAMETER_STR_FOLDER_PATH):
        os.makedirs(BACKTEST_PARAMETER_STR_FOLDER_PATH)

    filter_key_word_list = ['max_long_position', 'max_short_position', 'qty_per_trade']
    target_server = sorted(server_name_list)[-1]
    server_model = server_constant.get_server_model(target_server)
    session_strategy = server_model.get_db_session('strategy')
    strategy_parameter_value_str = ''
    for strategy_name in strategy_name_list:
        query_sql = "select `VALUE` from strategy.strategy_parameter where `NAME` = '%s' order by time desc limit 1;" \
                    % strategy_name
        query_result = session_strategy.execute(query_sql)
        for query_line in query_result:
            strategy_parameter_value_str = query_line[0]
            break
        strategy_parameter_value_dict = json.loads(strategy_parameter_value_str)
        parameter_item_list = ['[Account]1:0:0']
        for (key_parameter, value_parameter) in strategy_parameter_value_dict.items():
            if key_parameter == 'Account':
                continue
            if key_parameter == 'Target':
                continue
            filter_flag = False
            for filter_key_word in filter_key_word_list:
                if filter_key_word in key_parameter:
                    filter_flag = True
            if filter_flag:
                continue
            parameter_item_list.append('[%s]%s:0:0' % (key_parameter, value_parameter))
        parameter_list_str = ';'.join(parameter_item_list)

        with open(BACKTEST_PARAMETER_STR_FOLDER_PATH + '%s.txt' % strategy_name, 'w+') as fr:
            fr.write(parameter_list_str)


def get_strategy_backtest_info(strategy_name_list):
    custom_log.log_info_job('getting strategy backtest info...')
    if not os.path.exists(BACKTEST_INFO_STR_FOLDER_PATH):
        os.makedirs(BACKTEST_INFO_STR_FOLDER_PATH)

    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select ASSEMBLY_NAME, STRATEGY_NAME, NAME, INSTANCE_NAME, DATA_TYPE, DATE_NUM from " \
                "strategy.strategy_online where `ENABLE` = 1 and strategy_type = 'CTA'"
    query_result = session_strategy.execute(query_sql)
    for query_line in query_result:
        strategy_name = query_line[2]
        if strategy_name in strategy_name_list:
            backtest_info_str = '%s,%s,%s,%s,%s' % (query_line[0], query_line[1], query_line[3], query_line[4],
                                                    query_line[5])
        else:
            raise Exception("Error strategy_name:%s" % strategy_name)

        with open(BACKTEST_INFO_STR_FOLDER_PATH + '%s.csv' % strategy_name, 'w+') as fr:
            fr.write(backtest_info_str)


def get_strategy_server_parameter(server_name, strategy_name_list):
    custom_log.log_info_job('getting strategy server:%s parameter...' % server_name)
    if not os.path.exists(STRATEGY_SERVER_PARAMETER_FOLDER_PATH + '%s/' % server_name):
        os.makedirs(STRATEGY_SERVER_PARAMETER_FOLDER_PATH + '%s/' % server_name)

    server_model_server = server_constant.get_server_model(server_name)
    session_strategy = server_model_server.get_db_session('strategy')
    for strategy_name in strategy_name_list:
        query_sql = "select `VALUE` from strategy.strategy_parameter where `NAME` = '%s' order by time desc limit 1;" \
                    % strategy_name
        query_result = session_strategy.execute(query_sql)
        strategy_server_parameter = ''
        for query_line in query_result:
            strategy_server_parameter_str = query_line[0]
            strategy_server_parameter_value = json.loads(strategy_server_parameter_str)
            strategy_server_parameter = json.dumps(strategy_server_parameter_value)

        strategy_server_parameter_file_path = STRATEGY_SERVER_PARAMETER_FOLDER_PATH + '%s/%s.txt' \
                                                                                      % (server_name, strategy_name)
        with open(strategy_server_parameter_file_path, 'w+') as fr:
            fr.write(strategy_server_parameter)
        server_model_server.close()


def update_stratey_online(server_name_list, strategy_name_list):
    custom_log.log_info_job('updating strategy online...')
    session_strategy = server_host.get_db_session('strategy')
    for strategy_name in strategy_name_list:
        strategy_server_parameter_list = []
        for server_name in server_name_list:
            strategy_server_parameter_file_path = STRATEGY_SERVER_PARAMETER_FOLDER_PATH \
                                                  + '%s/%s.txt' % (server_name, strategy_name)
            with open(strategy_server_parameter_file_path, 'rb') as fr:
                for line in fr.readlines()[:1]:
                    strategy_server_parameter_list.append(line.replace('\n', ''))
                    break
        strategy_server_parameter_str = '|'.join(strategy_server_parameter_list)
        update_sql = "update strategy.strategy_online set `parameter_server` = '%s' where `NAME` = '%s';" \
                     % (strategy_server_parameter_str, strategy_name)
        session_strategy.execute(update_sql)
    session_strategy.commit()


def get_backtest_info_job(server_name_list):
    global server_host
    server_host = server_constant.get_server_model('host')

    strategy_name_list = get_strategy_name_list()
    build_backtest_strategy_group_dict()
    get_strategy_backtest_parameter(strategy_name_list, server_name_list)
    get_strategy_backtest_info(strategy_name_list)
    for server_name in server_name_list:
        get_strategy_server_parameter(server_name, strategy_name_list)
    update_stratey_online(server_name_list, strategy_name_list)
    server_host.close()


if __name__ == "__main__":
    cta_servers_list = server_constant.get_cta_servers()
    get_backtest_info_job(cta_servers_list)
