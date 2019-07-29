# -*- coding: utf-8 -*-
import json
import csv
from eod_aps.model.server_constans import server_constant
from eod_aps.model.schema_strategy import StrategyOnline
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.model.eod_const import const


email_utils = EmailUtils(const.EMAIL_DICT['group3'])
email_utils14 = EmailUtils(const.EMAIL_DICT['group14'])
email_content_list = []
instrument_db_dict = dict()

export_file_folder = 'H:/backtest_result_files'
date_file_base_path = 'Z:/data/future/backtest/data_history'
strategy_file_path = 'Z:/dailyjob/cta_update_info/strategy_parameter.csv'

account_dict = {'nanhua': ['All_Weather_1', 'All_Weather_2', 'All_Weather_3'],
                'zhongxin': ['absolute_return', 'steady_return'],
                'luzheng': ['All_Weather', ]}
base_position_parameter_list = ['max_long_position', 'max_short_position', 'qty_per_trade']
filter_parameter_list = ['Account', 'max_long_position', 'max_short_position', 'qty_per_trade']


def __read_strategy_file():
    strategy_online_list = []
    csvfile = file(strategy_file_path, 'rb')
    reader = csv.reader(csvfile)
    i = 0
    for line in reader:
        if i == 0:
            i += 1
            continue
        name = line[1]

        target_server_str = 'nanhua|zhongxin|luzheng'
        strategy_parameter_dict, strategy_parameter_str, instance_name = __rebuild_parameter_server(target_server_str, line[2])
        strategy_online = StrategyOnline()
        strategy_online.enable = 0
        strategy_online.strategy_type = 'CTA'
        strategy_online.target_server = target_server_str
        strategy_online.name = name
        strategy_online.strategy_name = name.split('.')[0]
        strategy_online.assembly_name = strategy_online.strategy_name + '_strategy'
        strategy_online.instance_name = instance_name
        strategy_online.data_type = 'minbar'
        strategy_online.date_num = 180
        strategy_online.parameter = strategy_parameter_str

        parameter_server_list = []
        for server_name in target_server_str.split('|'):
            parameter_server_list.append(strategy_parameter_dict[server_name])
        strategy_online.parameter_server = '|'.join(parameter_server_list)
        strategy_online_list.append(strategy_online)
    csvfile.close()
    return strategy_online_list


def __rebuild_parameter_server(target_server_str, strategy_parameter_value):
    strategy_parameter_dict = json.loads(strategy_parameter_value)
    instance_name = strategy_parameter_dict['Target']
    allowed_accounts = strategy_parameter_dict['Account']
    target_server_list = target_server_str.split('|')

    server_parameter_dict = dict()
    for server_name in target_server_list:
        account_list = account_dict[server_name]

        parameter_dict = dict()
        new_parameter_list = ['[Account]1:0:0']
        for (key_parameter, value_parameter) in strategy_parameter_dict.items():
            save_flag = True
            for position_parameter in filter_parameter_list:
                if position_parameter in key_parameter:
                    save_flag = False
                    break
            if save_flag:
                new_parameter_list.append('[%s]%s:0:0' % (key_parameter, value_parameter))
                parameter_dict[key_parameter] = value_parameter

        filter_account_list = []
        for account_name in account_list:
            if account_name not in allowed_accounts:
                continue
            filter_account_list.append(account_name)

        position_parameter_list = []
        for account_name in filter_account_list:
            for position_parameter_str in base_position_parameter_list:
                position_parameter = 'tq.%s.%s' % (account_name, position_parameter_str)
                position_parameter_list.append(position_parameter)

        for position_parameter in position_parameter_list:
            parameter_dict[position_parameter] = strategy_parameter_dict[position_parameter]

        parameter_dict['Account'] = ';'.join(filter_account_list)
        server_parameter_dict[server_name] = json.dumps(parameter_dict)
    return server_parameter_dict, ';'.join(new_parameter_list), instance_name


def __insert_db(strategy_online_list):
    server_model = server_constant.get_server_model('host')
    session = server_model.get_db_session('strategy')
    for strategy_online_db in strategy_online_list:
        session.merge(strategy_online_db)
    session.commit()


def backtest_init():
    try:
        strategy_online_list = __read_strategy_file()
        __insert_db(strategy_online_list)
    except:
        import traceback
        email_utils14.send_email_group_all('[Error]Backtest Init', traceback.format_exc(), 'html')
    return 0


if __name__ == '__main__':
    backtest_init()
