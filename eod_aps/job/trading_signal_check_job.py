# -*- coding: utf-8 -*-
import json
from eod_aps.job import *

strategy_filter_list = ['PairTrading.j_jm', 'PairTrading.j_jm_para2', 'PairTrading.j_jm_para3',
                        'PairTrading.j_jm_para4' , 'PairTrading.m_rm_para1', 'PairTrading.m_rm_para2',
                        'PairTrading.m_rm_para3', 'PairTrading.m_rm_para4']

account_list_dict = dict()
account_list_dict['nanhua'] = ['All_Weather_1', 'All_Weather_2', 'All_Weather_3']
account_list_dict['zhongxin'] = ['steady_return', 'huize01', 'hongyuan01']
account_list_dict['luzheng'] = ['All_Weather', ]
account_list_dict['guangfa'] = ['steady_return', ]
account_list_dict['huabao'] = ['steady_return', ]


def get_strategy_name_list(server_name):
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME` from strategy.strategy_online where `ENABLE` = 1 and strategy_type = 'CTA' and target_server like '%s'" \
                "order by `NAME` asc" % ('%' + server_name + '%', )
    query_result = session_strategy.execute(query_sql)
    strategy_name_list = []
    for query_line in query_result:
        if query_line[0] in strategy_filter_list:
            continue
        strategy_name_list.append(query_line[0])
    server_model_host.close()
    return strategy_name_list


def get_pf_account_id_dict(server_model, server_name, strategy_name_list):
    pf_account_id_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = "select `ID`, `NAME`, `FUND_NAME`, `GROUP_NAME` from portfolio.pf_account order by `ID` asc;"
    query_result = session_portfolio.execute(query_sql)
    for query_line in query_result:
        pf_account_id = query_line[0]
        strategy_name = query_line[3] + '.' + query_line[1]
        account_name = query_line[2].split('-')[-2]
        if strategy_name not in strategy_name_list:
            continue
        if account_name not in account_list_dict[server_name]:
            continue
        pf_account_id_dict[pf_account_id] = [strategy_name, account_name]
    return pf_account_id_dict


def get_id_pf_position_dict(server_model):
    id_pf_position_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = "select max(date) from portfolio.pf_position;"
    query_result = session_portfolio.execute(query_sql)
    for query_line in query_result:
        max_date = query_line[0]
    query_sql = "select `ID`, `LONG`, `SHORT` from portfolio.pf_position " \
                "where `DATE` = '%s' order by `ID` asc;" % max_date
    query_result = session_portfolio.execute(query_sql)
    for query_line in query_result:
        pf_account_id = query_line[0]
        pf_position_number = float(query_line[1] - query_line[2])
        id_pf_position_dict[pf_account_id] = pf_position_number
    return id_pf_position_dict


def get_strategy_parameter_info_dict(server_model, server_name, strategy_name_list):
    strategy_parameter_info_dict = dict()
    session_strategy = server_model.get_db_session('strategy')
    for strategy_name in strategy_name_list:
        query_sql = "select `VALUE` from strategy.strategy_parameter " \
                    "where `NAME` = '%s' order by time desc limit 1;" % strategy_name
        query_result = session_strategy.execute(query_sql)
        parameter_value = ""
        for query_line in query_result:
            parameter_value = query_line[0]
            break
        if parameter_value == "":
            continue
        parameter_value_dict = json.loads(parameter_value)
        account_list = parameter_value_dict['Account'].split(';')
        default_account_list = account_list_dict[server_name]
        max_position_dict = dict()
        for account_name in default_account_list:
            if 'tq.%s.max_long_position' % account_name not in parameter_value_dict:
                max_long_position = 0
            else:
                max_long_position = float(parameter_value_dict['tq.%s.max_long_position' % account_name])
            if 'tq.%s.max_short_position' % account_name not in parameter_value_dict:
                max_short_position = 0
            else:
                max_short_position = float(parameter_value_dict['tq.%s.max_short_position' % account_name])
            max_position_dict[account_name] = [max_long_position, max_short_position]
        strategy_parameter_info_dict[strategy_name] = [account_list, max_position_dict]
    return strategy_parameter_info_dict


def get_trading_signal_position_dict(server_model, strategy_name_list):
    trading_signal_position_dict = dict()
    session_strategy = server_model.get_db_session('strategy')
    for strategy_name in strategy_name_list:
        query_sql = "select `VALUE` from strategy.strategy_state " \
                    "where `NAME` = '%s' order by time desc limit 1;" % strategy_name
        query_result = session_strategy.execute(query_sql)
        state_value = ''
        for query_line in query_result:
            state_value = query_line[0].replace('\n', '')
            break
        if state_value == "":
            continue
        state_value_dict = json.loads(state_value)
        trading_signal = state_value_dict['lastTradingSignal']
        if trading_signal == 'Positive':
            trading_signal_position = 1
        elif trading_signal == 'Negative':
            trading_signal_position = -1
        else:
            trading_signal_position = 0
        trading_signal_position_dict[strategy_name] = trading_signal_position
    return trading_signal_position_dict


def position_check(server_name, pf_account_id_dict, id_pf_position_dict, strategy_parameter_info_dict,
                   trading_signal_position_dict):
    table_list = []
    html_title = 'Pf_Id,Strategy_Name,Account_Error,Position_Warning,Position_Error'
    for pf_account_id in sorted(pf_account_id_dict.keys()):
        [strategy_name, pf_account_name] = pf_account_id_dict[pf_account_id]
        if pf_account_id in id_pf_position_dict:
            pf_position_number = id_pf_position_dict[pf_account_id]
        else:
            pf_position_number = 0
        [parameter_account_list, max_long_short_dict] = strategy_parameter_info_dict[strategy_name]
        trading_signal = trading_signal_position_dict[strategy_name]

        # account check
        account_error_flag = False
        for account_name in account_list_dict[server_name]:
            if account_name not in parameter_account_list:
                account_error_flag = True

        # position warning check
        position_warning_flag = False
        max_long_short_list = max_long_short_dict[pf_account_name]
        if pf_position_number > max_long_short_list[0] or pf_position_number < -max_long_short_list[1]:
            position_warning_flag = True
        if pf_position_number == 0 and trading_signal != 0:
            if not max_long_short_list[0] == 0 or not max_long_short_list[1] == 0:
                position_warning_flag = True

        # position error check
        position_error_flag = False
        if trading_signal <= 0:
            if pf_position_number > 0:
                position_error_flag = True
        if trading_signal >= 0:
            if pf_position_number < 0:
                position_error_flag = True

        if (not account_error_flag) and (not position_warning_flag) and (not position_error_flag):
            continue
        tr_list = [pf_account_id, strategy_name]
        if account_error_flag:
            tr_list.append('Error(Error)')
        else:
            tr_list.append('/')
        if position_warning_flag:
            tr_list.append('Warning')
        else:
            tr_list.append('/')
        if position_error_flag:
            tr_list.append('Error(Error)')
        else:
            tr_list.append('/')
        table_list.append(tr_list)
    html_list = email_utils3.list_to_html(html_title, table_list)
    return html_list


def trading_position_check_job(server_name_list):
    email_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        strategy_name_list = get_strategy_name_list(server_name)
        pf_account_id_dict = get_pf_account_id_dict(server_model, server_name, strategy_name_list)
        id_pf_position_dict = get_id_pf_position_dict(server_model)
        strategy_parameter_info_dict = get_strategy_parameter_info_dict(server_model, server_name, strategy_name_list)
        trading_signal_position_dict = get_trading_signal_position_dict(server_model, strategy_name_list)
        html_list = position_check(server_name, pf_account_id_dict, id_pf_position_dict, strategy_parameter_info_dict,
                                   trading_signal_position_dict)
        email_list.append(server_name + ':')
        email_list.append('\n'.join(html_list))
    email_utils3.send_email_group_all('Trading Signal Check', '\n'.join(email_list), 'html')


if __name__ == "__main__":
    trading_position_check_job(['nanhua', 'zhongxin', 'luzheng'])
