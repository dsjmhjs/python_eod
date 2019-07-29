# -*- coding: utf-8 -*-
import json
from sqlalchemy import desc
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.strategy_online import StrategyOnline
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.job import *


email_utils = EmailUtils(EmailUtils.group2)
strategy_filter_list = ['PairTrading']


def __get_strategy_online_dict(server_model):
    session_strategy = server_model.get_db_session('strategy')

    strategy_online_dict = dict()
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.enable == 1):
        if strategy_online_db.strategy_name in strategy_filter_list:
            continue

        strategy_name_key = strategy_online_db.name.upper()
        strategy_online_dict[strategy_name_key] = strategy_online_db
    return strategy_online_dict


def __get_pf_account_list(server_model, strategy_online_dict):
    strategy_name_list = list(strategy_online_dict.keys())

    session_strategy = server_model.get_db_session('strategy')
    query_sql = "select name,time,value from (select  distinct name,time,value from strategy.strategy_parameter order by time desc) t group by name"
    query_result = session_strategy.execute(query_sql)

    strategy_account_dict = dict()
    for strategy_parameter_info in query_result:
        strategy_name = strategy_parameter_info[0].upper()
        if strategy_name not in strategy_name_list:
            continue

        strategy_value = strategy_parameter_info[2]
        parameter_dict = json.loads(strategy_value.replace('\n', ''))
        if 'Account' not in parameter_dict:
            continue

        account_used_str = parameter_dict['Account']
        strategy_account_dict[strategy_name] = account_used_str

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)

    pf_account_list = []
    for pf_account_db in query_pf_account:
        strategy_name_key = ('%s.%s' % (pf_account_db.group_name, pf_account_db.name)).upper()
        if strategy_name_key not in strategy_name_list:
            continue

        if strategy_name_key not in strategy_account_dict:
            continue

        account_used_str = strategy_account_dict[strategy_name_key]
        account_filter_str = pf_account_db.fund_name.split('-')[2]
        if account_filter_str not in account_used_str:
            continue

        pf_account_list.append(pf_account_db)
    return pf_account_list


def __validate_position(server_model, pf_account_list, strategy_online_dict):
    session_strategy = server_model.get_db_session('strategy')
    query_sql = "select name,time,value from (select  distinct name,time,value from strategy.strategy_state order by time desc) t group by name"
    query = session_strategy.execute(query_sql)
    strategy_state_dict = dict()
    for strategy_state_db in query:
        last_trading_signal = 'NA'
        parameter_dict = json.loads(strategy_state_db[2].replace('\n', ''))
        if 'lastTradingSignal' in parameter_dict:
            last_trading_signal = parameter_dict['lastTradingSignal']

        strategy_name_key = strategy_state_db[0].upper()
        strategy_state_dict[strategy_name_key] = last_trading_signal

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)

    email_content_list = []
    position_miss_list = []
    last_trading_signal_miss_list = []
    max_long_short_miss_list = []
    for pf_account_db in pf_account_list:
        strategy_name_key = ('%s.%s' % (pf_account_db.group_name, pf_account_db.name)).upper()
        strategy_online_db = strategy_online_dict[strategy_name_key]
        symbol = strategy_online_db.instance_name

        max_long_value = 0
        max_short_value = 0
        max_long_flag = False
        max_short_flag = False
        for parameter_pair in strategy_online_db.parameter.split(';'):
            account_filter_str = pf_account_db.fund_name.split('-')[2]
            if account_filter_str in parameter_pair and 'max_long' in parameter_pair:
                max_long_value = int(parameter_pair.split(']')[1].split(':')[0])
                max_long_flag = True
            elif account_filter_str in parameter_pair and 'max_short' in parameter_pair:
                max_short_value = -1 * int(parameter_pair.split(']')[1].split(':')[0])
                max_short_flag = True

        if not (max_long_flag and max_short_flag):
            max_long_short_miss_list.append('%s:%s  symbol:%s' % (pf_account_db.id, pf_account_db.fund_name, symbol))

        pf_position_db = query_pf_position.filter(PfPosition.id == str(pf_account_db.id), PfPosition.symbol.like(symbol + '%'))\
            .order_by(desc(PfPosition.date)).first()
        if pf_position_db is None:
            position_miss_list.append('%s:%s symbol:%s' % (pf_account_db.id, pf_account_db.fund_name, symbol))
            continue
        position = pf_position_db.long_avail - pf_position_db.short_avail

        if strategy_name_key not in strategy_state_dict:
            last_trading_signal_miss_list.append('%s:%s  symbol:%s' % (pf_account_db.id, pf_account_db.fund_name, symbol))
            continue
        last_trading_signal = strategy_state_dict[strategy_name_key]

        error_content_list = __position_check(position, last_trading_signal, max_long_value, max_short_value, pf_account_db)
        email_content_list.extend(error_content_list)

    email_error_list = __email_error_content(position_miss_list, last_trading_signal_miss_list, max_long_short_miss_list)
    email_content_list.extend(email_error_list)
    email_utils.send_email_group_all(unicode('LastTradingSignal与pf_position对比', 'utf-8'), '\n'.join(email_content_list))


def __position_check(position, last_trading_signal, max_long_value, max_short_value, pf_account_db):
    pf_account_id = pf_account_db.id
    fund_name = pf_account_db.fund_name

    error_content_list = []
    if position < max_short_value or position > max_long_value:
        error_content_list.append('%s:%s error: position out of limit!' % (pf_account_id, fund_name))
        error_content_list.append('position = %s, LastTradingSignal = %s, max_short_position = %s, max_long_position = %s'\
            % (position, last_trading_signal, max_short_value, max_long_value))
    else:
        last_trading_signal_sign = __sign_last_trading_signal(last_trading_signal)
        if __sign(position) != last_trading_signal_sign:
            if last_trading_signal_sign == 1 and __sign(position) == 0 and max_long_value == 0:
                pass
            elif last_trading_signal_sign == -1 and __sign(position) == 0 and max_short_value == 0:
                pass
            else:
                error_content_list.append('%s:%s error: position different!' % (pf_account_id, fund_name))
                error_content_list.append('position = %s, LastTradingSignal = %s, max_short_position = %s, max_long_position = %s' \
                      % (position, last_trading_signal, max_short_value, max_long_value))
    return error_content_list


def __email_error_content(position_miss_list, last_trading_signal_miss_list, max_long_short_miss_list):
    email_error_list = []

    if len(position_miss_list) > 0:
        email_error_list.append('\n\nposition_missing_list:')
        email_error_list.extend(position_miss_list)

    if len(last_trading_signal_miss_list) > 0:
        email_error_list.append('\n\nlastTradingSignal_missing_list:')
        email_error_list.extend(position_miss_list)

    if len(max_long_short_miss_list) > 0:
        email_error_list.append('\n\nmaxlongshort_missing_list:')
        email_error_list.extend(max_long_short_miss_list)
    return email_error_list


def __sign_last_trading_signal(signal):
    if signal == 'Positive':
        return 1
    elif signal == 'Negative':
        return -1
    else:
        return 0


def __sign(a):
    if a == 0:
        return 0
    else:
        return abs(a) / a


def trading_position_check_job():
    strategy_online_dict = __get_strategy_online_dict(server_host)

    server_model_nanhua = server_constant.get_server_model('nanhua_web')
    pf_account_list = __get_pf_account_list(server_model_nanhua, strategy_online_dict)
    __validate_position(server_model_nanhua, pf_account_list, strategy_online_dict)
    server_model_nanhua.close()


if __name__ == '__main__':
    trading_position_check_job()