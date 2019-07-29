# -*- coding: utf-8 -*-
import os
from eod_aps.job import *
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.schema_jobs import StrategyAccountChangeHistory, StrategyAccountTarget


def init_strategyaccount_target(server_list, filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    strategyaccount_target_list = []
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')

        pf_account_dict = dict()
        query_pf_account = session_portfolio.query(PfAccount)
        for pf_account_db in query_pf_account.filter(PfAccount.group_name.in_(('Event_Real', 'MultiFactor'))):
            pf_account_dict[pf_account_db.id] = pf_account_db.fund_name

        query_position = session_portfolio.query(PfPosition)
        for position_db in query_position.filter(PfPosition.id.in_(tuple(pf_account_dict.keys()), ),
                                                 PfPosition.date == filter_date_str):
            if not position_db.symbol.isdigit():
                continue
            strategyaccount_target = StrategyAccountTarget()
            strategyaccount_target.date = filter_date_str
            strategyaccount_target.server_name = server_name
            strategyaccount_target.fund_name = pf_account_dict[position_db.id]
            strategyaccount_target.symbol = position_db.symbol
            strategyaccount_target.volume = position_db.long
            strategyaccount_target_list.append(strategyaccount_target)
        server_model.close()

    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for strategyaccount_target_db in strategyaccount_target_list:
        session_jobs.merge(strategyaccount_target_db)
    session_jobs.commit()


def build_next_strategyaccount_target(filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    next_trading_day_str = date_utils.get_next_trading_day('%Y-%m-%d', filter_date_str)

    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    query_position = session_jobs.query(StrategyAccountTarget)
    for strategyaccount_target_db in query_position.filter(StrategyAccountTarget.date == filter_date_str):
        strategyaccount_target = StrategyAccountTarget()
        strategyaccount_target.date = next_trading_day_str
        strategyaccount_target.server_name = strategyaccount_target_db.server_name
        strategyaccount_target.fund_name = strategyaccount_target_db.fund_name
        strategyaccount_target.symbol = strategyaccount_target_db.symbol
        strategyaccount_target.volume = strategyaccount_target_db.volume
        session_jobs.add(strategyaccount_target)
    session_jobs.commit()
    server_host.close()


# 因调仓而变动
def modify_by_algo_change(filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    strategyaccount_change_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    query = session_jobs.query(StrategyAccountChangeHistory)
    for strategyaccount_change_history in query.filter(StrategyAccountChangeHistory.date == filter_date_str):
        strategyaccount_change_list.append(strategyaccount_change_history)

    strategyaccount_target_dict = dict()
    query_position = session_jobs.query(StrategyAccountTarget)
    for db_item in query_position.filter(StrategyAccountTarget.date == filter_date_str):
        dict_key = '%s|%s' % (db_item.server_name, db_item.fund_name)
        if dict_key in strategyaccount_target_dict:
            strategyaccount_target_dict[dict_key][db_item.symbol] = db_item
        else:
            symbol_dict = dict()
            symbol_dict[db_item.symbol] = db_item
            strategyaccount_target_dict[dict_key] = symbol_dict

    for strategyaccount_change_history in strategyaccount_change_list:
        change_file_path = '%s/%s/%s/%s.txt' % (STOCK_SELECTION_FOLDER, strategyaccount_change_history.server_name,
                                                filter_date_str.replace('-', ''), strategyaccount_change_history.fund_name)
        find_key = '%s|%s' % (strategyaccount_change_history.server_name, strategyaccount_change_history.fund_name)
        if find_key not in strategyaccount_target_dict:
            custom_log.log_error_job('Unfind:%s' % find_key)
            continue
        symbol_dict = strategyaccount_target_dict[find_key]
        symbol_dict = __modify_by_change_file(change_file_path, symbol_dict)
        for strategyaccount_target_db in symbol_dict.values():
            if strategyaccount_target_db.volume == 0:
                session_jobs.delete(strategyaccount_target_db)
            else:
                session_jobs.merge(strategyaccount_target_db)
    session_jobs.commit()
    server_host.close()


# 因每日调仓而变动
def modify_by_file(server_name, change_file_path):
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    fund_name = os.path.basename(change_file_path).split('.')[0]

    symbol_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    query_position = session_jobs.query(StrategyAccountTarget)
    for db_item in query_position.filter(StrategyAccountTarget.date == filter_date_str,
                                         StrategyAccountTarget.server_name == server_name,
                                         StrategyAccountTarget.fund_name == fund_name):
        symbol_dict[db_item.symbol] = db_item

    symbol_dict = __modify_by_change_file(change_file_path, symbol_dict)
    for strategyaccount_target_db in symbol_dict.values():
        session_jobs.merge(strategyaccount_target_db)
    session_jobs.commit()
    server_host.close()


# 因具体调仓文件而变动（主要是平仓操作）
def __modify_by_change_file(change_file_path, symbol_dict):
    change_volume_dict = dict()
    with open(change_file_path) as fr:
        for line in fr.readlines():
            line_item = line.replace('\n', '').split(',')
            change_volume_dict[line_item[0]] = int(line_item[1])

    new_symbol_dict = dict()
    base_strategyaccount_target = symbol_dict.values()[0]
    for (symbol, change_volume) in change_volume_dict.items():
        if symbol in symbol_dict:
            strategyaccount_target = symbol_dict[symbol]
            strategyaccount_target.volume += change_volume
        else:
            strategyaccount_target = StrategyAccountTarget()
            strategyaccount_target.date = base_strategyaccount_target.date
            strategyaccount_target.server_name = base_strategyaccount_target.server_name
            strategyaccount_target.fund_name = base_strategyaccount_target.fund_name
            strategyaccount_target.symbol = symbol
            strategyaccount_target.volume = change_volume
        new_symbol_dict[symbol] = strategyaccount_target
    return new_symbol_dict


def position_compare(server_name, filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    target_position_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    query_position = session_jobs.query(StrategyAccountTarget)
    for db_item in query_position.filter(StrategyAccountTarget.date == filter_date_str,
                                         StrategyAccountTarget.server_name == server_name):
        if db_item.fund_name in target_position_dict:
            target_position_dict[db_item.fund_name][db_item.symbol] = int(db_item.volume)
        else:
            symbol_dict = dict()
            symbol_dict[db_item.symbol] = int(db_item.volume)
            target_position_dict[db_item.fund_name] = symbol_dict
    server_host.close()

    pf_position_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    pf_account_dict = dict()
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.group_name.in_(('Event_Real', 'MultiFactor'))):
        pf_account_dict[pf_account_db.id] = pf_account_db.fund_name

    query_position = session_portfolio.query(PfPosition)
    for position_db in query_position.filter(PfPosition.id.in_(tuple(pf_account_dict.keys()), ),
                                             PfPosition.date == filter_date_str):
        fund_name = pf_account_dict[position_db.id]
        if fund_name in pf_position_dict:
            pf_position_dict[fund_name][position_db.symbol] = position_db.long
        else:
            symbol_dict = dict()
            symbol_dict[position_db.symbol] = position_db.long
            pf_position_dict[fund_name] = symbol_dict
    server_model.close()

    error_message_list = []
    for (fund_name, target_symbol_dict) in target_position_dict.items():
        pf_symbol_dict = pf_position_dict[fund_name]

        symbol_set = set()
        symbol_set.update(target_symbol_dict.keys())
        symbol_set.update(pf_symbol_dict.keys())
        symbol_list = list(symbol_set)
        symbol_list.sort()

        for symbol in symbol_list:
            target_volume = target_symbol_dict[symbol] if symbol in target_symbol_dict else 0
            pf_volume = pf_symbol_dict[symbol] if symbol in pf_symbol_dict else 0
            if target_volume != pf_volume:
                error_message_list.append([fund_name, symbol, target_volume, pf_volume])

    html_title = 'Fund_Name,Symbol,Target_volume,Pf_volume'
    html_message_list = email_utils2.list_to_html(html_title, error_message_list)
    email_utils2.send_email_group_all(u'交易校验_%s' % server_name, ''.join(html_message_list), 'html')


if __name__ == '__main__':
    # init_strategyaccount_target(('huabao', 'guoxin', 'citics'), '2017-10-27')
    # build_next_strategyaccount_target('2017-10-27')
    position_compare('guoxin')