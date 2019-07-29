# -*- coding: utf-8 -*-
# 检查account_trade_restrictions表是否需要新增数据
from itertools import islice
from eod_aps.model.schema_portfolio import RealAccount, AccountTradeRestrictions
from eod_aps.model.schema_common import Instrument
from eod_aps.server_python import *

instrument_type_enums = const.INSTRUMENT_TYPE_ENUMS


def __insert_account_trade_restrictions():
    trade_restrictions_dict = __read_account_trade_restrictions()

    global ratio_cfg_dict
    ratio_cfg_dict = __read_account_trade_restrictions_cfg()

    session_portfolio = server_model.get_db_session('portfolio')
    query = session_portfolio.query(RealAccount)

    account_trade_restrictions_list = []
    for account_db in query:
        if account_db.enable == 0:
            continue

        active_tickers = __query_active_tickers(account_db.accountid)
        allow_targets = account_db.allow_targets
        custom_account_type = None
        if 'any,future' in allow_targets:
            custom_account_type = 'future'
            future_restrictions_list = trade_restrictions_dict['Commodity_Future']
            future_restrictions_list.extend(trade_restrictions_dict['Index_Future'])
            future_restrictions_list.extend(trade_restrictions_dict['TreasuryBond_Future'])
            future_list = __insert_restrictions_future(account_db, active_tickers, future_restrictions_list)
            account_trade_restrictions_list.extend(future_list)

        if 'any,option' in allow_targets:
            custom_account_type = 'option'
            option_restrictions_list = trade_restrictions_dict['Stock_Option']
            option_restrictions_list.extend(trade_restrictions_dict['Commodity_Option'])
            option_list = __insert_restrictions_option(account_db, active_tickers, option_restrictions_list)
            account_trade_restrictions_list.extend(option_list)

        if 'any,mutualfund' in allow_targets:
            custom_account_type = 'mutualfund'
            mutualfund_restrictions_list = trade_restrictions_dict['Mutualfund']
            mutualfund_list = __insert_restrictions_mutualfund(account_db, active_tickers, mutualfund_restrictions_list)
            account_trade_restrictions_list.extend(mutualfund_list)

        # if 'any,mutualfund' in allow_targets:
        #     custom_account_type = 'stock'

        if 'all' not in active_tickers:
            account_trade_restrictions = AccountTradeRestrictions()
            account_trade_restrictions.account_id = account_db.accountid
            account_trade_restrictions.ticker = 'all'
            account_trade_restrictions.exchange_id = 0
            account_trade_restrictions.hedgeflag = 0
            if custom_account_type == 'future':
                account_trade_restrictions.max_operation = 1000000
            elif custom_account_type == 'mutualfund':
                account_trade_restrictions.max_operation = 1000000
            elif custom_account_type == 'option':
                account_trade_restrictions.max_operation = 15000
            account_trade_restrictions_list.append(account_trade_restrictions)

    for item_db in account_trade_restrictions_list:
        session_portfolio.add(item_db)
    session_portfolio.commit()
    server_model.close()


def __query_active_tickers(account_id):
    active_tickers = []
    query_sql = "select ticker from portfolio.account_trade_restrictions where account_id = '%s'" % account_id
    session_portfolio = server_model.get_db_session('portfolio')
    r = session_portfolio.execute(query_sql)
    for item in r.fetchall():
        active_tickers.append(str(item[0]))
    return active_tickers


def __read_account_trade_restrictions():
    trade_restrictions_dict = dict()
    cfg_file_path = '../../cfg/account_trade_restrictions.csv'
    with open(cfg_file_path, 'rb') as fr:
        for line in islice(fr, 1, None):
            line_item = line.replace('\r\n', '').split(',')
            trade_type = line_item[0]
            if trade_type in trade_restrictions_dict:
                trade_restrictions_dict[trade_type].append(line.replace('\r\n', ''))
            else:
                trade_restrictions_dict[trade_type] = [line.replace('\r\n', '')]
    return trade_restrictions_dict


def __read_account_trade_restrictions_cfg():
    temp_cfg_dict = dict()
    cfg_file_path = '../../cfg/account_trade_restrictions_config.csv'
    with open(cfg_file_path, 'rb') as fr:
        for line in islice(fr, 1, None):
            server_name, fund_name, trade_type, ticker_type, ratio = line.split(',')
            if ticker_type == '':
                dict_key = '%s|%s|%s' % (server_name, fund_name, trade_type)
            else:
                dict_key = '%s|%s|%s|%s' % (server_name, fund_name, trade_type, ticker_type)
            temp_cfg_dict[dict_key] = ratio
    return temp_cfg_dict


def __insert_restrictions_future(account_db, active_tickers, future_restrictions_list):
    trade_ticker_set = set()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == instrument_type_enums.Future, Instrument.del_flag == 0):
        ticker = str(instrument_db.ticker)
        if 'IC' in ticker:
            rebuild_ticker = 'SH000905'
            filter_ticker_type = 'SH000905'
        elif 'IF' in ticker:
            rebuild_ticker = 'SHSZ300'
            filter_ticker_type = 'SHSZ300'
        elif 'IH' in ticker:
            rebuild_ticker = 'SSE50'
            filter_ticker_type = 'SSE50'
        else:
            rebuild_ticker = ticker
            filter_ticker_type = filter(str.isalpha, ticker)
        trade_ticker_set.add((rebuild_ticker, filter_ticker_type))

    account_trade_restrictions_list = \
        __build_account_trade_restrictions(account_db, trade_ticker_set, active_tickers, future_restrictions_list)
    return account_trade_restrictions_list


def __insert_restrictions_option(account_db, active_tickers, option_restrictions_list):
    trade_ticker_set = set()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == instrument_type_enums.Option, Instrument.del_flag == 0):
        ticker = str(instrument_db.ticker)
        undl_tickers = str(instrument_db.undl_tickers)
        if instrument_db.ticker in active_tickers:
            continue

        if undl_tickers == "510050":
            filter_ticker_type = undl_tickers
        else:
            filter_ticker_type = filter(str.isalpha, undl_tickers)
        trade_ticker_set.add((ticker, filter_ticker_type))

    account_trade_restrictions_list = \
        __build_account_trade_restrictions(account_db, trade_ticker_set, active_tickers, option_restrictions_list)
    return account_trade_restrictions_list


def __insert_restrictions_mutualfund(account_db, active_tickers, option_restrictions_list):
    trade_ticker_set = set()
    trade_ticker_set.add(('510050', '510050'))

    account_trade_restrictions_list = \
        __build_account_trade_restrictions(account_db, trade_ticker_set, active_tickers, option_restrictions_list)
    return account_trade_restrictions_list


def __build_account_trade_restrictions(account_db, trade_ticker_set, active_tickers, restrictions_list):
    account_trade_restrictions_list = []
    for (save_ticker, filter_ticker_type) in trade_ticker_set:
        if save_ticker in active_tickers:
            continue

        find_flag = False
        for future_restrictions_info in restrictions_list:
            (trade_type, ticker_type, exchange_id, max_open, max_cancel,
             max_large_cancel, option_max_long, option_max_short) = future_restrictions_info.split(',')
            if filter_ticker_type == ticker_type:
                find_flag = True
                break

        if find_flag:
            ratio_value = 1
            find_ratio_key = '%s|%s|%s' % (lOCAL_SERVER_NAME, account_db.fund_name, trade_type)
            if find_ratio_key in ratio_cfg_dict:
                ratio_value = ratio_cfg_dict[find_ratio_key]

            find_ratio_key = '%s|%s|%s|%s' % (lOCAL_SERVER_NAME, account_db.fund_name, trade_type, ticker_type)
            if find_ratio_key in ratio_cfg_dict:
                ratio_value = ratio_cfg_dict[find_ratio_key]

            max_open = __round_down(max_open, ratio_value)
            max_cancel = __round_down(max_cancel, ratio_value)
            max_large_cancel = __round_down(max_large_cancel, ratio_value)
            option_max_long = __round_down(option_max_long, ratio_value)
            option_max_short = __round_down(option_max_short, ratio_value)

            account_trade_restrictions = AccountTradeRestrictions()
            account_trade_restrictions.account_id = account_db.accountid
            account_trade_restrictions.ticker = save_ticker
            account_trade_restrictions.exchange_id = exchange_id
            account_trade_restrictions.hedgeflag = 0
            account_trade_restrictions.max_open = max_open
            account_trade_restrictions.today_open = 0
            account_trade_restrictions.max_cancel = max_cancel
            account_trade_restrictions.today_cancel = 0
            account_trade_restrictions.max_large_cancel = max_large_cancel
            account_trade_restrictions.today_large_cancel = 0
            account_trade_restrictions.max_operation = 0
            account_trade_restrictions.today_operation = 0

            account_trade_restrictions.option_max_long = option_max_long
            account_trade_restrictions.option_long = 0
            account_trade_restrictions.option_max_short = option_max_short
            account_trade_restrictions.option_short = 0
            account_trade_restrictions_list.append(account_trade_restrictions)
        else:
            print 'Type UnFind:', filter_ticker_type
    return account_trade_restrictions_list


# 向下取整
def __round_down(number_a, number_b):
    number_multiplication = float(number_a) * float(number_b)
    return int(int(number_multiplication / float(10)) * 10)


def __update_account_trade_restrictions():
    session_portfolio = server_model.get_db_session('portfolio')
    query = session_portfolio.query(AccountTradeRestrictions)
    for account_trade_restrictions_db in query:
        account_trade_restrictions_db.today_operation = 0
        account_trade_restrictions_db.today_open = 0
        account_trade_restrictions_db.today_cancel = 0
        account_trade_restrictions_db.today_large_cancel = 0
        account_trade_restrictions_db.today_rejected = 0
        account_trade_restrictions_db.today_bid_amount = 0
        account_trade_restrictions_db.today_ask_amount = 0
        account_trade_restrictions_db.today_bid_canceled_amount = 0
        account_trade_restrictions_db.today_buy_amount = 0
        account_trade_restrictions_db.today_sell_amount = 0
        session_portfolio.merge(account_trade_restrictions_db)
    session_portfolio.commit()


def update_account_trade_restrictions(server_name):
    print 'account_trade_restrictions_update_job db[%s] Start!' % server_name
    global server_model
    server_model = server_constant_local.get_server_model(server_name)

    __insert_account_trade_restrictions()
    __update_account_trade_restrictions()

    server_model.close()
    print 'account_trade_restrictions_update_job db[%s] Stop!' % server_name


if __name__ == '__main__':
    update_account_trade_restrictions('host')

