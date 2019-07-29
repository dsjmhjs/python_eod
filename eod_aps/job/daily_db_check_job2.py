# -*- coding: utf-8 -*-
# 对每日更新的数据进行校验
import os
import json
import random
import pandas as pd
import numpy as np
from eod_aps.job import *
from sqlalchemy import desc
from eod_aps.model.schema_portfolio import RealAccount, PfPosition, AccountPosition, PfAccount
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.model.schema_strategy import StrategyParameter
from eod_aps.tools.wind_local_tools import w_ys, w_ys_close
from eod_aps.tools.instrument_tools import query_use_instrument_dict
from eod_aps.model.eod_const import const

email_list = []

# 本地ticker和windticker的转换
wind_ticker_local_dict = dict()
column_filter_list = ['update_date', 'close_update_time', 'prev_close_update_time', 'buy_commission', 'sell_commission',
                      'fair_price', 'max_limit_order_vol', 'max_market_order_vol', 'is_settle_instantly',
                      'inactive_date', 'close', 'volume', 'shortmarginratio', 'shortmarginratio_hedge',
                      'shortmarginratio_speculation', 'shortmarginratio_arbitrage', 'longmarginratio_hedge',
                      'longmarginratio', 'longmarginratio_speculation', 'longmarginratio_arbitrage',
                      'stamp_cost', 'copy']


def __wind_login():
    global w
    w = w_ys()


def __wind_close():
    w_ys_close()


def __wind_prev_close_dict(check_wind_ticker_list):
    pre_night_market_flag = date_utils.is_pre_night_market()
    filter_date_str = date_utils.get_next_trading_day('%Y-%m-%d') if pre_night_market_flag else \
        date_utils.get_today_str('%Y-%m-%d')

    wind_prev_close_dict = w.query_wsd_data("pre_close", check_wind_ticker_list, filter_date_str)
    return wind_prev_close_dict


def __price_check(server_name_list, check_type_dict):
    check_instrument_dict = dict()
    check_wind_ticker_list = []
    for (check_type_str, check_type_name) in check_type_dict.items():
        server_name = server_name_list[0]
        check_instrument_list = []
        for check_type_id in check_type_str.split('|'):
            dict_key = '%s|%s' % (server_name, check_type_id)
            check_instrument_list.extend(server_instrument_type_dict[dict_key])

        # 指数校验全部，其余随机校验15个
        if '6' != check_type_str:
            check_instrument_list = random.sample(check_instrument_list, 15)
        check_instrument_dict[check_type_str] = check_instrument_list

        for instrument_db in check_instrument_list:
            check_wind_ticker = __wind_ticker_convert(instrument_db)
            check_wind_ticker_list.append(check_wind_ticker)
    wind_prev_close_dict = __wind_prev_close_dict(check_wind_ticker_list)

    export_message_list = []
    for (type_str, instrument_list) in check_instrument_dict.items():
        type_name = check_type_dict[type_str]
        email_list.append('<br><br><li>Check %s Prev_Close</li>' % type_name)
        table_list = []
        html_title = 'Ticker,Wind,%s,Check Result' % ','.join(server_name_list)
        for instrument_db in instrument_list:
            wind_ticker = __wind_ticker_convert(instrument_db)
            wind_prev_close = wind_prev_close_dict[wind_ticker]

            if str(wind_prev_close) == 'nan':
                check_prev_close = None
                tr_list = [wind_ticker, '%s(Error)' % wind_prev_close]
            else:
                check_prev_close = wind_prev_close
                tr_list = [wind_ticker, wind_prev_close]

            error_flag = False
            for index, server_name in enumerate(server_name_list):
                dict_key = '%s|%s' % (server_name, instrument_db.ticker)
                server_prev_close = server_instrument_ticker_dict[dict_key].prev_close
                if server_prev_close is None:
                    tr_list.append('nan(Error)')
                    continue

                if index == 0 and check_prev_close is None:
                    check_prev_close = server_prev_close

                if type_name in ('Option', 'Index'):
                    server_prev_close = '%.4f' % server_prev_close
                    check_prev_close = '%.4f' % float(check_prev_close)
                elif type_name == 'Fund':
                    server_prev_close = '%.3f' % server_prev_close
                    check_prev_close = '%.3f' % float(check_prev_close)
                else:
                    server_prev_close = '%.2f' % server_prev_close
                    check_prev_close = '%.2f' % float(check_prev_close)

                if check_prev_close is not None and server_prev_close != check_prev_close:
                    error_flag = True
                    tr_list.append('%s(Error)' % server_prev_close)
                else:
                    tr_list.append(server_prev_close)
            tr_list.append('Error(Error)' if error_flag else '')
            table_list.append(tr_list)
            export_message_list.append('%s,%s,%s,%s' % (type_str, instrument_db.ticker, wind_ticker, wind_prev_close))
        table_list.sort()
        html_list = email_utils2.list_to_html(html_title, table_list)
        email_list.append(''.join(html_list))
    _export_price_check(export_message_list)


# 价格校验信息保存到文件中
def _export_price_check(export_message_list):
    export_message_list.insert(0, 'check_type,ticker,wind_ticker,wind_prev_close')

    save_file_folder = '%s/%s' % (PRICE_FILES_BACKUP_FOLDER, date_utils.get_today_str('%Y%m%d'))
    if not os.path.exists(save_file_folder):
        os.mkdir(save_file_folder)
    save_file_path = '%s/price_check_%s.csv' % (save_file_folder, date_utils.get_today_str('%Y-%m-%d'))
    with open(save_file_path, 'w') as fr:
        fr.write('\n'.join(export_message_list))


def __cross_market_check(server_model):
    email_list.append('<li>CrossMarket ETF Check</li>')
    session = server_model.get_db_session('common')
    query = session.query(Instrument)
    cross_market_etf_size = query.filter(Instrument.type_id == Instrument_Type_Enums.MutualFund,
                                         Instrument.cross_market == 1).count()
    email_list.append('crossmarket etf num:%s<br/><br/>' % cross_market_etf_size)


def __fund_pcf_check(server_name_list):
    etf_set_dict = dict()
    trading_day_error_dict = dict()
    today_filter_str = date_utils.get_today_str('%Y%m%d')
    for server_name in server_name_list:
        etf_set = set()
        trading_day_error_list = []
        for server_account_key in server_account_dict.keys():
            if server_name not in server_account_key:
                continue

            account_db = server_account_dict[server_account_key]
            if account_db.allowed_etf_list is None:
                continue
            allow_etf_str = account_db.allowed_etf_list
            for etf_ticker in allow_etf_str.split(';'):
                if etf_ticker.strip() != '':
                    etf_set.add(etf_ticker)

        for etf_ticker in list(etf_set):
            dict_key = '%s|%s' % (server_name, etf_ticker)
            if dict_key not in server_instrument_ticker_dict:
                continue
            instrument_db = server_instrument_ticker_dict[dict_key]
            if instrument_db.type_id == 16:
                etf_set.remove(instrument_db.ticker)
                continue

            if instrument_db.pcf is None or instrument_db.pcf == '':
                continue
            pcf_dict = json.loads(instrument_db.pcf)
            if pcf_dict['TradingDay'] == today_filter_str:
                if instrument_db.ticker in etf_set:
                    etf_set.remove(instrument_db.ticker)
            else:
                trading_day_error_list.append(instrument_db.ticker + '|' + pcf_dict['TradingDay'])

            if 'Components' not in pcf_dict:
                continue
        etf_set_dict[server_name] = etf_set
        trading_day_error_dict[server_name] = trading_day_error_list

    html_title = 'Index,%s' % ','.join(server_name_list)
    table_list = [['Allow ETF Error list'] +
                  ['<br/>'.join(etf_set_dict[server_name]) for server_name in server_name_list],
                  ['ETF TradingDay Error list'] +
                  ['<br/>'.join(trading_day_error_dict[server_name]) for server_name in server_name_list]]
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def __option_call_put_check(server_name_list):
    option_error_list_dict = dict()
    for server_name in server_name_list:
        dict_key = '%s|%s' % (server_name, 10)
        option_error_list = []
        for instrument_db in server_instrument_type_dict[dict_key]:
            name = instrument_db.name
            put_call = instrument_db.put_call
            if ('Call' in name) and (0 == put_call):
                option_error_list.append(instrument_db.ticker)
            elif ('Put' in name) and (1 == put_call):
                option_error_list.append(instrument_db.ticker)
            elif ('-C-' in name) and (0 == put_call):
                option_error_list.append(instrument_db.ticker)
            elif ('-P-' in name) and (1 == put_call):
                option_error_list.append(instrument_db.ticker)
        option_error_list_dict[server_name] = option_error_list

    email_list.append('<h4>Call&Put Check------------</h4>')
    html_title = ','.join(server_name_list)
    table_list = [option_error_list_dict[server_name] for server_name in server_name_list]
    if table_list:
        html_list = email_utils2.list_to_html(html_title, table_list)
        email_list.append(''.join(html_list))


def __option_track_undl_tickers_check(server_name_list):
    option_error_dict = dict()
    for server_name in server_name_list:
        dict_key = '%s|%s' % (server_name, 10)
        null_number = 0
        for instrument_db in server_instrument_type_dict[dict_key]:
            if instrument_db.track_undl_tickers is None:
                null_number += 1
        option_error_dict[server_name] = null_number

    email_list.append('<h4>track_undl_tickers Check------------</h4>')
    html_title = ','.join(server_name_list)
    tr_list = []
    for server_name in server_name_list:
        if option_error_dict[server_name] > 0:
            tr_list.append('%s(Error)' % option_error_dict[server_name])
        else:
            tr_list.append('%s' % option_error_dict[server_name])
    html_list = email_utils2.list_to_html(html_title, [tr_list])
    email_list.append(''.join(html_list))


def __account_position_check(server_name_list):
    account_id_set = set()
    for server_account_key in server_account_dict.keys():
        account_id = int(server_account_key.split('|')[1])
        account_id_set.add(account_id)
    account_id_list = list(account_id_set)
    account_id_list.sort()

    html_title = 'Account,%s' % ','.join(server_name_list)
    table_list = []
    for account_id in account_id_list:
        tr_list = [account_id, ]
        for server_name in server_name_list:
            server_account_key = '%s|%s' % (server_name, account_id)
            if server_account_key not in server_account_dict:
                tr_list.append('/')
                continue
            account_db = server_account_dict[server_account_key]

            server_position_key = '%s|%s' % (server_name, account_id)
            if server_position_key in server_position_dict:
                update_date = server_position_dict[server_position_key][0].update_date
                validate_number = int(date_utils.get_today_str('%H%M%S'))
                if validate_number > 200000:
                    if date_utils.datetime_toString(update_date, '%H%M%S') > 200000:
                        tr_list.append('%s_%s' % (account_db.accounttype, update_date))
                    else:
                        tr_list.append('%s_%s(Error)' % (account_db.accounttype, update_date))
                else:
                    tr_list.append('%s_%s' % (account_db.accounttype, update_date))
            else:
                tr_list.append('%s_%s(Error)' % (account_db.accounttype, 'Null'))
        table_list.append(tr_list)
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def __strategy_parameter_check(server_name_list):
    future_dict = dict()
    dict_key = '%s|%s' % (server_name_list[0], 1)
    for instrument_db in server_instrument_type_dict[dict_key]:
        future_dict[instrument_db.ticker] = instrument_db

    for server_name in server_name_list:
        email_list.append('<font>Strategy Parameter Check: %s</font><br/>' % server_name)

        strategy_check_result = []
        strategy_parameter_db = server_strategy_parameter_dict[server_name]
        strategy_name = strategy_parameter_db.name
        strategy_parameter_dict = json.loads(strategy_parameter_db.value)
        calendar_future_dict = dict()
        for (dict_key, dict_value) in strategy_parameter_dict.items():
            if 'BackFuture' in dict_key:
                back_future_name = dict_value
                if back_future_name == '' or back_future_name not in future_dict:
                    email_list.append('<font color=red>strategy:%s BackFuture:%s can not find!</font><br/>'
                                      % (strategy_name, dict_key))
                    continue
                else:
                    back_future_db = future_dict[back_future_name]
                    interval_days = date_utils.get_interval_days(date_utils.get_today_str("%Y-%m-%d %H:%M:%S"),
                                                                 date_utils.datetime_toString(
                                                                     back_future_db.expire_date, "%Y-%m-%d %H:%M:%S"))
                    if back_future_db.exchange_id != 25 and interval_days <= 33:
                        strategy_check_result.append(
                            '<font color=red>strategy:%s BackFuture:%s expire_date:%s less than 30days!</font><br/>' % \
                            (strategy_name, back_future_name,
                             back_future_db.expire_date.strftime("%Y-%m-%d")))

                ticker_type = filter(lambda x: not x.isdigit(), back_future_name)
                if ticker_type in calendar_future_dict:
                    calendar_future_dict[ticker_type].append(back_future_name)
                else:
                    calendar_future_dict[ticker_type] = [back_future_name, ]

                ticker_month = filter(lambda x: x.isdigit(), back_future_name)
                future_maincontract_db = maincontract_dict[ticker_type]
                main_symbol_month = filter(lambda x: x.isdigit(), future_maincontract_db.main_symbol)
                if str(ticker_month) < str(main_symbol_month):
                    strategy_check_result.append(
                        '<font color=red>strategy:%s BackFuture:%s, Main Contract is:%s</font><br/>' % (
                            strategy_name, back_future_name, future_maincontract_db.main_symbol))
            elif 'FrontFuture' in dict_key:
                front_future_name = dict_value
                if front_future_name == '' or front_future_name not in future_dict:
                    email_list.append('<font color=red>strategy:%s FrontFuture:%s can not find!</font><br/>' %
                                      (strategy_name, dict_key))
                    continue
                else:
                    front_future_db = future_dict[front_future_name]
                    interval_days = date_utils.get_interval_days(date_utils.get_today_str("%Y-%m-%d %H:%M:%S"),
                                                                 date_utils.datetime_toString(
                                                                     front_future_db.expire_date, "%Y-%m-%d %H:%M:%S"))

                    if front_future_db.exchange_id != 25 and interval_days <= 30:
                        strategy_check_result.append(
                            '<font color=red>strategy:%s FrontFuture:%s expire_date:%s less than 30days!</font><br/>' % \
                            (strategy_name, front_future_name,
                             front_future_db.expire_date.strftime("%Y-%m-%d")))

                ticker_type = filter(lambda x: not x.isdigit(), front_future_name)
                if ticker_type in calendar_future_dict:
                    calendar_future_dict[ticker_type].append(front_future_name)
                else:
                    calendar_future_dict[ticker_type] = [front_future_name, ]

                ticker_month = filter(lambda x: x.isdigit(), front_future_name)
                future_maincontract_db = maincontract_dict[ticker_type]
                main_symbol_month = filter(lambda x: x.isdigit(), future_maincontract_db.main_symbol)
                if str(ticker_month) < str(main_symbol_month):
                    strategy_check_result.append(
                        '<font color=red>strategy:%s FrontFuture:%s, Main Contract is:%s</font><br/>' % (
                            strategy_name, front_future_name, future_maincontract_db.main_symbol))

        for (key, ticker_list) in calendar_future_dict.items():
            if len(ticker_list) != 2:
                continue
            if ticker_list[0] == ticker_list[1]:
                strategy_check_result.append(
                    '<font color=red>Ticker:%s FrontFuture and BackFuture Is Same!</font><br/>' % ticker_list[0])

        if len(strategy_check_result) > 0:
            strategy_check_result.insert(0, '<li>Server:%s, Check Strategy Parameter</li>' % server_name)
            email_list.extend(strategy_check_result)
            email_utils16.send_email_group_all('[Warning]Strategy Parameter Check',
                                               '\n'.join(strategy_check_result), 'html')


def __account_trade_restrictions_check(server_name_list):
    email_list.append('<li>Check Today Cancel</li>')

    html_title = ',%s' % ','.join(server_name_list)
    tr_list = ['account trade restrictions']
    for server_name in server_name_list:
        today_cancel_sum_num = server_cancel_number_dict[server_name]
        if today_cancel_sum_num == 0 or today_cancel_sum_num is None:
            tr_list.append('%s(Error)' % today_cancel_sum_num)
        else:
            tr_list.append(today_cancel_sum_num)

    html_list = email_utils2.list_to_html(html_title, [tr_list])
    email_list.append(''.join(html_list))


def get_instrument_information(server_name):
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')

    query_sql_column = "select COLUMN_NAME from information_schema.COLUMNS " \
                       "where table_name = 'instrument' and table_schema = 'common'"
    query_result_column = session_common.execute(query_sql_column)
    column_list = []
    for query_line in query_result_column:
        if query_line[0] not in column_filter_list:
            column_list.append(query_line[0])
    column_list = sorted(column_list)

    query_sql_instrument_value = "select %s from common.instrument where del_flag = 0 order by id" % \
                                 ','.join(column_list)
    query_result_instrument_value = session_common.execute(query_sql_instrument_value)

    instrument_dict = dict()
    ticker_list = []
    for result_line in query_result_instrument_value:
        instrument_value_dict = dict()
        ticker_name = None
        for index, column in enumerate(column_list):
            if column.upper() == "TICKER":
                ticker_name = result_line[index]
                ticker_list.append(ticker_name)
            instrument_value_dict[column] = result_line[index]

        if not ticker_name is None:
            instrument_dict[ticker_name] = instrument_value_dict
    server_model.close()
    return instrument_dict, column_list, ticker_list


def __check_future_ticker(server_name, future_ticker):
    check_info_list = []
    future_type = filter(lambda x: not x.isdigit(), future_ticker)
    future_index_str = filter(lambda x: x.isdigit(), future_ticker)

    future_maincontract_db = maincontract_dict[future_type]
    validate_index_str = filter(lambda x: x.isdigit(), future_maincontract_db.main_symbol)
    if future_index_str < validate_index_str:
        check_info_list.append('Not Main')

    future_dict = dict()
    dict_key = '%s|%s' % (server_name, 1)
    for instrument_db in server_instrument_type_dict[dict_key]:
        future_dict[instrument_db.ticker] = instrument_db
    front_future_db = future_dict[future_ticker]
    interval_days = date_utils.get_interval_days(date_utils.get_today_str("%Y-%m-%d %H:%M:%S"),
                                                 date_utils.datetime_toString(
                                                     front_future_db.expire_date, "%Y-%m-%d %H:%M:%S"))
    if front_future_db.exchange_id != 25 and interval_days <= 30:
        check_info_list.append('ExpireDate less than 30days!')
    return check_info_list


def __position_check(server_name_list):
    future_ticker_list = []
    dict_key = '%s|%s' % (server_name_list[0], 1)
    for instrument_db in server_instrument_type_dict[dict_key]:
        future_ticker_list.append(instrument_db.ticker)
    table_list = []
    for server_name in server_name_list:
        for dict_key in server_pf_position_dict.keys():
            if server_name not in dict_key:
                continue
            pf_position_list = server_pf_position_dict[dict_key]
            for pf_position_db in pf_position_list:
                future_symbol = pf_position_db.symbol.split(' ')[0]
                if future_symbol not in future_ticker_list:
                    continue

                check_info_list = __check_future_ticker(server_name, future_symbol)
                if len(check_info_list) > 0:
                    table_list.append(
                        (server_name, 'pf_position', pf_position_db.symbol, ';'.join(check_info_list)))

        for dict_key in server_position_dict.keys():
            if server_name not in dict_key:
                continue
            position_list = server_position_dict[dict_key]
            for position_db in position_list:
                future_symbol = position_db.symbol.split(' ')[0]
                if future_symbol not in future_ticker_list:
                    continue

                check_info_list = __check_future_ticker(server_name, future_symbol)
                if len(check_info_list) > 0:
                    table_list.append(
                        (server_name, 'account_position', position_db.symbol, '\n'.join(check_info_list)))
    if table_list:
        html_title = 'Server_Name,Position_Type,Symbol,Error_Message'
        email_list.append(''.join(email_utils2.list_to_html(html_title, table_list)))


def __instrument_check(server_name_list):
    instrument_ticker_list = []
    filter_server_name = server_name_list[0]
    for dict_key in server_instrument_ticker_dict.keys():
        if filter_server_name not in dict_key:
            continue
        instrument_ticker_list.append(server_instrument_ticker_dict[dict_key].ticker)

    check_column_list = []
    for column_name in dir(Instrument):
        if '__' in column_name or column_name in column_filter_list:
            continue
        check_column_list.append(column_name)

    table_list = []
    for column_name in check_column_list:
        for instrument_ticker in instrument_ticker_list:
            tr_list = [column_name, instrument_ticker]
            error_flag = False
            check_value = '|'
            for server_name in server_name_list:
                dict_key = '%s|%s' % (server_name, instrument_ticker)
                instrument_db = server_instrument_ticker_dict[dict_key]
                column_value = getattr(instrument_db, column_name)
                if check_value == '|':
                    check_value = column_value

                if column_value != check_value:
                    error_flag = True
                    tr_list.append('%s(Error)' % column_value)
                else:
                    tr_list.append(column_value)

            if error_flag:
                table_list.append(tr_list)

    html_title = 'Column,Ticker,%s' % ','.join(server_name_list)
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def __wind_ticker_convert(instrument_db):
    wind_ticker_str = None
    if instrument_db.exchange_id == 18:
        if instrument_db.type_id == 6:
            wind_ticker_str = '%s.SH' % instrument_db.ticker_exch_real
        else:
            wind_ticker_str = '%s.SH' % instrument_db.ticker
    elif instrument_db.exchange_id == 19:
        wind_ticker_str = '%s.SZ' % instrument_db.ticker
    elif instrument_db.exchange_id == 19:
        wind_ticker_str = '%s.SZ' % instrument_db.ticker
    elif instrument_db.exchange_id == 20:
        wind_ticker_str = '%s.SHF' % instrument_db.ticker
    elif instrument_db.exchange_id == 21:
        wind_ticker_str = '%s.DCE' % instrument_db.ticker
    elif instrument_db.exchange_id == 22:
        wind_ticker_str = '%s.CZC' % instrument_db.ticker
    elif instrument_db.exchange_id == 25:
        wind_ticker_str = '%s.CFE' % instrument_db.ticker
    elif instrument_db.exchange_id == 35:
        wind_ticker_str = '%s.INE' % instrument_db.ticker
    return wind_ticker_str


def __build_db_dict(server_name_list, build_level=1):
    global maincontract_dict, server_account_dict, server_position_dict, server_pf_position_dict, \
        server_instrument_type_dict, server_instrument_ticker_dict, server_strategy_parameter_dict, \
        server_cancel_number_dict
    maincontract_dict = dict()
    server_account_dict = dict()
    server_position_dict = dict()
    server_pf_position_dict = dict()
    server_instrument_type_dict = dict()
    server_instrument_ticker_dict = dict()
    server_strategy_parameter_dict = dict()
    server_cancel_number_dict = dict()

    today_filter_str = date_utils.get_today_str('%Y-%m-%d')
    server_host = server_constant.get_server_model('host')
    if build_level == 1:
        session_common = server_host.get_db_session('common')
        query = session_common.query(FutureMainContract)
        for future_maincontract_db in query:
            maincontract_dict[future_maincontract_db.ticker_type] = future_maincontract_db

    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(RealAccount)
        for account_db in query.filter(RealAccount.enable == 1):
            server_account_key = '%s|%s' % (server_name, account_db.accountid)
            server_account_dict[server_account_key] = account_db

        query = session_portfolio.query(AccountPosition)
        for position_db in query.filter(AccountPosition.date == today_filter_str):
            server_position_key = '%s|%s' % (server_name, position_db.id)
            if server_position_key in server_position_dict:
                server_position_dict[server_position_key].append(position_db)
            else:
                server_position_dict[server_position_key] = [position_db]

        if build_level <= 2:
            instrument_list = query_use_instrument_dict(server_name)
            for instrument_db in instrument_list:
                dick_key = '%s|%s' % (server_name, instrument_db.type_id)
                if dick_key in server_instrument_type_dict:
                    server_instrument_type_dict[dick_key].append(instrument_db)
                else:
                    server_instrument_type_dict[dick_key] = [instrument_db]

                dick_key = '%s|%s' % (server_name, instrument_db.ticker)
                server_instrument_ticker_dict[dick_key] = instrument_db

        if build_level == 1:
            query = session_portfolio.query(PfPosition)
            for pf_position_db in query.filter(PfPosition.date == today_filter_str):
                server_position_key = '%s|%s' % (server_name, pf_position_db.id)
                if server_position_key in server_pf_position_dict:
                    server_pf_position_dict[server_position_key].append(pf_position_db)
                else:
                    server_pf_position_dict[server_position_key] = [pf_position_db]

            if server_model.is_night_session:
                query_sql = "select sum(today_cancel) from account_trade_restrictions t where t.today_cancel > 0"
                today_cancel_number = session_portfolio.execute(query_sql).first()[0]
                server_cancel_number_dict[server_name] = today_cancel_number

            if server_model.is_calendar_server:
                calendar_strategy_name = 'CalendarMA.SU'
                session_strategy = server_model.get_db_session('strategy')
                strategy_parameter_db = session_strategy.query(StrategyParameter).filter(
                    StrategyParameter.name == calendar_strategy_name).order_by(desc(StrategyParameter.time)).first()
                server_strategy_parameter_dict[server_name] = strategy_parameter_db
        server_model.close()
    server_host.close()


def __calendar_parameter_create_check(server_name_list):
    html_list = []
    # build back future dict
    back_future_dict = dict()
    server_model_host = server_constant.get_server_model('host')
    session_common = server_model_host.get_db_session('common')
    query_sql = "select `ticker_type`, `next_main_symbol` from common.future_main_contract;"
    query_result = session_common.execute(query_sql)
    for query_line in query_result:
        future_name = query_line[0]
        back_future_name = query_line[1]
        back_future_dict[future_name.lower()] = back_future_name

    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_strategy = server_model.get_db_session('strategy')
        query_sql = 'select `VALUE` from strategy.strategy_parameter where `NAME` = "CalendarMA.SU" ' \
                    'order by time desc limit 1'
        query_result = session_strategy.execute(query_sql)
        for query_line in query_result:
            calendar_parameter_str = query_line[0]
            calendar_parameter_dict = json.loads(calendar_parameter_str)
            for [parameter_key, parameter_value] in sorted(calendar_parameter_dict.items()):
                if 'BackFuture' in parameter_key and parameter_value == '':
                    future_name = parameter_key.split('.')[0]
                    back_future = back_future_dict[future_name.lower()]
                    query_sql2 = "select * from common.instrument where ticker = '%s';" % back_future
                    query_result2 = session_strategy.execute(query_sql2)
                    back_future_exist_flag = False
                    for query_result2_item in query_result2:
                        back_future_exist_flag = True

                    if back_future_exist_flag:
                        html_list.append('<font color=red>%s in CalendarMA has been created. Set enable = 1!</font><br>'
                                         % back_future)
        server_model.close()
    server_model_host.close()
    email_list.append(''.join(html_list))


def db_check_job(server_name_list):
    __wind_login()
    global email_list
    email_list = []

    subject = 'DailyCheck Result'

    __build_db_dict(server_name_list)

    email_list.append('<li>Check Account Position</li>')
    __account_position_check(server_name_list)

    check_type_dict = {'1': 'Future', '4': 'Stock', '6': 'Index', '7|15|16': 'Fund', '10': 'Option'}
    __price_check(server_name_list, check_type_dict)

    email_list.append('<br><br><li>Check Fund PCF Information</li>')
    __fund_pcf_check(server_name_list)

    email_list.append('<br><br><li>Check Option Call/Put</li>')
    __option_call_put_check(server_name_list)

    email_list.append('<br><br><li>Check Option Track_undl_tickers</li>')
    __option_track_undl_tickers_check(server_name_list)

    email_list.append('<br><br><li>Check Strategy Parameter</li>')
    calendar_server_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        if server_model.is_calendar_server:
            calendar_server_list.append(server_name)
    __strategy_parameter_check(calendar_server_list)

    night_session_server_list = server_constant.get_night_session_servers()
    email_list.append('<br><br><li>Check Account Trade Restrictions</li>')
    __account_trade_restrictions_check(night_session_server_list)

    email_list.append('<br><br><li>Check Position</li>')
    __position_check(server_name_list)

    # email_list.append('<br><br><li>Check Instrument</li>')
    # __instrument_check(server_name_list)

    email_list.append('<br><br><li>Check calendar parameter create</li>')
    __calendar_parameter_create_check(calendar_server_list)

    email_utils2.send_email_group_all(subject, '\n'.join(email_list), 'html')
    __wind_close()


def db_check_future_job(server_name_list):
    __wind_login()
    global email_list
    email_list = []

    subject = 'DailyCheck Result'
    build_level = 2
    __build_db_dict(server_name_list, build_level)

    email_list.append('<li>check Account Position</li>')
    __account_position_check(server_name_list)

    check_type_dict = {'1': 'Future', }
    __price_check(server_name_list, check_type_dict)

    email_utils2.send_email_group_all(subject, '\n'.join(email_list), 'html')
    __wind_close()


def account_check_job(server_name_list):
    global email_list
    email_list = []

    subject = 'Account Check Result'
    build_level = 3
    __build_db_dict(server_name_list, build_level)

    email_list.append('<li>check Account Position</li>')
    __account_position_check(server_name_list)

    email_utils2.send_email_group_all(subject, '\n'.join(email_list), 'html')


exchange_list = [Exchange_Type_Enums.CG, Exchange_Type_Enums.CS, Exchange_Type_Enums.SHF,
                 Exchange_Type_Enums.DCE, Exchange_Type_Enums.ZCE, Exchange_Type_Enums.CFF,
                 Exchange_Type_Enums.INE
                 ]
check_type_dict = {'1': 'Future', '4': 'Stock', '6': 'Index', '7|15|16': 'Fund', '10': 'Option'}


class DailyCheck(object):
    def __init__(self, server_list):
        self.__date_str = date_utils.get_today_str('%Y-%m-%d')
        self.__next_date_str = date_utils.get_next_trading_day('%Y-%m-%d')
        self.__server_list = server_list
        self.__main_contract_df = pd.DataFrame()
        self.__instrument_df = pd.DataFrame()
        self.__account_position_df = pd.DataFrame()
        self.__calendar_parameter_df = pd.DataFrame()
        self.__pf_position_df = pd.DataFrame()
        self.__check_email_list = []

    def __load_from_db(self):
        host_name = 'host'
        server_host = server_constant.get_server_model(host_name)
        session_common = server_host.get_db_session('common')
        future_main_contract_list = []
        for item in session_common.query(FutureMainContract):
            future_main_contract_list.append([item.ticker_type, item.main_symbol])
        self.__main_contract_df = pd.DataFrame(future_main_contract_list, columns=['Future_Type', 'MainFuture'])

        instrument_list = []
        query = session_common.query(Instrument)
        for item in query.filter(Instrument.del_flag == 0, Instrument.exchange_id.in_(exchange_list)):
            instrument_list.append([item.ticker, item.ticker_exch_real, item.exchange_id, item.type_id,
                                    item.name, item.put_call, item.pcf, item.track_undl_tickers, item.expire_date,
                                    item.prev_close])
        instrument_df = pd.DataFrame(instrument_list,
                                     columns=['Ticker', 'Ticker_Exch_Real', 'Exchange_ID', 'Type_ID', 'Name',
                                              'Put_Call', 'PCF', 'Track_Undl_Tickers', 'Expire_Date',
                                              '%s_Prev_Close' % host_name])
        check_instrument_df = pd.DataFrame()
        for (type_id_str, type_name) in check_type_dict.items():
            temp_instrument_df = instrument_df[instrument_df['Type_ID'].isin(type_id_str.split('|'))]
            if type_id_str != '6':
                temp_instrument_df = temp_instrument_df.sample(n=15)
            check_instrument_df = pd.concat([check_instrument_df, temp_instrument_df])
        check_instrument_df.loc[:, 'Ticker_Wind'] = check_instrument_df.apply(
            lambda row: self.__format_ticker_wind(row), axis=1)
        wind_prev_close_df = self.__wind_prev_close_df(list(check_instrument_df['Ticker_Wind']))
        check_instrument_df = pd.merge(check_instrument_df, wind_prev_close_df, how='left', on=['Ticker_Wind'])

        account_position_list = []
        pf_position_list = []
        calendar_parameter_list = []
        for server_name in self.__server_list:
            server_model = server_constant.get_server_model(server_name)
            session_common = server_model.get_db_session('common')
            server_instrument_list = []
            for item in session_common.query(Instrument).filter(Instrument.ticker.in_(check_instrument_df['Ticker'])):
                server_instrument_list.append([item.ticker, item.prev_close])
            server_instrument_df = pd.DataFrame(server_instrument_list,
                                                columns=['Ticker', '%s_Prev_Close' % server_name])
            check_instrument_df = pd.merge(check_instrument_df, server_instrument_df, how='left', on=['Ticker'])

            session_portfolio = server_model.get_db_session('portfolio')
            account_dict = {x.accountid: x for x in
                            session_portfolio.query(RealAccount).filter(RealAccount.enable == 1)}
            for item in session_portfolio.query(AccountPosition).filter(AccountPosition.date == self.__date_str):
                if item.symbol == 'CNY' or item.id not in account_dict:
                    continue
                real_account_db = account_dict[item.id]
                account_position_list.append([server_name, item.id, real_account_db.accountname,
                                              real_account_db.accounttype, item.symbol, item.long, item.short,
                                              item.update_date])

            pf_account_dict = {x.id: x for x in session_portfolio.query(PfAccount)}
            for item in session_portfolio.query(PfPosition).filter(PfPosition.date == self.__date_str):
                if item.id == -1 or item.id not in pf_account_dict:
                    continue
                pf_account_db = pf_account_dict[item.id]
                pf_position_list.append([server_name, item.id, pf_account_db.name, pf_account_db.fund_name,
                                         item.symbol, item.long, item.short])

            if server_model.is_calendar_server:
                calendar_strategy_name = 'CalendarMA.SU'
                session_strategy = server_model.get_db_session('strategy')
                strategy_parameter_db = session_strategy.query(StrategyParameter).filter(
                    StrategyParameter.name == calendar_strategy_name).order_by(desc(StrategyParameter.time)).first()
                strategy_parameter_dict = json.loads(strategy_parameter_db.value)

                future_type_list = [x.split('.')[0] for x in strategy_parameter_dict.keys()]
                for future_type in list(set(future_type_list)):
                    if '%s.FrontFuture' % future_type not in strategy_parameter_dict and \
                            '%s.BackFuture' % future_type not in strategy_parameter_dict:
                        continue
                    calendar_parameter_list.append([server_name, calendar_strategy_name, future_type,
                                                    strategy_parameter_dict['%s.FrontFuture' % future_type],
                                                    strategy_parameter_dict['%s.BackFuture' % future_type]])

        self.__account_position_df = pd.DataFrame(account_position_list,
                                                  columns=['Server', 'Account_ID', 'Account_Name', 'Account_Type',
                                                           'Symbol', 'Long', 'Short', 'Update_Date'])
        self.__pf_position_df = pd.DataFrame(pf_position_list, columns=['Server', 'Account_ID', 'Account_Name',
                                                                        'Fund_Name', 'Symbol', 'Long', 'Short'])
        self.__calendar_parameter_df = pd.DataFrame(calendar_parameter_list, columns=['Server', 'Strategy_Name',
                                                                                      'Future_Type', 'FrontFuture',
                                                                                      'BackFuture'])
        self.__instrument_df = check_instrument_df

    def __format_ticker_wind(self, row):
        wind_ticker_str = None
        if row['Exchange_ID'] == 18:
            if row['Type_ID'] == 6:
                wind_ticker_str = '%s.SH' % row['Ticker_Exch_Real']
            else:
                wind_ticker_str = '%s.SH' % row['Ticker']
        elif row['Exchange_ID'] == 19:
            wind_ticker_str = '%s.SZ' % row['Ticker']
        elif row['Exchange_ID'] == 19:
            wind_ticker_str = '%s.SZ' % row['Ticker']
        elif row['Exchange_ID'] == 20:
            wind_ticker_str = '%s.SHF' % row['Ticker']
        elif row['Exchange_ID'] == 21:
            wind_ticker_str = '%s.DCE' % row['Ticker']
        elif row['Exchange_ID'] == 22:
            wind_ticker_str = '%s.CZC' % row['Ticker']
        elif row['Exchange_ID'] == 25:
            wind_ticker_str = '%s.CFE' % row['Ticker']
        elif row['Exchange_ID'] == 35:
            wind_ticker_str = '%s.INE' % row['Ticker']
        return wind_ticker_str

    def __wind_prev_close_df(self, ticker_list):
        w = w_ys()
        pre_night_market_flag = date_utils.is_pre_night_market()
        query_date_str = self.__next_date_str if pre_night_market_flag else self.__date_str
        prev_close_dict = w.query_wsd_data("pre_close", ticker_list, query_date_str)
        prev_close_list = [[ticker, prev_close] for (ticker, prev_close) in prev_close_dict.items()]
        prev_close_df = pd.DataFrame(prev_close_list, columns=['Ticker_Wind', 'Wind_Prev_Close'])
        w_ys_close()
        return prev_close_df

    def daily_check_index(self):
        self.__load_from_db()
        # self.__check_position()
        # self.__check_prev_close()
        # self.__check_etf_pcf()
        self.__check_put_call()
        self.__check_track_undl_tickers()

    def __validate_update_date(self, row):
        validate_number = int(date_utils.get_today_str('%H%M%S'))
        if validate_number > 200000:
            if date_utils.datetime_toString(row['Update_Date'], '%H%M%S') < 200000:
                check_update_date = row['Update_Date']
            else:
                check_update_date = '%s(Error)' % row['Update_Date']
        else:
            check_update_date = row['Update_Date']
        return check_update_date

    def __check_position(self):
        self.__check_email_list.append('<li>Check Account Position</li>')

        check_position_df = self.__account_position_df[['Server', 'Account_ID', 'Account_Type', 'Update_Date']].groupby(
            ['Server', 'Account_ID', 'Account_Type']).max().reset_index()

        check_position_df.loc[:, 'Check_Update_Date'] = check_position_df.apply(
            lambda row: self.__validate_update_date(row), axis=1)
        check_position_df.loc[:, 'Update_Info'] = check_position_df.apply(
            lambda row: '%s_%s' % (row['Account_Type'], row['Check_Update_Date']), axis=1)
        check_result_df = check_position_df.pivot_table(index='Account_ID', columns='Server', values='Update_Info',
                                                        aggfunc=np.sum)
        check_result_df['Account_ID'] = check_result_df.index
        self.__check_email_list.append(check_result_df.to_html(index=False))

    def __check_prev_close(self):
        self.__check_email_list.append('<li>Check Prev Close</li>')

        filter_title = ['Ticker', 'Type_ID']
        temp_server_list = ['host', 'Wind'] + list(self.__server_list)
        for server_name in temp_server_list:
            filter_title.append('%s_Prev_Close' % server_name)
        prev_close_df = self.__instrument_df[filter_title]
        prev_close_df.loc[:, 'Check_Flag'] = prev_close_df.apply(lambda row: self.__validate_prev_close(row), axis=1)
        prev_close_df.loc[:, 'Type'] = prev_close_df.apply(lambda row: self.__format_type(row['Type_ID']), axis=1)
        self.__check_email_list.append(prev_close_df.to_html(index=False))

    def __validate_prev_close(self, row):
        validate_str = ''
        wind_prev_close = row['Wind_Prev_Close']
        host_prev_close = row['host_Prev_Close']
        if str(wind_prev_close) == 'nan':
            validate_str = 'Warning'
        elif wind_prev_close != host_prev_close:
            validate_str = 'Error(Error)'
        else:
            for server_name in self.__server_list:
                server_prev_close = row['%s_Prev_Close' % server_name]
                if wind_prev_close != server_prev_close:
                    validate_str = 'Error(Error)'
                    break
        return validate_str

    def __format_type(self, type_id):
        type_str = ''
        for type_id_str, type_name in check_type_dict.items():
            type_id_list = type_id_str.split('|')
            if str(type_id) in type_id_list:
                type_str = type_name
                break
        return type_str

    def __check_etf_pcf(self):
        pass

    def __check_put_call(self):
        self.__check_email_list.append('<br><br><li>Check Option Call/Put</li>')
        option_df = self.__instrument_df[self.__instrument_df['Type_ID'] == 10]
        option_df.loc[:, 'Error_Flag'] = option_df.apply(lambda row: self.__validate_option(row), axis=1)
        error_option_df = option_df[option_df['Error_Flag'] == 1]

        self.__check_email_list.append(error_option_df.to_html(index=False))

    def __validate_option(self, row):
        error_flag = 0
        name = row['Name']
        put_call = row['Put_Call']
        if ('Call' in name) and (0 == put_call):
            error_flag = 1
        elif ('Put' in name) and (1 == put_call):
            error_flag = 1
        elif ('-C-' in name) and (0 == put_call):
            error_flag = 1
        elif ('-P-' in name) and (1 == put_call):
            error_flag = 1
        return error_flag

    def __check_track_undl_tickers(self):
        self.__check_email_list.append('<br><br><li>Check Option Track_Undl_Tickers</li>')
        option_df = self.__instrument_df[(self.__instrument_df['Type_ID'] == 10) &
                                         (self.__instrument_df['Track_Undl_Tickers'] is None)]
        self.__check_email_list.append(option_df.to_html(index=False))

    def __check_calendar_parameter(self):
        calendar_df = pd.merge(self.__calendar_parameter_df, self.__main_contract_df, how='left', on=['Future_Type'])
        instrument_df = self.__instrument_df[['Ticker', 'Expire_Date']]

        future_instrument_df = instrument_df.rename(columns={'Ticker': 'FrontFuture', 'Expire_Date': 'Front_Expire_Date'})
        calendar_df = pd.merge(calendar_df, future_instrument_df, how='left', on=['FrontFuture'])
        back_instrument_df = instrument_df.rename(columns={'Ticker': 'BackFuture', 'Expire_Date': 'Back_Expire_Date'})
        calendar_df = pd.merge(calendar_df, back_instrument_df, how='left', on=['BackFuture'])

        calendar_df.loc[:, 'Error_Message'] = calendar_df.apply(
            lambda row: self.__validate_calendar_parameter(row), axis=1)

    def __validate_calendar_parameter(self, row):
        main_future = row['MainFuture']
        front_future = row['FrontFuture']
        front_expire_date = row['Front_Expire_Date']
        back_future = row['BackFuture']
        back_expire_date = row['Back_Expire_Date']

        error_message_list = []
        if front_future == '' or back_future == '':
            error_message_list.append('BackFuture Or FrontFuture Is Null')
        else:
            interval_days = date_utils.get_interval_days(date_utils.get_now(), front_expire_date)
            # if back_future_db.exchange_id != 25 and interval_days <= 33:


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    # db_check_job(trade_servers_list)
    daily_check = DailyCheck(('guoxin', 'nanhua'))
    daily_check.daily_check_index()
