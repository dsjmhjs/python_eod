# -*- coding: utf-8 -*-
# 对每日更新的数据进行校验
import os
import json
import random
import pandas as pd
from eod_aps.job import *
from sqlalchemy import desc
from eod_aps.model.schema_portfolio import RealAccount, PfPosition, AccountPosition
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.model.schema_strategy import StrategyParameter, StrategyOnline
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
FORMAT_STR = '%Y-%m-%d %H:%M:%S'


def __wind_login():
    global w
    w = w_ys()


def __wind_close():
    w_ys_close()


def __wind_prev_close_dict(check_instrument_dict):
    check_wind_ticker_list = []
    pre_night_market_flag = date_utils.is_pre_night_market()
    filter_date_str = date_utils.get_next_trading_day('%Y-%m-%d') if pre_night_market_flag else \
        date_utils.get_today_str('%Y-%m-%d')

    for check_type_str, check_instrument_list in check_instrument_dict.items():
        for instrument_db in check_instrument_list:
            check_wind_ticker = __wind_ticker_convert(instrument_db)
            check_wind_ticker_list.append(check_wind_ticker)
        wind_prev_close_dict = w.query_wsd_data("pre_close", check_wind_ticker_list, filter_date_str)

        if check_type_str != '6':
            for wind_ticker, wind_prev_close in wind_prev_close_dict.items():
                if str(wind_prev_close) == 'nan':
                    del wind_prev_close_dict[str(wind_ticker)]
            wind_prev_close_dict = {key: value for key, value in wind_prev_close_dict.items()[:15]}

    return wind_prev_close_dict


def __price_check(server_name_list, check_type_dict):
    wind_prev_close_dict = dict()
    check_instrument_dict = dict()

    for (check_type_str, check_type_name) in check_type_dict.items():
        server_name = server_name_list[0]
        check_instrument_list = []
        for check_type_id in check_type_str.split('|'):
            dict_key = '%s|%s' % (server_name, check_type_id)
            check_instrument_list.extend(server_instrument_type_dict[dict_key])

        # 指数校验全部，其余随机校验15个
        if '6' != check_type_str:
            check_instrument_list = random.sample(check_instrument_list, 20)
        check_instrument_dict[check_type_str] = check_instrument_list
        wind_prev_close_dict.update(__wind_prev_close_dict({check_type_str: check_instrument_list}))

    export_message_list = []
    for (type_str, instrument_list) in check_instrument_dict.items():
        type_name = check_type_dict[type_str]
        email_list.append('<br><br><li>Check %s Prev_Close</li>' % type_name)
        table_list = []
        html_title = 'Ticker,Wind,%s,Check Result' % ','.join(server_name_list)

        for instrument_db in instrument_list:
            wind_ticker = __wind_ticker_convert(instrument_db)
            if wind_ticker in wind_prev_close_dict.keys():
                wind_prev_close = wind_prev_close_dict[wind_ticker]
                if str(wind_prev_close) == 'nan':
                    check_prev_close = None
                    tr_list = [wind_ticker, '%s(Warning)' % wind_prev_close]
                else:
                    check_prev_close = wind_prev_close
                    tr_list = [wind_ticker, wind_prev_close]
            else:
                continue
            error_flag = False
            for index, server_name in enumerate(server_name_list):
                dict_key = '%s|%s' % (server_name, instrument_db.ticker)
                server_prev_close = server_instrument_ticker_dict[dict_key].prev_close
                if server_prev_close is None:
                    tr_list.append('nan(Warning)')
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

    validate_date = int(date_utils.get_today_str('%Y%m%d'))
    validate_time = int(date_utils.get_today_str('%H%M%S'))

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
                position_date = int(date_utils.datetime_toString(update_date, '%Y%m%d'))
                position_time = int(date_utils.datetime_toString(update_date, '%H%M%S'))

                if 83000 <= validate_time < 153000:
                    if position_date == validate_date and 83000 <= position_time < 153000:
                        tr_list.append('%s_%s' % (account_db.accounttype, update_date))
                    else:
                        tr_list.append('%s_%s(Error)' % (account_db.accounttype, update_date))
                elif 153000 <= validate_time < 203000:
                    if position_date == validate_date and 153000 <= position_time < 203000:
                        tr_list.append('%s_%s' % (account_db.accounttype, update_date))
                    else:
                        tr_list.append('%s_%s(Error)' % (account_db.accounttype, update_date))
                elif 203000 <= validate_time:
                    if position_date == validate_date and 203000 <= position_time:
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


def __calendarma_parameter_check(server_name_list):
    future_dict = dict()
    dict_key = '%s|%s' % (server_name_list[0], 1)
    for instrument_db in server_instrument_type_dict[dict_key]:
        future_dict[instrument_db.ticker] = instrument_db

    for server_name in server_name_list:
        email_list.append('<font>Strategy Parameter Check: %s</font><br/>' % server_name)

        strategy_check_result = []
        strategy_parameter_db = calendarma_parameter_dict[server_name]
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

                future_maincontract_db = maincontract_dict[ticker_type]
                if back_future_name != future_maincontract_db.next_main_symbol:
                    strategy_check_result.append(
                        '<font color=red>strategy:%s BackFuture:%s, Next Main Contract is:%s</font><br/>' % (
                            strategy_name, back_future_name, future_maincontract_db.next_main_symbol))
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
            tr_list.append('%s(Warning)' % today_cancel_sum_num)
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

        if ticker_name is not None:
            instrument_dict[ticker_name] = instrument_value_dict
    server_model.close()
    return instrument_dict, column_list, ticker_list


def __check_future_ticker(server_name, future_ticker, chang_future_types):
    check_info_list = []
    future_type = filter(lambda x: not x.isdigit(), future_ticker)
    main_contract_db = maincontract_dict[future_type]

    future_dict = dict()
    dict_key = '%s|%s' % (server_name, 1)
    for instrument_db in server_instrument_type_dict[dict_key]:
        future_dict[instrument_db.ticker] = instrument_db
    front_future_db = future_dict[future_ticker]
    interval_days = date_utils.get_interval_days(date_utils.get_today_str(FORMAT_STR),
                                                 date_utils.datetime_toString(front_future_db.expire_date, FORMAT_STR))
    if main_contract_db.warning_days >= interval_days:
        check_info_list.append('ExpireDate less than %sdays!(Error)' % main_contract_db.warning_days)

    future_index_str = filter(lambda x: x.isdigit(), future_ticker)
    validate_index_str = filter(lambda x: x.isdigit(), main_contract_db.main_symbol)
    if future_index_str < validate_index_str:
        if future_type in chang_future_types:
            check_info_list.append('Not Main(Warning)')
        else:
            check_info_list.append('Not Main(Error)')

    # 不可交易检查
    not_trading_flag = False
    interval_days = date_utils.get_interval_days(date_utils.get_next_trading_day(FORMAT_STR),
                                                 date_utils.datetime_toString(front_future_db.expire_date, FORMAT_STR))
    if future_type in ('sc', 'fu'):
        if interval_days <= 12:
            not_trading_flag = True
    else:
        if interval_days <= 20 or (date_utils.get_today().year == front_future_db.expire_date.year and
                                   date_utils.get_today().month == front_future_db.expire_date.month):
            not_trading_flag = True
    not_trading_str = 'True(Error)' if not_trading_flag else ''
    return check_info_list, not_trading_str


def __position_check(server_name_list):
    change_future_types = __read_future_main_contract()
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

                check_info_list, not_trading_str = __check_future_ticker(server_name, future_symbol, change_future_types)
                if len(check_info_list) > 0:
                    table_list.append(
                        (server_name, future_symbol, 'pf_position', not_trading_str, ';'.join(check_info_list)))

        for dict_key in server_position_dict.keys():
            if server_name not in dict_key:
                continue
            position_list = server_position_dict[dict_key]
            for position_db in position_list:
                future_symbol = position_db.symbol.split(' ')[0]
                if future_symbol not in future_ticker_list:
                    continue

                check_info_list, not_trading_str = __check_future_ticker(server_name, future_symbol, change_future_types)
                if len(check_info_list) > 0:
                    table_list.append(
                        (server_name, position_db.symbol, 'account_position', not_trading_str, ';'.join(check_info_list)))
    if table_list:
        table_list.sort()
        html_title = 'Server_Name,Symbol,Position_Type,No_Trading_Flag,Error_Message'
        email_list.append(''.join(email_utils2.list_to_html(html_title, table_list)))

        error_info_list = [x for x in table_list if '(Error)' in x[4]]
        if error_info_list:
            error_email_list = email_utils12.list_to_html(html_title, error_info_list)
            error_email_list.insert(0, u'注:No_Trading_Flag表示即将被TradePlat禁止交易')
            email_utils12.send_email_group_all(u'期货异常持仓报告', ''.join(error_email_list), 'html')


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
        server_instrument_type_dict, server_instrument_ticker_dict, calendarma_parameter_dict, \
        server_cancel_number_dict
    maincontract_dict = dict()
    server_account_dict = dict()
    server_position_dict = dict()
    server_pf_position_dict = dict()
    server_instrument_type_dict = dict()
    server_instrument_ticker_dict = dict()
    calendarma_parameter_dict = dict()
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
                calendarma_parameter_dict[server_name] = strategy_parameter_db
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


def __strategy_parameter_check(server_name_list):
    online_strategy_list = []
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    for item in session_strategy.query(StrategyOnline).filter(StrategyOnline.enable == 1,
                                                              StrategyOnline.strategy_type == 'CTA'):
        for temp_server_name in item.target_server.split('|'):
            online_strategy_list.append('%s|%s' % (temp_server_name, item.name))

    main_contract_list = [item.main_symbol for (key, item) in maincontract_dict.items()]

    table_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        if not server_model.is_cta_server:
            continue

        session_server_strategy = server_model.get_db_session('strategy')
        query_sql = "select name, time, value from (select distinct name, time, value from strategy.strategy_parameter \
order by time desc) t group by name"
        query_result = session_server_strategy.execute(query_sql)
        for strategy_parameter_info in query_result:
            strategy_name = strategy_parameter_info[0]
            strategy_time = strategy_parameter_info[1]

            parameter_dict = json.loads(strategy_parameter_info[2])
            if 'Target' not in parameter_dict:
                continue

            parameter_symbol = parameter_dict['Target']
            if ';' in parameter_symbol:
                parameter_symbol = parameter_symbol.split(';')[0]

            if parameter_symbol in main_contract_list:
                continue

            if '%s|%s' % (server_name, strategy_name) not in online_strategy_list:
                continue

            table_list.append((strategy_time, strategy_name, '%s(Error)' % parameter_symbol))
    html_title = 'Strategy_Time,Strategy_Name,Target'
    email_list.append(''.join(email_utils2.list_to_html(html_title, table_list)))


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

    email_list.append('<br><br><li>Check CalendarMA Parameter</li>')
    calendar_server_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        if server_model.is_calendar_server:
            calendar_server_list.append(server_name)
    __calendarma_parameter_check(calendar_server_list)

    email_list.append('<br><br><li>Check CTA Strategy parameter</li>')
    __strategy_parameter_check(server_name_list)

    night_session_server_list = server_constant.get_night_session_servers()
    email_list.append('<br><br><li>Check Account Trade Restrictions</li>')
    __account_trade_restrictions_check(night_session_server_list)

    email_list.append('<br><br><li>Check Position</li>')
    __position_check(server_name_list)

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


def __read_future_main_contract():
    config_file_path = '%s/future_main_contract_change_info.csv' % MAIN_CONTRACT_CHANGE_FILE_FOLDER
    data_list = pd.read_csv(config_file_path)
    filter_date_str = date_utils.get_last_trading_day(format_str='%Y-%m-%d')

    data_list['Date'] = data_list['Date'].astype(str)
    ticker_type_list = data_list[data_list['Date'] == filter_date_str]['Ticker_Type'].tolist()
    return ticker_type_list


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    # account_check_job(trade_servers_list)
    # __read_future_main_contract()
    db_check_job(trade_servers_list)
