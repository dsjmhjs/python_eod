# -*- coding: utf-8 -*-
# 对每日更新的数据进行校验
import os
import json
import math
from datetime import datetime
from sqlalchemy import desc
from sqlalchemy import not_
from sqlalchemy.sql.expression import func
from eod_aps.model.realaccount import RealAccount
from eod_aps.model.future_main_contract import FutureMainContract
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.account_position import AccountPosition
from eod_aps.job import up_down_limit_check
from eod_aps.model.strategy_parameter import StrategyParameter
from eod_aps.tools.wind_local_tools import w_ys, w_ys_close
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.job import *

email_utils = EmailUtils(EmailUtils.group2)
email_list = []

# 本地ticker和windticker的转换
wind_ticker_local_dict = dict()
column_filter_list = ['UPDATE_DATE', 'CLOSE_UPDATE_TIME', 'PREV_CLOSE_UPDATE_TIME', 'BUY_COMMISSION', 'SELL_COMMISSION',
                      'FAIR_PRICE', 'MAX_LIMIT_ORDER_VOL', 'MAX_MARKET_ORDER_VOL','IS_SETTLE_INSTANTLY',
                      'INACTIVE_DATE', 'CLOSE', 'volume', 'SHORTMARGINRATIO', 'SHORTMARGINRATIO_HEDGE',
                      'SHORTMARGINRATIO_SPECULATION', 'SHORTMARGINRATIO_ARBITRAGE', 'LONGMARGINRATIO_HEDGE',
                      'LONGMARGINRATIO', 'LONGMARGINRATIO_SPECULATION', 'LONGMARGINRATIO_ARBITRAGE']

PRICE_FILES_FOLDER = '%s/price_files' % server_host.server_path_dict['data_share_folder']


def __wind_login():
    global w
    w = w_ys()


def __wind_close():
    w_ys_close()


def __wind_prev_close_dict(check_instrument_dict):
    # convert instrument to wind ticker list
    all_wind_ticker_list = []
    for (check_type, (check_ticker_list, wind_ticker_list)) in check_instrument_dict.items():
        all_wind_ticker_list.extend(wind_ticker_list)

    now_time = long(date_utils.get_today_str('%H%M%S'))
    if now_time > 200000:
        filter_date_str = date_utils.get_next_trading_day('%Y-%m-%d')
    else:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    wind_prev_close_dict = dict()
    wind_data = w.wsd(all_wind_ticker_list, "pre_close", filter_date_str, filter_date_str, "Fill=Previous")
    if wind_data['Data'][0][0] == 'No Content':
        task_logger.error('Wind Query Result:No Content')
        return
    data_list = wind_data['Data']
    for i in range(0, len(all_wind_ticker_list)):
        ticker = all_wind_ticker_list[i]
        ticker_prev_close = data_list[0][i]
        wind_prev_close_dict[ticker] = ticker_prev_close
    return wind_prev_close_dict


def __price_check(server_name_list, check_type_dict):
    check_instrument_dict = __build_check_instrument_dict(check_type_dict)
    wind_prev_close_dict = __wind_prev_close_dict(check_instrument_dict)
    server_prev_close_dict = __query_server_price_dict(server_name_list, check_instrument_dict)
    for (check_type, (check_ticker_list, wind_ticker_list)) in check_instrument_dict.items():
        type_name = check_type_dict[check_type]
        email_list.append('<br><br><li>Check %s Prev_Close</li>' % type_name)

        html_title = 'Ticker,Wind,%s,Check Result' % ','.join(server_name_list)

        table_list = []
        check_ticker_list.sort()
        for check_ticker in check_ticker_list:
            tr_list = []
            ticker_wind = wind_ticker_local_dict[check_ticker]
            prev_close_wind = wind_prev_close_dict[ticker_wind]
            if str(prev_close_wind) != 'nan':
                tr_list.append(ticker_wind)
                tr_list.append(prev_close_wind)
            else:
                tr_list.append(ticker_wind)
                tr_list.append('%s(Error)' % prev_close_wind)

            prev_close_server_temp = -1000000
            error_flag = False
            for server_name in server_name_list:
                prev_close_server = server_prev_close_dict[server_name][ticker_wind]
                if math.fabs(prev_close_server) != prev_close_server_temp and prev_close_server_temp != -1000000:
                    error_flag = True
                if math.fabs(float(prev_close_server) - float(prev_close_wind)) < 0.001 or str(prev_close_wind) == 'nan':
                    if check_type == '10':
                        tr_list.append('%.4f' % prev_close_server)
                    elif check_type == '7,15,16':
                        tr_list.append('%.3f' % prev_close_server)
                    else:
                        tr_list.append('%.2f' % prev_close_server)
                else:
                    error_flag = True
                    if check_type == '10':
                        tr_list.append('%.4f(Error)' % prev_close_server)
                    elif check_type == '7,15,16':
                        tr_list.append('%.3f(Error)' % prev_close_server)
                    else:
                        tr_list.append('%.2f(Error)' % prev_close_server)

            if error_flag:
                tr_list.append('Error(Error)')
            else:
                tr_list.append('')
            table_list.append(tr_list)

        html_list = email_utils.list_to_html(html_title, table_list)
        email_list.append(''.join(html_list))
    _export_price_check(check_instrument_dict, wind_prev_close_dict)


# 价格校验信息保存到文件中
def _export_price_check(check_instrument_dict, wind_prev_close_dict):
    export_message_list = ['check_type,ticker,wind_ticker,wind_prev_close']
    for (check_type, (check_ticker_list, wind_ticker_list)) in check_instrument_dict.items():
        for check_ticker in check_ticker_list:
            wind_ticker = wind_ticker_local_dict[check_ticker]
            wind_prev_close = wind_prev_close_dict[wind_ticker]
            export_message_list.append('%s,%s,%s,%s' % (check_type, check_ticker, wind_ticker, wind_prev_close))

    save_file_folder = '%s/%s' % (PRICE_FILES_FOLDER, date_utils.get_today_str('%Y%m%d'))
    if not os.path.exists(save_file_folder):
        os.mkdir(save_file_folder)
    save_file_path = '%s/price_check_%s.csv' % (save_file_folder, date_utils.get_today_str('%Y-%m-%d'))
    with open(save_file_path, 'w') as fr:
        fr.write('\n'.join(export_message_list))


def __cross_market_check(server_model):
    email_list.append('<li>CrossMarket ETF Check</li>')
    session = server_model.get_db_session('common')
    query = session.query(Instrument)
    cross_market_etf_size = query.filter(Instrument.type_id == 7, Instrument.cross_market == 1).count()
    email_list.append('crossmarket etf num:%s<br/><br/>' % cross_market_etf_size)


def __fund_pcf_check(server_name_list):
    etf_set_dict = dict()
    trading_day_error_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        today_filter_str = datetime.now().strftime('%Y%m%d')
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(RealAccount)

        etf_set = set()
        for account_db in query.filter(not_(RealAccount.allowed_etf_list is None)):
            allow_etf_str = account_db.allowed_etf_list
            for etfTicker in allow_etf_str.split(';'):
                if etfTicker.strip() != '':
                    etf_set.add(etfTicker)

        session = server_model.get_db_session('common')
        query = session.query(Instrument)
        trading_day_error_list = []
        for instrument_db in query.filter(Instrument.type_id.in_((7, 15, 16))):
            if instrument_db.type_id == 16:
                if instrument_db.ticker in etf_set:
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
        server_model.close()

    html_title = 'Index,%s' % ','.join(server_name_list)
    table_list = [
        ['Allow ETF Error list'] + ['<br/>'.join(etf_set_dict[server_name]) for server_name in server_name_list],
        ['ETF TradingDay Error list'] + ['<br/>'.join(trading_day_error_dict[server_name]) for server_name in
                                         server_name_list]]
    html_list = email_utils.list_to_html(html_title, table_list)

    email_list.append(''.join(html_list))


def __option_callput_check(server_name_list):
    option_error_list_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session = server_model.get_db_session('common')
        query = session.query(Instrument)
        option_error_list = []
        for instrument_db in query.filter(Instrument.type_id == 10):
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
    html_list = email_utils.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def __option_track_undl_tickers_check(server_name_list):
    option_error_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session = server_model.get_db_session('common')
        query = session.query(Instrument)
        null_number = query.filter(Instrument.type_id == 10, Instrument.track_undl_tickers is None).count()
        option_error_dict[server_name] = null_number
        server_model.close()

    email_list.append('<h4>track_undl_tickers Check------------</h4>')
    html_title = ','.join(server_name_list)
    tr_list = []
    for server_name in server_name_list:
        if option_error_dict[server_name] > 0:
            tr_list.append('%s(Error)' % option_error_dict[server_name])
        else:
            tr_list.append('%s' % option_error_dict[server_name])
    html_list = email_utils.list_to_html(html_title, [tr_list])
    email_list.append(''.join(html_list))


def __account_position_check(server_name_list):
    account_position_dict_server = dict()
    account_dict_server = dict()
    account_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        today_filter_str = datetime.now().strftime('%Y-%m-%d')
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(AccountPosition)

        account_position_dict = dict()
        for position_db in query.filter(AccountPosition.date == today_filter_str):
            if position_db.id in account_position_dict:
                account_position_dict[position_db.id].append(position_db)
            else:
                account_position_dict[position_db.id] = [position_db]
        account_position_dict_server[server_name] = account_position_dict

        query = session_portfolio.query(RealAccount)
        account_dict = dict()
        for account_db in query:
            account_dict[account_db.accountid] = account_db
            if account_db.accountid not in account_list:
                account_list.append(account_db.accountid)
        account_dict_server[server_name] = account_dict
        server_model.close()

    account_list = sorted(account_list)

    html_title = 'Account,%s' % ','.join(server_name_list)
    table_list = []
    for account_id in account_list:
        tr_list = [account_id,]
        for server_name in server_name_list:
            if account_dict_server[server_name].has_key(account_id):
                account_db = account_dict_server[server_name][account_id]
                if account_db.enable == 1:
                    if account_id in account_position_dict_server[server_name]:
                        account_position_db = account_position_dict_server[server_name][account_id][0]
                        update_date = account_position_db.update_date
                    else:
                        update_date = 'Null'
                    if update_date != 'Null':
                        if int(datetime.now().strftime('%H%M%S')) > 200000:
                            if update_date > datetime.strptime(datetime.now().strftime('&Y-%m-%d') + ' 20:00:00', '&Y-%m-%d %H:%M:%S'):
                                tr_list.append('%s_%s' % (account_db.accounttype, update_date))
                            else:
                                tr_list.append('%s_%s(Error)' % (account_db.accounttype, update_date))
                        else:
                            tr_list.append('%s_%s' % (account_db.accounttype, update_date))
                    else:
                        tr_list.append('%s_%s(Error)' % (account_db.accounttype, update_date))
                else:
                    tr_list.append('/')
            else:
                tr_list.append('/')
        table_list.append(tr_list)
    html_list = email_utils.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def __strategy_parameter_check(server_name_list):
    session = server_host.get_db_session('common')
    query = session.query(Instrument)
    future_dict = dict()
    for instrument_db in query.filter(Instrument.type_id == 1):
        future_dict[instrument_db.ticker] = instrument_db

    maincontract_dict = dict()
    query = session.query(FutureMainContract)
    for future_maincontract_db in query:
        maincontract_dict[future_maincontract_db.ticker_type] = future_maincontract_db

    for server_name in server_name_list:
        email_list.append('<font>Strategy Parameter Check: %s</font><br/>' % server_name)
        server_model = ServerConstant().get_server_model(server_name)
        server_session = server_model.get_db_session('strategy')

        strategy_check_result = []
        for strategy_name_group in server_session.query(StrategyParameter.name).group_by(StrategyParameter.name).all():
            strategy_name = strategy_name_group[0]
            if 'Calendar' not in strategy_name:
                continue
            strategy_parameter_db = server_session.query(StrategyParameter).filter(StrategyParameter.name == strategy_name).order_by(desc(StrategyParameter.time)).first()
            strategy_parameter_dict = json.loads(strategy_parameter_db.value)

            calendar_future_dict = dict()
            for (dict_key, dict_value) in strategy_parameter_dict.items():
                if 'BackFuture' in dict_key:
                    back_future_name = dict_value
                    if back_future_name not in future_dict:
                        email_list.append('<font color=red>strategy:%s BackFuture:%s can not find!</font><br/>' % (strategy_name, back_future_name))
                    else:
                        back_future_db = future_dict[back_future_name]
                        if back_future_db.exchange_id != 25 and (datetime.strptime(str(back_future_db.expire_date), '%Y-%m-%d') - datetime.now()).days <= 33:
                            strategy_check_result.append('<font color=red>strategy:%s BackFuture:%s expire_date:%s less than 30days!</font><br/>' % \
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
                        strategy_check_result.append('<font color=red>strategy:%s BackFuture:%s, Main Contract is:%s</font><br/>' % (
                                    strategy_name, back_future_name, future_maincontract_db.main_symbol))
                elif 'FrontFuture' in dict_key:
                    front_future_name = dict_value
                    if front_future_name not in future_dict:
                        email_list.append('<font color=red>strategy:%s FrontFuture:%s can not find!</font><br/>' % (strategy_name, front_future_name))
                    else:
                        front_future_db = future_dict[front_future_name]
                        if front_future_db.exchange_id != 25 and (datetime.strptime(str(front_future_db.expire_date), '%Y-%m-%d') - datetime.now()).days <= 30:
                            strategy_check_result.append('<font color=red>strategy:%s FrontFuture:%s expire_date:%s less than 30days!</font><br/>' % \
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
                        strategy_check_result.append('<font color=red>strategy:%s FrontFuture:%s, Main Contract is:%s</font><br/>' % (
                                    strategy_name, front_future_name, future_maincontract_db.main_symbol))

            for (key, ticker_list) in calendar_future_dict.items():
                if len(ticker_list) != 2:
                    continue
                if ticker_list[0] == ticker_list[1]:
                    strategy_check_result.append(
                        '<font color=red>Ticker:%s FrontFuture and BackFuture is same!</font><br/>' % ticker_list[0])

        if len(strategy_check_result) > 0:
            strategy_check_result.insert(0, '<li>server:%s, Check Strategy Parameter</li>' % server_name)
            email_list.extend(strategy_check_result)
            EmailUtils(EmailUtils.group4).send_email_group_all('[Error]Strategy Parameter Check', '\n'.join(strategy_check_result), 'html')
        server_model.close()


def __account_trade_restrictions_check(server_name_list):
    email_list.append('<li>Check Today Cancel</li>')

    html_title = ',%s' % ','.join(server_name_list)
    tr_list = ['account trade restrictions']
    for server_name in server_name_list:
        server_model = ServerConstant().get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_sql = "select sum(today_cancel) from portfolio.account_trade_restrictions t where t.today_cancel > 0"
        today_cancel_sum_num = session_portfolio.execute(query_sql).first()[0]
        if today_cancel_sum_num == 0 or today_cancel_sum_num is None:
            tr_list.append('%s(Error)' % today_cancel_sum_num)
        else:
            tr_list.append(today_cancel_sum_num)
        server_model.close()

    html_list = email_utils.list_to_html(html_title, [tr_list])
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
        i = 0
        for column in column_list:
            if column.upper() == "TICKER":
                ticker_name = result_line[i]
                ticker_list.append(ticker_name)
            instrument_value_dict[column] = result_line[i]
            i += 1
        instrument_dict[ticker_name] = instrument_value_dict
    server_model.close()
    return instrument_dict, column_list, ticker_list


def __position_check(server_name_list):
    server_host = server_constant.get_server_model('host')
    session = server_host.get_db_session('common')
    maincontract_list = []
    query = session.query(FutureMainContract)
    for future_maincontract_db in query:
        maincontract_list.append(future_maincontract_db.pre_main_symbol)

    date_str = date_utils.get_today_str('%Y-%m-%d')
    for server_name in server_name_list:
        server_model = ServerConstant().get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_pf_position = session_portfolio.query(PfPosition)
        for pf_position_db in query_pf_position.filter(PfPosition.date >= date_str):
            if pf_position_db.symbol.isdigit():
                continue
            filter_symbol = pf_position_db.symbol.split(' ')[0]
            if filter_symbol in maincontract_list:
                email_list.append('<font color=red>pf_position---server:%s, date:%s, id:%s, symbol:%s</font><br/>' % \
                                  (server_name, pf_position_db.date, pf_position_db.id, pf_position_db.symbol))
        query_position = session_portfolio.query(AccountPosition)
        for account_position_db in query_position.filter(AccountPosition.date >= date_str):
            if not account_position_db.symbol.isdigit():
                continue
            filter_symbol = account_position_db.symbol.split(' ')[0]
            if filter_symbol in maincontract_list:
                email_list.append('<font color=red>account_position---server:%s, date:%s, id:%s, symbol:%s</font><br/>' % \
                                  (server_name, pf_position_db.date, pf_position_db.id, pf_position_db.symbol))
        server_model.close()


def instrument_check(server_name_list):
    instrument_info_dict = dict()
    column_list_dict = dict()
    ticker_list_dict = dict()
    global email_list

    instrument_info_origin, column_list_origin, ticker_list_origin = get_instrument_information(server_name_list[0])

    for server_name in server_name_list[1:]:
        instrument_info, column_list, ticker_list = get_instrument_information(server_name)
        instrument_info_dict[server_name] = instrument_info
        column_list_dict[server_name] = column_list
        ticker_list_dict[server_name] = ticker_list

    column_list_merge = column_list_origin
    ticker_list_merge = ticker_list_origin
    for server_name in server_name_list[1:]:
        column_list_merge = list(set(column_list_merge + column_list_dict[server_name]))
        ticker_list_merge = list(set(ticker_list_merge + ticker_list_dict[server_name]))
        if column_list_dict[server_name] != column_list_origin:
            email_list.append("%s: column list error!\n\n" % server_name)
        if ticker_list_dict[server_name] != ticker_list_origin:
            email_list.append("%s: ticker list error!\n\n" % server_name)

    html_title = 'Column,Ticker,%s' % ','.join(server_name_list)
    table_list = []
    for ticker in ticker_list_merge:
        for column in column_list_merge:
            value_origin = instrument_info_origin[ticker][column]
            error_flag = False
            for server_name in server_name_list[1:]:
                value = instrument_info_dict[server_name][ticker][column]
                if value != value_origin:
                    error_flag = True
            if error_flag:
                tr_list = [column, ticker, value_origin]
                for server_name in server_name_list[1:]:
                    value = instrument_info_dict[server_name][ticker][column]
                    if value != value_origin:
                        tr_list.append('%s(Error)' % value)
                    else:
                        tr_list.append(value)
                table_list.append(tr_list)
    html_list = email_utils.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def __wind_ticker_convert(instrument_db):
    wind_ticker_str = ''
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
    return wind_ticker_str


def __build_check_instrument_dict(check_type_dict):
    session = server_host.get_db_session('common')
    query = session.query(Instrument)

    check_instrument_dict = dict()
    for (instrument_type, type_name) in check_type_dict.items():
        check_ticker_list = []
        wind_ticker_list = []
        if instrument_type == '6':
            for instrument_db in query.filter(Instrument.type_id.in_(tuple(instrument_type.split('|'))), Instrument.del_flag == 0):
                check_ticker_list.append(instrument_db.ticker)
                wind_ticker = __wind_ticker_convert(instrument_db)
                wind_ticker_list.append(wind_ticker)
                wind_ticker_local_dict[instrument_db.ticker] = wind_ticker
        else:
            for instrument_db in query.filter(Instrument.type_id.in_(tuple(instrument_type.split('|'))), Instrument.prev_close > 0,
                                              Instrument.del_flag == 0).order_by(func.random()).limit(15):
                check_ticker_list.append(instrument_db.ticker)
                wind_ticker = __wind_ticker_convert(instrument_db)
                wind_ticker_list.append(wind_ticker)
                wind_ticker_local_dict[instrument_db.ticker] = wind_ticker
        check_instrument_dict[instrument_type] = (check_ticker_list, wind_ticker_list)
    return check_instrument_dict


def __query_server_price_dict(server_name_list, check_instrument_dict):
    ticker_list = []
    for (check_type, (check_ticker_list, wind_ticker_list)) in check_instrument_dict.items():
        ticker_list.extend(check_ticker_list)

    server_prev_close_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session = server_model.get_db_session('common')
        query = session.query(Instrument)

        prev_close_dict = dict()
        for instrument_db in query.filter(Instrument.ticker.in_(tuple(ticker_list)), Instrument.del_flag == 0):
            wind_ticker = wind_ticker_local_dict[instrument_db.ticker]
            prev_close_dict[wind_ticker] = instrument_db.prev_close
        server_prev_close_dict[server_name] = prev_close_dict
        server_model.close()
    return server_prev_close_dict


def db_check_job(server_name_list):
    __wind_login()
    global email_list
    email_list = []
    subject = 'DailyCheck Check Result'

    email_list.append('<li>Check Account Position</li>')
    __account_position_check(server_name_list)

    check_type_dict = {'1': 'Future', '4': 'Stock', '6': 'Index', '7|15|16': 'Fund', '10': 'Option'}
    __price_check(server_name_list, check_type_dict)

    email_list.append('<br><br><li>Check Fund PCF Information</li>')
    __fund_pcf_check(server_name_list)

    email_list.append('<br><br><li>Check Option Call/Put</li>')
    __option_callput_check(server_name_list)

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

    email_list.append('<br><br><li>Check Instrument</li>')
    instrument_check(server_name_list)

    email_utils.send_email_group_all(subject, '\n'.join(email_list), 'html')
    __wind_close()


def db_check_future_job(server_name_list):
    __wind_login()
    global email_list
    email_list = []
    subject = 'DailyCheck Check Result'

    email_list.append('<li>check Account Position</li>')
    __account_position_check(server_name_list)

    check_type_dict = {'1': 'Future',}
    __price_check(server_name_list, check_type_dict)

    email_utils.send_email_group_all(subject, '\n'.join(email_list), 'html')

    __wind_close()


def account_check_job(server_name_list):
    global email_list
    email_list = []
    subject = 'Account Check Result'

    email_list.append('<li>check Account Position</li>')
    __account_position_check(server_name_list)

    email_utils.send_email_group_all(subject, '\n'.join(email_list), 'html')


def up_down_limit_check_job():
    email_list.append('start check uplimit and downlimit:<br/>')
    udc_result = up_down_limit_check.start()
    email_list.append(udc_result)


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    db_check_job(trade_servers_list)