# -*- coding: utf-8 -*-
# 每日统计各多因子策略的收益情况，并发送邮件通知
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.trade2 import Trade2
from eod_aps.model.trade2_history import Trade2History
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from itertools import islice
import numpy as np

date_utils = DateUtils()
sum_pl_dict = dict()
strategy_pnl_dict = dict()
date_set = set()
attachment_file_list = []

strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm']

def __build_pf_position_list(server_model, pf_account_db, filter_date_str):
    pf_position_list = []

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_pf_position.filter(PfPosition.id == str(pf_account_db.id), PfPosition.date == filter_date_str):
        pf_position_list.append(pf_position_db)
    return pf_position_list


def __build_trade_dict(server_model, pf_account_db, filter_date_str):
    trade_list = []
    session_om = server_model.get_db_session('om')
    trad2e_db_list = session_om.query(Trade2History)
    strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
    for trade_db in trad2e_db_list.filter(Trade2History.strategy_id == strategy_id):
        date_str = trade_db.time.strftime("%Y-%m-%d")
        if date_str != filter_date_str:
            continue
        trade_list.append(trade_db)
    return trade_list


def server_enter(server_model, pf_account_db, date_str):
    instrument_db_dict = __build_ticker_exchange(server_model)
    pf_position_list = __build_pf_position_list(server_model, pf_account_db, date_str)
    trade_dict = __build_trade_dict(server_model, pf_account_db, date_str)

    stock_file_dict = __read_stock_daily_file(date_str)
    future_file_dict = __read_future_daily_file(date_str)
    stock_nominalprice_dict = __read_stock_nominalprice_file(date_str)
    future_nominalprice_dict = __read_future_nominalprice_file(date_str)


    total_buy_money = 0.0
    total_sell_money = 0.0
    future_open_trade_list = []
    # if date_str in trade_dict:
    #     trade_list = trade_dict[date_str]
    #     for trade_db in trade_list:
    #         ticker = trade_db.symbol.split(' ')[0]
    #         instrument_db = instrument_db_dict[ticker]
    #         if trade_db.trade_type == 0:
    #             if trade_db.qty > 0:
    #                 total_buy_money += float(trade_db.price) * abs(trade_db.qty) * (1 + 0.00025)
    #             else:
    #                 total_sell_money += float(trade_db.price) * abs(trade_db.qty) * (1 - 0.00125)
    #         elif trade_db.trade_type == 2:
    #             total_buy_money += float(trade_db.price) * abs(trade_db.qty) * float(instrument_db.fut_val_pt) * (
    #             0.5 + 0.000026)
    #         elif trade_db.trade_type == 3:
    #             future_open_trade_list = __get_future_open_trade_list(trade_db.symbol, pf_account_db)
    #             for future_open_trade in future_open_trade_list:
    #                 total_sell_money += float(future_open_trade.price) * abs(future_open_trade.qty) \
    #                                     * float(instrument_db.fut_val_pt) * 0.5 + (float(
    #                     future_open_trade.price) - float(trade_db.price)) * abs(future_open_trade.qty) * \
    #                                                                               float(instrument_db.fut_val_pt) * (
    #                                                                               1 - 0.000026)
    #     total_money_change = total_sell_money - total_buy_money
    # else:
    #     total_money_change = 0.0
    value_dict = dict()
    output_list = []
    for pf_position_db in pf_position_list:
        ticker_prev_equity = 0.0
        ticker_equity1 = 0.0
        ticker_equity2 = 0.0
        instrument_db = instrument_db_dict[pf_position_db.symbol]
        if instrument_db.type_id == 4:
            (ticker_prev_close, ticker_close_price) = stock_file_dict[pf_position_db.symbol]
            ticker_nominalprice = stock_nominalprice_dict[pf_position_db.symbol]

            ticker_prev_equity += float(pf_position_db.long) * float(ticker_prev_close)
            ticker_equity1 += float(pf_position_db.long) * float(ticker_close_price)
            ticker_equity2 += float(pf_position_db.long) * float(ticker_nominalprice)
        elif instrument_db.type_id == 1:
            ticker_prev_close = 'None'
            ticker_close_price = future_file_dict[pf_position_db.symbol]
            ticker_nominalprice = future_nominalprice_dict[pf_position_db.symbol]

            future_open_trade_list = __get_future_open_trade_list(server_model, pf_position_db.symbol, pf_account_db)
            for future_open_trade in future_open_trade_list:
                ticker_equity1 += float(abs(future_open_trade.qty)) * float(future_open_trade.price) * float(
                    instrument_db.fut_val_pt) * 0.5 \
                                + (float(future_open_trade.price) - float(ticker_close_price)) * float(
                    abs(future_open_trade.qty)) * float(instrument_db.fut_val_pt)

                ticker_equity2 = float(abs(future_open_trade.qty)) * float(future_open_trade.price) * float(
                    instrument_db.fut_val_pt) * 0.5 \
                                + (float(future_open_trade.price) - float(ticker_nominalprice)) * float(
                    abs(future_open_trade.qty)) * float(instrument_db.fut_val_pt)
        value_dict[pf_position_db.symbol] = ticker_equity1

        deviation = ticker_equity1 - ticker_equity2
        print '%s,%s,%s,%s,%s,%s,%s,%s,%s' % (date_str, pf_position_db.symbol, ticker_prev_close, ticker_close_price, ticker_nominalprice, ticker_prev_equity, ticker_equity1, ticker_equity2, deviation)
        output_list.append('%s,%s,%s,%s,%s,%s,%s,%s,%s' % (date_str, pf_position_db.symbol, ticker_prev_close, ticker_close_price, ticker_nominalprice, ticker_prev_equity, ticker_equity1, ticker_equity2, deviation))
    title = 'date_str, symbol, ticker_prev_close, ticker_close_price, ticker_nominalprice, ticker_prev_equity, ticker_equity1, ticker_equity2,deviation'
    output_list.insert(0, title)

    file_object = open('E:/dailyFiles/check/check_result_%s.csv' % pf_account_db.id, 'w')
    file_object.write('\n'.join(output_list))
    file_object.close()



def __read_stock_nominalprice_file(date_str):
    stock_nominalprice_dict = dict()
    fr = open('Z:/dailyjob/stock_%s.csv' % date_str.replace('-', ''))
    for line in fr.readlines():
        line_items = line.split(',')
        ticker = line_items[0].split(' ')[0]
        nominalprice = line_items[10]
        stock_nominalprice_dict[ticker] = nominalprice
    return stock_nominalprice_dict


def __read_future_nominalprice_file(date_str):
    future_nominalprice_dict = dict()
    fr = open('Z:/dailyjob/future_%s.csv' % date_str.replace('-', ''))
    for line in fr.readlines():
        line_items = line.split(',')
        ticker = line_items[0].split(' ')[0]
        nominalprice = line_items[10]
        future_nominalprice_dict[ticker] = nominalprice
    return future_nominalprice_dict


def __get_future_open_trade_list(server_model, symbol, pf_account_db):
    trade_list = []
    session_om = server_model.get_db_session('om')
    trad2e_db_list = session_om.query(Trade2)
    strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
    for trade_db in trad2e_db_list.filter(Trade2.strategy_id == strategy_id, Trade2.symbol.like('%' + symbol + '%'), Trade2.trade_type == 2):
        trade_list.append(trade_db)
    return trade_list


def __read_stock_daily_file(date_str):
    server_model = ServerConstant().get_server_model('local118')
    session_portfolio = server_model.get_db_session('dump_wind')
    query_sql = "select t.S_INFO_WINDCODE, t.S_DQ_PRECLOSE, t.S_DQ_CLOSE from dump_wind.ASHAREEODPRICES t where t.TRADE_DT='%s'" % (date_str.replace('-', ''),)

    stock_file_dict = dict()
    for db_item in session_portfolio.execute(query_sql):
        ticker = db_item[0].split('.')[0]
        stock_file_dict[ticker] = (db_item[1], db_item[2])
    return stock_file_dict


def __read_future_daily_file(date_str):
    server_model = ServerConstant().get_server_model('local118')
    session_portfolio = server_model.get_db_session('dump_wind')
    query_sql = "select t.S_INFO_WINDCODE, t.S_DQ_CLOSE from dump_wind.CINDEXFUTURESEODPRICES t where t.TRADE_DT='%s'" % (date_str.replace('-', ''),)

    future_file_dict = dict()
    for db_item in session_portfolio.execute(query_sql):
        ticker = db_item[0].split('.')[0]
        future_file_dict[ticker] = db_item[1]
    return future_file_dict


def __build_ticker_exchange(server_model):
    instrument_dict = dict()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument).filter(Instrument.type_id.in_((1, 4)))
    for instrument_db in query:
        instrument_dict[instrument_db.ticker] = instrument_db
    return instrument_dict

def get_account_id(server_model, account_id):
    session_portfolio = server_model.get_db_session('portfolio')

    query_pf_account = session_portfolio.query(PfAccount)
    pf_account_db = query_pf_account.filter(PfAccount.id == str(account_id)).first()
    return pf_account_db


def daily_return_report_check(account_id, date_str):
    server_model = ServerConstant().get_server_model('huabao')
    pf_account_db = get_account_id(server_model, account_id)
    server_enter(server_model, pf_account_db, date_str)


if __name__ == '__main__':
    # daily_return_report_check('172', '2017-03-27')
    server_model = ServerConstant().get_server_model('host')
    __build_ticker_exchange(server_model)

