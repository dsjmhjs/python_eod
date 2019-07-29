# -*- coding: utf-8 -*-
# 每日统计日内策略的收益情况
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.account_position import AccountPosition
from eod_aps.model.trade2_history import Trade2History
from eod_aps.job import *
import pandas as pd
import os
from WindPy import *
from eod_aps.model.pnl_file_fixer import PnlFileChange

date_stock_file_dict = dict()
date_future_file_dict = dict()
instrument_db_dict = dict()
close_price_dict = dict()
pf_position_dict = dict()
trade_dict = dict()
account_position_dict = dict()

date_set = set()
attachment_file_list = []

strategy_name_list = ['StkIntraDayStrategy', 'StkIntraDayLeadLagStrategy']
base_save_folder_template = 'Z:/dailyjob/Report/IntraDayStrategy_%s'
# base_save_folder_template = 'E:/dailyFiles/report/IntraDayStrategy_%s'


def __build_close_price_dict(server_model, pf_account_id_list):
    w.start()
    session_portfolio = server_model.get_db_session('portfolio')
    date_info = session_portfolio.execute('select min(date) from portfolio.pf_position t where t.ID in (%s)' % \
                                          ','.join(pf_account_id_list)).first()

    global trading_start_date
    trading_start_date = date_utils.string_toDatetime('2017-04-07').date()
    start_date = date_utils.get_last_trading_day('%Y-%m-%d', date_info[0].strftime('%Y-%m-%d'))
    end_date = date_utils.get_today()

    ticker_list = []
    ticker_wind_list = []
    query_sql = 'select symbol from portfolio.pf_position t where t.ID in (%s) group by symbol' % ','.join(pf_account_id_list)
    for ticker_items in session_portfolio.execute(query_sql):
        ticker_str = ticker_items[0]
        if ticker_str == 'CNY':
            continue

        ticker_wind_str = __get_wind_ticker(ticker_str)
        ticker_list.append(ticker_str)
        ticker_wind_list.append(ticker_wind_str)

    wind_data = w.wsd(ticker_wind_list, "close", start_date, end_date, "Fill=Previous")
    if wind_data.Data[0][0] == 'No Content':
        task_logger.error('Wind Query Result:No Content')
        return

    data_list = wind_data.Data
    for i in range(0, len(wind_data.Times)):
        date_item = wind_data.Times[i]
        for j in range(0, len(ticker_list)):
            ticker = ticker_list[j]
            ticker_close_price = data_list[j][i]
            dict_key = '%s|%s' % (date_item.strftime('%Y-%m-%d'), ticker)
            close_price_dict[dict_key] = ticker_close_price
    w.close()


def __build_pf_position_dict(server_model, pf_account_id_list):
    pf_position_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_pf_position.filter(PfPosition.id.in_(tuple(pf_account_id_list))):
        date_str = pf_position_db.date.strftime("%Y-%m-%d")
        dict_key = '%s|%s' % (date_str, pf_position_db.symbol)
        if dict_key in pf_position_dict:
            pf_position_dict[dict_key].append(pf_position_db)
        else:
            pf_position_dict[dict_key] = [pf_position_db]


def __build_trade_dict(server_model, pf_account_db_list):
    global trade_dict
    trade_dict = dict()
    session_om = server_model.get_db_session('om')
    trade_list = session_om.query(Trade2History)

    strategy_list = []
    for pf_account_db in pf_account_db_list:
        strategy_name = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        strategy_list.append(strategy_name)

    ticker_set = set()
    for trade_db in trade_list.filter(Trade2History.strategy_id.in_(tuple(strategy_list))):
        ticker = trade_db.symbol.split(' ')[0]
        date_str = trade_db.time.strftime("%Y-%m-%d")
        dict_key = '%s|%s' % (date_str, ticker)
        if dict_key in trade_dict:
            trade_dict[dict_key].append(trade_db)
        else:
            trade_dict[dict_key] = [trade_db]
        ticker_set.add(ticker)
    return ticker_set


def tading_ticker_report(server_model, strategy_name, tading_ticker):
    global date_stock_file_dict
    global date_future_file_dict

    start_date, end_date = trading_start_date, date_utils.get_today()
    if start_date is None:
        return
    trading_day_list = date_utils.get_trading_day_list(start_date, end_date)

    report_result = []
    for i in range(0, len(trading_day_list)):
        date_set.add(trading_day_list[i])
        date_str = trading_day_list[i].strftime("%Y-%m-%d")
        dict_query_key = '%s|%s' % (date_str, tading_ticker)

        total_buy_money = 0.0
        total_sell_money = 0.0
        position_qty = 0

        if dict_query_key in trade_dict:
            trade_list = trade_dict[dict_query_key]
            for trade_db in trade_list:
                if trade_db.trade_type == 0:
                    if trade_db.qty > 0:
                        total_buy_money += float(trade_db.price) * abs(trade_db.qty) * (1 + 0.00025)
                    else:
                        total_sell_money += float(trade_db.price) * abs(trade_db.qty) * (1 - 0.00125)
                position_qty += trade_db.qty

            total_money_change = total_sell_money - total_buy_money
        else:
            continue

        dict_query_key = '%s|%s' % (date_str, tading_ticker)
        close_price = close_price_dict[dict_query_key]
        position_pl = position_qty * close_price
        pnl = total_money_change + position_pl

        last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', date_str)
        dict_query_key = '%s|%s' % (last_trading_day, tading_ticker)
        if dict_query_key not in account_position_dict:
            task_logger.error('unfind:%s' % dict_query_key)
            continue
        account_position = account_position_dict[dict_query_key]
        close_price = close_price_dict[dict_query_key]
        account_money = float(account_position.long) * close_price

        if account_money > 0:
            return_rate = pnl * 100 / account_money
        else:
            return_rate = 0
        report_result.append('%s,%s,%s,%s,%s,%s,%s,%.3f%%' \
                    % (date_str, total_buy_money, total_sell_money, total_money_change, position_pl, pnl, account_money, return_rate))

    if len(report_result) == 0:
        return

    save_folder = base_save_folder_template % date_str
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)

    file_path = save_folder + '/strategy_report_%s_%s_%s.csv' % (strategy_name, tading_ticker, now_date_str)
    report_result.insert(0,
                         'date,total_buy_money,total_sell_money,total_money_change,position_pl,pnl,equity_base,return_rate')
    file_object = open(file_path, 'w+')
    file_object.write('\n'.join(report_result))
    file_object.close()
    attachment_file_list.append(file_path)
    server_model.close()


def __read_stock_daily_file(date_list):
    query_date_list = []
    for date_item in date_list:
        query_date_list.append("'" + date_item.strftime("%Y%m%d") + "'")

    server_model = server_constant.get_server_model('local118')
    session_portfolio = server_model.get_db_session('dump_wind')
    query_sql = "select t.TRADE_DT, t.S_INFO_WINDCODE, t.S_DQ_CLOSE from dump_wind.ASHAREEODPRICES t where t.TRADE_DT in (%s)" % ','.join(
        query_date_list)

    date_stock_file_dict = dict()
    for db_item in session_portfolio.execute(query_sql):
        date_str = db_item[0]
        ticker = db_item[1].split('.')[0]
        if date_str in date_stock_file_dict:
            date_stock_file_dict[date_str][ticker] = db_item[2]
        else:
            ticker_dict = dict()
            ticker_dict[ticker] = db_item[2]
            date_stock_file_dict[date_str] = ticker_dict
    return date_stock_file_dict


def __read_future_daily_file(date_list):
    query_date_list = []
    for date_item in date_list:
        query_date_list.append("'" + date_item.strftime("%Y%m%d") + "'")

    server_model = server_constant.get_server_model('local118')
    session_portfolio = server_model.get_db_session('dump_wind')
    query_sql = "select t.TRADE_DT, t.S_INFO_WINDCODE, t.S_DQ_CLOSE from dump_wind.CINDEXFUTURESEODPRICES t where t.TRADE_DT in (%s)" % ','.join(
        query_date_list)

    date_future_file_dict = dict()
    for db_item in session_portfolio.execute(query_sql):
        date_str = db_item[0]
        ticker = db_item[1].split('.')[0]
        if date_str in date_future_file_dict:
            date_future_file_dict[date_str][ticker] = db_item[2]
        else:
            ticker_dict = dict()
            ticker_dict[ticker] = db_item[2]
            date_future_file_dict[date_str] = ticker_dict
    return date_future_file_dict


def __build_ticker_exchange(server_model):
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id.in_((1, 4))):
        instrument_db_dict[instrument_db.ticker] = instrument_db


def get_pf_account_info_list(server_model, strategy_name):
    pf_account_db_list = []
    pf_account_id_list = []

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.fund_name.like('%' + strategy_name + '%'),
                                                     PfAccount.fund_name.like('%steady_return%')):
        pf_account_db_list.append(pf_account_db)
        pf_account_id_list.append(str(pf_account_db.id))
    return pf_account_db_list, pf_account_id_list


def __get_wind_ticker(ticker):
    instrument_db = instrument_db_dict[ticker]
    ticker_wind_str = ''
    if instrument_db.exchange_id == 18:
            ticker_wind_str = '%s.SH' % instrument_db.ticker
    elif instrument_db.exchange_id == 19:
        ticker_wind_str = '%s.SZ' % instrument_db.ticker
    elif instrument_db.exchange_id == 19:
        ticker_wind_str = '%s.SZ' % instrument_db.ticker
    elif instrument_db.exchange_id == 20:
        ticker_wind_str = '%s.SHF' % instrument_db.ticker
    elif instrument_db.exchange_id == 21:
        ticker_wind_str = '%s.DCE' % instrument_db.ticker
    elif instrument_db.exchange_id == 22:
        ticker_wind_str = '%s.CZC' % instrument_db.ticker
    elif instrument_db.exchange_id == 25:
        ticker_wind_str = '%s.CFE' % instrument_db.ticker
    return ticker_wind_str


def __build_account_position_dict(server_model, ticker_set):
    session_portfolio = server_model.get_db_session('portfolio')
    query = session_portfolio.query(AccountPosition)

    for position_db in query.filter(AccountPosition.symbol.in_(tuple(ticker_set))):
        date_str = position_db.date.strftime("%Y-%m-%d")
        dict_key = '%s|%s' % (date_str, position_db.symbol)
        account_position_dict[dict_key] = position_db


def __integration_report_file(strategy_name, ticker_set):
    save_folder = base_save_folder_template % now_date_str

    exists_trading_ticker_list = []

    data = pd.DataFrame()
    for trading_ticker in ticker_set:
        report_file_name = 'strategy_report_%s_%s_%s.csv' % (strategy_name, trading_ticker, now_date_str)
        report_file_path = os.path.join(save_folder, report_file_name)
        if not os.path.exists(report_file_path):
            continue
        else:
            exists_trading_ticker_list.append(trading_ticker)
        df = pd.read_csv(report_file_path)

        df_pnl = df[['date', 'pnl', 'equity_base', 'return_rate']]
        df_pnl.columns = ['date', 'pnl_%s' % trading_ticker, 'equity_base_%s'% trading_ticker, 'return_rate_%s'% trading_ticker]
        if len(data) == 0:
            data = df_pnl
        else:
            data = pd.merge(data, df_pnl, on='date', how='outer')

    data = data.fillna(0)
    data.index = data['date']
    data = data.sort_values('date')
    data = data.drop('date', axis=1)

    pnl_cols = ['pnl_%s' % x for x in exists_trading_ticker_list]
    data_pnl = data[pnl_cols]
    data_pnl.columns = ['%s' % x for x in exists_trading_ticker_list]

    data_pnl[strategy_name] = data_pnl.apply(lambda x: x.sum(), axis=1)
    data_pnl.loc['Pnl_Total'] = data_pnl.apply(lambda x: x.sum())
    data_pnl.to_csv(os.path.join(save_folder, '%s_pnl_report_%s.csv' % (strategy_name, now_date_str)), index=True)

    ret_cols = ['return_rate_%s' % x for x in exists_trading_ticker_list]
    data_ret = data[ret_cols]
    for filed in ret_cols:
        data_ret[filed] = data_ret[filed].astype('str').apply(lambda x: x.replace('%', '')).astype('float')
    data_ret[strategy_name] = data_ret.mean(axis=1)

    ret_cols.append(strategy_name)
    for filed in ret_cols:
        data_ret[filed] = data_ret[filed].astype('str') + '%'

    data_ret = data_ret.T
    fields = [x for x in data_ret.columns.values]
    data_ret['Cum_Ret'] = 1.0

    data_temp = pd.DataFrame()
    for filed in fields:
        data_temp[filed + '_temp'] = data_ret[filed].astype('str').apply(lambda x: x.replace('%', '')).astype('float')
        data_ret['Cum_Ret'] *= data_temp[filed + '_temp'] / 100 + 1
    data_ret['Cum_Ret'] = (data_ret['Cum_Ret'] - 1) * 100
    data_ret['Cum_Ret'] = data_ret['Cum_Ret'].apply(lambda x: '%.3f%%' % x)
    data_ret = data_ret.T

    save_column_list = ['%s' % x for x in exists_trading_ticker_list]
    save_column_list.append(strategy_name)
    data_ret.columns = save_column_list
    data_ret.to_csv(os.path.join(save_folder, '%s_ret_report_%s.csv' % (strategy_name, now_date_str)), index=True)


def daily_return_report_job2():
    global now_date_str
    now_date_str = date_utils.get_today_str('%Y-%m-%d')

    server_name = 'huabao'
    server_model = server_constant.get_server_model(server_name)
    __build_ticker_exchange(server_model)

    for strategy_name in strategy_name_list:
        pf_account_db_list, pf_account_id_list = get_pf_account_info_list(server_model, strategy_name)
        __build_close_price_dict(server_model, pf_account_id_list)

        __build_pf_position_dict(server_model, pf_account_id_list)
        ticker_set = __build_trade_dict(server_model, pf_account_db_list)
        __build_account_position_dict(server_model, ticker_set)

        for tading_ticker in ticker_set:
            tading_ticker_report(server_model, strategy_name, tading_ticker)
        __integration_report_file(strategy_name, ticker_set)
        PnlFileChange(base_save_folder_template % now_date_str, 'ret', now_date_str.replace('-', ''), strategy_name).pnl_fix_process()
    server_model.close()




if __name__ == '__main__':
    daily_return_report_job2()
    # __integration_report_file('StkIntraDayStrategy', ('300374', '002746', '300282'))
