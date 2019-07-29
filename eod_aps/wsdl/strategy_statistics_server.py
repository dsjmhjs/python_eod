# -*- coding: utf-8 -*-
from decimal import Decimal
from eod_aps.model.schema_common import Instrument
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from WindPy import *

server_name = 'huabao'
date_utils = DateUtils()
instrument_db_dict = dict()
close_price_dict = dict()


def __get_instrument_dict():
    instrument_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 4):
        instrument_dict[instrument_db.ticker] = instrument_db
    return instrument_dict


def __query_strategy_list(session_portfolio, strategy_name):
    query_pf_account = session_portfolio.query(PfAccount)
    pf_account_list = []
    for pf_account_db in query_pf_account.filter(PfAccount.fund_name.like('%' + strategy_name + '%'),
                                                 PfAccount.fund_name.like('%steady_return%')):
        pf_account_list.append(pf_account_db)
    return pf_account_list


def __strategy_position_info(session_portfolio, pf_account_list, date_str):
    query_pf_position = session_portfolio.query(PfPosition)

    strategy_position_dict = dict()
    for pf_account in pf_account_list:
        pf_position_dict = dict()
        for pf_position_db in query_pf_position.filter(PfPosition.id == pf_account.id,
                                                       PfPosition.date == date_str):
            if pf_position_db.long > 0:
                pf_position_dict[pf_position_db.symbol] = pf_position_db.long
            elif pf_position_db.short > 0:
                pf_position_dict[pf_position_db.symbol] = -pf_position_db.short
        strategy_position_dict[pf_account.name] = pf_position_dict
    return strategy_position_dict


def query_strategy_info(strategy_name):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    date_str = '2017-05-22'
    pf_account_list = __query_strategy_list(session_portfolio, strategy_name)
    strategy_position_dict = __strategy_position_info(session_portfolio, pf_account_list, date_str)

    return_list = []
    for (strategy_name, pf_position_dict) in strategy_position_dict.items():
        return_list.append('-----strategy_name:%s----' % strategy_name)
        for (ticker, volume) in pf_position_dict.items():
            return_list.append('%s:%s' % (ticker, volume))
    return '\n'.join(return_list)


def query_strategy_money_long(strategy_name):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    pf_account_list = __query_strategy_list(session_portfolio, strategy_name)
    date_str = '2017-05-22'
    strategy_position_dict = __strategy_position_info(session_portfolio, pf_account_list, date_str)
    instrument_dict = __get_instrument_dict()
    return_list = []
    strategy_name_list = strategy_position_dict.keys()
    strategy_name_list.sort()
    for strategy_name_item in strategy_name_list:
        pf_position_dict = strategy_position_dict[strategy_name_item]
        total_money = Decimal(0.0)
        for (ticker, volume) in pf_position_dict.items():
            if volume <= 0:
                continue
            instrument_db = instrument_dict[ticker]
            total_money += Decimal(volume) * instrument_db.prev_close
        return_list.append('%s,%s' % (strategy_name_item, total_money))
    return_list.insert(0, 'strategy_name,long_money')
    return return_list


def __query_date_info(session_portfolio, pf_account_list):
    pf_account_id_list = []
    for pf_account in pf_account_list:
        pf_account_id_list.append(str(pf_account.id))

    query_sql = "select date from portfolio.pf_position a where a.id in (%s) group by date" % ','.join(pf_account_id_list)
    date_list = []
    for date_str in session_portfolio.execute(query_sql):
        date_list.append(date_str[0])

    date_utils = DateUtils()
    return date_utils.get_trading_day_list(date_list[0], date_list[-1])


def __build_close_price_dict(start_date, end_date, ticker_list):
    w.start()

    ticker_wind_list = []
    for ticker in ticker_list:
        ticker_wind_str = __get_wind_ticker(ticker)
        ticker_wind_list.append(ticker_wind_str)

    wind_data = w.wsd(ticker_wind_list, "close", start_date, end_date, "Fill=Previous")
    if wind_data.Data[0][0] == 'No Content':
        print 'No Content:'
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


def __build_ticker_exchange(server_model):
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 4):
        instrument_db_dict[instrument_db.ticker] = instrument_db


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



def query_strategy_money_history(strategy_name):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    __build_ticker_exchange(server_model)

    query_sql = "select a.DATE,a.SYMBOL,a.`LONG`,a.SHORT from portfolio.pf_position a left join portfolio.pf_account b \
                    on a.id = b.id where b.FUND_NAME like '%" + strategy_name +"%'"

    date_set = set()
    ticker_set = set()
    date_dict = dict()
    for data_info in session_portfolio.execute(query_sql):
        date_str = data_info[0]
        ticker = data_info[1]
        volume_long = data_info[2]
        volume_short = data_info[3]
        if date_str in date_dict:
            date_dict[date_str].append([ticker, volume_long, volume_short])
        else:
            date_dict[date_str] = []
            date_dict[date_str].append([ticker, volume_long, volume_short])
        date_set.add(date_str)
        if ticker.isdigit():
            ticker_set.add(ticker)

    date_list = list(date_set)
    date_list.sort()
    ticker_list = list(ticker_set)
    __build_close_price_dict(date_list[0], date_list[-1], ticker_list)

    out_put_list = []
    for date_str in date_list[:-1]:
        ticker_info_list = date_dict[date_str]
        date_str = date_str.strftime('%Y-%m-%d')
        total_money = Decimal(0.0)
        future_dict = dict()
        for ticker_info in ticker_info_list:
            ticker = ticker_info[0]
            volume_long = ticker_info[1]
            volume_short = ticker_info[2]
            if ticker.isdigit():
                dict_key = '%s|%s' % (date_str, ticker)
                prev_close = Decimal(close_price_dict[dict_key])
                if volume_long > 0:
                    total_money += prev_close * volume_long
                elif volume_short > 0:
                    total_money -= prev_close * volume_short
            else:
                if ticker in future_dict:
                    future_dict[ticker] += volume_short
                else:
                    future_dict[ticker] = volume_short

        for (future_ticker, volume) in future_dict.items():
            out_put_list.append('%s,%s,%s' % (date_str, future_ticker, volume))
        out_put_list.append('%s,%s,%s' % (date_str, 'money', total_money))
    print '\n'.join(out_put_list)

    file_object = open('E:/dailyFiles/report/%s_report.csv' % strategy_name , 'w')
    out_put_list.insert(0, 'date,ticker,value')
    file_object.write('\n'.join(out_put_list))
    file_object.close()


if __name__ == '__main__':
    # for strategy_name in ('Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'CSI300_MV10Norm'):
    #     print '\n'.join(query_strategy_money_long(strategy_name))
    for strategy_name in ('Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'CSI300_MV10Norm'):
        query_strategy_money_history(strategy_name)