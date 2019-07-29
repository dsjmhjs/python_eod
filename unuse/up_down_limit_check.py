# -*- coding: utf-8 -*-
# 对期货的uplimit和downlimit进行校验
import os
import math
import codecs
from decimal import Decimal
from eod_aps.model.BaseModel import *
from eod_aps.job import *

st_stock_list = []  # 存储st股票的ticker
# 不校验的货币基金
filter_check_list = {'519800', '519801', '519808', '519858', '519878', '519888', '519889', '519898', '519899'}
email_list = []

def read_lts_file(filePath):
    email_list.append('Start read file:' + filePath)
    fr = codecs.open(filePath, 'r', 'gbk')
    for line in fr.xreadlines():
        baseModel = BaseModel()
        for tempStr in line.split('|')[1].split(','):
            tempArray = tempStr.replace('\n', '').split(':', 1)
            setattr(baseModel, tempArray[0].strip(), tempArray[1])
        if 'OnRspQryInstrument' in line:
            product_id = getattr(baseModel, 'ProductID', '')
            product_class = getattr(baseModel, 'ProductClass', '')
            if (product_class == '6') and ((product_id == 'SZA') or (product_id == 'SHA') or (product_id == 'CY')):
                instrument_id = getattr(baseModel, 'InstrumentID', '')
                instrument_name = getattr(baseModel, 'InstrumentName', '')
                if instrument_name.startswith('S') or \
                        instrument_name.startswith('ST') or \
                        instrument_name.startswith('*ST'):
                    st_stock_list.append(instrument_id)


def limit_check_stock(mysql_utils):
    query_sql = 'select TICKER,PREV_CLOSE,TICK_SIZE_TABLE,UPLIMIT,DOWNLIMIT from common.instrument where type_id = 4 \
                and inactive_date is null'
    for item in mysql_utils.query_all(query_sql):
        (ticker, prev_close, tick_size_table, uplimit_db, downlimit_db) = item
        if (uplimit_db is None) or (downlimit_db is None):
            email_list.append('ticker:%s uplimit or downlimit in db is None' % ticker)
            continue

        if ticker in st_stock_list:
            uplimit_theory = prev_close * Decimal(1.05)
            downlimit_theory = prev_close * Decimal(0.95)
        else:
            uplimit_theory = prev_close * Decimal(1.1)
            downlimit_theory = prev_close * Decimal(0.9)

        check_tick_size = Decimal(tick_size_table.split(':')[1])
        if math.fabs(uplimit_theory - uplimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_close:%s, uplimit_db:%s, uplimit_theory:%s error!' % (
                ticker, prev_close, uplimit_db, uplimit_theory))
        if math.fabs(downlimit_theory - downlimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_close:%s, downlimit_db:%s, downlimit_theory:%s error!' % (
                ticker, prev_close, downlimit_db, downlimit_theory))


def limit_check_option(mysql_utils):
    query_sql = 'select TICKER,PREV_SETTLEMENTPRICE,STRIKE,TICK_SIZE_TABLE,UPLIMIT,DOWNLIMIT,PUT_CALL,\
                    UNDL_TICKERS,EXPIRE_DATE from common.instrument  where type_id = 10'
    for item in mysql_utils.query_all(query_sql):
        (ticker, prev_settlementprice, strike, tick_size_table, uplimit_db, downlimit_db, put_call, undl_tickers,
         expire_date) = item
        if prev_settlementprice is None:
            continue
        prev_settlementprice = float(prev_settlementprice)
        strike = float(strike)
        uplimit_db = float(uplimit_db)
        downlimit_db = float(downlimit_db)

        query_sql = 'select PREV_CLOSE from common.instrument where ticker = %s'
        query_param = (undl_tickers,)
        undl_ticker_price = float(mysql_utils.query_one(query_sql, query_param)[0])

        if put_call == 1:
            up_range_value = max(undl_ticker_price * 0.005,
                                 min((undl_ticker_price * 2 - strike), undl_ticker_price) * 0.1)
            down_range_value = undl_ticker_price * 0.1
        else:
            up_range_value = max(strike * 0.005,
                                 min((strike * 2 - undl_ticker_price), undl_ticker_price) * 0.1)
            down_range_value = undl_ticker_price * 0.1

        uplimit_theory = prev_settlementprice + round(up_range_value, 4)
        downlimit_theory = prev_settlementprice - round(down_range_value, 4)
        if date_utils.get_today_str('%Y-%m-%d') == expire_date.strftime('%Y-%m-%d'):
            downlimit_theory = 0.0001

        check_tick_size = float(tick_size_table.split(':')[1])
        if math.fabs(uplimit_theory - uplimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_settlementprice:%s, uplimit_db:%s, uplimit_theory:%s error!' % (
                ticker, prev_settlementprice, uplimit_db, uplimit_theory))
        if downlimit_theory < 0:
            downlimit_theory = 0.0001
        if math.fabs(downlimit_theory - downlimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_settlementprice:%s, downlimit_db:%s, downlimit_theory:%s error!' % (
                ticker, prev_settlementprice, downlimit_db, downlimit_theory))


def limit_check_future(mysql_utils):
    query_sql = 'select TICKER,PREV_SETTLEMENTPRICE,TICK_SIZE_TABLE,UPLIMIT,DOWNLIMIT from common.instrument where type_id = 1 \
                and exchange_id = 25'
    for item in mysql_utils.query_all(query_sql):
        (ticker, prev_settlementprice, tick_size_table, uplimit_db, downlimit_db) = item
        if (uplimit_db is None) or (downlimit_db is None):
            email_list.append('ticker:%s uplimit or downlimit in db is None' % ticker)
            continue

        if ticker.startswith('TF'):
            uplimit_theory = prev_settlementprice * Decimal(1.012)
            downlimit_theory = prev_settlementprice * Decimal(0.988)
        elif ticker.startswith('T'):
            uplimit_theory = prev_settlementprice * Decimal(1.02)
            downlimit_theory = prev_settlementprice * Decimal(0.98)
        else:
            uplimit_theory = prev_settlementprice * Decimal(1.1)
            downlimit_theory = prev_settlementprice * Decimal(0.9)

        check_tick_size = Decimal(tick_size_table.split(':')[1])
        if math.fabs(uplimit_theory - uplimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_close:%s, uplimit_db:%s, uplimit_theory:%s error!' % (
                ticker, prev_settlementprice, uplimit_db, uplimit_theory))
        if math.fabs(downlimit_theory - downlimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_close:%s, downlimit_db:%s, downlimit_theory:%s error!' % (
                ticker, prev_settlementprice, downlimit_db, downlimit_theory))


def read_ctp_file(file_path):
    market_array = []
    fr = open(file_path)
    for line in fr.readlines():
        baseModel = BaseModel()
        if len(line.strip()) == 0:
            continue
        for tempStr in line.split('|')[1].split(','):
            temp_array = tempStr.replace('\n', '').split(':', 1)
            setattr(baseModel, temp_array[0].strip(), temp_array[1])
        if 'OnRspQryDepthMarketData' in line:
            market_array.append(baseModel)
    return market_array


def limit_check_commodity_futures(pre_ctp_file_path, ctp_file_path):
    pre_market_array = read_ctp_file(pre_ctp_file_path)
    market_array = read_ctp_file(ctp_file_path)

    pre_market_dict = dict()
    for market_info in pre_market_array:
        instrument_id = getattr(market_info, 'InstrumentID', '')
        pre_market_dict[instrument_id] = market_info

    for market_info in market_array:
        instrument_id = getattr(market_info, 'InstrumentID', '')
        if instrument_id not in pre_market_dict:
            email_list.append(instrument_id + ' not found!')
            continue
        pre_market_info = pre_market_dict[instrument_id]
        pre_SettlementPrice = float(getattr(pre_market_info, 'PreSettlementPrice', ''))
        pre_UpperLimitPrice = float(getattr(pre_market_info, 'UpperLimitPrice', ''))
        pre_LowerLimitPrice = float(getattr(pre_market_info, 'LowerLimitPrice', ''))

        settlementPrice = float(getattr(market_info, 'PreSettlementPrice', ''))
        upperLimitPrice = float(getattr(market_info, 'UpperLimitPrice', ''))
        lowerLimitPrice = float(getattr(market_info, 'LowerLimitPrice', ''))

        if math.fabs(pre_UpperLimitPrice / pre_SettlementPrice - upperLimitPrice / settlementPrice) > 0.02:
            email_list.append('ticker:%s, pre_SettlementPrice:%s, uplimit_percent:%s, pre_uplimit_percent:%s error!' % (
                instrument_id, settlementPrice, upperLimitPrice / settlementPrice,
                pre_UpperLimitPrice / pre_SettlementPrice))
        if math.fabs(pre_LowerLimitPrice / pre_SettlementPrice - lowerLimitPrice / settlementPrice) > 0.02:
            email_list.append('ticker:%s, pre_SettlementPrice:%s, lowerLimit_percent:%s, pre_lowerLimit_percent:%s error!' % (
                instrument_id, settlementPrice, lowerLimitPrice / settlementPrice,
                pre_LowerLimitPrice / pre_SettlementPrice))

def limit_check_fund(mysql_utils):
    query_sql = 'select TICKER,PREV_CLOSE,TICK_SIZE_TABLE,UPLIMIT,DOWNLIMIT from common.instrument where type_id in (7,15,16)'
    for item in mysql_utils.query_all(query_sql):
        (ticker, prev_close, tick_size_table, uplimit_db, downlimit_db) = item
        if ticker in filter_check_list:
            continue

        if (uplimit_db is None) or (downlimit_db is None):
            email_list.append( 'ticker:%s uplimit or downlimit in db is None' % ticker)
            continue

        uplimit_theory = prev_close * Decimal(1.1)
        downlimit_theory = prev_close * Decimal(0.9)

        check_tick_size = Decimal(tick_size_table.split(':')[1])
        if math.fabs(uplimit_theory - uplimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_close:%s, uplimit_db:%s, uplimit_theory:%s error!' % (
                ticker, prev_close, uplimit_db, uplimit_theory))
        if math.fabs(downlimit_theory - downlimit_db) > check_tick_size:
            email_list.append('ticker:%s, prev_close:%s, downlimit_db:%s, downlimit_theory:%s error!' % (
                ticker, prev_close, downlimit_db, downlimit_theory))

def start():
    last_day_str = date_utils.get_last_trading_day('%Y-%m-%d')

    for rt, dirs, files in os.walk(DATAFETCHER_MESSAGEFILE_FOLDER):
        for file_name in files:
            if ('LTS_QD' in file_name) and (last_day_str in file_name):
                lts_qd_file_path = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, file_name)
            if ('CTP_TD' in file_name) and (last_day_str in file_name):
                ctp_file_path = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, file_name)
            if ('CTP_TD' in file_name) and (last_day_str in file_name):
                pre_ctp_file_path = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, file_name)

    # email_list.append('1.check stock up_limit and down_limit')
    # read_lts_file(lts_qd_file_path)
    # limit_check_stock(mysql_utils)
    # email_list.append('2.check option up_limit and down_limit')
    # limit_check_option(mysql_utils)
    # email_list.append('3.check fund up_limit and down_limit')
    # limit_check_fund(mysql_utils)
    # email_list.append('4.check future up_limit and down_limit')
    # limit_check_future(mysql_utils)
    return '<br/>'.join(email_list)

if __name__ == '__main__':
    start()

