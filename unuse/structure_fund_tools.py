# -*- coding: utf-8 -*-
# 检查account_trade_restrictions表是否需要新增数据
import threading
from itertools import islice

from eod_aps.model.realaccount import RealAccount
from eod_aps.model.account_trade_restrictions import AccountTradeRestrictions
from eod_aps.model.instrument import Instrument
from eod_aps.model.server_constans import ServerConstant


def read_structure_fund_file(server_name):
    structure_fund_file_dict = dict()
    file_path = 'E:/market_file/structure_fund_list.csv'
    input_file = open(file_path)
    for line in input_file.xreadlines():
        line_item = line.split(',')
        structure_fund_file_dict[line_item[0]] = (line_item[1], line_item[2], line_item[3])

    structure_fund_db_dict = dict()
    host_server_model = ServerConstant().get_server_model(server_name)
    session_common = host_server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 16):
        structure_fund_db_dict[instrument_db.ticker] = instrument_db

    index_dict = dict()
    for instrument_db in query.filter(Instrument.type_id == 6):
        index_dict[instrument_db.ticker] = instrument_db

    for parent_ticker in structure_fund_file_dict.keys():
        print 'Parent Ticker:', parent_ticker
        if parent_ticker not in structure_fund_db_dict:
            print '   unfind in db'
            continue

        parent_instrument_db = structure_fund_db_dict[parent_ticker]
        if parent_instrument_db.tranche is not None:
            print '   tranche error'
        (sub_ticker_db1, sub_ticker_db2) = parent_instrument_db.undl_tickers.split(';')
        (sub_ticker1, sub_ticker2, undel_index) = structure_fund_file_dict[parent_ticker]
        if sub_ticker_db1 != sub_ticker1 or sub_ticker_db2 != sub_ticker2:
            print 'error'

        sub_instrument_db1 = structure_fund_db_dict[sub_ticker_db1]
        sub_instrument_db2 = structure_fund_db_dict[sub_ticker_db2]

        if sub_instrument_db1.tranche != 'A':
            print 'sub ticker:%s tranche is not A' % sub_instrument_db1.ticker
        if sub_instrument_db2.tranche != 'B':
            print 'sub ticker:%s tranche is not B' % sub_instrument_db2.ticker

        if sub_instrument_db1.undl_tickers is not None and sub_instrument_db1.undl_tickers not in index_dict:
            print 'sub ticker:%s, undl_tickers:%s error' % (sub_instrument_db1.ticker, sub_instrument_db1.undl_tickers)

        if sub_instrument_db2.undl_tickers is not None and sub_instrument_db2.undl_tickers not in index_dict:
            print 'sub ticker:%s, undl_tickers:%s error' % (sub_instrument_db2.ticker, sub_instrument_db2.undl_tickers)



if __name__ == '__main__':
    read_structure_fund_file('huabao')
