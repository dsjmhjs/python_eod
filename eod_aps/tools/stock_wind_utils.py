# -*- coding: utf-8 -*-
# 通过wind接口来获取行情数据信息
# from WindPy import *
from eod_aps.model.schema_common import Instrument
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const
from eod_aps.tools.wind_local_tools import w_ys, w_ys_close


class StockWindUtils(object):
    """
        股票数据常用工具类
    """
    w = None
    instrument_dict = dict()

    def __init__(self):
        pass

    def __enter__(self):
        self.w = w_ys()
        server_model = server_constant.get_server_model('host')
        session_common = server_model.get_db_session('common')
        query = session_common.query(Instrument)
        for instrument_db in query:
            self.instrument_dict[instrument_db.ticker] = instrument_db
        return self

    def get_prev_close(self, date_str, ticker):
        ticker = self.rebuild_wind_ticker(ticker)
        if ticker is None:
            return

        ticker_wind_list = [ticker]
        wind_data_dict = self.w.query_wsd_data("pre_close", ticker_wind_list, date_str)
        prev_close_value = 0
        if wind_data_dict:
            prev_close_value = wind_data_dict.values()[0]
        return prev_close_value

    def get_prev_close_dict(self, date_str, ticker_list):
        ticker_dict = {}
        wind_ticker_list = []
        for ticker in ticker_list:
            wind_ticker_str = self.rebuild_wind_ticker(ticker)
            if wind_ticker_str is None:
                continue
            ticker_dict[wind_ticker_str] = ticker
            wind_ticker_list.append(wind_ticker_str)

        wind_prev_close_dict = self.w.query_wsd_data("pre_close", wind_ticker_list, date_str)

        prev_close_dict = dict()
        for (wind_ticker, ticker_prev_close) in wind_prev_close_dict.items():
            ticker = ticker_dict[wind_ticker]
            prev_close_dict[ticker] = ticker_prev_close
        return prev_close_dict

    # 获取股票前收价
    def get_close(self, date_str, ticker):
        ticker = self.rebuild_wind_ticker(ticker)
        if ticker is None:
            return

        ticker_wind_list = [ticker]
        wind_close_dict = self.w.query_wsd_data("close", ticker_wind_list, date_str)
        close_value = 0
        if wind_close_dict:
            close_value = wind_close_dict.values()[0]
        return close_value

    def get_close_dict(self, date_str, ticker_list):
        ticker_dict = {}
        wind_ticker_list = []
        for ticker in ticker_list:
            wind_ticker_str = self.rebuild_wind_ticker(ticker)
            if wind_ticker_str is None:
                continue
            ticker_dict[wind_ticker_str] = ticker
            wind_ticker_list.append(wind_ticker_str)
        wind_close_dict = self.w.query_wsd_data("close", wind_ticker_list, date_str)

        close_price_dict = dict()
        for (wind_ticker, ticker_close) in wind_close_dict.items():
            ticker = ticker_dict[wind_ticker]
            close_price_dict[ticker] = ticker_close
        return close_price_dict

    def get_ipo_date(self, ticker, date_str):
        wind_ticker = self.__wind_commonstock(ticker)
        ipo_date_dict = self.w.query_wsd_data("ipo_date", [wind_ticker, ], date_str)

        ipo_date_value = 0
        if ipo_date_dict:
            ipo_date_value = ipo_date_dict.values()[0][:8]
        return ipo_date_value

    def get_ticker_list(self, ticker_type_list):
        ticker_list = []
        for (ticker, instrument_db) in self.instrument_dict.items():
            if instrument_db.type_id not in ticker_type_list:
                continue
            ticker_list.append(ticker)
        return ticker_list

    def __wind_commonstock(self, ticker):
        if ticker.startswith('0') or ticker.startswith('3'):
            wind_ticker = '%s.SZ' % ticker
        elif ticker.startswith('6'):
            wind_ticker = '%s.SH' % ticker
        return wind_ticker

    def rebuild_wind_ticker(self, ticker):
        if ticker not in self.instrument_dict:
            print 'Error input ticker:', ticker
            return None
        instrument_db = self.instrument_dict[ticker]
        if instrument_db.exchange_id == const.EXCHANGE_TYPE_ENUMS.CG:
            if instrument_db.type_id == const.INSTRUMENT_TYPE_ENUMS.Index:
                ticker_wind_str = '%s.SH' % instrument_db.ticker_exch_real
            else:
                ticker_wind_str = '%s.SH' % instrument_db.ticker
        elif instrument_db.exchange_id == const.EXCHANGE_TYPE_ENUMS.CS:
            ticker_wind_str = '%s.SZ' % instrument_db.ticker
        elif instrument_db.exchange_id == const.EXCHANGE_TYPE_ENUMS.SHF:
            ticker_wind_str = '%s.SHF' % instrument_db.ticker
        elif instrument_db.exchange_id == const.EXCHANGE_TYPE_ENUMS.DCE:
            ticker_wind_str = '%s.DCE' % instrument_db.ticker
        elif instrument_db.exchange_id == const.EXCHANGE_TYPE_ENUMS.ZCE:
            ticker_wind_str = '%s.CZC' % instrument_db.ticker
        elif instrument_db.exchange_id == const.EXCHANGE_TYPE_ENUMS.CFF:
            ticker_wind_str = '%s.CFE' % instrument_db.ticker
        else:
            return None
        return ticker_wind_str

    def __exit__(self, type, value, traceback):
        w_ys_close()


if __name__ == '__main__':
    with StockWindUtils() as stock_utils:
        print stock_utils.get_ipo_date('000001', '2017-07-20')


