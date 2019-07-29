# -*- coding: utf-8 -*-
from eod_aps.model.schema_jobs import SpecialTickers
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.job import *


class SpecialTickersInitJob(object):
    def __init__(self):
        self.__special_ticker_dict = {}

    def start_index(self):
        self.__query_special_tickers()
        self.__insert_to_db()

    def __query_special_tickers(self):
        with StockUtils() as stock_utils:
            self.__special_ticker_dict['Suspend'] = stock_utils.get_suspend_stock()
            self.__special_ticker_dict['ST'] = stock_utils.get_st_stock()
            self.__special_ticker_dict['Low_Stop'] = stock_utils.get_yzd_stocks()
            self.__special_ticker_dict['High_Stop'] = stock_utils.get_yzz_stocks()

    def __insert_to_db(self):
        ticker_dict = dict()
        for (special_key, ticker_list) in self.__special_ticker_dict.items():
            for ticker in ticker_list:
                ticker_dict.setdefault(ticker, []).append(special_key)

        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        date_str = date_utils.get_today_str('%Y%m%d')
        for (ticker, special_key_list) in ticker_dict.items():
            special_ticker = SpecialTickers()
            special_ticker.date = date_str
            special_ticker.ticker = ticker
            special_ticker.describe = ';'.join(special_key_list)
            session_jobs.merge(special_ticker)
        session_jobs.commit()


if __name__ == "__main__":
    special_tickers_init = SpecialTickersInitJob()
    special_tickers_init.start_index()

