# -*- coding: utf-8 -*-
import pandas as pd
from eod_aps.model.eod_const import const
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.tools.ysquant_manager_tools import get_basic_info_data

date_utils = DateUtils()
stock_utils = StockUtils()
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


class InstrumentCheckTools(object):
    """
        Instrument檢查工具类
    """
    def __init__(self, check_server='host'):
        self.__trading_future_list = self.__query_future_main_contract()

        server_model = server_constant.get_server_model(check_server)
        session_common = server_model.get_db_session('common')
        self.__instrument_df = self.__query_instrument_df(session_common)
        self.__error_ticker_dict = dict()

    def check_index(self):
        self.__check_pre_close()
        self.__check_track_undl_tickers()
        # self.__check_option()
        self.__check_future()
        self.__check_is_settle_instantly()
        check_result = self.__send_email()
        return check_result

    def __query_future_main_contract(self):
        server_model = server_constant.get_server_model('host')
        session_common = server_model.get_db_session('common')
        query = session_common.query(FutureMainContract)
        future_list = []
        for x in query:
            future_list.extend([x.pre_main_symbol, x.main_symbol, x.next_main_symbol])
        return list(set(future_list))

    def __query_instrument_df(self, session_common):
        pre_listed_ticker = stock_utils.get_pre_listed_ticker()

        query = session_common.query(Instrument)
        instrument_list = []
        for instrument_db in query.filter(Instrument.del_flag == 0):
            if instrument_db.type_id == 1 and instrument_db.ticker not in self.__trading_future_list:
                continue

            if instrument_db.ticker in pre_listed_ticker:
                continue

            instrument_item_dict = instrument_db.to_dict()
            instrument_list.append(instrument_item_dict)
        instrument_df = pd.DataFrame(instrument_list)
        instrument_df.index = instrument_df['id']
        instrument_df = instrument_df.fillna(0)
        return instrument_df[(-instrument_df['ticker'].str.startswith('7')) &
                             (instrument_df['exchange_id'].isin([18, 19, 20, 21, 22, 25, 35])) &
                             (-instrument_df['type_id'].isin([19, ]))]

    def __check_pre_close(self):
        if date_utils.is_pre_night_market():
            filter_df = self.__instrument_df[self.__instrument_df['session'].str.find('21:00:00') > -1]
        else:
            filter_df = self.__instrument_df

            include_ticker_list = self.__query_market_tickers()
            exclude_df = filter_df[(filter_df['type_id'] == 4) & (-filter_df["ticker"].isin(include_ticker_list))]
            idx = set(filter_df.index.values).difference(set(exclude_df.index.values))
            filter_df = filter_df.reindex(list(idx))

            filter_date = date_utils.get_today_str('%Y-%m-%d')
            filter_df["effective_since"] = filter_df["effective_since"].astype(str)
            exclude_df = filter_df[(filter_df['type_id'] == 10) & (filter_df["effective_since"] == filter_date)]
            idx = set(filter_df.index.values).difference(set(exclude_df.index.values))
            filter_df = filter_df.reindex(list(idx))
        check_fields = ['prev_close']
        self.__check_df_fields(filter_df, check_fields)

        filter_df = filter_df[-filter_df['type_id'].isin([6, 19])]
        check_fields = ['uplimit', 'downlimit']
        self.__check_df_fields(filter_df, check_fields)

    # 过滤当日新上市和未上市股票
    def __query_market_tickers(self):
        market_ticker_list = []
        stock_basic_data_dict = get_basic_info_data().to_dict('index')
        for (ticker, ticker_info_dict) in stock_basic_data_dict.items():
            if ticker_info_dict['list_date'] == date_utils.get_today_str():
                continue

            if ticker_info_dict['list_date'] != '' and ticker_info_dict['delist_date'] == '':
                market_ticker_list.append(ticker)
        market_ticker_list.sort()
        return market_ticker_list

    def __check_track_undl_tickers(self):
        filter_df = self.__instrument_df[self.__instrument_df['type_id'] == 10]
        check_fields = ['track_undl_tickers']
        self.__check_df_fields(filter_df, check_fields)

    def __check_option(self):
        filter_df = self.__instrument_df[self.__instrument_df['type_id'].isin([7, 15, 16])]
        check_fields = ['pcf']
        self.__check_df_fields(filter_df, check_fields)

    def __check_future(self):
        future_df = self.__instrument_df[(self.__instrument_df['type_id'] == 1) &
                                         (self.__instrument_df['ticker'].isin(self.__trading_future_list))]
        check_fields = ['longmarginratio', 'shortmarginratio']
        self.__check_df_fields(future_df, check_fields)

    def __check_is_settle_instantly(self):
        filter_df = self.__instrument_df[self.__instrument_df['type_id'].isin([1, 15])]
        check_fields = ['is_settle_instantly']
        self.__check_df_fields(filter_df, check_fields)

    def __check_df_fields(self, check_df, check_fields):
        for field_name in check_fields:
            check_result_df = check_df[(check_df[field_name].isnull()) | (check_df[field_name] == 0)]
            for ind in check_result_df.index.values:
                ticker = check_df.at[ind, 'ticker']
                self.__error_ticker_dict.setdefault(ticker, []).append(field_name)

    def __send_email(self):
        if len(self.__error_ticker_dict) == 0:
            return True

        instrument_dict = dict()
        for (dict_key, dict_value) in self.__instrument_df.to_dict("index").items():
            instrument_dict[dict_value['ticker']] = dict_value

        email_list = []
        for (x, fields) in self.__error_ticker_dict.items():
            instrument_item_dict = instrument_dict[x]
            email_list.append([x, instrument_item_dict['type_id'], instrument_item_dict['prev_close_update_time'],
                               '%s(Error)' % ','.join(fields)])
        email_list.sort()
        html_list = email_utils2.list_to_html('Ticker,Type_ID,Update_Time,Error Fields', email_list)
        email_utils2.send_email_group_all('Instrument Check!', ''.join(html_list), 'html')
        return False


if __name__ == '__main__':
    instrument_check_tools = InstrumentCheckTools('huabao')
    print instrument_check_tools.check_index()
    # instrument_check_tools.query_market_tickers()
