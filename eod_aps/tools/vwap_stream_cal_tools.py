# -*- coding: utf-8 -*-
import os
import traceback
from decimal import Decimal
import pandas as pd
import numpy as np
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.model.schema_jobs import DailyVwapAnalyse
from eod_aps.model.server_constans import server_constant

data_path = const.EOD_CONFIG_DICT['data_file_folder']
Algo_Type_Enums = const.ALGO_TYPE_ENUMS
custom_enum_utils = CustomEnumUtils()
algo_type_dict = custom_enum_utils.enum_to_dict(Algo_Type_Enums, inversion_flag=True)
include_algo_types = [Algo_Type_Enums.SigmaVWAP_AI, Algo_Type_Enums.SigmaVWAP, Algo_Type_Enums.SigmaVWAP_3]


def direct(x):
    if x > 0:
        return 'b'
    else:
        return 's'


class VwapCalTools():
    def __init__(self, start_date):
        self.__start_date = start_date
        self.__start_date2 = start_date.replace('-', '')
        self.__market_data_folder = os.path.join(data_path, 'wind', 'stock', self.__start_date2, 'market_data')
        self.__avg_price_dict = dict()
        self.__order_df = None
        self.__error_messages = []

    def start_index(self):
        """
            入口函数
        :return:
        """
        self.__query_avg_price_dict()
        self.__query_order_df()
        self.__check_data_rely()
        if not self.__error_messages:
            order_df = self.__deal_trade()
            self.__build_report_df(order_df)
        return self.__error_messages

    def __check_data_rely(self):
        if len(self.__avg_price_dict) == 0:
            self.__error_messages.append("Wind数据库ASHAREEODPRICES表数据缺失")
        if self.__order_df.empty:
            self.__error_messages.append("118数据库aggregation.order表数据缺失")
        if not os.path.exists(self.__market_data_folder):
            self.__error_messages.append("股票行情数据文件缺失")

    def __query_avg_price_dict(self):
        server_model = server_constant.get_server_model('wind_db')
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = "select S_INFO_WINDCODE, S_DQ_AVGPRICE from ASHAREEODPRICES where TRADE_DT = '%s'" % \
                    self.__start_date2
        self.__avg_price_dict = {x[0]: x[1] for x in session_dump_wind.execute(query_sql)}

    def __query_order_df(self):
        server_model = server_constant.get_server_model('local118')
        session_aggregation = server_model.get_db_session('aggregation')
        query_sql = "select * from aggregation.order where CREATE_TIME >= '%s' and CREATE_TIME <= '%s'" % \
                    ('%s 09:00:00' % self.__start_date, '%s 09:30:00' % self.__start_date)
        data_list = []
        for x in session_aggregation.execute(query_sql):
            item_list = list(x)
            ticker = item_list[5].split(' ')[0]
            if not ticker.isdigit():
                continue
            if int(item_list[21]) not in include_algo_types:
                continue
            item_list[21] = algo_type_dict[int(item_list[21])]

            ticker_wind = '%s.SH' % ticker if ticker[0] == '6' else '%s.SZ' % ticker
            item_list.append(ticker_wind)
            data_list.append(item_list)

        columns = ['id', 'server_name', 'sys_id', 'account', 'hedge_flag', 'symbol',
                   'direction', 'type', 'trade_type', 'status', 'op_status', 'property',
                   'create_time', 'transaction_time', 'user_id', 'strategy_id', 'parent_ord_id',
                   'qty', 'price', 'ex_qty', 'ex_price', 'algo_type', 'ticker_wind'
                    ]
        order_df = pd.DataFrame(data_list, columns=columns)

        order_df = order_df[['create_time', 'symbol', 'direction', 'ex_qty', 'ex_price', 'strategy_id', 'server_name',
                             'account', 'algo_type', 'ticker_wind']]
        order_df['cashflow'] = order_df['ex_qty'] * order_df['ex_price'] * (-1) * order_df['direction']

        order_df['feerate'] = np.nan
        b1 = order_df['server_name'] == 'guosen'
        b2 = order_df['server_name'] == 'huabao'
        b3 = order_df['direction'] == 1
        b4 = order_df['direction'] == -1
        b5 = order_df['symbol'] > 399999
        b6 = order_df['symbol'] <= 399999

        order_df.loc[b1 & b3 & b5, 'feerate'] = Decimal(0.0001811)
        order_df.loc[b1 & b3 & b6, 'feerate'] = Decimal(0.0001556)
        order_df.loc[b1 & b4 & b5, 'feerate'] = Decimal(0.0011811)
        order_df.loc[b1 & b4 & b6, 'feerate'] = Decimal(0.0011556)
        order_df.loc[b2 & b3, 'feerate'] = Decimal(0.000252)
        order_df.loc[b2 & b4, 'feerate'] = Decimal(0.001252)

        order_df['direction'] = order_df['direction'].apply(lambda x: direct(x))
        order_df['amt'] = order_df['ex_qty'] * order_df['ex_price']
        order_df['cost'] = order_df['amt'] * order_df['feerate']
        order_df['netcash'] = order_df['cashflow'] - order_df['cost']
        self.__order_df = order_df

    def __deal_trade(self):
        df_result = pd.DataFrame()
        for group1_key, group1 in self.__order_df.groupby(['symbol', 'ticker_wind']):
            ticker, ticker_wind = group1_key
            market_vwap = float(self.__avg_price_dict[ticker_wind])
            try:
                spread = self.__query_ticker_spread(ticker, self.__start_date2)
            except IOError:
                self.__error_messages.append(u"股票行情数据文件缺失,ticker:%s" % ticker)
                continue

            for group2_key, group2 in group1.groupby(['server_name', 'account', 'algo_type', ]):
                server, account, strategy = group2_key
                buy_data = group2[group2['direction'] == 'b']
                sell_data = group2[group2['direction'] == 's']
                buy_vwap, sell_vwap, buy_slippage, sell_slippage = 0, 0, 0, 0

                buy_amt = buy_data['amt'].astype('float').sum()
                buy_vol = buy_data['ex_qty'].astype('float').sum()
                if buy_vol != 0:
                    buy_vwap = buy_amt / buy_vol
                    buy_slippage = (buy_vwap - market_vwap) / market_vwap

                sell_amt = sell_data['amt'].astype('float').sum()
                sell_vol = sell_data['ex_qty'].astype('float').sum()
                if sell_vol != 0:
                    sell_vwap = sell_amt / sell_vol
                    sell_slippage = (market_vwap - sell_vwap) / market_vwap

                cost = group2['cost'].astype('float').sum()
                net_cash = group2['netcash'].astype('float').sum()

                result = [ticker, server, account, strategy, buy_vwap, buy_vol, sell_vwap, sell_vol,
                          market_vwap, spread, cost, net_cash, buy_slippage, sell_slippage]
                header_ = ['ticker', 'server', 'account', 'strategy', 'buy_vwap', 'buy_vol', 'sell_vwap',
                           'sell_vol', 'market_vwap', 'spread', 'cost', 'netcash', 'buy_slippage', 'sell_slippage']
                result = pd.DataFrame(result).T
                result.columns = header_
                df_result = pd.concat([df_result, result])
        return df_result

    def __build_report_df(self, order_df):
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')

        for group1_key, group1 in order_df.groupby(['server', 'account']):
            server, account = group1_key
            for strategy, group2 in group1.groupby(['strategy', ]):
                group2['buy_amt'] = group2['buy_vwap'] * group2['buy_vol']
                group2['sell_amt'] = group2['sell_vwap'] * group2['sell_vol']
                group2['market_buy_amt'] = group2['market_vwap'] * group2['buy_vol']
                group2['market_sell_amt'] = group2['market_vwap'] * group2['sell_vol']

                buy_amt = group2['buy_amt'].sum()
                sell_amt = group2['sell_amt'].sum()
                market_buy_amt = group2['market_buy_amt'].sum()
                market_sell_amt = group2['market_sell_amt'].sum()

                avg_buy_slippage = (buy_amt / market_buy_amt) - 1
                avg_sell_slippage = 1 - (sell_amt / market_sell_amt)
                avg_slippage = (buy_amt * avg_buy_slippage + sell_amt * avg_sell_slippage) / (buy_amt + sell_amt)

                daily_vwap_analyse = DailyVwapAnalyse()
                daily_vwap_analyse.date = self.__start_date
                daily_vwap_analyse.server = server
                daily_vwap_analyse.account = account
                daily_vwap_analyse.strategy = strategy
                daily_vwap_analyse.avg_buy_slippage = avg_buy_slippage
                daily_vwap_analyse.avg_sell_slippage = avg_sell_slippage
                daily_vwap_analyse.buy_amt = buy_amt
                daily_vwap_analyse.sell_amt = sell_amt
                daily_vwap_analyse.avg_slippage = avg_slippage
                session_jobs.merge(daily_vwap_analyse)

        for strategy, group1 in order_df.groupby(['strategy', ]):
            group1['buy_amt'] = group1['buy_vwap'] * group1['buy_vol']
            group1['sell_amt'] = group1['sell_vwap'] * group1['sell_vol']
            group1['market_buy_amt'] = group1['market_vwap'] * group1['buy_vol']
            group1['market_sell_amt'] = group1['market_vwap'] * group1['sell_vol']

            buy_amt = group1['buy_amt'].sum()
            sell_amt = group1['sell_amt'].sum()
            market_buy_amt = group1['market_buy_amt'].sum()
            market_sell_amt = group1['market_sell_amt'].sum()

            avg_buy_slippage = (buy_amt / market_buy_amt) - 1
            avg_sell_slippage = 1 - (sell_amt / market_sell_amt)
            avg_slippage = (buy_amt * avg_buy_slippage + sell_amt * avg_sell_slippage) / (buy_amt + sell_amt)

            daily_vwap_analyse = DailyVwapAnalyse()
            daily_vwap_analyse.date = self.__start_date
            daily_vwap_analyse.server = 'all'
            daily_vwap_analyse.account = 'all'
            daily_vwap_analyse.strategy = strategy
            daily_vwap_analyse.avg_buy_slippage = avg_buy_slippage
            daily_vwap_analyse.avg_sell_slippage = avg_sell_slippage
            daily_vwap_analyse.buy_amt = buy_amt
            daily_vwap_analyse.sell_amt = sell_amt
            daily_vwap_analyse.avg_slippage = avg_slippage
            session_jobs.merge(daily_vwap_analyse)
        session_jobs.commit()

    def __query_ticker_spread(self, ticker, day):
        ticker = ticker.split(' ')[0]
        file_name = '%s_%s_market_data.csv' % (ticker, day)
        file_path = os.path.join(self.__market_data_folder, file_name)
        df = pd.read_csv(file_path, usecols=['datetime', 'last_prc', 'ask_prc1', 'bid_prc1', 'bid_vol1', 'ask_vol1'],
                         dtype={'last_prc': float, 'ask_prc1': float, 'bid_prc1': float})
        b1 = df['ask_prc1'] != 0
        b2 = df['bid_prc1'] != 0
        b3 = df['ask_prc1'] > df['bid_prc1']
        day_ = '-'.join([day[:4], day[4:6], day[6:8]])
        b4 = df['datetime'] >= '%s 09:30:00' % day_
        b5 = df['datetime'] <= '%s 11:30:00' % day_
        b6 = df['datetime'] >= '%s 13:00:00' % day_
        b7 = df['datetime'] <= '%s 15:00:00' % day_

        b8 = df['bid_vol1'] > 0
        b9 = df['ask_vol1'] > 0

        df = df[b1 & b2 & b3 & ((b4 & b5) | (b6 & b7)) & b8 & b9]
        df['spread'] = (df['ask_prc1'] - df['bid_prc1']) * 2 / (df['ask_prc1'] + df['bid_prc1'])
        df = df[df['spread'] < 0.1]
        return df['spread'].mean()


if __name__ == '__main__':
    vwap_cal_tools = VwapCalTools('2019-04-24')
    print vwap_cal_tools.start_index()
