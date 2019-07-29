# -*- coding: utf-8 -*-
import json
import os
import pickle
import threading
import time
import traceback
from threading import Timer

import redis
from eod_aps.model.schema_history import ServerRisk
from eod_aps.model.schema_jobs import FundInfo, RiskManagement
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.tools.tradeplat_message_tools import *
import pandas as pd
import numpy as np
from eod_aps.tools.tradeplat_position_tools import RiskView
from eod_aps.tools.ysquant_manager_tools import get_daily_data
from eod_aps.job import *


index_list = ['SSE50', 'SHSZ300', 'SH000905']
strategy_group_map = {
            'Multi_Factor': ['MultiFactor', ],
            'Event_Real': ['Earning', 'Institution', 'Inflow']
        }
trading_time = [['09:25', '11:30'], ['13:01', '15:02']]


class AlphaCalculationJob(object):
    def __init__(self):
        self.server_model = server_constant.get_server_model('local118')
        self.r = redis.Redis(host=self.server_model.ip, db=4)
        self.pipeline_redis = self.r.pipeline()
        self.__instrument_df = self.__query_instrument_df()
        self.__minute_demo = self.__create_minute_demo()

        self.__basket_dict = dict()
        self.__ret_dict = dict()

    def start_run(self):
        t = threading.Thread(target=self.alpha_calculation_thread, args=())
        t.start()

    def alpha_calculation_thread(self):
        try:
            self.get_index_basket()
            strategy_list = ['Multi_Factor']
            map(lambda x: self.get_strategy_basket(x), strategy_list)
            self.init_strategy_basket_info(strategy_list)

            validate_number = int(date_utils.get_today_str('%H%M%S'))
            while 92000 <= validate_number <= 150200:
                Timer(5, self.__recv_market_info_timer, []).start()
                time.sleep(30)
                validate_number = int(date_utils.get_today_str('%H%M%S'))
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_job(error_msg)
            email_utils2.send_email_group_all('[Error]__alpha_calculation_thread.', error_msg)

    def get_index_basket(self):
        redis_title_name = 'market_ret:basket_list:Index'
        self.__reset_redis_list(redis_title_name)
        map(lambda x: self.r.lpush(redis_title_name, x), index_list)

        for ind in index_list:
            data = pd.DataFrame([[ind, 1.0, 0]], columns=['symbol', 'weight', 'long_value'])
            data.index = data['symbol']
            self.__basket_dict[ind] = data

    def get_strategy_basket(self, strategy_group_name):
        redis_title_name = 'market_ret:basket_list:%s' % strategy_group_name
        self.__reset_redis_list(redis_title_name)

        event_real_basket = list()
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
        session_server = self.server_model.get_db_session('aggregation')

        for basket_name in strategy_group_map[strategy_group_name]:
            query_sql = "select a.symbol,sum(a.`LONG`) from aggregation.pf_position a \
left join aggregation.pf_account b on a.SERVER_NAME = b.SERVER_NAME and a.id = b.id \
where a.DATE = '%s' and b.FUND_NAME like '%%%s%%' group by a.SYMBOL" % (filter_date_str, basket_name)
            data_list = [[x[0], float(x[1])] for x in session_server.execute(query_sql)]
            if len(data_list) == 0:
                continue

            self.r.lpush(redis_title_name, basket_name)
            data_df = pd.DataFrame(data_list, columns=['symbol', 'long_value'])
            data_df = self.__calcluate_db_weight_table(data_df)

            self.__basket_dict[basket_name] = data_df
            event_real_basket.append(data_df)

        if len(event_real_basket) <= 1:
            return
        event_real_total = pd.concat(event_real_basket)
        event_real_total = event_real_total.groupby('symbol').sum()
        event_real_total['symbol'] = event_real_total.index
        event_real_total = self.__calcluate_db_weight_table(event_real_total)
        total_basket_name = '%s_Total' % strategy_group_name
        self.r.lpush(redis_title_name, total_basket_name)
        self.__basket_dict[total_basket_name] = event_real_total

    def init_strategy_basket_info(self, strategy_list):
        redis_title_name = 'market_ret:strategy_list'
        self.__reset_redis_list(redis_title_name)
        map(lambda x: self.r.lpush(redis_title_name, x), strategy_list)

        for basket in self.__basket_dict.keys():
            temp_name = 'market_ret:basket_weight:%s' % basket
            data = self.__basket_dict[basket]
            self.r.set(temp_name, data[['weight']].to_dict())
            self.__ret_dict[basket] = self.__minute_demo.copy()

    def __recv_market_info_timer(self):
        if not self.__is_trading_time():
            return

        # total_dict = dict()
        # fr = open('../../cfg/aggregator_pickle_data.pickle', 'rb')
        # for pool_name in ('market_dict', 'instrument_view_dict', 'order_dict', 'order_view_tree_dict', 'trade_list',
        #                   'risk_dict', 'position_dict', 'position_update_time'):
        #     total_dict[pool_name] = pickle.load(fr)
        # fr.close()
        # instrument_view_dict = total_dict['instrument_view_dict']
        # market_msg_dict = total_dict['market_dict']
        market_msg_dict = const.EOD_POOL['market_dict']
        instrument_view_dict = const.EOD_POOL['instrument_view_dict']

        ticker_info_list = []
        for (instrument_key, market_msg) in market_msg_dict.items():
            instrument_view = instrument_view_dict[instrument_key]

            ticker = instrument_view.Ticker
            last_price = instrument_view.NominalPrice
            ticker_info_list.append([str(ticker), last_price])
        data = pd.DataFrame(ticker_info_list, columns=['symbol', 'last_prc'])

        result = pd.merge(self.__instrument_df, data, how='left', on=['symbol'])
        result.index = result.symbol
        result = result.dropna()
        result['ret'] = result['last_prc'] / result['prev_close'] - 1

        minute = date_utils.get_today_str('%Y-%m-%d %H:%M:00')

        self.__update_pnl(result, minute)

    def __update_pnl(self, data, minute):
        redis_key = 'market_ret:ticker_ret'
        self.pipeline_redis.set(redis_key, data[['ret']].to_dict())
        for basket_name in self.__basket_dict.keys():
            nominal_amount = (self.__basket_dict[basket_name]['long_value'] * data['last_prc']).sum()
            basket_ret = (self.__basket_dict[basket_name]['weight'] * data['ret']).sum()

            self.__ret_dict[basket_name].at[minute, 'ret'] = basket_ret
            redis_key = 'market_ret:basket_ret:%s' % basket_name
            self.pipeline_redis.set(redis_key, self.__ret_dict[basket_name].to_dict())
            redis_key = 'market_ret:nominal_amount:%s' % basket_name
            self.pipeline_redis.set(redis_key, nominal_amount)
        self.pipeline_redis.execute()

    def __calcluate_db_weight_table(self, data):
        """
        :param data: dataframe, include long_value(volume) with index 'symbol' and field 'volume'
        :return: dataframe with weight
        """
        data['status'] = data['symbol'].apply(lambda x: 1 if x.isdigit() else 0)
        # filter IC, IF, IH
        data = data[data['status'] == 1]
        data = pd.merge(data, self.__instrument_df, how='left', on=['symbol'])
        data['ticker_money'] = data['long_value'] * data['prev_close']
        basket_all_money = data['ticker_money'].sum()
        data['weight'] = data['ticker_money'] / basket_all_money
        data.index = data['symbol']
        return data

    @staticmethod
    def __query_instrument_df():
        instrument_dict = query_instrument_dict('host', [Instrument_Type_Enums.Index, Instrument_Type_Enums.CommonStock])
        instrument_list = [[str(k), float(v.prev_close)] for (k, v) in instrument_dict.items() if v.prev_close is not None]
        instrument_df = pd.DataFrame(instrument_list, columns=['symbol', 'prev_close'])
        return instrument_df

    @staticmethod
    def __create_minute_demo():
        today = date_utils.get_today_str('%Y-%m-%d')
        idx_am = pd.timedelta_range(start='09:25:00', end='11:30:00',
                                    freq='1min', closed='right')

        idx_pm = pd.timedelta_range(start='13:00:00', end='15:00:00',
                                    freq='1min', closed='right')
        idx = idx_am.append(idx_pm)
        idx = map(lambda x: '%s %s' % (today, str(x).split(' ')[-1]), idx)
        df = pd.DataFrame(-1000.0, index=idx, columns=['ret'])
        return df

    def __reset_redis_list(self, redis_title_name):
        if self.r.llen(redis_title_name) > 0:
            self.r.ltrim(redis_title_name, 1, 0)

    @staticmethod
    def __is_trading_time():
        trading_flag = False
        now_str = date_utils.get_today_str("%H:%M")
        for (str_time, end_time) in trading_time:
            if str_time < now_str <= end_time:
                trading_flag = True
                break
        return trading_flag


if __name__ == '__main__':
    alpha_calculation_job = AlphaCalculationJob()
    alpha_calculation_job.alpha_calculation_thread()
