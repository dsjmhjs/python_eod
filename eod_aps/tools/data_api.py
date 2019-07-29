import redis
import pandas as pd
import numpy as np
from eod_aps.model.eod_const import const

host, port, db = const.EOD_CONFIG_DICT['redis_address'].split('|')
r = redis.Redis(host=host, port=int(port), db=int(db))


class DataReceiver(object):
    def __init__(self, tag='market_ret'):
        self.tag = tag

    def get_stratey_list(self):
        # strategy_list = ['Event_Real', 'Multi_Factor', 'Report_File', 'Index']
        # 'Index' and 'Report_File' not in strategy_list
        redis_name = '%s:strategy_list' % self.tag
        return r.lrange(redis_name, 0, -1)

    def get_basket_of_strategy(self, strategy):
        redis_name = '%s:basket_list:%s' % (self.tag, strategy)
        return r.lrange(redis_name, 0, -1)

    def get_tickers_ret(self):
        ret_str = r.get('%s:ticker_ret' % self.tag)
        ret_str = ret_str.replace('nan', '0').replace('inf', '0')
        data_dict = eval(ret_str)
        data = pd.DataFrame(data_dict)
        return data

    def get_minute_return_of_basket(self, basket):
        redis_name = '%s:basket_ret:%s' % (self.tag, basket)
        data_dict = eval(r.get(redis_name))
        data = pd.DataFrame(data_dict)
        data.replace(-1000.0, np.nan, inplace=True)
        data.fillna(method='bfill', inplace=True)
        data['unix'] = pd.to_datetime(data.index)
        data['unix'] = data.unix.apply(lambda x: x.value / 1000000)
        data = data[['unix', 'ret']].head(240)
        return data

    def get_nominal_amount_of_basket(self, basket):
        redis_name = '%s:nominal_amount:%s' % (self.tag, basket)
        data = r.get(redis_name)
        return data

    def get_weight_of_basket(self, basket):
        redis_name = '%s:basket_weight:%s' % (self.tag, basket)
        data = r.get(redis_name)
        data = data.replace("nan", "-1")
        data_dict = eval(data)
        data = pd.DataFrame(data_dict)
        return data


def get_redis_data_demo():
    dr = DataReceiver()
    strategy_list = dr.get_stratey_list()
    print strategy_list
    print dr.get_tickers_ret()
    basket_list = dr.get_basket_of_strategy(strategy_list[-1])
    print basket_list
    print dr.get_weight_of_basket(basket_list[0])
    print dr.get_minute_return_of_basket(basket_list[0])


if __name__ == '__main__':
    get_redis_data_demo()
