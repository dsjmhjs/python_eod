# -*- coding: utf-8 -*-
import math
from eod_aps.model.eod_const import const
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.stock_wind_utils import StockWindUtils
from decimal import Decimal

date_utils = DateUtils()


class Position_View(object):
    """
        持仓信息
    """
    date = None
    symbol = None
    hedgeflag = None
    long = 0
    short = 0
    yd_position_long = 0
    yd_position_short = 0

    def __init__(self, pf_position):
        self.date = pf_position.date
        self.symbol = pf_position.symbol
        self.hedgeflag = pf_position.hedgeflag
        self.long = pf_position.long
        self.short = pf_position.short
        self.yd_position_long = pf_position.yd_position_long
        self.yd_position_short = pf_position.yd_position_short


class Trade_View(object):
    """
        订单信息
    """
    time = None
    symbol = None
    qty = 0
    price = 0
    trade_type = None

    def __init__(self, trade2_history):
        self.time = trade2_history.time
        self.symbol = trade2_history.symbol.split(' ')[0]
        self.qty = trade2_history.qty
        self.price = trade2_history.price
        self.trade_type = trade2_history.trade_type


class PerformanceCalculation(object):
    """
        统计类
    """
    pre_date_str = None
    pre_position_list = []
    date_str = None
    position_list = []
    trade_list = []
    close_dict = dict()
    prev_close_dict = dict()
    instrument_db_dict = dict()

    def __init__(self, pre_position_array, position_array, trade_list):
        self.pre_date_str = pre_position_array[0]
        self.pre_position_list = self.__sum_position(pre_position_array[1])
        self.date_str = position_array[0]
        self.position_list = self.__sum_position(position_array[1])
        self.trade_list = self.__sum_trade(trade_list)

    def __sum_position(self, position_list):
        temp_position_dict = dict()
        for position_info in position_list:
            if position_info.symbol in temp_position_dict:
                temp_position_info = temp_position_dict[position_info.symbol]
                temp_position_info.long += position_info.long
                temp_position_info.short += position_info.short
            else:
                temp_position_dict[position_info.symbol] = position_info
        return temp_position_dict.values()

    def __sum_trade(self, trade_list):
        temp_trade_dict = dict()
        for trade_info in trade_list:
            if trade_info.symbol.isdigit():
                if trade_info.symbol in temp_trade_dict:
                    temp_trade_info = temp_trade_dict[trade_info.symbol]
                    temp_trade_info.qty += trade_info.qty
                else:
                    temp_trade_dict[trade_info.symbol] = trade_info
            else:
                temp_trade_dict[trade_info.symbol] = trade_info
        return temp_trade_dict.values()

    def set_prev_close_dict(self, prev_close_dict):
        self.prev_close_dict = prev_close_dict

    def set_close_dict(self, close_dict):
        self.close_dict = close_dict

    def set_instrument_db_dict(self, instrument_db_dict):
        self.instrument_db_dict = instrument_db_dict

    def trade_validate(self):
        pass

    def report_index(self):
        with StockWindUtils() as stock_wind_utils:
            ticker_type_list = [const.INSTRUMENT_TYPE_ENUMS.CommonStock, const.INSTRUMENT_TYPE_ENUMS.Future]
            common_ticker_list = stock_wind_utils.get_ticker_list(ticker_type_list)
            self.close_dict = stock_wind_utils.get_close_dict(self.date_str, common_ticker_list)
            self.prev_close_dict = stock_wind_utils.get_close_dict(self.pre_date_str, common_ticker_list)

        stock_value_total, hedge_value_total = self.__calculation_market_value(self.position_list)
        position_pnl, trade_pnl = self.__calculation_pnl()
        buy_money, sell_money = self.__calculation_trade_cny()
        return buy_money, sell_money, stock_value_total, hedge_value_total, position_pnl, trade_pnl

    def position_makeup_report(self):
        stock_value_total, hedge_value_total = self.__calculation_market_value(self.position_list)
        csi300_position_list = self.__position_index_filter('SHSZ300')
        csi300_value_total, csi300_hedge_total = self.__calculation_market_value(csi300_position_list)
        zz500_position_list = self.__position_index_filter('SH000905')
        zz500_value_total, zz500_hedge_total = self.__calculation_market_value(zz500_position_list)
        return stock_value_total, hedge_value_total, csi300_value_total, zz500_value_total

    def __calculation_pnl(self):
        position_pnl = self.__calculation_position_pnl()
        trade_pnl = self.__calculation_trade_pnl()
        return position_pnl, trade_pnl

    def __calculation_market_value(self, position_list):
        stock_value_total = 0.0
        hedge_value_total = 0.0
        for position_info in position_list:
            instrument_db = self.instrument_db_dict[position_info.symbol]
            close_price = self.close_dict[position_info.symbol]
            if math.isnan(close_price):
                print 'symbol:%s close_price is NaN' % position_info.symbol
                continue

            if instrument_db.type_id == 4:
                stock_value_total += position_info.long * instrument_db.fut_val_pt * close_price
            elif instrument_db.type_id == 1:
                hedge_value_total += (position_info.long - position_info.short) * instrument_db.fut_val_pt * close_price
        return stock_value_total, hedge_value_total

    def __calculation_position_pnl(self):
        position_pnl = 0.0
        for position_info in self.position_list:
            if position_info.long== 0 and position_info.short == 0:
                continue

            instrument_db = self.instrument_db_dict[position_info.symbol]
            close_price = self.close_dict[position_info.symbol]
            prev_close_price = self.prev_close_dict[position_info.symbol]

            if instrument_db.type_id == 4:
                position_pnl += position_info.long * instrument_db.fut_val_pt * (close_price - prev_close_price)
            elif instrument_db.type_id == 1:
                position_pnl += (position_info.long - position_info.short) * instrument_db.fut_val_pt * (close_price - prev_close_price)
        return position_pnl

    def __calculation_trade_pnl(self):
        trade_pnl = 0.0
        for trade_info in self.trade_list:
            close_price = self.close_dict[trade_info.symbol]
            prev_close_price = self.prev_close_dict[trade_info.symbol]
            if trade_info.qty > 0:
                trade_pnl += (close_price - trade_info.price) * trade_info.qty
            else:
                trade_pnl += (prev_close_price - trade_info.price) * trade_info.qty
        return trade_pnl

    # 计算交易导致资金变化
    def __calculation_trade_cny(self):
        buy_money = 0.0
        sell_money = 0.0
        for trade_info in self.trade_list:
            instrument_db = self.instrument_db_dict[trade_info.symbol]
            if trade_info.trade_type == 0:
                if trade_info.qty > 0:
                    buy_money += trade_info.price * trade_info.qty * (1 + 0.00025)
                else:
                    sell_money += trade_info.price * abs(trade_info.qty) * (1 - 0.00125)
            elif trade_info.trade_type == 2:
                if trade_info.qty > 0:
                    buy_money += trade_info.price * abs(trade_info.qty) * instrument_db.fut_val_pt * (instrument_db.longmarginratio + 0.000026)
                else:
                    buy_money += trade_info.price * abs(trade_info.qty) * instrument_db.fut_val_pt * (instrument_db.longmarginratio + 0.000026)
            elif trade_info.trade_type == 3:
                sell_money += (trade_info.price * abs(trade_info.qty)) * instrument_db.fut_val_pt * (instrument_db.longmarginratio - 0.000026)
        return buy_money, sell_money

    def __position_index_filter(self, index_ticker):
        index_db = self.instrument_db_dict[index_ticker]
        indx_members = index_db.indx_members
        index_ticker_list = []
        for index_ticker in indx_members.split(';'):
            index_ticker_list.append(index_ticker)

        index_position_list = []
        for position_info in self.position_list:
            if position_info.symbol in index_ticker_list:
                index_position_list.append(position_info)
        return index_position_list


if __name__ == '__main__':
    pass