# -*- coding: utf-8 -*-
import copy


class MinuteBarInfo(object):
    """
        分钟bar
    """
    ticker = ''
    date_time = ''
    open = ''
    high = ''
    low = ''
    close = ''
    volume = ''
    bid1 = ''
    bid_size1 = ''
    ask1 = ''
    ask_size1 = ''
    vwap = ''

    def __init__(self):
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self.bid1 = 0
        self.bid_size1 = 0
        self.ask1 = 0
        self.ask_size1 = 0

    def print_info(self):
        return 'ticker:%s,date_time:%s,open:%s,high:%s,low:%s,close:%s,volume:%s' % \
               (self.ticker, self.date_time, self.open, self.high, self.low, self.close, int(self.volume))

    def info_str(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
               (self.date_time, self.open, self.high, self.low, self.close, int(self.volume), self.bid1,
                int(self.bid_size1), self.ask1, int(self.ask_size1))

    def info_str2(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
               (self.date_time, self.open, self.high, self.low, self.close, int(self.volume), self.bid1,
                int(self.bid_size1), self.ask1, int(self.ask_size1), self.vwap)

    def copy(self):
        return copy.deepcopy(self)


class QuoteBarInfo(object):
    """
        QuoteBar
    """
    ticker = ''
    date_time = ''
    price = 0.0
    volume = 0
    ask1 = 0.0
    ask2 = 0.0
    ask3 = 0.0
    ask4 = 0.0
    ask5 = 0.0
    ask_size1 = 0.0
    ask_size2 = 0.0
    ask_size3 = 0.0
    ask_size4 = 0.0
    ask_size5 = 0.0
    bid1 = 0.0
    bid2 = 0.0
    bid3 = 0.0
    bid4 = 0.0
    bid5 = 0.0
    bid_size1 = 0.0
    bid_size2 = 0.0
    bid_size3 = 0.0
    bid_size4 = 0.0
    bid_size5 = 0.0
    nominal_price = 0.0

    def __init__(self):
        self.price = 0.0
        self.volume = 0
        self.ask1 = 0
        self.ask2 = 0
        self.ask3 = 0
        self.ask4 = 0
        self.ask5 = 0
        self.ask_size1 = 0
        self.ask_size2 = 0
        self.ask_size3 = 0
        self.ask_size4 = 0
        self.ask_size5 = 0
        self.bid1 = 0
        self.bid2 = 0
        self.bid3 = 0
        self.bid4 = 0
        self.bid5 = 0
        self.bid_size1 = 0
        self.bid_size2 = 0
        self.bid_size3 = 0
        self.bid_size4 = 0
        self.bid_size5 = 0
        self.nominal_price = 0

    def print_info(self):
        return 'ticker:%s,date_time:%s,price:%s,volume:%s,ask1:%s,ask_size1:%s,bid1:%s,bid_size1:%s' % \
               (self.ticker, self.date_time, self.price, self.volume, self.ask1, self.ask_size1, self.bid1,
                self.bid_size1)

    def to_quote_str(self):
        return '%s,%s,%s,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s,%s,0,%s' % \
               (self.date_time, self.price, self.volume, self.bid1, self.bid_size1, self.bid2, self.bid_size2
                , self.bid3, self.bid_size3, self.bid4, self.bid_size4, self.bid5, self.bid_size5
                , self.ask1, self.ask_size1, self.ask2, self.ask_size2, self.ask3, self.ask_size3
                , self.ask4, self.ask_size4, self.ask5, self.ask_size5, self.nominal_price)

    def copy(self):
        return copy.deepcopy(self)
