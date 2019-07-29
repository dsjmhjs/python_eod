# -*- coding: utf-8 -*-
import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
import datetime
import copy
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, DateTime, Date, Float
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class RealAccount(BaseModel):
    __tablename__ = 'real_account'
    accountid = Column(Integer, primary_key=True)
    accountname = Column(CHAR(45))
    accounttype = Column(CHAR(45))
    accountconfig = Column(CHAR(3600))
    file_name_1 = Column(CHAR(45))
    file_content_1 = Column(CHAR(3600))
    file_name_2 = Column(CHAR(45))
    file_content_2 = Column(CHAR(3600))
    allowed_etf_list = Column(CHAR(3600))
    allow_targets = Column(CHAR(100))
    allow_margin_trading = Column(Integer)
    fund_name = Column(CHAR(45))
    enable = Column(Integer)
    accountsuffix = Column(CHAR(10))
    # accountcategory = Column(CHAR(2))
    allow_arbitrage_targets = Column(CHAR(100))
    allow_hedge_targets = Column(CHAR(100))


class AccountPosition(BaseModel):
    __tablename__ = 'account_position'
    date = Column(Date, primary_key=True)
    id = Column(Integer, primary_key=True)
    symbol = Column(CHAR(45), primary_key=True)
    hedgeflag = Column(CHAR(10), primary_key=True)
    long = Column(Float, default=0)
    long_cost = Column(Float, default=0)
    long_avail = Column(Float, default=0)
    day_long = Column(Float, default=0)
    day_long_cost = Column(Float, default=0)
    short = Column(Float, default=0)
    short_cost = Column(Float, default=0)
    short_avail = Column(Float, default=0)
    day_short = Column(Float, default=0)
    day_short_cost = Column(Float, default=0)
    fee = Column(Float, default=0)
    close_price = Column(Float)
    note = Column(CHAR(3600))
    delta = Column(Float)
    gamma = Column(Float)
    theta = Column(Float)
    vega = Column(Float)
    rho = Column(Float)
    yd_position_long = Column(Float, default=0)
    yd_position_short = Column(Float, default=0)
    yd_long_remain = Column(Float, default=0)
    yd_short_remain = Column(Float, default=0)
    prev_net = Column(Float, default=0)
    purchase_avail = Column(Float, default=0)
    frozen = Column(Float, default=0)
    update_date = Column(DateTime, default=datetime.datetime)

    td_buy_long = 0
    td_sell_short = 0
    td_pur_red = 0
    td_merge_split = 0

    def __init__(self):
        self.td_buy_long = 0
        self.td_sell_short = 0
        self.td_pur_red = 0
        self.td_merge_split = 0
        self.long = 0
        self.long_cost = 0.0
        self.long_avail = 0
        self.yd_position_long = 0
        self.yd_long_remain = 0
        self.short = 0
        self.short_cost = 0.0
        self.short_avail = 0
        self.yd_position_short = 0
        self.yd_short_remain = 0

    def print_info(self):
        print 'yd_position_long:', self.yd_position_long, ',td_buy_long:', self.td_buy_long, \
',td_sell_short:', self.td_sell_short,',td_pur_red:', self.td_pur_red, ',td_merge_split:', self.td_merge_split


class PfAccount(BaseModel):
    __tablename__ = 'pf_account'
    id = Column(Integer)
    name = Column(CHAR(45), primary_key=True)
    fund_name = Column(CHAR(100), primary_key=True)
    group_name = Column(CHAR(45), primary_key=True)
    effective_date = Column(DateTime)
    description = Column(CHAR(1000))


class PfPosition(BaseModel):
    __tablename__ = 'pf_position'
    date = Column(Date, primary_key=True)
    id = Column(Integer, primary_key=True)
    symbol = Column(CHAR(45), primary_key=True)
    hedgeflag = Column(CHAR(10), primary_key=True)
    long = Column(Float, default=0)
    long_cost = Column(Float, default=0)
    long_avail = Column(Float, default=0)
    day_long = Column(Float, default=0)
    day_long_cost = Column(Float, default=0)
    short = Column(Float, default=0)
    short_cost = Column(Float, default=0)
    short_avail = Column(Float, default=0)
    day_short = Column(Float, default=0)
    day_short_cost = Column(Float, default=0)
    fee = Column(Float, default=0)
    close_price = Column(Float)
    note = Column(CHAR(3600))
    delta = Column(Float)
    gamma = Column(Float)
    theta = Column(Float)
    vega = Column(Float)
    rho = Column(Float)
    yd_position_long = Column(Float, default=0)
    yd_position_short = Column(Float, default=0)
    yd_long_remain = Column(Float, default=0)
    yd_short_remain = Column(Float, default=0)
    prev_net = Column(Float, default=0)
    purchase_avail = Column(Float, default=0)

    def __init__(self):
        self.long = 0
        self.long_avail = 0
        self.long_cost = 0
        self.short = 0
        self.short_avail = 0
        self.short_cost = 0
        self.yd_position_long = 0
        self.yd_position_short = 0
        self.yd_long_remain = 0
        self.yd_short_remain = 0

        self.delta = 1.000000
        self.gamma = 0.000000
        self.theta = 0.000000
        self.vega = 0.000000
        self.rho = 0.000000

    def merge(self, pf_position):
        self.long += pf_position.long
        self.long_cost += pf_position.long_cost
        self.long_avail += pf_position.long_avail
        self.short += pf_position.short
        self.short_cost += pf_position.short_cost
        self.short_avail += pf_position.short_avail
        self.yd_position_long += pf_position.yd_position_long
        self.yd_position_short += pf_position.yd_position_short
        self.yd_long_remain += pf_position.yd_long_remain
        self.yd_short_remain += pf_position.yd_short_remain
        self.prev_net += pf_position.prev_net
        return self

    def copy(self):
        return copy.deepcopy(self)

    def print_info(self):
        print 'date:%s,id:%s,symbol:%s,long:%s' % (self.date, self.id, self.symbol, self.long)


class OmaQuota(BaseModel):
    __tablename__ = 'oma_quota'
    date = Column(Date, primary_key=True)
    investor_id = Column(CHAR(45), primary_key=True)
    symbol = Column(CHAR(45), primary_key=True)
    sell_quota = Column(Integer)
    buy_quota = Column(Integer)


class AccountTradeRestrictions(BaseModel):
    __tablename__ = 'account_trade_restrictions'
    account_id = Column(Integer, primary_key=True)
    ticker = Column(CHAR(45), primary_key=True)
    exchange_id = Column(Integer, primary_key=True)
    hedgeflag = Column(CHAR(45), primary_key=True)
    max_open = Column(Integer)
    today_open = Column(Integer)

    max_cancel = Column(Integer)
    today_cancel = Column(Integer)

    max_large_cancel = Column(Integer)
    today_large_cancel = Column(Integer)

    max_operation = Column(Integer)
    today_operation = Column(Integer)

    option_max_long = Column(Integer)
    option_long = Column(Integer)

    option_max_short = Column(Integer)
    option_short = Column(Integer)

    max_order_flow_speed = Column(Integer)
    max_cancel_ratio_threshold = Column(Integer)
    max_cancel_ratio = Column(Float)
    min_fill_ratio_threshold = Column(Integer)
    min_fill_ratio_alarm = Column(Float)
    min_fill_ratio_block = Column(Float)
    max_buy_quota = Column(Float)

    today_rejected = Column(Integer)
    today_bid_amount = Column(Float)
    today_ask_amount = Column(Float)
    today_bid_canceled_amount = Column(Float)
    today_buy_amount = Column(Float)
    today_sell_amount = Column(Float)

    def __init__(self):
        self.max_open = 0
        self.today_open = 0
        self.max_cancel = 0
        self.today_cancel = 0
        self.max_large_cancel = 0
        self.today_large_cancel = 0
        self.max_operation = 0
        self.today_operation = 0
        self.option_max_long = 0
        self.option_long = 0
        self.option_max_short = 0
        self.option_short = 0

    def copy(self):
        return copy.deepcopy(self)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
