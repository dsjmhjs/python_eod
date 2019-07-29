# -*- coding: utf-8 -*-
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class Order2(BaseModel):
    __tablename__ = 'order'
    id = Column(Integer, primary_key=True)
    sys_id = Column(CHAR(45))
    account = Column(CHAR(45))
    hedgeflag = Column(CHAR(45))
    symbol = Column(CHAR(45))
    direction = Column(Integer)
    type = Column(Integer)
    trade_type = Column(Integer)
    status = Column(Integer)
    op_status = Column(Integer)
    property = Column(Integer)
    create_time = Column(DateTime)
    transaction_time = Column(DateTime)
    user_id = Column(CHAR(45))
    strategy_id = Column(CHAR(45))
    parent_ord_id = Column(CHAR(45))
    qty = Column(Integer)
    price = Column(Float)
    ex_qty = Column(Integer)
    ex_price = Column(Float)
    algo_type = Column(Integer)


class OrderBroker(BaseModel):
    __tablename__ = 'order_broker'
    id = Column(Integer, primary_key=True)
    sys_id = Column(CHAR(64))
    account = Column(CHAR(32))
    symbol = Column(CHAR(32))
    direction = Column(CHAR(45))
    trade_type = Column(Integer)
    status = Column(Integer)
    submit_status = Column(Integer)
    insert_time = Column(DateTime)
    qty = Column(Integer)
    price = Column(Float)
    ex_qty = Column(Integer)
    ex_price = Column(Float)

    def to_string(self):
        return 'account:%s,sys_id:%s,symbol:%s,direction:%s,trade_type:%s,qty:%s,price:%s,status:%s' % \
               (self.account, self.sys_id, self.symbol, self.direction, self.trade_type, self.qty, self.price,
                self.status)


class OrderHistory(BaseModel):
    __tablename__ = 'order_history'
    id = Column(Integer, primary_key=True)
    sys_id = Column(CHAR(45))
    account = Column(CHAR(45))
    hedgeflag = Column(CHAR(45))
    symbol = Column(CHAR(45))
    direction = Column(Integer)
    type = Column(Integer)
    trade_type = Column(Integer)
    status = Column(Integer)
    op_status = Column(Integer)
    property = Column(Integer)
    create_time = Column(DateTime)
    transaction_time = Column(DateTime)
    user_id = Column(CHAR(45))
    strategy_id = Column(CHAR(45))
    parent_ord_id = Column(CHAR(45))
    qty = Column(Integer)
    price = Column(Float)
    ex_qty = Column(Integer)
    ex_price = Column(Float)
    algo_type = Column(Integer)


class Trade2(BaseModel):
    __tablename__ = 'trade2'
    id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    symbol = Column(CHAR(45))
    qty = Column(Integer)
    price = Column(Float)
    fee = Column(Float, default=0)
    trade_type = Column(CHAR(45))
    strategy_id = Column(CHAR(45))
    account = Column(CHAR(45))
    hedgeflag = Column(CHAR(10))
    order_id = Column(CHAR(10))
    self_cross = Column(Integer)    # 为1表示是否自成交订单，

    def print_info(self):
        print 'time:', self.time, ',symbol:', self.symbol, ',qty:', self.qty, ',price:', self.price, ',qty:', \
            self.qty, ',trade_type:', self.trade_type, ',strategy_id:', self.strategy_id, ',account:', self.account, \
            ',order_id:', self.order_id


class Trade2History(BaseModel):
    __tablename__ = 'trade2_history'
    id = Column(Integer, primary_key=True)
    time = Column(DateTime, primary_key=True)
    symbol = Column(CHAR(45))
    qty = Column(Integer)
    price = Column(Float)
    fee = Column(Float, default=0)
    trade_type = Column(CHAR(45))
    strategy_id = Column(CHAR(45))
    account = Column(CHAR(45))
    hedgeflag = Column(CHAR(10))
    order_id = Column(CHAR(45))
    self_cross = Column(Integer)    # 为1表示是否自成交订单，
    trade_id = Column(CHAR(45))

    def __init__(self):
        self.hedgeflag = 0
        self.order_id = ''
        self.self_cross = 0

    def print_info(self):
        print 'time:', self.time, ',symbol:', self.symbol, ',qty:', self.qty, ',price:', self.price, ',qty:', \
            self.qty, ',trade_type:', self.trade_type, ',strategy_id:', self.strategy_id, ',account:', self.account, \
            ',order_id:', self.order_id, ',trade_id:', self.trade_id


class TradeBroker(BaseModel):
    __tablename__ = 'trade2_broker'
    id = Column(Integer, primary_key=True)
    trade_id = Column(CHAR(255))
    time = Column(DateTime)
    symbol = Column(CHAR(45))
    qty = Column(Integer)
    price = Column(Float)
    trade_type = Column(CHAR(45))
    account = Column(CHAR(45))
    order_id = Column(CHAR(45))
    direction = Column(CHAR(45))
    offsetflag = Column(CHAR(45))
    type = Column(CHAR(45))
    hedgeflag = Column(CHAR(10))

    def print_info(self):
        print 'trade_id:', self.trade_id, ',account:', self.account, ',time:', self.time, ',symbol:', self.symbol, \
            ',qty:', self.qty, ',trade_type:', self.trade_type, ',direction:', self.direction, ',offsetflag:', \
            self.offsetflag
