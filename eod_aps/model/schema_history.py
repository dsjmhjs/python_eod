# -*- coding: utf-8 -*-
import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column, Float, Boolean, CHAR, Integer, DateTime
from sqlalchemy.types import Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class HolidayInfo(BaseModel):
    __tablename__ = 'holiday_list'
    holiday = Column(Date, primary_key=True)
    weight = Column(Float)
    is_holiday = Column(Boolean)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class PhoneTradeInfo(BaseModel):
    __tablename__ = 'phone_trade_info'
    id = Column(Integer, primary_key=True)
    user = Column(CHAR(100))
    server_name = Column(CHAR(100))
    fund = Column(CHAR(100))
    strategy1 = Column(CHAR(200))
    strategy2 = Column(CHAR(200))
    symbol = Column(CHAR(100))

    direction = Column(Integer)
    tradetype = Column(Integer)
    hedgeflag = Column(Integer)
    exprice = Column(Float)
    exqty = Column(Integer)
    iotype = Column(Integer)
    update_time = Column(DateTime)

    def __init__(self):
        self.user = ''
        self.server_name = ''
        self.fund = ''
        self.strategy1 = ''
        self.strategy2 = ''
        self.symbol = ''
        self.direction = 0
        self.tradetype = 0
        self.hedgeflag = 0
        self.exprice = 0.0
        self.exqty = 0
        self.iotype = 0

    def print_str(self):
        print '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
        (self.fund, self.strategy1, self.symbol, self.direction,
         self.tradetype, self.hedgeflag, self.exprice, self.exqty, self.iotype,
         self.strategy2)


class ServerRisk(BaseModel):
    __tablename__ = 'server_risk'
    server_name = Column(CHAR(45), primary_key=True)
    date = Column(Date, primary_key=True)
    strategy_name = Column(CHAR(45), primary_key=True)
    position_pl = Column(Integer)
    trading_pl = Column(Integer)
    fee = Column(Integer)
    stocks_pl = Column(Integer)
    future_pl = Column(Integer)
    total_pl = Column(Integer)
    total_bought_value = Column(Integer)
    total_sold_value = Column(Integer)
    total_stocks_value = Column(Integer)
    total_future_value = Column(Integer)
    delta = Column(Integer)
    gamma = Column(Integer)
    vega = Column(Integer)
    theta = Column(Integer)