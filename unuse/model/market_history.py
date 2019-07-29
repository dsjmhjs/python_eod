# -*- coding: utf-8 -*-
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, Float, String, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class MarketHistory(BaseModel):
    __tablename__ = 'market_history'
    date = Column(Date, primary_key=True)
    ticker_exch = Column(CHAR(20), primary_key=True)
    ticker = Column(CHAR(20))
    exchange_id = Column(Integer)
    type_id = Column(Integer)
    pre_close = Column(Float)
    open = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    uplimit = Column(Float)
    downlimit = Column(Float)
    amt = Column(Float)
    oi = Column(Float)
    settle = Column(Float)

    def info_str(self):
        return '%s,%s,%s,%s,%s,%s' % (self.date, self.open, self.uplimit, self.downlimit, self.close, self.volume)
