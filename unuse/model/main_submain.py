# -*- coding: utf-8 -*-
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, Float, String, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class MainSubmainInfo(BaseModel):
    __tablename__ = 'main_submain'
    exchange_id = Column(CHAR(20))
    undl_tickers = Column(CHAR(20))
    start_date = Column(Date)
    end_date = Column(Date)
    main_contract = Column(CHAR(20), primary_key=True)
    sub_main_contract = Column(CHAR(20))
