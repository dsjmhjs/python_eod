# -*- coding: utf-8 -*-
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, Float, String, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class InvestmentCalendar(BaseModel):
    __tablename__ = 'investment_calendar'
    date = Column(Date, primary_key=True)
    event = Column(CHAR(200), primary_key=True)
    plate = Column(CHAR(200))
    stocks = Column(CHAR(200))
