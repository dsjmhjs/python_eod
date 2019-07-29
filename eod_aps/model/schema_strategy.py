# -*- coding: utf-8 -*-
import copy
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, DateTime, Text, Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class StrategyGrouping(BaseModel):
    __tablename__ = 'strategy_grouping'
    id = Column(Integer, primary_key=True)
    group_name = Column(CHAR(100))
    sub_name = Column(CHAR(100))
    strategy_name = Column(CHAR(100))


class StrategyParameter(BaseModel):
    __tablename__ = 'strategy_parameter'
    time = Column(DateTime, primary_key=True)
    name = Column(CHAR(45), primary_key=True)
    value = Column(Text)

    @classmethod
    def get_column_list(cls):
        column_list = []
        for c in cls.__table__.columns:
            column_list.append('`%s`' % c.name.strip())
        column_str = '(%s)' % ','.join(column_list)
        return column_str

    @property
    def get_value_list(self):
        value_list = []
        for c in self.__table__.columns:
            if (getattr(self, c.name, None)) is not None:
                if c.type != 'INTEGER' and c.type != 'FLOAT':
                    value_list.append("\'%s\'" % str(getattr(self, c.name)))
                else:
                    value_list.append(getattr(self, c.name))
            else:
                if c.type == 'INTEGER' or c.type == 'FLOAT':
                    value_list.append(0)
                else:
                    value_list.append('Null')
        value_str = '(%s)' % (','.join(value_list))
        return value_str


class StrategyState(BaseModel):
    __tablename__ = 'strategy_state'
    time = Column(DateTime(), primary_key=True)
    name = Column(CHAR(45), primary_key=True)
    value = Column(Text)
    # value = Column(TEXT(1000))

    def copy(self):
        return copy.deepcopy(self)


class StrategyChangeHistory(BaseModel):
    __tablename__ = 'strategy_change_history'
    id = Column(Integer, primary_key=True)
    enable = Column(Integer)
    name = Column(CHAR(45))
    change_type = Column(CHAR(45))  # online/downline
    parameter_server = Column(CHAR(2000))
    update_time = Column(DateTime)
    change_server_name = Column(CHAR(45))


class StrategyOnline(BaseModel):
    __tablename__ = 'strategy_online'
    id = Column(Integer, primary_key=True)
    enable = Column(Integer)
    strategy_type = Column(CHAR(45))
    target_server = Column(CHAR(45))
    assembly_name = Column(CHAR(45))
    strategy_name = Column(CHAR(45))
    instance_name = Column(CHAR(45))
    name = Column(CHAR(45))
    parameter = Column(CHAR(1000))
    parameter_server = Column(CHAR(1000))
    # minbar或者quote
    data_type = Column(CHAR(45))
    # 设置加载多少天的数据
    date_num = Column(Integer)
    # local_server = Column(CHAR(100))

    def copy(self):
        return copy.deepcopy(self)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StrategyServerParameter(BaseModel):
    __tablename__ = 'strategy_server_parameter'
    date = Column(Date, primary_key=True)
    server_name = Column(CHAR(100), primary_key=True)
    strategy_name = Column(CHAR(100), primary_key=True)
    account_name = Column(CHAR(100), primary_key=True)
    max_long_position = Column(Integer)
    max_short_position = Column(Integer)
    qty_per_trade = Column(Integer)


class StrategyServerParameterChange(BaseModel):
    __tablename__ = 'strategy_server_parameter_change'
    date = Column(Date, primary_key=True)
    server_name = Column(CHAR(100), primary_key=True)
    strategy_name = Column(CHAR(100), primary_key=True)
    account_name = Column(CHAR(100), primary_key=True)
    max_long_position = Column(CHAR(100))
    max_short_position = Column(CHAR(100))
    qty_per_trade = Column(CHAR(100))
