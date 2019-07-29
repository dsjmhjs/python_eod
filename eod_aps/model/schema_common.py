# -*- coding: utf-8 -*-
import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
import copy
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, Float, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class User(BaseModel):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    user_name = Column(CHAR(40))
    password = Column(CHAR(40))
    user_type = Column(CHAR(100))


class UserDomain(BaseModel):
    __tablename__ = 'user_domain'
    user_id = Column(Integer, primary_key=True)
    domain_id = Column(Integer, primary_key=True)
    ip = Column(CHAR(1000))
    report = Column(CHAR(1024))
    id = Column(Integer)
    strategy_info = Column(CHAR(2048))
    target_info = Column(CHAR(2048))
    fundaccount_filter = Column(CHAR(45))
    broker_name = Column(CHAR(45))
    broker_access_prefix = Column(CHAR(45))
    enabledmac = Column(CHAR(1024))


class Instrument(BaseModel):
    __tablename__ = 'instrument'
    id = Column(Integer, autoincrement=True)
    ticker = Column(CHAR(20), primary_key=True)
    exchange_id = Column(Integer, primary_key=True)
    type_id = Column(Integer)
    market_sector_id = Column(Integer)
    name = Column(CHAR(3600))
    round_lot_size = Column(Integer)
    tick_size_table = Column(CHAR(3600))
    prev_close = Column(Float)
    prev_settlementprice = Column(Float)
    session = Column(CHAR(1024))
    market_status_id = Column(Integer)
    fut_val_pt = Column(Float)
    cost_per_contract = Column(Float)
    ticker_sptrader = Column(CHAR(45))
    ticker_kgi = Column(CHAR(45))
    ticker_exch = Column(CHAR(45))
    ticker_exch_real = Column(CHAR(45))
    contract_month_year = Column(CHAR(2))
    undl_spot_id = Column(Integer)
    fut_chain = Column(CHAR(8049))
    fut_roll_date = Column(CHAR(108))
    indx_members = Column(CHAR(1000))
    crncy = Column(CHAR(45))
    base_crncy = Column(CHAR(45))
    undl_tickers = Column(CHAR(1000))
    commission_rate_type = Column(CHAR(1000))
    related_equities = Column(CHAR(1000))
    slippage = Column(Float)
    buy_commission = Column(Float)
    sell_commission = Column(Float)
    short_sell_commission = Column(Float)
    ticker_isin = Column(CHAR(64))
    prev_close_update_time = Column(DateTime)
    close = Column(Float)
    close_update_time = Column(DateTime)
    eqy_sh_out_real = Column(Float)
    strike = Column(Float)
    put_call = Column(Integer)
    expire_date = Column(Date)
    ticker_bloomberg = Column(CHAR(45))
    ticker_sedol = Column(CHAR(45))
    indx_mweight = Column(CHAR(1000))
    current_last_trade_dt = Column(Date)
    nav = Column(Float)
    prev_nav = Column(Float)
    pcf = Column(CHAR(5000))
    ticker_primmkt = Column(CHAR(45))
    volume = Column(Integer)
    volume_tdy = Column(Integer)
    cross_market = Column(Integer)
    # redemption_instantly = Column(Integer)
    max_market_order_vol = Column(Integer)
    min_market_order_vol = Column(Integer)
    max_limit_order_vol = Column(Integer)
    min_limit_order_vol = Column(Integer)
    create_date = Column(Date)
    effective_since = Column(Date)
    prev_pcf = Column(CHAR(5000))
    tranche = Column(CHAR(20))
    longmarginratio = Column(Float)
    shortmarginratio = Column(Float)
    longmarginratio_speculation = Column(Float)
    shortmarginratio_speculation = Column(Float)
    longmarginratio_hedge = Column(Float)
    shortmarginratio_hedge = Column(Float)
    longmarginratio_arbitrage = Column(Float)
    shortmarginratio_arbitrage = Column(Float)
    multiplier = Column(Integer)
    inactive_date = Column(Date)
    fair_price = Column(Float)
    # liquid_flag = Column(Integer)
    margin_trading_long_ratio = Column(Float)
    margin_trading_short_ratio = Column(Float)
    conversion_rate = Column(Float)
    uplimit = Column(Float)
    downlimit = Column(Float)
    # redemfee = Column(Float)
    update_date = Column(DateTime)
    is_settle_instantly = Column(Integer)
    is_purchase_to_redemption_instantly = Column(Integer)
    is_buy_to_redpur_instantly = Column(Integer)
    is_redpur_to_sell_instantly = Column(Integer)
    option_margin_factor1 = Column(Float)
    option_margin_factor2 = Column(Float)
    contract_adjustment = Column(Integer)
    track_undl_tickers = Column(CHAR(45))
    del_flag = Column(Integer, default=0)

    def __init__(self):
        self.cross_market = 0
        self.contract_adjustment = 0
        self.del_flag = 0

    def copy(self):
        return copy.deepcopy(self)

    def to_sql(self):
        column_list = []
        value_list = []
        for c in self.__table__.columns:
            column_list.append('`%s`' % c.name.strip())
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
        sql = "REPLACE INTO instrument(%s) VALUES (%s);" % (','.join(column_list), ','.join(value_list))
        return sql

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

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class InstrumentExtend(BaseModel):
    __tablename__ = 'instrument_extend'
    ticker = Column(CHAR(20), primary_key=True)
    exchange_id = Column(Integer, primary_key=True)
    adv20 = Column(Float)


class Instrument_All(BaseModel):
    __tablename__ = 'instrument_all'
    id = Column(Integer)
    ticker = Column(CHAR(20), primary_key=True)
    exchange = Column(CHAR(45), primary_key=True)
    type = Column(CHAR(45))
    market_sector = Column(CHAR(45))
    name = Column(CHAR(3600))
    round_lot_size = Column(Integer)
    tick_size_table = Column(CHAR(3600))
    prev_close = Column(Float)
    prev_settlementprice = Column(Float)
    session = Column(CHAR(1024))
    market_status = Column(CHAR(45))
    fut_val_pt = Column(Float)
    cost_per_contract = Column(Float)
    ticker_sptrader = Column(CHAR(45))
    ticker_kgi = Column(CHAR(45))
    ticker_exch = Column(CHAR(45))
    contract_month_year = Column(CHAR(2))
    undl_spot_id = Column(Integer)
    fut_chain = Column(CHAR(8049))
    fut_roll_date = Column(CHAR(108))
    crncy = Column(CHAR(45))
    base_crncy = Column(CHAR(45))
    undl_tickers = Column(CHAR(1000))
    related_equities = Column(CHAR(1000))
    slippage = Column(Float)
    buy_commission = Column(Float)
    sell_commission = Column(Float)
    short_sell_commission = Column(Float)
    ticker_isin = Column(CHAR(64))
    prev_close_update_time = Column(DateTime)
    close = Column(Float)
    close_update_time = Column(DateTime)
    eqy_sh_out_real = Column(Float)
    strike = Column(Float)
    put_call = Column(Integer)
    expire_date = Column(Date)
    indx_mweight = Column(CHAR(1000))
    nav = Column(Float)
    prev_nav = Column(Float)
    pcf = Column(CHAR(5000))
    ticker_primmkt = Column(CHAR(45))
    volume = Column(Integer)
    cross_market = Column(Integer)
    prev_pcf = Column(CHAR(5000))
    tranche = Column(CHAR(20))
    longmarginratio = Column(Float)
    shortmarginratio = Column(Float)
    longmarginratio_speculation = Column(Float)
    shortmarginratio_speculation = Column(Float)
    longmarginratio_hedge = Column(Float)
    shortmarginratio_hedge = Column(Float)
    longmarginratio_arbitrage = Column(Float)
    shortmarginratio_arbitrage = Column(Float)
    multiplier = Column(Integer)
    inactive_date = Column(Date)
    fair_price = Column(Float)
    # liquid_flag = Column(Integer)
    margin_trading_long_ratio = Column(Float)
    margin_trading_short_ratio = Column(Float)
    conversion_rate = Column(Float)
    uplimit = Column(Float)
    downlimit = Column(Float)
    is_settle_instantly = Column(Integer)
    is_purchase_to_redemption_instantly = Column(Integer)
    is_buy_to_redpur_instantly = Column(Integer)
    is_redpur_to_sell_instantly = Column(Integer)
    option_margin_factor1 = Column(Float)
    option_margin_factor2 = Column(Float)
    contract_adjustment = Column(Integer)
    track_undl_tickers = Column(CHAR(45))

    def __init__(self):
        self.cross_market = 0
        self.contract_adjustment = 0


class InstrumentCommissionRate(BaseModel):
    __tablename__ = 'instrument_commission_rate'
    ticker_type = Column(CHAR(20), primary_key=True)
    open_ratio_by_money = Column(Float)
    open_ratio_by_volume = Column(Float)

    close_ratio_by_money = Column(Float)
    close_ratio_by_volume = Column(Float)

    close_today_ratio_by_money = Column(Float)
    close_today_ratio_by_volume = Column(Float)

    def info_str(self):
        return 'ticker_type:%s, open_ratio_by_money:%s, open_ratio_by_volume:%s' % \
               (self.ticker_type, self.open_ratio_by_money, self.open_ratio_by_volume)

    def copy(self):
        return copy.deepcopy(self)


class AppInfo(BaseModel):
    __tablename__ = 'server_info'
    server_ip = Column(CHAR(20), primary_key=True)
    server_name = Column(CHAR(40))
    app_name = Column(CHAR(40))
    start_file = Column(CHAR(40), primary_key=True)
    level = Column(Integer)


class FutureMainContract(BaseModel):
    __tablename__ = 'future_main_contract'
    ticker_type = Column(CHAR(20), primary_key=True)
    exchange_id = Column(Integer, primary_key=True)
    pre_main_symbol = Column(CHAR(11))
    main_symbol = Column(CHAR(11))
    next_main_symbol = Column(CHAR(11))
    night_flag = Column(CHAR(11))
    warning_days = Column(Integer)
    update_flag = Column(CHAR(11))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
