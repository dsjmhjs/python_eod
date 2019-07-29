# -*- coding: utf-8 -*-
import copy
import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, Float, String, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class InstrumentHistory(BaseModel):
    __tablename__ = 'instrument_history'
    id = Column(Integer)
    ticker = Column(CHAR(20), primary_key=True)
    exchange_id = Column(Integer, primary_key=True)
    type_id = Column(Integer)
    thours = Column(CHAR(200))
    ticker_wind = Column(CHAR(45), primary_key=True)

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
    # cross_market = Column(Integer)
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
    # 可立即买卖 0:False 1:True
    is_settle_instantly = Column(Integer)
    # 申购可立即赎回
    is_purchase_to_redemption_instantly = Column(Integer)
    # 买入可立即申赎
    is_buy_to_redpur_instantly = Column(Integer)
    # 申购可立即卖出
    is_redpur_to_sell_instantly = Column(Integer)
    option_margin_factor1 = Column(Float)
    option_margin_factor2 = Column(Float)

    def info_str(self):
        return 'ticker:%s, thours:%s' % (self.ticker, self.thours)


    def copy(self):
        return copy.deepcopy(self)