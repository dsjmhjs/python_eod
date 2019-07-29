# -*- coding: utf-8 -*-
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
import copy
from sqlalchemy import Column, CHAR, Float, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.types import Date, DateTime
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class DailyReturnHistory(BaseModel):
    __tablename__ = 'daily_return_history'
    date = Column(Date, primary_key=True)
    ticker = Column(CHAR(45), primary_key=True)
    prev_close = Column(Float)
    close = Column(Float)
    return_rate = Column(Float)


class LocalParameters(BaseModel):
    __tablename__ = 'local_parameters'
    id = Column(Integer, primary_key=True)
    server_name = Column(CHAR(40))
    ip = Column(CHAR(40))
    db_ip = Column(CHAR(40))
    db_user = Column(CHAR(40))
    db_password = Column(CHAR(40))
    db_port = Column(CHAR(40))
    smtp_server = Column(CHAR(40))
    smtp_port = Column(CHAR(40))
    smtp_username = Column(CHAR(40))
    smtp_password = Column(CHAR(40))
    smtp_from = Column(CHAR(40))
    local_server_ips = Column(CHAR(200))


class LocalServerList(BaseModel):
    __tablename__ = 'local_server_list'
    id = Column(Integer, primary_key=True)
    name = Column(CHAR(40))
    ip = Column(CHAR(40))
    port = Column(CHAR(40))
    user = Column(CHAR(40))
    pwd = Column(CHAR(40))
    db_ip = Column(CHAR(40))
    db_user = Column(CHAR(40))
    db_password = Column(CHAR(40))
    db_port = Column(CHAR(40))
    connect_address = Column(CHAR(40))
    anaconda_home_path = Column(CHAR(150))
    group_list = Column(CHAR(150))
    enable = Column(Boolean)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class TradeServerList(BaseModel):
    __tablename__ = 'trade_server_list'
    id = Column(Integer, primary_key=True)
    name = Column(CHAR(40))
    ip = Column(CHAR(40))
    port = Column(CHAR(40))
    ip_reserve = Column(CHAR(40))
    port_reserve = Column(Integer)
    user = Column(CHAR(40))
    pwd = Column(CHAR(40))
    db_ip = Column(CHAR(40))
    db_port = Column(Integer)
    db_ip_reserve = Column(CHAR(40))
    db_port_reserve = Column(Integer)
    db_user = Column(CHAR(40))
    db_password = Column(CHAR(40))
    connect_address = Column(CHAR(40))
    check_port_list = Column(CHAR(40))
    etf_base_folder = Column(CHAR(40))
    data_source_type = Column(CHAR(40))
    market_source_type = Column(CHAR(40))
    market_file_template = Column(CHAR(200))
    market_file_localpath = Column(CHAR(40))
    strategy_group_list = Column(CHAR(200))
    is_trade_stock = Column(Boolean)
    is_trade_future = Column(Boolean)
    is_night_session = Column(Boolean)
    is_cta_server = Column(Boolean)
    is_calendar_server = Column(Boolean)
    is_oma_server = Column(Boolean)
    download_market_file_flag = Column(Boolean)
    server_parameter = Column(CHAR(2000))
    path_parameter = Column(CHAR(2000))
    enable = Column(Boolean)


class DepositServerList(BaseModel):
    __tablename__ = 'deposit_server_list'
    id = Column(Integer, primary_key=True)
    name = Column(CHAR(40))
    ip = Column(CHAR(40))
    db_ip = Column(CHAR(40))
    db_user = Column(CHAR(40))
    db_password = Column(CHAR(40))
    db_port = Column(CHAR(40))
    connect_address = Column(CHAR(40))
    ftp_type = Column(CHAR(40))
    ftp_user = Column(CHAR(40))
    ftp_password = Column(CHAR(100))
    ftp_wsdl_address = Column(CHAR(100))
    ftp_upload_folder = Column(CHAR(100))
    ftp_download_folder = Column(CHAR(100))
    is_trade_stock = Column(Boolean)
    is_cta_server = Column(Boolean)
    is_ftp_monitor = Column(Boolean)
    strategy_group_list = Column(CHAR(200))
    enable = Column(Boolean)


class ProjectDict(BaseModel):
    __tablename__ = 'project_dict'
    id = Column(Integer, primary_key=True)
    dict_type = Column(CHAR(40))
    dict_name = Column(CHAR(40))
    dict_value = Column(CHAR(255))
    dict_desc = Column(CHAR(255))


class HardWareInfo(BaseModel):
    __tablename__ = 'hardware_info'
    id = Column(Integer, primary_key=True)
    location = Column(CHAR(100))
    type = Column(CHAR(100))
    ip = Column(CHAR(100))
    user_name = Column(CHAR(100))
    operating_system = Column(CHAR(100))
    marc = Column(CHAR(100))
    asset_number = Column(CHAR(100))
    describe = Column(CHAR(255))
    enable = Column(Boolean)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class FundInfo(BaseModel):
    __tablename__ = 'fund_info'
    id = Column(Integer, primary_key=True)
    name = Column(CHAR(50))
    name_chinese = Column(CHAR(100))
    name_alias = Column(CHAR(100))
    create_time = Column(Date)
    expiry_time = Column(Date)
    target_servers = Column(CHAR(100))
    describe = Column(CHAR(255))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class FundAccountInfo(BaseModel):
    __tablename__ = 'Fund_account_info'
    id = Column(Integer, primary_key=True)
    account_name = Column(CHAR(50))
    product_name = Column(CHAR(50))
    type = Column(CHAR(100))
    server = Column(CHAR(100))
    kechuang_plate = Column(CHAR(100))
    inclusion_strategy = Column(CHAR(100))
    broker = Column(CHAR(100))
    service_charge = Column(Float(100))
    hedging_limit = Column(Float(100))
    investor = Column(CHAR(100))
    matters_attention = Column(CHAR(255))
    margin_trading = Column(CHAR(100))
    copper_options = Column(CHAR(100))
    describe = Column(CHAR(255))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class FundChangeInfo(BaseModel):
    __tablename__ = 'fund_change_info'
    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer)
    date = Column(Date)
    # Purchase/Redemption
    type = Column(CHAR(20))
    change_money = Column(Float)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class RiskManagement(BaseModel):
    __tablename__ = 'risk_management'
    id = Column(Integer, primary_key=True)
    name = Column(CHAR(50))
    parameters = Column(CHAR(1000))
    monitor_index = Column(CHAR(100))
    frequency = Column(Integer)
    fund_risk_list = Column(CHAR(1000))
    describe = Column(CHAR(1000))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StrategyIntradayParameter(BaseModel):
    __tablename__ = 'strategy_intraday_parameter'
    strategy_name = Column(CHAR(100), primary_key=True)
    fund_name = Column(CHAR(45), primary_key=True)
    parameter = Column(CHAR(45))
    parameter_value = Column(Float)


class StrategyAccountInfo(BaseModel):
    __tablename__ = 'strategyaccount_info'
    id = Column(Integer, primary_key=True)
    server_name = Column(CHAR(45))
    fund = Column(CHAR(100))
    group_name = Column(CHAR(45))
    strategy_name = Column(CHAR(45))
    all_number = Column(CHAR(100))
    exclude_number = Column(CHAR(100))
    exclude_ticker = Column(CHAR(500))
    last_number = Column(CHAR(45))
    target_future = Column(CHAR(45))
    update_date = Column(Date)


class StrategyAccountChangeHistory(BaseModel):
    __tablename__ = 'strategyaccount_change_history'
    date = Column(CHAR(45), primary_key=True)
    server_name = Column(CHAR(100), primary_key=True)
    fund_name = Column(CHAR(45), primary_key=True)
    operation_type = Column(CHAR(45), primary_key=True)
    change_money = Column(Integer)
    cutdown_ratio = 0.0


class StrategyAccountTarget(BaseModel):
    __tablename__ = 'strategyaccount_target'
    date = Column(CHAR(45), primary_key=True)
    server_name = Column(CHAR(100), primary_key=True)
    fund_name = Column(CHAR(45), primary_key=True)
    symbol = Column(CHAR(45), primary_key=True)
    volume = Column(Integer)

    def copy(self):
        return copy.deepcopy(self)


class UserList(BaseModel):
    __tablename__ = 'user_list'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(CHAR(255))
    password = Column(CHAR(255))
    strategy_group_list = Column(CHAR(2000))
    describe = Column(CHAR(2000))
    role_id = Column(Integer)

    def __str__(self):
        return self.user_id


class EodMessage(BaseModel):
    __tablename__ = 'eod_message'
    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(CHAR(255), nullable=False)
    create_time = Column(CHAR)
    read_flag = Column(Boolean, default=0)
    title = Column(CHAR(255))
    user_id = Column(Integer, ForeignKey('user_list.id'))

    user = relationship("UserList", backref="eodmessage")

    def __str__(self):
        return self.title


class OptionTrade(BaseModel):
    __tablename__ = 'option_trade'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    server_name = Column(CHAR(45))
    fund_name = Column(CHAR(45))
    ticker = Column(CHAR(45))
    price = Column(Float)
    volume = Column(Integer)
    direction = Column(CHAR(45))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StatementInfo(BaseModel):
    __tablename__ = 'statement_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    fund = Column(CHAR(50))
    account = Column(CHAR(50))
    date = Column(Date)
    type = Column(CHAR(50))
    confirm_date = Column(Date)
    net_asset_value = Column(Float)
    request_money = Column(Float)
    confirm_money = Column(Float)
    confirm_units = Column(Float)
    fee = Column(Float)
    performance_pay = Column(Float)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class AssetValueInfo(BaseModel):
    __tablename__ = 'asset_value_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_str = Column(Date)
    product_name = Column(CHAR(50))
    net_asset_value = Column(CHAR(100))
    unit_net = Column(CHAR(10))
    sum_value = Column(CHAR(50))
    real_capital = Column(CHAR(50))
    nav_change = Column(Float)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class DockerModelTicker(BaseModel):
    __tablename__ = 'docker_model_ticker'
    ticker = Column(CHAR(100), primary_key=True)
    date = Column(Date)


class SpecialTickers(BaseModel):
    __tablename__ = 'special_tickers'
    date = Column(Date, primary_key=True)
    ticker = Column(CHAR(40), primary_key=True)
    describe = Column(CHAR(40))


class StrategyTickerParameter(BaseModel):
    __tablename__ = 'strategy_ticker_parameter'
    server_name = Column(CHAR(45), primary_key=True)
    ticker = Column(CHAR(45), primary_key=True)
    strategy = Column(CHAR(100), primary_key=True)
    parameter = Column(CHAR(45))
    is_trade = Column(Integer, default=0)
    is_market = Column(Integer, default=0)
    is_rebuild = Column(Integer, default=0)


class DailyVwapAnalyse(BaseModel):
    __tablename__ = 'daily_vwap_analyse'
    date = Column(Date, primary_key=True)
    server = Column(CHAR(100), primary_key=True)
    account = Column(CHAR(100), primary_key=True)
    strategy = Column(CHAR(100), primary_key=True)
    avg_buy_slippage = Column(Float)
    avg_sell_slippage = Column(Float)
    buy_amt = Column(Float)
    sell_amt = Column(Float)
    avg_slippage = Column(Float)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
