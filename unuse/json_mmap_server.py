# !/usr/bin/python
# -*- coding: utf-8 -*-
# 通过共享内存方式加载数据
import mmap
import ConfigParser
import threading
import time
import datetime
from SimpleXMLRPCServer import SimpleXMLRPCServer
from eod_aps.model.app_info import AppInfo
from eod_aps.model.holiday_info import HolidayInfo
from eod_aps.model.jsonmmap import ObjectMmap
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from eod_aps.model.deposit_server_list import *
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.realaccount import RealAccount
from eod_aps.model.server_model import HostModel, LocalServerModel, TradeServerModel, DepositServerModel
from eod_aps.model.strategy_grouping import StrategyGrouping
from eod_aps.tools.ysquant_manager_tools import get_daily_data, get_basic_info_data



class JsonMmapServer():
    def __init__(self):
        self.eod_config_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_WRITE, tagname='eod_config_mm')
        self.email_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_WRITE, tagname='email_mm')
        self.server_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_WRITE, tagname='server_mm')
        self.server_group_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_WRITE, tagname='server_group_mm')
        self.server_account_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_WRITE, tagname='server_account_mm')
        self.server_pf_account_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_WRITE, tagname='server_pf_account_mm')
        self.stock_basic_data_mm = ObjectMmap(-1, 1024 * 1024 * 5, access=mmap.ACCESS_WRITE, tagname='stock_basic_data_mm')

    def load_eod_data(self):
        threads = []
        t = threading.Thread(target=self.load_eod_data_thread, args=())
        threads.append(t)

        for t in threads:
            t.start()
        return 1

    def reload_eod_data(self):
        eod_config_dict, email_dict, server_dict, server_group_dict, server_account_dict, server_pf_account_dict, \
        stock_basic_data_dict = self.__load_from_db()

        self.eod_config_mm.jsonwrite(eod_config_dict)
        self.email_mm.jsonwrite(email_dict)
        self.server_mm.jsonwrite(server_dict)
        self.server_group_mm.jsonwrite(server_group_dict)
        self.server_account_mm.jsonwrite(server_account_dict)
        self.server_pf_account_mm.jsonwrite(server_pf_account_dict)
        self.stock_basic_data_mm.jsonwrite(stock_basic_data_dict)
        print 'DaTa ReLoad Over!'
        return 1

    def load_eod_data_thread(self):
        while True:
            eod_config_dict, email_dict, server_dict, server_group_dict, server_account_dict, server_pf_account_dict, \
            stock_basic_data_dict = self.__load_from_db()

            self.eod_config_mm.jsonwrite(eod_config_dict)
            self.email_mm.jsonwrite(email_dict)
            self.server_mm.jsonwrite(server_dict)
            self.server_group_mm.jsonwrite(server_group_dict)
            self.server_account_mm.jsonwrite(server_account_dict)
            self.server_pf_account_mm.jsonwrite(server_pf_account_dict)
            self.stock_basic_data_mm.jsonwrite(stock_basic_data_dict)
            print 'DaTa Load Over!'
            time.sleep(60 * 60)

    def __load_from_db(self):
        cp = ConfigParser.SafeConfigParser()
        path = os.path.dirname(__file__)
        cp.read(path + '/../cfg/config.txt')

        db_ip = cp.get('host', 'db_ip')
        db_port = cp.get('host', 'db_port')
        db_user = cp.get('host', 'db_user')
        db_password = cp.get('host', 'db_password')
        db_name = 'jobs'
        Session = sessionmaker()
        db_connect_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8;compress=true' % (
            db_user, db_password, db_ip, db_port, db_name)
        engine = create_engine(db_connect_string, echo=False, poolclass=NullPool)
        Session.configure(bind=engine)
        session_job = Session()

        # 定义数据缓存容器
        eod_config_dict = dict()
        email_dict = dict()
        server_dict = dict()
        server_group_dict = dict()

        # ----------------------加载本地运维机器相关信息------------------------
        # 邮件相关配置
        local_parameters = session_job.query(LocalParameters).first()
        eod_config_dict['smtp_from'] = local_parameters.smtp_from
        eod_config_dict['smtp_server'] = local_parameters.smtp_server
        eod_config_dict['smtp_port'] = local_parameters.smtp_port
        eod_config_dict['smtp_username'] = local_parameters.smtp_username
        eod_config_dict['smtp_password'] = local_parameters.smtp_password
        eod_config_dict['local_server_ips'] = local_parameters.local_server_ips

        # 相关字典数据
        project_dict = session_job.query(ProjectDict)
        for project_item in project_dict:
            if project_item.dict_type == 'Email_dict':
                email_dict[project_item.dict_name] = project_item.dict_value.split(',')
            else:
                eod_config_dict[project_item.dict_name] = project_item.dict_value

        host_name = 'host'
        host_server = HostModel(host_name)
        host_server.load_parameter(local_parameters)
        server_dict['server_host'] = [local_parameters, ]

        # ----------------------加载本地服务器相关信息------------------------
        server_dict['local_server'] = []
        for local_server_item in session_job.query(LocalServerList):
            server_dict['local_server'].append(local_server_item)

        server_model_dict = dict()
        # ----------------------加载托管服务器（直控或者托管）相关信息------------------------
        server_dict['trade_server'] = []
        for trade_server_item in session_job.query(TradeServerList).order_by(TradeServerList.id):
            server_dict['trade_server'].append(trade_server_item)
            trade_server_model = TradeServerModel(trade_server_item.name)
            trade_server_model.load_parameter(trade_server_item)
            server_model_dict[trade_server_model.name] = trade_server_model

        server_dict['deposit_server'] = []
        for deposit_server_item in session_job.query(DepositServerList):
            server_dict['deposit_server'].append(deposit_server_item)
            deposit_server_model = DepositServerModel(deposit_server_item.name)
            deposit_server_model.load_parameter(deposit_server_item)
            server_model_dict[deposit_server_model.name] = deposit_server_model
        session_job.close()

        session_common = host_server.get_db_session('common')
        services_list = []
        for service_name_item in session_common.query(AppInfo.app_name).group_by(AppInfo.app_name):
            services_list.append(service_name_item[0])
        eod_config_dict['service_list'] = services_list
        session_common.close()

        # 加载策略分组数据
        strategy_grouping_dict = dict()
        session_strategy = host_server.get_db_session('strategy')
        for strategy_grouping_db in session_strategy.query(StrategyGrouping):
            if strategy_grouping_db.group_name in strategy_grouping_dict:
                sub_group_dict = strategy_grouping_dict[strategy_grouping_db.group_name]
            else:
                sub_group_dict = dict()

            if strategy_grouping_db.sub_name in sub_group_dict:
                sub_group_dict[strategy_grouping_db.sub_name].append(strategy_grouping_db.strategy_name)
            else:
                sub_group_dict[strategy_grouping_db.sub_name] = [strategy_grouping_db.strategy_name, ]
            strategy_grouping_dict[strategy_grouping_db.group_name] = sub_group_dict
        eod_config_dict['strategy_grouping_dict'] = strategy_grouping_dict
        session_strategy.close()

        # instrument_dict = dict()
        # session_common = host_server.get_db_session('common')
        # for instrument_db in session_strategy.query(Instrument):
        #     instrument_dict[instrument_db.ticker] = instrument_db
        # session_common.close()

        holiday_list = []
        session_history = host_server.get_db_session('history')
        for holiday_info_db in session_history.query(HolidayInfo):
            holiday_list.append(holiday_info_db.holiday.strftime('%Y-%m-%d'))
        session_history.close()
        eod_config_dict['holiday_list'] = holiday_list
        # ----------------------托管服务器分组------------------------
        server_group_dict['trade_server_list'] = []
        server_group_dict['deposit_server_list'] = []
        server_group_dict['night_session_server_list'] = []
        server_group_dict['cta_server_list'] = []
        server_group_dict['stock_server_list'] = []
        server_group_dict['calendar_server_list'] = []
        server_group_dict['tdf_server_list'] = []
        server_group_dict['oma_server_list'] = []
        server_group_dict['commodity_future_server_list'] = []
        server_group_dict['mktcenter_server_list'] = []
        server_group_dict['market_server_list'] = []
        server_group_dict['fix_server_list'] = []
        server_group_dict['ts_server_list'] = []
        server_group_dict['future_market_server'] = ''
        server_group_dict['etf_base_server'] = ''

        for (server_name, server_model) in server_model_dict.items():
            if server_model.type == 'trade_server':
                server_group_dict['trade_server_list'].append(server_name)
                if server_model.is_night_session:
                    server_group_dict['night_session_server_list'].append(server_name)
                if server_model.is_cta_server:
                    server_group_dict['cta_server_list'].append(server_name)
                if server_model.is_calendar_server:
                    server_group_dict['calendar_server_list'].append(server_name)
                if server_model.is_oma_server:
                    server_group_dict['oma_server_list'].append(server_name)
                if server_model.is_trade_future:
                    server_group_dict['commodity_future_server_list'].append(server_name)

                if 'TDF' in server_model.market_source_type:
                    server_group_dict['tdf_server_list'].append(server_name)
                if server_model.market_file_template is not None and server_model.market_file_template != '':
                    server_group_dict['mktcenter_server_list'].append(server_name)
                if server_model.data_source_type != '':
                    server_group_dict['market_server_list'].append(server_name)

                if server_model.data_source_type == 'CTP':
                    server_group_dict['future_market_server'] = server_name
                if server_model.etf_base_folder is not None and server_model.etf_base_folder != '':
                    server_group_dict['etf_base_server'] = server_name

            if server_model.type == 'deposit_server':
                server_group_dict['deposit_server_list'].append(server_name)

            if server_model.type in ('trade_server', 'deposit_server') and server_model.is_trade_stock:
                server_group_dict['stock_server_list'].append(server_name)

        # 缓存账户和策略账户的数据
        server_account_dict = dict()
        server_pf_account_dict = dict()
        for (server_name, server_model) in server_model_dict.items():
            if server_model.type in ('trade_server', 'deposit_server'):
                server_account_dict[server_name] = []
                server_pf_account_dict[server_name] = []
                session_portfolio = server_model.get_db_session('portfolio')
                for real_account_db in session_portfolio.query(RealAccount):
                    server_account_dict[server_name].append(real_account_db)

                for pf_account_db in session_portfolio.query(PfAccount):
                    server_pf_account_dict[server_name].append(pf_account_db)
                session_portfolio.close()

        last_day = datetime.datetime.now() + datetime.timedelta(days=-1)
        while last_day.strftime('%Y-%m-%d') in holiday_list:
            last_day = last_day + datetime.timedelta(days=-1)
        filter_day_str = last_day.strftime('%Y%m%d')
        stock_market_data_dict = get_daily_data(filter_day_str, ["est_pe_fy1", "market_value"]).to_dict('index')
        stock_basic_data_dict = get_basic_info_data().to_dict('index')
        for (symbol, stock_info_dict) in stock_basic_data_dict.items():
            if symbol in stock_market_data_dict:
                stock_info_dict['est_pe_fy1'] = stock_market_data_dict[symbol]['est_pe_fy1']
                stock_info_dict['market_value'] = stock_market_data_dict[symbol]['market_value']
            stock_basic_data_dict[symbol] = stock_info_dict

        return eod_config_dict, email_dict, server_dict, server_group_dict, server_account_dict, server_pf_account_dict,\
    stock_basic_data_dict


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.11.42', 9999))
    json_mmap_server = JsonMmapServer()
    json_mmap_server.load_eod_data()
    s.register_instance(json_mmap_server)
    s.serve_forever()
