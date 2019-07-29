# !/usr/bin/python
# -*- coding: utf-8 -*-
# 通过共享内存方式加载数据
import os
import pickle
import ConfigParser
import datetime
import traceback
from collections import OrderedDict
from SimpleXMLRPCServer import SimpleXMLRPCServer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from eod_aps.model.schema_jobs import LocalParameters, LocalServerList, TradeServerList, DepositServerList, \
    ProjectDict, StrategyAccountInfo
from eod_aps.model.schema_portfolio import RealAccount, PfAccount
from eod_aps.model.schema_common import AppInfo
from eod_aps.model.schema_history import HolidayInfo
from eod_aps.model.schema_strategy import StrategyGrouping
from eod_aps.model.server_model import HostModel, LocalServerModel, TradeServerModel, DepositServerModel
from eod_aps.tools.ysquant_manager_tools import get_daily_data, get_basic_info_data

BASE_PATH = os.path.dirname(os.path.abspath(__file__))


class PickleDataServer(object):
    """
        数据预加载工具
    """

    def __init__(self):
        self.__config_file_path = BASE_PATH + '/../cfg/config.txt'
        self.__pickle_file_path = BASE_PATH + '/../cfg/eod_pickle_data.pickle'

    def load_eod_data(self):
        # 定义数据缓存容器
        self.__eod_config_dict = dict()
        self.__email_dict = dict()
        self.__server_dict = OrderedDict()
        self.__server_group_dict = dict()
        self.__server_account_dict = dict()
        self.__server_pf_account_dict = dict()
        self.__stock_basic_data_dict = dict()
        self.__intraday_account_dict = dict()

        self.__load_from_db()

        fw = open(self.__pickle_file_path, 'wb')
        pickle.dump(self.__eod_config_dict, fw, -1)
        pickle.dump(self.__email_dict, fw)
        pickle.dump(self.__server_dict, fw)
        pickle.dump(self.__server_group_dict, fw)
        pickle.dump(self.__server_account_dict, fw)
        pickle.dump(self.__server_pf_account_dict, fw)
        pickle.dump(self.__stock_basic_data_dict, fw)
        pickle.dump(self.__intraday_account_dict, fw)
        fw.close()
        print 'DaTa Load Over!'
        return True

    def __load_from_db(self):
        cp = ConfigParser.SafeConfigParser()
        cp.read(self.__config_file_path)

        db_ip = cp.get('host', 'db_ip')
        db_port = cp.get('host', 'db_port')
        db_user = cp.get('host', 'db_user')
        db_password = cp.get('host', 'db_password')
        db_name = 'jobs'
        Session = sessionmaker()
        db_connect_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8;compress=true' % \
                            (db_user, db_password, db_ip, db_port, db_name)
        engine = create_engine(db_connect_string, echo=False, poolclass=NullPool)
        Session.configure(bind=engine)
        session_job = Session()

        # ----------------------加载本地运维机器相关信息------------------------
        # 邮件相关配置
        local_parameters = session_job.query(LocalParameters).first()
        self.__eod_config_dict['host_ip'] = local_parameters.ip
        self.__eod_config_dict['smtp_from'] = local_parameters.smtp_from
        self.__eod_config_dict['smtp_server'] = local_parameters.smtp_server
        self.__eod_config_dict['smtp_port'] = local_parameters.smtp_port
        self.__eod_config_dict['smtp_username'] = local_parameters.smtp_username
        self.__eod_config_dict['smtp_password'] = local_parameters.smtp_password

        # 相关字典数据
        project_dict = session_job.query(ProjectDict)
        for project_item in project_dict:
            if project_item.dict_type == 'Email_dict':
                self.__email_dict[project_item.dict_name] = project_item.dict_value.split(',')
            else:
                self.__eod_config_dict[project_item.dict_name] = project_item.dict_value

        host_name = 'host'
        host_server = HostModel(host_name)
        host_server.load_parameter(local_parameters, project_dict)
        self.__server_dict[host_name] = host_server
        # ----------------------加载本地服务器相关信息------------------------
        for local_server_item in session_job.query(LocalServerList).filter(LocalServerList.enable == 1):
            local_server_model = LocalServerModel(local_server_item.name)
            local_server_model.load_parameter(local_server_item)
            self.__server_dict[local_server_item.name] = local_server_model

        # ----------------------加载托管服务器（直控或者托管）相关信息------------------------
        for trade_server_item in session_job.query(TradeServerList).filter(TradeServerList.enable == 1) \
                .order_by(TradeServerList.id):
            trade_server_model = TradeServerModel(trade_server_item.name)
            trade_server_model.load_parameter(trade_server_item)
            self.__server_dict[trade_server_model.name] = trade_server_model

        for deposit_server_item in session_job.query(DepositServerList).filter(DepositServerList.enable == 1):
            deposit_server_model = DepositServerModel(deposit_server_item.name)
            deposit_server_model.load_parameter(deposit_server_item)
            self.__server_dict[deposit_server_model.name] = deposit_server_model

        intraday_account_dict = dict()
        for item in session_job.query(StrategyAccountInfo):
            intraday_account_dict.setdefault(item.server_name, []).append(item.fund)
        self.__intraday_account_dict = {x: list(set(y)) for x, y in intraday_account_dict.items()}
        session_job.close()

        session_common = host_server.get_db_session('common')
        services_list = []
        for service_name_item in session_common.query(AppInfo.app_name).group_by(AppInfo.app_name):
            services_list.append(service_name_item[0])
        self.__eod_config_dict['service_list'] = services_list
        session_common.close()

        # 加载策略分组数据
        strategy_grouping_dict = dict()
        session_strategy = host_server.get_db_session('strategy')
        for strategy_grouping_db in session_strategy.query(StrategyGrouping):
            strategy_grouping_dict.setdefault(strategy_grouping_db.group_name, {})
            sub_group_dict = strategy_grouping_dict[strategy_grouping_db.group_name]

            sub_group_dict.setdefault(strategy_grouping_db.sub_name, []).append(strategy_grouping_db.strategy_name)
            strategy_grouping_dict[strategy_grouping_db.group_name] = sub_group_dict
        self.__eod_config_dict['strategy_grouping_dict'] = strategy_grouping_dict
        session_strategy.close()

        holiday_list = []
        session_history = host_server.get_db_session('history')
        for holiday_info_db in session_history.query(HolidayInfo):
            holiday_list.append(holiday_info_db.holiday.strftime('%Y-%m-%d'))
        session_history.close()
        self.__eod_config_dict['holiday_list'] = holiday_list
        # ----------------------托管服务器分组------------------------
        for (server_name, server_model) in self.__server_dict.items():
            if server_model.type == 'trade_server':
                self.__server_group_dict.setdefault('trade_server_list', []).append(server_name)
                if server_model.is_night_session:
                    self.__server_group_dict.setdefault('night_session_server_list', []).append(server_name)
                if server_model.is_cta_server:
                    self.__server_group_dict.setdefault('cta_server_list', []).append(server_name)
                if server_model.is_cta_server and server_model.is_night_session:
                    self.__server_group_dict.setdefault('night_cta_server_list', []).append(server_name)
                if server_model.is_calendar_server:
                    self.__server_group_dict.setdefault('calendar_server_list', []).append(server_name)
                if server_model.is_oma_server:
                    self.__server_group_dict.setdefault('oma_server_list', []).append(server_name)
                if server_model.is_trade_future:
                    self.__server_group_dict.setdefault('commodity_future_server_list', []).append(server_name)
                if server_model.download_market_file_flag:
                    self.__server_group_dict.setdefault('download_market_file_server_list', []).append(server_name)
                if 'TDF' in server_model.market_source_type:
                    self.__server_group_dict.setdefault('tdf_server_list', []).append(server_name)
                if server_model.market_file_template is not None and server_model.market_file_template != '':
                    self.__server_group_dict.setdefault('mktcenter_server_list', []).append(server_name)
                if server_model.data_source_type != '':
                    self.__server_group_dict.setdefault('market_server_list', []).append(server_name)

                if server_model.data_source_type == 'CTP':
                    self.__server_group_dict['future_market_server'] = server_name
                if server_model.data_source_type == 'HUABAO':
                    self.__server_group_dict['stock_market_server'] = server_name
                if server_model.etf_base_folder is not None and server_model.etf_base_folder != '':
                    self.__server_group_dict['etf_base_server'] = server_name

            if server_model.type == 'deposit_server':
                self.__server_group_dict.setdefault('deposit_server_list', []).append(server_name)
            if server_model.type in ('trade_server', 'deposit_server') and server_model.is_trade_stock:
                self.__server_group_dict.setdefault('stock_server_list', []).append(server_name)
            if server_model.type == 'local_server':
                if server_model.group_list and 'ysquant' in server_model.group_list:
                    self.__server_group_dict.setdefault('ysquant_server_list', []).append(server_name)

        # 缓存账户和策略账户的数据, 账户交易限制数据
        for (server_name, server_model) in self.__server_dict.items():
            if server_model.type in ('trade_server', 'deposit_server'):
                session_portfolio = server_model.get_db_session('portfolio')
                for real_account_db in session_portfolio.query(RealAccount):
                    self.__server_account_dict.setdefault(server_name, []).append(real_account_db)

                for pf_account_db in session_portfolio.query(PfAccount):
                    self.__server_pf_account_dict.setdefault(server_name, []).append(pf_account_db)
                session_portfolio.close()

        last_day = datetime.datetime.now() + datetime.timedelta(days=-1)
        while last_day.strftime('%Y-%m-%d') in holiday_list:
            last_day = last_day + datetime.timedelta(days=-1)
        filter_day_str = last_day.strftime('%Y%m%d')
        try:
            stock_market_data_dict = get_daily_data(filter_day_str, ["est_pe_fy1", "market_value"]).to_dict('index')
            temp_basic_data_dict = get_basic_info_data().to_dict('index')
            for (symbol, stock_info_dict) in temp_basic_data_dict.items():
                if symbol in stock_market_data_dict:
                    stock_info_dict['est_pe_fy1'] = stock_market_data_dict[symbol]['est_pe_fy1']
                    stock_info_dict['market_value'] = stock_market_data_dict[symbol]['market_value']
                self.__stock_basic_data_dict[symbol] = stock_info_dict
        except Exception:
            error_msg = traceback.format_exc()
            print error_msg


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('0.0.0.0', 9999))
    pickle_data_server = PickleDataServer()
    pickle_data_server.load_eod_data()
    s.register_instance(pickle_data_server)
    s.serve_forever()
