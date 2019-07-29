# -*- coding: utf-8 -*-
import pickle
from xmlrpclib import ServerProxy
from eod_aps.model.schema_jobs import *
from eod_aps.model.eod_const import const


class ServerConstant(object):
    """
        工程所用常量
    """
    server_dict = dict()
    server_group_dict = dict()

    def __init__(self):
        if len(const.SERVER_DICT) == 0:
            self.__init_by_mmap()
        self.server_dict = const.SERVER_DICT

    def reload_by_mmap(self):
        pickle_data_server = ServerProxy('http://127.0.0.1:9999')
        pickle_data_server.load_eod_data()
        self.__init_by_mmap()

    def __init_by_mmap(self):
        path = os.path.dirname(__file__)
        fr = open(path + '/../../cfg/eod_pickle_data.pickle', 'rb')
        eod_config_dict = pickle.load(fr)
        email_dict = pickle.load(fr)
        server_dict = pickle.load(fr)
        server_group_dict = pickle.load(fr)
        server_account_dict = pickle.load(fr)
        server_pf_account_dict = pickle.load(fr)
        stock_basic_data_dict = pickle.load(fr)
        intraday_account_dict = pickle.load(fr)
        fr.close()

        const.SERVER_DICT.update(server_dict)
        self.server_dict = const.SERVER_DICT

        eod_config_dict['server_account_dict'] = server_account_dict
        eod_config_dict['server_pf_account_dict'] = server_pf_account_dict
        eod_config_dict['stock_basic_data_dict'] = stock_basic_data_dict
        const.EOD_CONFIG_DICT.update(eod_config_dict)
        const.EMAIL_DICT.update(email_dict)
        const.SERVER_GROUP_DICT.update(server_group_dict)

        const.INTRADAY_ACCOUNT_DICT.update(intraday_account_dict)

    def get_server_model(self, server_name):
        return self.server_dict[server_name]

    def get_connect_address(self, server_name):
        return self.server_dict[server_name].connect_address

    def get_all_servers(self):
        server_list = ['host', ]
        server_list.extend(self.get_all_trade_servers())
        return server_list

    def get_all_local_servers(self):
        server_list = ['host', ]
        server_list.extend(self.get_trade_servers())
        return server_list

    # include trade_server|deposit_server
    def get_all_trade_servers(self):
        server_list = []
        server_list.extend(self.get_trade_servers())
        server_list.extend(self.get_deposit_servers())
        return server_list

    def get_trade_servers(self):
        return const.SERVER_GROUP_DICT['trade_server_list']

    def get_deposit_servers(self):
        return const.SERVER_GROUP_DICT['deposit_server_list']

    def get_night_session_servers(self):
        return const.SERVER_GROUP_DICT['night_session_server_list']

    def get_cta_servers(self):
        return const.SERVER_GROUP_DICT['cta_server_list']

    def get_ctp_servers(self):
        """
            包含CTP账号的服务器
        :return:
        """
        ctp_server_list = []
        for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
            server_model = self.get_server_model(server_name)
            for real_account in account_list:
                if server_model.type == 'trade_server' and real_account.accounttype == 'CTP':
                    ctp_server_list.append(server_name)
                    break
        return ctp_server_list

    def get_night_cta_servers(self):
        return const.SERVER_GROUP_DICT['night_cta_server_list']

    def get_download_market_servers(self):
        return const.SERVER_GROUP_DICT['download_market_file_server_list']

    def get_stock_servers(self):
        return const.SERVER_GROUP_DICT['stock_server_list']

    def get_stock_servers2(self):
        stock_servers2 = []
        stock_servers = const.SERVER_GROUP_DICT['stock_server_list']
        for server_name in stock_servers:
            server_model = self.get_server_model(server_name)
            if server_model.type == 'trade_server':
                stock_servers2.append(server_name)
        return stock_servers2

    def get_calendar_servers(self):
        return const.SERVER_GROUP_DICT['calendar_server_list']

    # def get_tdf_servers(self):
    #     return const.SERVER_GROUP_DICT['tdf_server_list']

    def get_oma_servers(self):
        return const.SERVER_GROUP_DICT['oma_server_list'] if 'oma_server_list' in const.SERVER_GROUP_DICT else []

    def get_commodity_future_servers(self):
        return const.SERVER_GROUP_DICT['commodity_future_server_list']

    def get_mktcenter_servers(self):
        return const.SERVER_GROUP_DICT['mktcenter_server_list']

    def get_market_servers(self):
        return const.SERVER_GROUP_DICT['market_server_list']

    def get_future_market_server(self):
        return const.SERVER_GROUP_DICT['future_market_server']

    def get_stock_market_server(self):
        return const.SERVER_GROUP_DICT['stock_market_server']

    def get_etf_base_server(self):
        return const.SERVER_GROUP_DICT['etf_base_server']

    def get_fix_servers(self):
        fix_server_list = []
        for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
            for real_account in account_list:
                if real_account.accounttype == 'GUOXIN':
                    fix_server_list.append(server_name)
                    break
        return fix_server_list

    def get_ts_servers(self):
        ts_server_list = []
        for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
            for real_account in account_list:
                if real_account.accounttype == 'TS':
                    ts_server_list.append(server_name)
                    break
        return ts_server_list

    # CTA,IntraDay
    def get_servers_by_strategy(self, strategy_group_name):
        temp_server_list = []
        for server_name in const.SERVER_GROUP_DICT['trade_server_list'] + const.SERVER_GROUP_DICT[
            'deposit_server_list']:
            server_model = self.get_server_model(server_name)
            if server_model.strategy_group_list is None or strategy_group_name not in server_model.strategy_group_list:
                continue
            temp_server_list.append(server_name)
        return temp_server_list

    def get_ysquant_servers(self):
        return const.SERVER_GROUP_DICT['ysquant_server_list']


server_constant = ServerConstant()
