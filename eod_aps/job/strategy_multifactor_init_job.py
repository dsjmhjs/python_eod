# -*- coding: utf-8 -*-
import os
import shutil
import traceback
from eod_aps.job import *
from eod_aps.model.eod_const import const
from eod_aps.job.algo_file_build_job import StrategyBasketInfo
from eod_aps.model.schema_jobs import StrategyTickerParameter

operation_enums = const.BASKET_FILE_OPERATION_ENUMS


def strategy_multifactor_init_job(server_name, total_email_list1, total_email_list2):
    try:
        # 生成调仓文件
        custom_log.log_info_job('Server:%s Strategy:MultiFactor Init Start.' % server_name)
        strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Change)
        email_trade_list, email_detail_list = strategy_basket_info.strategy_basket_file_build()
        total_email_list1.extend(email_trade_list)
        total_email_list2.extend(email_detail_list)
        custom_log.log_info_job('Server:%s Strategy:MultiFactor Init Stp1.' % server_name)

        # 数据库记录，并拷贝MultiFactor配置文件
        strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Add)
        email_trade_list, email_detail_list = strategy_basket_info.strategy_basket_file_build()
        total_email_list1.extend(email_trade_list)
        total_email_list2.extend(email_detail_list)
        custom_log.log_info_job('Server:%s Strategy:MultiFactor Init Stp2.' % server_name)

        strategy_multifactor_build = StrategyMultiFactorBuild(server_name)
        strategy_multifactor_build.start_index()
        custom_log.log_info_job('Server:%s Strategy:MultiFactor Init Stop.' % server_name)
    except Exception:
        error_msg = traceback.format_exc()
        email_utils2.send_email_group_all('[Error]strategy_multifactor_init:server:%s,operation:%s!' % \
                                          (server_name, operation_enums), error_msg)


class StrategyMultiFactorBuild(object):
    def __init__(self, server_name):
        self.__server_name = server_name
        self.__filter_date = date_utils.get_today_str()

        # local_tradeplat_folder = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        # self.__server_multiFactor_folder = '%s/cfg/intraday_multifactor/HighFreqCalculator' % local_tradeplat_folder
        # self.__multiFactor_ticker_list = []
        # self.__deepLearning_ticker_list = []

    def start_index(self):
        self.__save_parameter_db()
        # self.__build_parameter_file()

    def __save_parameter_db(self):
        strategy_type = 'Stock_MultiFactor'
        change_basket_folder = '%s/%s/%s_change' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__filter_date)
        add_basket_folder = '%s/%s/%s_add' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__filter_date)

        ticker_set = set()
        for folder_path in (change_basket_folder, add_basket_folder):
            for file_name in os.listdir(folder_path):
                if not file_name.endswith('.txt'):
                    continue
                basket_file_path = '%s/%s' % (folder_path, file_name)
                with open(basket_file_path) as fr:
                    for line in fr.readlines():
                        ticker = line.split(',')[0]
                        ticker_set.add(ticker)

        multi_factor_ticker_list = list(ticker_set)
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        session_jobs.query(StrategyTickerParameter).filter(StrategyTickerParameter.server_name == self.__server_name,
                                                           StrategyTickerParameter.strategy == strategy_type).delete()

        for ticker in multi_factor_ticker_list:
            x = StrategyTickerParameter()
            x.server_name = self.__server_name
            x.ticker = ticker
            x.strategy = strategy_type
            x.is_trade = 1
            x.is_market = 1
            x.is_rebuild = 1
            session_jobs.merge(x)
        session_jobs.commit()

    # def __build_parameter_file(self):
    #     if os.path.exists(self.__server_multiFactor_folder):
    #         shutil.rmtree(self.__server_multiFactor_folder, True)
    #
    #     server_host = server_constant.get_server_model('host')
    #     session_jobs = server_host.get_db_session('jobs')
    #     for x in session_jobs.query(StrategyTickerParameter)\
    #                          .filter(StrategyTickerParameter.server_name == self.__server_name,
    #                                  StrategyTickerParameter.strategy == 'Stock_DeepLearning'):
    #         self.__deepLearning_ticker_list.append(x.ticker)
    #
    #     for ticker in self.__multiFactor_ticker_list:
    #         if ticker in self.__deepLearning_ticker_list:
    #             continue
    #
    #         ticker_source_folder = '%s/%s' % (VWAP_PARAMETER_PRODUCT_FOLDER, ticker)
    #         if not os.path.exists(ticker_source_folder):
    #             continue
    #         ticker_target_folder = '%s/%s' % (self.__server_multiFactor_folder, ticker)
    #         shutil.copytree(ticker_source_folder, ticker_target_folder)


if __name__ == '__main__':
    # server_name = 'guosen'
    # strategy_multifactor_init_job(server_name, [], [])
    pass
