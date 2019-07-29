# -*- coding: utf-8 -*-
import os
import shutil
import traceback
from eod_aps.job import *
from itertools import islice

from eod_aps.model.custom_exception import FileMissException
from eod_aps.model.schema_jobs import SpecialTickers, StrategyTickerParameter


def stock_deeplearning_init_job(server_name):
    try:
        custom_log.log_info_job('Server:%s Strategy:StockDeepLearning Init Start.' % server_name)
        stock_deeplearning_build = StockDeepLearningBuild(server_name)
        stock_deeplearning_build.start_index()
        custom_log.log_info_job('Server:%s Strategy:StockDeepLearning Stop.' % server_name)
    except Exception:
        error_msg = traceback.format_exc()
        email_utils2.send_email_group_all('[Error]stock_deeplearning_init_job:server:%s' % server_name, error_msg)


class StockDeepLearningBuild(object):
    def __init__(self, server_name):
        self.__filter_date = date_utils.get_today_str()
        self.__filter_date2 = date_utils.get_today_str('%Y-%m-%d')
        self.__server_name = server_name
        self.__error_message_list = []

        # 可做日内交易股票列表
        self.__intraday_ticker_list = []
        self.__future_ticker_list = []

        local_tradeplat_folder = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        self.__server_config_folder = '%s/tf_config' % local_tradeplat_folder

        # 日内策略配置文件
        self.__model_file_folder = '%s/%s/MLP-1T-regression' % (BASE_STKINTRADAY_MODEL_FOLDER, self.__filter_date)
        self.__config_file_folder = '%s/%s' % (BASE_STKINTRADAY_CONFIG_FOLDER, self.__filter_date)
        self.__stock_ref_file = '%s/stock_ref.csv' % self.__config_file_folder
        self.__future_ref_file = '%s/future_ref.csv' % self.__config_file_folder

    def start_index(self):
        self.__parameter_file_check()
        self.__load_config_files()
        self.__copy_config_files()
        self.__save_parameter_db()

    def __parameter_file_check(self):
        check_file_list = [self.__stock_ref_file, self.__future_ref_file]
        for check_file_path in check_file_list:
            if not os.path.exists(check_file_path):
                error_content = 'StockDeepLearning File:%s Missing!(Error)' % check_file_path
                email_utils2.send_email_group_all('StockDeepLearning File Missing!', error_content)
                raise FileMissException

    def __load_config_files(self):
        with open(self.__stock_ref_file) as input_file:
            for line_info in islice(input_file, 1, None):
                line_items = line_info.replace('\n', '').replace('"', '').split(',')
                ticker = line_items[0].zfill(6)
                self.__intraday_ticker_list.append(ticker)

        with open(self.__future_ref_file) as input_file:
            for line_info in islice(input_file, 1, None):
                line_items = line_info.replace('\n', '').replace('"', '').split(',')
                ticker = line_items[0]
                self.__future_ticker_list.append(ticker)

    def __copy_config_files(self):
        source_folder = self.__config_file_folder
        target_folder = self.__server_config_folder
        if os.path.exists(target_folder):
            shutil.rmtree(target_folder)
        shutil.copytree(source_folder, target_folder)

    def __save_parameter_db(self):
        strategy_type = 'Stock_DeepLearning'
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        session_jobs.query(StrategyTickerParameter).filter(StrategyTickerParameter.server_name == self.__server_name,
                                                           StrategyTickerParameter.strategy == strategy_type).delete()

        filter_ticker_list = []
        for x in session_jobs.query(SpecialTickers).filter(SpecialTickers.date == self.__filter_date2):
            if 'ST' in x.describe or 'Suspend' in x.describe:
                filter_ticker_list.append(x.ticker)

        for ticker in self.__intraday_ticker_list:
            if ticker in filter_ticker_list:
                continue

            x = StrategyTickerParameter()
            x.server_name = self.__server_name
            x.ticker = ticker
            x.strategy = strategy_type
            x.parameter = 'Intraday'
            x.is_trade = 1
            x.is_market = 1
            x.is_rebuild = 1
            session_jobs.merge(x)

        for ticker in self.__future_ticker_list:
            x = StrategyTickerParameter()
            x.server_name = self.__server_name
            x.ticker = ticker
            x.strategy = strategy_type
            x.is_trade = 0
            x.is_market = 1
            x.is_rebuild = 0
            session_jobs.merge(x)
        session_jobs.commit()


if __name__ == '__main__':
    # server_name = 'citics'
    # stock_deeplearning_init_job(server_name)
    pass
