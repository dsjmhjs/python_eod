# -*- coding: utf-8 -*-
import os
import shutil
import traceback
from eod_aps.job import *
from itertools import islice

from eod_aps.model.custom_exception import FileMissException
from eod_aps.model.schema_jobs import SpecialTickers, StrategyTickerParameter


def index_deeplearning_init_job(server_name):
    try:
        custom_log.log_info_job('Server:%s Strategy:IndexDeepLearning Init Start.' % server_name)
        index_deeplearning_build = IndexDeepLearningBuild(server_name)
        index_deeplearning_build.start_index()
        custom_log.log_info_job('Server:%s Strategy:IndexDeepLearning Stop.' % server_name)
    except Exception:
        error_msg = traceback.format_exc()
        email_utils2.send_email_group_all('[Error]index_deeplearning_init_job:server:%s' % server_name, error_msg)


class IndexDeepLearningBuild(object):
    def __init__(self, server_name):
        self.__filter_date = date_utils.get_today_str()
        self.__filter_date2 = date_utils.get_today_str('%Y-%m-%d')
        self.__server_name = server_name
        self.__error_message_list = []

        local_tradeplat_folder = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        self.__server_config_folder = '%s/ic_config' % local_tradeplat_folder

        # 配置文件
        self.__config_file_folder = '%s/%s' % (BASE_INTRADAY_INDEX_CONFIG_FOLDER, self.__filter_date)
        self.__targets_path = '%s/targets.csv' % self.__config_file_folder
        self.__targets_future_ref_path = '%s/targets_future_ref.csv' % self.__config_file_folder
        self.__targets_stock_ref_path = '%s/targets_stock_ref.csv' % self.__config_file_folder

    def start_index(self):
        self.__parameter_file_check()
        self.__copy_config_files()
        self.__save_parameter_db()

    def __parameter_file_check(self):
        check_file_list = [self.__targets_path, self.__targets_future_ref_path, self.__targets_stock_ref_path]
        for check_file_path in check_file_list:
            if not os.path.exists(check_file_path):
                error_content = 'IndexDeepLearning File:%s Missing!(Error)' % check_file_path
                email_utils2.send_email_group_all('IndexDeepLearning File Missing!', error_content, 'html')
                raise FileMissException

    def __copy_config_files(self):
        source_folder = self.__config_file_folder
        target_folder = self.__server_config_folder
        if os.path.exists(target_folder):
            shutil.rmtree(target_folder)
        shutil.copytree(source_folder, target_folder)

    def __save_parameter_db(self):
        strategy_type = 'Index_DeepLearning'
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        session_jobs.query(StrategyTickerParameter).filter(StrategyTickerParameter.server_name == self.__server_name,
                                                           StrategyTickerParameter.strategy == strategy_type).delete()

        filter_ticker_list = []
        for x in session_jobs.query(SpecialTickers).filter(SpecialTickers.date == self.__filter_date2):
            if 'ST' in x.describe or 'Suspend' in x.describe:
                filter_ticker_list.append(x.ticker)

        ticker_parameter_dict = dict()
        with open(self.__targets_path) as input_file:
            for line_info in islice(input_file, 1, None):
                line_items = line_info.replace('\n', '').replace('"', '').split(',')
                ticker = line_items[0].zfill(6)
                x = StrategyTickerParameter()
                x.server_name = self.__server_name
                x.ticker = ticker
                x.strategy = strategy_type
                x.is_trade = 1
                ticker_parameter_dict[ticker] = x

        with open(self.__targets_future_ref_path) as input_file:
            for line_info in islice(input_file, 1, None):
                line_items = line_info.replace('\n', '').replace('"', '').split(',')
                ticker = line_items[0]
                x = StrategyTickerParameter() if ticker not in ticker_parameter_dict else ticker_parameter_dict[ticker]
                x.server_name = self.__server_name
                x.ticker = ticker
                x.strategy = strategy_type
                x.is_market = 1
                x.is_rebuild = 0
                ticker_parameter_dict[ticker] = x

        with open(self.__targets_stock_ref_path) as input_file:
            for line_info in islice(input_file, 1, None):
                line_items = line_info.replace('\n', '').replace('"', '').split(',')
                ticker = line_items[0].zfill(6)
                x = StrategyTickerParameter()
                x.server_name = self.__server_name
                x.ticker = ticker
                x.strategy = strategy_type
                x.is_market = 1
                x.is_rebuild = 0
                ticker_parameter_dict[ticker] = x

        for (ticker, strategy_ticker_parameter) in ticker_parameter_dict.items():
            if ticker in filter_ticker_list:
                continue
            session_jobs.merge(strategy_ticker_parameter)
        session_jobs.commit()


if __name__ == '__main__':
    # server_name = 'citics_test'
    index_deeplearning_init_job('huabao')
    pass

