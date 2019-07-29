# -*- coding: utf-8 -*-
# 修改策略的配置文件及更新数据库
import csv
import os
import pickle
import re
import shutil
import tarfile
import json
import time
import traceback
from eod_aps.model.schema_common import Instrument
from eod_aps.model.schema_jobs import StrategyIntradayParameter, StrategyTickerParameter
from progressbar import *
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.model.obj_to_sql import to_many_sql
from eod_aps.model.schema_strategy import StrategyParameter, StrategyOnline
from eod_aps.job import *
from eod_aps.job.tensorflow_init import Tensorflow_init
from eod_aps.job.algo_file_build_job import StrategyBasketInfo
from eod_aps.job.server_manage_job import start_servers_tradeplat

Instrument_Type_Enums = const.INSTRUMENT_TYPE_ENUMS
Exchange_Type_Enums = const.EXCHANGE_TYPE_ENUMS
operation_enums = const.BASKET_FILE_OPERATION_ENUMS

stock_utils = StockUtils()
CONTAINER_SPLIT_SIZE = 4


def split_by_len(l, s):
    """
        按照长度切割list
    """
    return [l[i:i + s] for i in range(len(l)) if i % s == 0]


def split_by_number(l, s):
    """
        按照个数切割list
    """
    result_list = []
    size = len(l) / s
    for i in range(0, s):
        if i == s - 1:
            result_list.append(l[i * size:])
        else:
            result_list.append(l[i * size: (i + 1) * size])
    return result_list


class TradeplatInit(object):
    def __init__(self, server_name):
        self.__filter_date = date_utils.get_today_str()
        self.__filter_date2 = date_utils.get_today_str('%Y-%m-%d')
        self.__server_name = server_name
        self.__local_tradeplat_folder = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name

        # 持仓股票列表
        self.__ticker_parameter_list = []
        self.__error_message_list = []

    def start_work(self):
        widgets = ['Percentage: ', Percentage(), ' Step: ', SimpleProgress(), ' ', Bar(),
                   ' ', ETA(), ' ', FileTransferSpeed()]
        progress_bar = ProgressBar(widgets=widgets, maxval=8)
        progress_bar.start()

        # 加载配置文件信息
        self.__load_from_db()
        progress_bar.update(2)

        # -----------生成配置文件------------------
        # tf_calculator_init = TFCalculatorInit(self.__server_name)
        strategy_loader_init = StrategyLoaderInit(self.__server_name, self.__ticker_parameter_list)
        strategy_loader_init.start_index()
        progress_bar.update(4)

        mktdt_center_init = MktDTCenterInit(self.__server_name, self.__ticker_parameter_list)
        mktdt_center_init.start_index()
        progress_bar.update(5)

        hf_calculator_init = HFCalculatorInit(self.__server_name, self.__ticker_parameter_list)
        hf_calculator_init.start_index()
        progress_bar.update(6)

        # -----------导出文件并打包,本地服务器上传文件------------------
        self.__export_pickle_file()
        tradeplat_file_name = 'tradeplat_%s.tar.gz' % self.__filter_date
        self.__tar_tradeplat_file(self.__local_tradeplat_folder, tradeplat_file_name)
        progress_bar.update(7)

        self.__upload_tradeplat_file(self.__local_tradeplat_folder, tradeplat_file_name)
        progress_bar.update(8)
        progress_bar.finish()

    def __load_from_db(self):
        instrument_dict = query_instrument_dict('host', [Instrument_Type_Enums.CommonStock, Instrument_Type_Enums.Future])

        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for x in session_jobs.query(StrategyTickerParameter)\
                             .filter(StrategyTickerParameter.server_name == self.__server_name):
            instrument_db = instrument_dict[x.ticker]
            x.exchange_id = instrument_db.exchange_id
            x.prev_close = instrument_db.prev_close
            self.__ticker_parameter_list.append(x)

    def __export_pickle_file(self):
        server_model = server_constant.get_server_model(self.__server_name)
        if server_model.type != 'deposit_server':
            return
        pickle_file_folder = '%s/pickle_file' % self.__local_tradeplat_folder
        if os.path.exists(pickle_file_folder):
            shutil.rmtree(pickle_file_folder, True)
        os.mkdir(pickle_file_folder)

        session = server_model.get_db_session('strategy')
        filter_date_key = '%' + self.__filter_date2 + '%'
        obj_list = [x for x in session.query(StrategyParameter).filter(StrategyParameter.time.like(filter_date_key))]
        strategy_parameter_obj_list = to_many_sql(StrategyParameter, obj_list, 'strategy.strategy_parameter')
        pickle_file_name = 'STRATEGYPARAMETER_' + self.__filter_date2 + '.pickle'
        pickle_file_path = '%s/%s' % (pickle_file_folder, pickle_file_name)
        with open(pickle_file_path, 'wb') as f:
            pickle.dump(strategy_parameter_obj_list, f, True)

        server_host = server_constant.get_server_model('host')
        session_common = server_host.get_db_session('common')
        query = session_common.query(Instrument)
        obj_list = [x for x in query.filter(Instrument.del_flag == 0)]
        daily_instrument_obj_list = to_many_sql(Instrument, obj_list, 'common.instrument')
        pickle_file_name = 'INSTRUMENT_' + self.__filter_date2 + '.pickle'
        pickle_file_path = '%s/%s' % (pickle_file_folder, pickle_file_name)
        with open(pickle_file_path, 'wb') as f:
            pickle.dump(daily_instrument_obj_list, f, True)

    # 生成压缩文件，用于上传
    def __tar_tradeplat_file(self, folder_path, tar_file_name):
        tar = tarfile.open(os.path.join(folder_path, tar_file_name), "w:gz")
        for root, dir_str, files in os.walk(os.path.join(folder_path, 'cfg')):
            root_ = os.path.relpath(root, start=folder_path)
            for file_name in files:
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))

        for root, dir_str, files in os.walk(os.path.join(folder_path, 'tf_config')):
            root_ = os.path.relpath(root, start=folder_path)
            for file_name in files:
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))

        for root, dir_str, files in os.walk(os.path.join(folder_path, 'pickle_file')):
            root_ = os.path.relpath(root, start=folder_path)
            for file_name in files:
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))

        for root, dir_str, files in os.walk(os.path.join(folder_path, 'ic_config')):
            root_ = os.path.relpath(root, start=folder_path)
            for file_name in files:
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))

        for root, dir_str, files in os.walk(os.path.join(folder_path, 'update_sql')):
            root_ = os.path.relpath(root, start=folder_path)
            for file_name in files:
                # 只上传当天的文件
                if self.__filter_date2 not in file_name:
                    continue
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))
        tar.close()

    def __upload_tradeplat_file(self, local_tradeplat_folder, tradeplat_file_name):
        server_model = server_constant.get_server_model(self.__server_name)
        if server_model.type != 'trade_server':
            return

        # 上传压缩包
        server_tradeplat_folder = server_model.server_path_dict['tradeplat_project_folder']
        source_file_path = '%s/%s' % (local_tradeplat_folder, tradeplat_file_name)
        target_file_path = '%s/%s' % (server_tradeplat_folder, tradeplat_file_name)
        server_model.upload_file(source_file_path, target_file_path)

        # 清理并解压缩文件夹
        run_cmd_list = ['cd %s' % server_tradeplat_folder,
                        'rm -rf ./cfg/intraday_multifactor/HighFreqCalculator/*',
                        'rm -rf ./cfg/intraday_leadlag/HighFreqCalculator/*',
                        'rm -rf ./tf_config/*',
                        'tar -zxf %s' % tradeplat_file_name,
                        'rm -rf *.tar.gz']
        server_model.run_cmd_str(';'.join(run_cmd_list))

    def __upload_tradeplat_file2(self, local_tradeplat_folder, tradeplat_file_name):
        server_model = server_constant.get_server_model(self.__server_name)
        # 上传压缩包
        source_file_path = '%s/%s' % (local_tradeplat_folder, tradeplat_file_name)
        if server_model.type == 'trade_server':
            server_tradeplat_folder = server_model.server_path_dict['tradeplat_project_folder']
            target_file_path = '%s/%s' % (server_tradeplat_folder, tradeplat_file_name)
        else:
            target_file_path = '%s/%s/%s' % (server_model.ftp_upload_folder, self.__filter_date, tradeplat_file_name)
        server_model.upload_file(source_file_path, target_file_path)

        if server_model.type == 'trade_server':
            # 清理并解压缩文件夹
            run_cmd_list = ['cd %s' % server_tradeplat_folder,
                            # 'rm -rf ./models/*',
                            'tar -zxf %s' % tradeplat_file_name,
                            'rm -rf *.tar.gz']
            server_model.run_cmd_str(';'.join(run_cmd_list))


class StrategyLoaderInit(object):
    """
        处理StrategyLoader相关配置文件
    """
    def __init__(self, server_name, ticker_parameter_list):
        self.__server_name = server_name
        self.__local_tradeplat_path = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        self.__local_tradeplat_cfg_path = '%s/cfg' % self.__local_tradeplat_path
        self.__stock_ref_path = '%s/tf_config/stock_ref.csv' % self.__local_tradeplat_path
        self.__future_targets_path = '%s/ic_config/targets.csv' % self.__local_tradeplat_path
        self.__parameter_dict_path = BASE_PARAMETER_DICT_FILE_PATH
        self.__ticker_parameter_list = ticker_parameter_list
        self.__container_dict = dict()

    def start_index(self):
        """
            入口函數
        """
        self.__build_cfg_file()
        self.__update_strategy_parameter()

    def __build_cfg_file(self):
        base_content_list = []
        template_file_name = 'config.strategyloader.txt_base'
        with open('%s/%s' % (self.__local_tradeplat_cfg_path, template_file_name)) as fr:
            for line in fr.readlines():
                base_content_list.append(line.replace("\n", ""))

        strategyload_list, strategyload_fut_list = [], []
        strategyload_list.extend(base_content_list)
        strategyload_fut_list.extend(base_content_list)

        strategyload_list.extend(self.__query_cta_strategy())
        strategyload_list.extend(self.__query_stock_intraday_strategy())
        strategyload_fut_list.extend(self.__query_index_intraday_strategy())

        cfg_file_name = 'config.strategyloader.txt'
        with open('%s/%s' % (self.__local_tradeplat_cfg_path, cfg_file_name), 'w') as fr:
            fr.write('\n'.join(strategyload_list))

        cfg_file_name = 'config.strategyloader_fut.txt'
        with open('%s/%s' % (self.__local_tradeplat_cfg_path, cfg_file_name), 'w') as fr:
            fr.write('\n'.join(strategyload_fut_list))

    def __query_cta_strategy(self):
        content_list = []
        server_host = server_constant.get_server_model('host')
        session_strategy = server_host.get_db_session('strategy')
        query = session_strategy.query(StrategyOnline)
        for strategy_online_db in query.filter(StrategyOnline.enable == 1, StrategyOnline.strategy_type == 'CTA',
                                               StrategyOnline.target_server.like('%' + self.__server_name + '%')):
            content_list.append('')
            content_list.append('[Strategy.lib%s.%s]' % (strategy_online_db.assembly_name, strategy_online_db.name))
            content_list.append('WatchList = %s' % strategy_online_db.instance_name)
        return content_list

    def __query_stock_intraday_strategy(self):
        content_list = []
        trade_ticker_list = [x.ticker for x in self.__ticker_parameter_list if x.parameter == 'Intraday' and x.is_trade == 1]
        if trade_ticker_list:
            container_ticker_list = split_by_number(trade_ticker_list, CONTAINER_SPLIT_SIZE)
            for i in range(0, len(container_ticker_list)):
                temp_ticker_list = container_ticker_list[i]
                content_list.append('')
                content_list.append('[Strategy.libstk_intra_day_strategy.StkIntraDay.container%s]' % (i + 1,))
                content_list.append('WatchList = ' + ';'.join(temp_ticker_list))
                content_list.append('ParaList =  ')
                self.__container_dict['StkIntraDayStrategy.container%s' % (i + 1,)] = temp_ticker_list
        return content_list

    def __query_index_intraday_strategy(self):
        content_list = []
        trade_ticker_list = [x.ticker for x in self.__ticker_parameter_list if x.strategy == 'Index_DeepLearning' and x.is_trade == 1]
        if trade_ticker_list:
            content_list.append('')
            content_list.append('[Strategy.libfut_intra_day_strategy.FutIntraDay.container1]')
            content_list.append('WatchList = ' + ';'.join(trade_ticker_list))
            content_list.append('ParaList =  ')
        return content_list

    @staticmethod
    def __format_max_place_ratio(input_value):
        # 四舍五入
        ratio_value = 0.
        check_value = int(float(input_value) + 0.5)
        if check_value < 3:
            ratio_value = 0.5
        elif 3 <= check_value < 5:
            ratio_value = 0.3
        elif 5 <= check_value < 10:
            ratio_value = 0.2
        elif 10 <= check_value < 20:
            ratio_value = 0.1
        elif 20 <= check_value < 40:
            ratio_value = 0.1
        elif check_value >= 40:
            ratio_value = 0.05
        # return ratio_value
        return 0.05

    def __rebuild_stock_ref_file(self):
        max_place_ratio_dict = dict()
        mean_turnover_dict = dict()
        with open(self.__stock_ref_path) as fr:
            reader = csv.reader(fr)
            for row in reader:
                if reader.line_num == 1:
                    ticker_index = row.index('ticker')
                    mean_index = row.index('trade_count_mean')
                    mean_turnover_index = row.index('mean_turnover')
                else:
                    ticker = row[ticker_index]
                    mean_value = row[mean_index]
                    max_place_ratio_value = self.__format_max_place_ratio(mean_value)
                    max_place_ratio_dict[ticker] = max_place_ratio_value
                    mean_turnover_dict[ticker] = row[mean_turnover_index]
        return max_place_ratio_dict, mean_turnover_dict

    def __update_strategy_parameter(self):
        server_model = server_constant.get_server_model(self.__server_name)
        if 'Stock_DeepLearning' not in server_model.strategy_group_list:
            return

        intraday_account_list = const.INTRADAY_ACCOUNT_DICT[self.__server_name] if self.__server_name in const.INTRADAY_ACCOUNT_DICT else []
        max_place_ratio_dict, mean_turnover_dict = self.__rebuild_stock_ref_file()
        parameter_dict = dict()
        error_ticker_list = []
        for dict_line in csv.DictReader(open(self.__parameter_dict_path, 'r')):
            parameter_dict = dict_line

        net_position_percent_dict = dict()
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for item in session_jobs.query(StrategyIntradayParameter) \
                .filter(StrategyIntradayParameter.strategy_name == 'StkIntraDayStrategy',
                        StrategyIntradayParameter.parameter == 'net_position_percent'):
            net_position_percent_dict[item.fund_name] = float(item.parameter_value)

        prev_close_dict = {x.ticker: x.prev_close for x in self.__ticker_parameter_list}

        server_model = server_constant.get_server_model(self.__server_name)
        session_strategy = server_model.get_db_session('strategy')
        for container, ticker_list in self.__container_dict.items():
            tmp_dict = dict(Account=';'.join(intraday_account_list))
            strategy_parameter_obj = StrategyParameter()
            strategy_parameter_obj.time = date_utils.get_now()
            strategy_parameter_obj.name = container
            for ticker in ticker_list:
                tmp_dict['%s_coefficient' % ticker] = parameter_dict['coefficient']
                tmp_dict['%s_param_set' % ticker] = parameter_dict['param_set']
                tmp_dict['%s_max_place_ratio' % ticker] = max_place_ratio_dict[ticker]
                tmp_dict['%s_enabled' % ticker] = 1
                tmp_dict['%s_output_detail_log' % ticker] = 1

                prev_close = float(prev_close_dict[ticker])
                mean_turnover = float(mean_turnover_dict[ticker])
                for fund_name in intraday_account_list:
                    position_percent = net_position_percent_dict[fund_name]
                    max_net_position = int(mean_turnover * position_percent / prev_close) if mean_turnover != 0 else 0
                    tmp_dict['%s_max_net_position.%s' % (ticker, fund_name)] = max_net_position
                    if max_net_position == 0:
                        error_ticker_list.append('%s_%s' % (fund_name, ticker))
            strategy_parameter_obj.value = json.dumps(tmp_dict, indent=4)
            session_strategy.merge(strategy_parameter_obj)
        session_strategy.commit()
        session_strategy.close()

        if len(error_ticker_list) > 0:
            email_utils2.send_email_group_all('[ERROR]max_net_position error tickers:%s' % self.__server_name,
                                              '\n'.join(error_ticker_list))


class MktDTCenterInit(object):
    """
        处理MktDTCenter相关配置文件
    """

    def __init__(self, server_name, ticker_parameter_list):
        self.__server_name = server_name

        self.__local_tradeplat_path = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        self.__cfg_file_path = '%s/cfg' % self.__local_tradeplat_path
        self.__ticker_parameter_list = ticker_parameter_list

        self.__market_rebuild_stock_list = []
        self.__market_stock_list = []
        self.__market_future_list = []

        self.__merger_ticker_list = []

    def start_index(self):
        self.__format_ticker_list()
        self.__build_aggregator_ticker_file()
        self.__build_cg_ticker_file()
        self.__build_cs_ticker_file()
        self.__build_merger_file()

    def __format_ticker_list(self):
        rebuild_stock_list = []
        market_stock_list = []
        market_future_list = []
        for x in self.__ticker_parameter_list:
            if x.is_market == 0:
                continue

            if x.exchange_id == Exchange_Type_Enums.CFF:
                market_future_list.append('%s,%s' % (x.ticker, x.exchange_id))
            elif x.is_rebuild == 1 and x.exchange_id == Exchange_Type_Enums.CS:
                rebuild_stock_list.append('%s,%s' % (x.ticker, x.exchange_id))
            else:
                market_stock_list.append('%s,%s' % (x.ticker, x.exchange_id))

        # 去重
        market_stock_list = [x for x in market_stock_list if x not in rebuild_stock_list]
        self.__market_rebuild_stock_list = list(set(rebuild_stock_list))
        self.__market_stock_list = list(set(market_stock_list))
        self.__market_future_list = list(set(market_future_list))

    def __build_aggregator_ticker_file(self):
        if self.__server_name == 'huabao':
            aggregator_app = 'fh3'
        elif self.__server_name == 'zhongtai':
            aggregator_app = 'rb2'
        else:
            aggregator_app = 'rb3'
        save_file_name = '%s_instruments.csv' % aggregator_app

        file_content_list = ['TICKER,EXCHANGE_ID,TYPE_ID,']
        for content_str in (self.__market_stock_list + self.__market_rebuild_stock_list):
            ticker, exchange_id = content_str.split(',')
            file_content_list.append('%s,%s,4,' % (ticker, exchange_id))

        with open('%s/%s' % (self.__cfg_file_path, save_file_name), 'w') as fr:
            fr.write('\n'.join(file_content_list))

    def __build_cg_ticker_file(self):
        if self.__server_name == 'huabao':
            cg_rb_app = 'rb4'
        elif self.__server_name == 'zhongtai':
            cg_rb_app = 'rb2'
        else:
            cg_rb_app = 'rb7'
        save_file_name = '%s_instruments.csv' % cg_rb_app

        file_content_list = ['TICKER,EXCHANGE_ID,TYPE_ID,']
        for content_str in self.__market_stock_list:
            ticker, exchange_id = content_str.split(',')
            file_content_list.append('%s,%s,4,' % (ticker, exchange_id))
            self.__merger_ticker_list.append([ticker, exchange_id, cg_rb_app])

        with open('%s/%s' % (self.__cfg_file_path, save_file_name), 'w') as fr:
            fr.write('\n'.join(file_content_list))

    def __build_cs_ticker_file(self):
        if self.__server_name == 'huabao':
            cs_rb_app_list = ['rb3', 'rb7', 'rb8']
        elif self.__server_name == 'zhongtai':
            cs_rb_app_list = ['rb2']
        else:
            cs_rb_app_list = ['rb4', 'rb5', 'rb6']
        split_ticker_list = split_by_number(self.__market_rebuild_stock_list, len(cs_rb_app_list))

        for i in range(0, len(cs_rb_app_list)):
            cs_rb_app = cs_rb_app_list[i]
            save_file_name = '%s_instruments.csv' % cs_rb_app
            temp_ticker_list = split_ticker_list[i]

            file_content_list = ['TICKER,EXCHANGE_ID,TYPE_ID,']
            for content_str in temp_ticker_list:
                ticker, exchange_id = content_str.split(',')
                file_content_list.append('%s,%s,4,' % (ticker, exchange_id))
                self.__merger_ticker_list.append([ticker, exchange_id, cs_rb_app])

            with open('%s/%s' % (self.__cfg_file_path, save_file_name), 'w') as fr:
                fr.write('\n'.join(file_content_list))

    def __build_merger_file(self):
        if self.__server_name in ('guosen', 'citics'):
            rb_from = 'rb8'

            mg1_file_name = 'mg1_pre_bind_map_file.csv'
            file_content_list = ['TICKER,EXCHANGE_ID,RECEIVE_FROM,']
            for content_str in self.__market_future_list:
                ticker, exchange_id = content_str.split(',')
                file_content_list.append('%s,%s,%s,' % (ticker, exchange_id, rb_from))
            with open('%s/%s' % (self.__cfg_file_path, mg1_file_name), 'w') as fr:
                fr.write('\n'.join(file_content_list))

        mg2_file_name = 'mg2_pre_bind_map_file.csv'
        file_content_list = ['TICKER,EXCHANGE_ID,RECEIVE_FROM,']
        for (ticker, exchange_id, rb_app) in self.__merger_ticker_list:
            file_content_list.append('%s,%s,%s,' % (ticker, exchange_id, rb_app))

        if len(file_content_list) > 1000:
            email_utils2.send_email_group_all('[ERROR]Server:%s mg2_pre_bind_map_file.csv' % self.__server_name,
                                              'Ticker Size:%s > 1000' % len(file_content_list))

        with open('%s/%s' % (self.__cfg_file_path, mg2_file_name), 'w') as fr:
            fr.write('\n'.join(file_content_list))


class TFCalculatorInit(object):
    """
        处理TFCalculator相关配置文件
    """
    def __init__(self, server_name):
        self.__server_name = server_name
        self.__filter_date = date_utils.get_today_str()

    def log_monitor(self):
        server_model = server_constant.get_server_model(self.__server_name)
        log_cmd_list = ["cd %s" % server_model.server_path_dict['tradeplat_log_folder'],
                        "tail -n 20 error_tfcalculator_%s*.log" % self.__filter_date
                        ]
        log_message_list = server_model.run_cmd_str2(";".join(log_cmd_list))
        if not log_message_list:
            return

        error_message_list = []
        for log_message in log_message_list:
            reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<log_type>.*)\] \[(?P<log_level>.*)\] (?P<log_content>.*)'
            try:
                reg = re.compile(reg_line)
                reg_match = reg.match(log_message)
                line_dict = reg_match.groupdict()
                if 'error' == line_dict['log_level'] and 'is invalid' in line_dict['log_content']:
                    pass
                elif 'error' == line_dict['log_level' ] and 'tensorflow serving return er_code=' in line_dict['log_content']:
                    pass
                else:
                    continue

                log_time = date_utils.string_toDatetime(line_dict['log_time'], "%Y-%m-%d %H:%M:%S.%f")
                if (date_utils.get_now() - log_time).seconds < 60:
                    error_message_list.append(log_message)
            except Exception:
                pass
        return error_message_list


class HFCalculatorInit(object):
    """
        处理HFCalculator相关配置文件
    """

    def __init__(self, server_name, ticker_parameter_list):
        self.__server_name = server_name

        local_tradeplat_path = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        self.__local_tradeplat_cfg_path = '%s/cfg' % local_tradeplat_path
        self.__ticker_parameter_list = ticker_parameter_list

    def start_index(self):
        self.__build_cfg_file()

    def __build_cfg_file(self):
        ticker_list = [x.ticker for x in self.__ticker_parameter_list if x.strategy == 'Stock_MultiFactor']
        ai_ticker_list = [x.ticker for x in self.__ticker_parameter_list if x.strategy == 'Stock_DeepLearning']
        filter_ticker_list = [x for x in ticker_list if x not in ai_ticker_list]

        content_list = []
        template_file_name = 'config.hfcalculator.txt_base'
        with open('%s/%s' % (self.__local_tradeplat_cfg_path, template_file_name)) as fr:
            for line in fr.readlines():
                content_list.append(line.replace("\n", ""))
        content_list.append('')
        content_list.append('[HFCalculator]')
        content_list.append('Instruments = ' + ','.join(filter_ticker_list))
        content_list.append('Dependencys = ')
        content_list.append('StartSampleTickTime = 09:30:00.000')
        content_list.append('InstrumentsGroupSize = 50')
        content_list.append('DependencysGroupSize = 200')
        content_list.append('Test = 0')
        content_list.append('Interval = 10000')
        content_list.append('ExportFile =')
        content_list.append('LeadLagMaxDelaySeconds = 60')
        content_list.append('FactorConfigPath =./cfg')

        cfg_file_name = 'config.hfcalculator.txt'
        with open('%s/%s' % (self.__local_tradeplat_cfg_path, cfg_file_name), 'w') as fr:
            fr.write('\n'.join(content_list))


def tradeplat_init_index_job(server_name, total_email_list):
    try:
        custom_log.log_info_job('Server:%s TradePlat Init Start.' % server_name)
        tradeplat_init = TradeplatInit(server_name)
        tradeplat_init.start_work()

        server_model = server_constant.get_server_model(server_name)
        if 'Stock_MultiFactor' in server_model.strategy_group_list:
            strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Change)
            # strategy_basket_info.split_sigmavwap_ai()
            error_message_list = strategy_basket_info.check_basket_file()
            total_email_list.extend(error_message_list)
        custom_log.log_info_job('Server:%s TradePlat Init Stp1.' % server_name)

        if server_model.type == 'trade_server':
            tensorflow_init = Tensorflow_init(server_name)
            tensorflow_init.op_docker('restart', 'stkintraday_d1')
            tensorflow_init.check_tensorflow_status()
            tensorflow_init.check_server_proxy_status()
            custom_log.log_info_job('Server:%s TradePlat Init Stp2.' % server_name)

            start_servers_tradeplat((server_name,))
            custom_log.log_info_job('Server:%s TradePlat Init Stp3.' % server_name)

            # 重启后需要再发送一次策略启动命令
            time.sleep(10)
            from eod_aps.job.start_server_strategy_job import start_server_strategy_job
            start_server_strategy_job((server_name,))
        custom_log.log_info_job('Server:%s TradePlat Init Stop!' % server_name)
    except Exception:
        error_msg = traceback.format_exc()
        email_utils2.send_email_group_all('[Error]tradeplat_init_index:server:%s,operation:%s!' % \
                                          (server_name, operation_enums), error_msg)


if __name__ == '__main__':
    server_name = 'zhongtai'
    tradeplat_init = TradeplatInit(server_name)
    tradeplat_init.start_work()
