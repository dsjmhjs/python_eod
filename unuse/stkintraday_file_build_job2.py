# -*- coding: utf-8 -*-
# 修改策略的配置文件及更新数据库

import os
import json
import shutil
import tarfile
import traceback
from itertools import islice
from eod_aps.model.schema_portfolio import AccountPosition
from eod_aps.model.schema_jobs import Strategy_Intraday_Parameter
from eod_aps.model.schema_strategy import StrategyParameter
from eod_aps.tools.factordata_file_rebuild import build_factordata_cfg_file
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.job import *

stock_utils = StockUtils()
SPLIT_LIST_SIZE = 5


class Stkintraday_Mode(object):
    def __init__(self, server_name):
        self.email_content_list = []
        self.server_name = server_name
        self.date_str = date_utils.get_today_str('%Y-%m-%d')
        self.date_str2 = date_utils.get_today_str('%Y%m%d')
        self.multifactor_parameter_list = []
        self.leadLag_parameter_list = []
        self.basket_parameter_list = []

        self.tradeplat_file_folder = TRADEPLAT_FILE_FOLDER_TEMPLATE % server_name
        self.local_backup_folder = STOCK_INTRADAY_BACKUP_FOLDER
        if not os.path.exists(self.tradeplat_file_folder):
            os.mkdir(self.tradeplat_file_folder)

        self.parameter_dict_file_path = self.tradeplat_file_folder + '/parameter_dict.csv'
        self.multifactor_folder = self.tradeplat_file_folder + '/MultiFactorWeightData'

        self.cfg_folder = self.tradeplat_file_folder + '/cfg'
        self.highfreq_folder = self.cfg_folder + '/intraday_multifactor/HighFreqCalculator'
        self.leadlag_folder = self.cfg_folder + '/intraday_leadlag/HighFreqCalculator'
        for temp_folder in (self.multifactor_folder, self.highfreq_folder, self.leadlag_folder):
            if os.path.exists(temp_folder):
                shutil.rmtree(temp_folder, True)
            os.mkdir(temp_folder)

        type_list = [Instrument_Type_Enum.CommonStock, ]
        self.instrument_dict = query_instrument_dict('host', type_list)

        self.parameter_title_list = []
        self.parameter_dict = dict()

        with open(self.parameter_dict_file_path, 'rb') as fr:
            file_items = fr.readlines()
            for i in range(0, len(file_items)):
                if i == 0:
                    self.parameter_title_list = file_items[i].replace('\n', '').split(',')
                else:
                    line_items = file_items[i].replace('\n', '').split(',')
                    self.parameter_dict[line_items[0]] = file_items[i].replace('\n', '')

    # 生成压缩文件，用于上传
    def __tar_tradeplat_file(self, file_path, tar_file_name):
        tar = tarfile.open(os.path.join(file_path, tar_file_name), "w:gz")
        for root, dir_str, files in os.walk(os.path.join(file_path, 'cfg')):
            root_ = os.path.relpath(root, start=file_path)
            for file_name in files:
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))

        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
        for root, dir_str, files in os.walk(os.path.join(file_path, 'update_sql')):
            root_ = os.path.relpath(root, start=file_path)
            for file_name in files:
                # 只上传当天的文件
                if filter_date_str not in file_name:
                    continue
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))
        tar.close()


    def start_work(self):
        if self.server_name == 'huabao':
            self.__read_parameter_file(MULTIFACTOR_PARAMETER_FILE_PATH_TEMPLATE % self.date_str2, 'StkIntraDayStrategy')
            self.__read_parameter_file(LEADLAG_PARAMETER_FILE_PATH, 'StkIntraDayLeadLagStrategy')

        self.__read_basketfile()
        self.__download_parameter_file()
        self.__divide_tickers()
        self.__modify_cfg_local()

        self.__upload_tradeplat_file()

        if self.server_name == 'huabao':
            self.__modify_database()
            self.__save_strategy_intraday_parameter()
            self.__backup_files()
        self.__send_email()

    # 生成压缩文件，用于上传
    def __zip_tradeplat_file(self, file_path, tar_file_name):
        tar = tarfile.open(os.path.join(file_path, tar_file_name), "w:gz")
        for root, dir_str, files in os.walk(os.path.join(file_path, 'cfg')):
            root_ = os.path.relpath(root, start=file_path)
            for file_name in files:
                full_path = os.path.join(root, file_name)
                tar.add(full_path, arcname=os.path.join(root_, file_name))
        tar.close()

    def __upload_tradeplat_file(self):
        update_sql_file_name = 'update_%s.sql' % date_utils.get_today_str('%Y-%m-%d')
        update_sql_file_path = os.path.join(self.tradeplat_file_folder, 'update_sql', update_sql_file_name)
        if not os.path.exists(update_sql_file_path):
            with open(update_sql_file_path, 'w') as fr:
                fr.write('')

        tradeplat_file_name = 'tradeplat_%s.tar.gz' % date_utils.get_today_str('%Y-%m-%d')
        self.__tar_tradeplat_file(self.tradeplat_file_folder, tradeplat_file_name)

        server_model = server_constant.get_server_model(self.server_name)
        if server_model.type != 'trader_server':
            return

        tradeplat_project_folder = server_model.server_path_dict['tradeplat_project_folder']

        # 上传压缩包
        source_file_path = self.tradeplat_file_folder + '/' + tradeplat_file_name
        target_file_path = tradeplat_project_folder + '/' + tradeplat_file_name
        server_model.upload_file(source_file_path, target_file_path)

        # 清理并解压缩文件夹
        run_cmd_list = ['cd %s' % tradeplat_project_folder,
                        'rm -rf ./cfg/intraday_multifactor/HighFreqCalculator/*',
                        'rm -rf ./cfg/intraday_leadlag/HighFreqCalculator/*',
                        'tar -zxf %s' % tradeplat_file_name,
                        'rm -rf *.tar.gz']
        server_model.run_cmd_str(';'.join(run_cmd_list))

    def __modify_cfg_local(self):
        if self.server_name == 'huabao':
            self.__modify_strategyloader()
            self.__modify_fh_sh()
            self.__modify_fh_sz()
            self.__modify_rb_sz_huabao()
            self.__modify_mg_sz()
            self.__modify_mktdtcenter_config()
        elif self.server_name == 'guoxin':
            self.__modify_rb_sz_guoxin()
        self.__modify_config_hfcalculator()

    # file_type:StkIntraDayStrategy/StkIntraDayLeadLagStrategy
    def __read_parameter_file(self, file_path, file_type):
        parameter_list = []
        if not os.path.exists(file_path):
            self.email_content_list.append('<font color=red>[Error]Parameter File:%s is missing!</font><br>' % file_path)
            return

        strategy_version = None
        with open(file_path, 'rb') as fr:
            for line in islice(fr, 1, None):
                line_items = line.replace('\n', '').split(',')
                strategy_intraday_parameter = Strategy_Intraday_Parameter()
                strategy_intraday_parameter.date = self.date_str
                strategy_intraday_parameter.strategy_name = file_type
                strategy_intraday_parameter.ticker = line_items[1]
                strategy_intraday_parameter.parameter = line_items[2]
                strategy_version = line_items[3]
                parameter_list.append(strategy_intraday_parameter)
        self.email_content_list.append('[Parameter File]:%s, [Strategy_Version]:%s<br>' %
                                       (file_path, strategy_version))

        if file_type == 'StkIntraDayStrategy':
            self.multifactor_parameter_list.extend(parameter_list)
        elif file_type == 'StkIntraDayLeadLagStrategy':
            self.leadLag_parameter_list.extend(parameter_list)

    # 读取持仓文件
    def __read_basketfile(self):
        parameter_list = []
        base_file_path = '%s/%s/%s_change' % (STOCK_SELECTION_FOLDER, self.server_name, self.date_str2)
        file_type = 'StkIntraDayStrategy'
        for basket_file_name in os.listdir(base_file_path):
            if not basket_file_name.endswith('.txt'):
                continue
            basket_file_path = '%s/%s' % (base_file_path, basket_file_name)
            with open(basket_file_path) as fr:
                for line in fr.readlines():
                    ticker = line.split(',')[0]
                    strategy_intraday_parameter = Strategy_Intraday_Parameter()
                    strategy_intraday_parameter.date = self.date_str
                    strategy_intraday_parameter.strategy_name = file_type
                    strategy_intraday_parameter.ticker = ticker
                    parameter_list.append(strategy_intraday_parameter)
        self.basket_parameter_list.extend(parameter_list)

    def __download_multifactor_files(self, suspend_stock_list, parameter_list):
        file_name_template = '%s_%s_mid_quote_fwd_ret_%s.csv'
        interval_list = ['30s', '60s', '120s', '300s']

        temp_parameter_list = []

        now_date = date_utils.get_now()
        trading_day_list = date_utils.get_interval_trading_day_list(now_date, -5)
        for algo_parameter in parameter_list:
            if algo_parameter.ticker in suspend_stock_list:
                self.email_content_list.append('<font color=red>[Error]ticker:%s is suspend!</font><br>' % algo_parameter.ticker)
                continue

            # 判断是否存在对应的文件
            exists_day_str = None
            for trading_day_str in trading_day_list:
                test_file_name = file_name_template % (algo_parameter.ticker, trading_day_str, interval_list[0])
                if os.path.exists('%s/%s/%s' % (MULTIFACTOR_PARAMETER_FILE_FOLDER, algo_parameter.ticker, test_file_name)):
                    exists_day_str = trading_day_str
                    break
            if exists_day_str is None:
                self.email_content_list.append('<font color=red>[Error]ticker:%s, multifactor parameter file is missing!</font><br>' % algo_parameter.ticker)
                continue

            temp_parameter_list.append(algo_parameter)
            download_server_path = '%s/%s' % (MULTIFACTOR_PARAMETER_FILE_FOLDER, algo_parameter.ticker)
            download_local_path = '%s/%s' % (self.multifactor_folder, algo_parameter.ticker)
            if not os.path.exists(download_local_path):
                os.mkdir(download_local_path)

            # task_logger.info('Download ticker:%s multifactor parameter file' % algo_parameter.ticker)
            for interval_str in interval_list:
                file_name = file_name_template % (algo_parameter.ticker, exists_day_str, interval_str)
                source_file_path = '%s/%s' % (download_server_path, file_name)
                target_file_path = '%s/%s' % (download_local_path, file_name)
                shutil.copy(source_file_path, target_file_path)
        return temp_parameter_list

    def __download_leadlag_files(self, suspend_stock_list, parameter_list):
        temp_parameter_list = []
        for algo_parameter in parameter_list:
            if algo_parameter.ticker in suspend_stock_list:
                self.email_content_list.append('<font color=red>[Error]ticker:%s is suspend!</font><br>' % algo_parameter.ticker)
                continue

            date_index = date_utils.get_now()
            download_server_path = '%s/%s/%s' % (LEADLAG_PARAMETER_FILE_FOLDER, date_index.strftime('%Y%m%d'), algo_parameter.ticker)
            find_index = 1
            while not os.path.exists(download_server_path) and find_index <= 5:
                date_index = date_utils.get_last_day(-find_index, start_date=date_index)
                download_server_path = '%s/%s/%s' % (
                    LEADLAG_PARAMETER_FILE_FOLDER, date_index.strftime('%Y%m%d'), algo_parameter.ticker)
                find_index += 1

            if not os.path.exists(download_server_path):
                self.email_content_list.append(
                    '<font color=red>[Error]ticker:%s, leadLag parameter file is missing!</font><br>' % algo_parameter.ticker)
                continue
            temp_parameter_list.append(algo_parameter)

            download_local_path = '%s/%s' % (self.leadlag_folder, algo_parameter.ticker)
            try:
                shutil.copytree(download_server_path, download_local_path)
                self.email_content_list.append('LeadLag ticker:%s,parameter:%s,file date:%s' % \
                                               (algo_parameter.ticker, algo_parameter.parameter,
                                                date_index.strftime('%Y%m%d')))
            except Exception:
                error_msg = traceback.format_exc()
                custom_log.log_error_job(error_msg)
                self.email_content_list.append('Download Error! ticker:%s' % algo_parameter.ticker)
                continue
            finally:
                pass
        return temp_parameter_list

    # 下载参数文件
    def __download_parameter_file(self):
        suspend_stock_list = stock_utils.get_suspend_stock()
        self.multifactor_parameter_list = self.__download_multifactor_files(suspend_stock_list, self.multifactor_parameter_list)
        self.basket_parameter_list = self.__download_multifactor_files(suspend_stock_list, self.basket_parameter_list)
        self.leadLag_parameter_list = self.__download_leadlag_files(suspend_stock_list, self.leadLag_parameter_list)

        table_list = []
        for algo_parameter in self.multifactor_parameter_list:
            ticker_date_str = build_factordata_cfg_file(self.multifactor_folder, self.highfreq_folder, algo_parameter.ticker)
            table_list.append(['StkIntraDayStrategy',algo_parameter.ticker, algo_parameter.parameter, ticker_date_str])

        for algo_parameter in self.basket_parameter_list:
            ticker_date_str = build_factordata_cfg_file(self.multifactor_folder, self.highfreq_folder, algo_parameter.ticker)
            table_list.append(['MultiFactor', algo_parameter.ticker, algo_parameter.parameter, ticker_date_str])

        table_title = 'Type,Ticker,Parameter Index,File Date'
        self.email_content_list.extend(email_utils7.list_to_html(table_title, table_list))

    # 对ticker进行分类
    def __divide_tickers(self):
        self.ticker_set = set()
        self.sh_ticker_set = set()
        self.sz_ticker_set = set()
        self.dependencys_set = set()

        for algo_parameter in self.leadLag_parameter_list:
            lead_file_path = os.path.join(self.leadlag_folder, algo_parameter.ticker, 'LEAD.csv')
            with open(lead_file_path) as fr:
                for line in islice(fr, 1, None):
                    ticker = line.split(',')[0].zfill(6)
                    self.ticker_set.add(ticker)
                    self.dependencys_set.add(ticker)

        all_parameter_list = self.multifactor_parameter_list + self.leadLag_parameter_list + self.basket_parameter_list
        for algo_parameter in all_parameter_list:
            self.ticker_set.add(algo_parameter.ticker)
            if algo_parameter.ticker in self.dependencys_set:
                self.dependencys_set.remove(algo_parameter.ticker)

        for ticker in self.ticker_set:
            instrument = self.instrument_dict[ticker]
            if instrument.exchange_id == 18:
                self.sh_ticker_set.add(ticker)
            elif instrument.exchange_id == 19:
                self.sz_ticker_set.add(ticker)

    def __modify_strategyloader(self):
        strategyloader_content_list = []
        with open(self.cfg_folder + '/config.strategyloader.txt_base') as fr:
            for line in fr.readlines():
                strategyloader_content_list.append(line.replace("\n", ""))

        conten_list = self.__build_strategyloader(self.multifactor_parameter_list, 'StkIntraDayStrategy')
        strategyloader_content_list.extend(conten_list)
        conten_list = self.__build_strategyloader(self.leadLag_parameter_list, 'StkIntraDayLeadLagStrategy')
        strategyloader_content_list.extend(conten_list)

        with open(self.cfg_folder + '/config.strategyloader.txt', 'w') as fr:
            fr.write('\n'.join(strategyloader_content_list))

    def __build_strategyloader(self, parameter_list, list_type):
        content_list = []
        split_parameter_list = self.__splist(parameter_list, SPLIT_LIST_SIZE)
        for i in range(0, len(split_parameter_list)):
            algo_parameter_list = split_parameter_list[i]
            content_list.append('')
            if list_type == 'StkIntraDayStrategy':
                content_list.append('[Strategy.libstk_intra_day_strategy.StkIntraDay.container%s]' % (i + 1,))
            elif list_type == 'StkIntraDayLeadLagStrategy':
                content_list.append('[Strategy.libstk_intra_day_strategy.StkIntraDayLeadLag.container%s]' % (i + 1,))
            ticker_list = []
            for algo_parameter in algo_parameter_list:
                ticker_list.append(algo_parameter.ticker)
            content_list.append('WatchList = ' + ';'.join(ticker_list))
            content_list.append('ParaList =  ')
        return content_list

    # 修改config.hfcalculator.txt
    def  __modify_config_hfcalculator(self):
        hfsampler_content_list = []
        with open(self.cfg_folder + '/config.hfcalculator.txt_base') as fr:
            for line in fr.readlines():
                hfsampler_content_list.append(line.replace("\n", ""))

        instruments_list = list(self.ticker_set)
        dependencys_list = list(self.dependencys_set)
        hfsampler_content_list.append('')
        hfsampler_content_list.append('[HFCalculator]')
        hfsampler_content_list.append('Instruments = ' + ','.join(instruments_list))
        hfsampler_content_list.append('Dependencys = ' + ','.join(dependencys_list))
        hfsampler_content_list.append('StartSampleTickTime = 09:30:00.000')
        hfsampler_content_list.append('InstrumentsGroupSize = 50')
        hfsampler_content_list.append('DependencysGroupSize = 200')
        hfsampler_content_list.append('Test = 0')
        hfsampler_content_list.append('Interval = 10000')
        hfsampler_content_list.append('ExportFile =')
        hfsampler_content_list.append('LeadLagMaxDelaySeconds = 60')
        hfsampler_content_list.append('FactorConfigPath =./cfg')

        with open(self.cfg_folder + '/config.hfcalculator.txt', 'w') as fr:
            fr.write('\n'.join(hfsampler_content_list))

    # fh13_instruments.csv 添加上海的ticker
    def __modify_fh_sh(self):
        sh_fh_content = ['TICKER,EXCHANGE_ID,TYPE_ID,']
        for ticker in self.sh_ticker_set | self.dependencys_set:
            instrument = self.instrument_dict[ticker]
            if instrument.exchange_id == 18:
                sh_fh_content.append('%s,18,4,' % ticker)
            elif instrument.exchange_id == 19:
                sh_fh_content.append('%s,19,4,' % ticker)

        with open(self.cfg_folder + '/fh13_instruments.csv', 'w') as fr:
            fr.write('\n'.join(sh_fh_content))

    # fh7_instruments.csv，添加深圳的ticker
    def __modify_fh_sz(self):
        fh_file_name = 'fh7_instruments.csv'

        server_model = server_constant.get_server_model(self.server_name)
        source_file_path = '%s/cfg/%s' % (server_model.server_path_dict['tradeplat_project_folder'], fh_file_name)
        target_file_path = self.cfg_folder + '/' + fh_file_name
        server_model.download_file(source_file_path, target_file_path)

        with open(target_file_path) as fr:
            line_items = fr.readlines()
            temp_ticker_dict = dict()
            for i in range(0, len(line_items)):
                if i == 0:
                    title = line_items[i].replace('\n', '')
                else:
                    ticker = line_items[i].replace('\n', '').split(',')[0]
                    temp_ticker_dict[ticker] = line_items[i].replace('\n', '')

        for ticker in self.sh_ticker_set | self.sz_ticker_set:
            instrument = self.instrument_dict[ticker]
            if instrument.exchange_id == 19:
                temp_ticker_dict[ticker] = '%s,19,4,' % ticker
            else:
                temp_ticker_dict[ticker] = '%s,18,4,' % ticker

        messaget_content_list = [title]
        for (ticker, ticker_str) in temp_ticker_dict.items():
            messaget_content_list.append(ticker_str)

        with open(target_file_path, 'w') as fr:
            fr.write('\n'.join(messaget_content_list))

    # rb7_instruments.csv，添加深圳的
    def __modify_rb_sz_huabao(self):
        sz_ticker_dict = dict()
        for algo_parameter in self.multifactor_parameter_list + self.leadLag_parameter_list:
            instrument = self.instrument_dict[algo_parameter.ticker]
            if instrument.exchange_id == 19:
                sz_ticker_dict[algo_parameter.ticker] = '%s,19,4,' % algo_parameter.ticker

        messaget_content_list = []
        for (ticker, ticker_str) in sz_ticker_dict.items():
            messaget_content_list.append(ticker_str)

        mg2_ticker_dict = dict()
        title = 'TICKER,EXCHANGE_ID,TYPE_ID,'
        file_ticker_size = 50
        start_index = 0
        for rb_file_name in ('rb7_instruments.csv', 'rb8_instruments.csv', 'rb9_instruments.csv'):
            target_file_path = self.cfg_folder + '/' + rb_file_name
            end_index = start_index + file_ticker_size
            if end_index > len(messaget_content_list):
                end_index = len(messaget_content_list)
            save_message_list = messaget_content_list[start_index:end_index]

            if rb_file_name == 'rb7_instruments.csv':
                from_rb_name = 'rb3'
            elif rb_file_name == 'rb8_instruments.csv':
                from_rb_name = 'rb7'
            elif rb_file_name == 'rb9_instruments.csv':
                from_rb_name = 'rb8'

            for ticker_str in save_message_list:
                ticker = ticker_str.split(',')[0]
                instrument = self.instrument_dict[ticker]
                if instrument.exchange_id == 18:
                    mg2_ticker_dict[ticker] = '%s,18,%s,' % (ticker, from_rb_name)
                elif instrument.exchange_id == 19:
                    mg2_ticker_dict[ticker] = '%s,19,%s,' % (ticker, from_rb_name)

            start_index += end_index
            save_message_list.insert(0, title)
            with open(target_file_path, 'w') as fr:
                fr.write('\n'.join(save_message_list))

        # 修改mg2_pre_bind_map_file.csv
        mg_file_name = 'mg2_pre_bind_map_file.csv'
        title = 'TICKER,EXCHANGE_ID,RECEIVE_FROM,'
        local_file_path = self.cfg_folder + '/' + mg_file_name

        for ticker in self.sh_ticker_set | self.dependencys_set:
            instrument = self.instrument_dict[ticker]
            if instrument.exchange_id == 18:
                mg2_ticker_dict[ticker] = '%s,18,rb4,' % ticker
            elif instrument.exchange_id == 19:
                mg2_ticker_dict[ticker] = '%s,19,rb4,' % ticker

        if len(mg2_ticker_dict) > 1000:
            email_utils7.send_email_group_all('[Error]mg2_pre_bind_map_file',
                                             'ticker size is:%s bigger than 1000' % len(sz_ticker_dict))

        messaget_content_list = [title]
        for (ticker, ticker_str) in mg2_ticker_dict.items():
            messaget_content_list.append(ticker_str)

        with open(local_file_path, 'w') as fr:
            fr.write('\n'.join(messaget_content_list))


    # rb7_instruments.csv，添加深圳的
    def __modify_rb_sz_guoxin(self):
        file_ticker_size = 50
        title = 'TICKER,EXCHANGE_ID,TYPE_ID,'

        rb3_ticker_list = []
        mg1_content_list = []
        mg2_content_list = []
        sz_tickerlist = list(self.sz_ticker_set)
        sz_tickerlist_item = self.__splist(sz_tickerlist, file_ticker_size)
        for i in range(0, 3):
            if i < len(sz_tickerlist_item):
                sz_tickerlist_temp = sz_tickerlist_item[i]
            else:
                sz_tickerlist_temp = []
            if i == 0:
                rb_file_name = 'rb4_instruments.csv'
                rb_from = 'rb4'
            elif i == 1:
                rb_file_name = 'rb5_instruments.csv'
                rb_from = 'rb5'
            elif i == 2:
                rb_file_name = 'rb6_instruments.csv'
                rb_from = 'rb6'
            target_file_path = self.cfg_folder + '/' + rb_file_name

            save_message_list = []
            for ticker in sz_tickerlist_temp:
                save_message_list.append('%s,19,4,' % ticker)
                rb3_ticker_list.append('%s,19,4,' % ticker)
                mg1_content_list.append('%s,19,%s,' % (ticker, rb_from))
                mg2_content_list.append('%s,19,%s,' % (ticker, rb_from))
            save_message_list.insert(0, title)
            with open(target_file_path, 'w') as fr:
                fr.write('\n'.join(save_message_list))

        target_file_path = self.cfg_folder + '/rb3_instruments.csv'
        rb3_ticker_list.insert(0, title)
        with open(target_file_path, 'w') as fr:
            fr.write('\n'.join(rb3_ticker_list))

        rb8_ticker_list = []
        for ticker in self.sh_ticker_set:
            rb_from = 'rb8'
            rb8_ticker_list.append('%s,18,4,' % ticker)
            mg2_content_list.append('%s,19,%s,' % (ticker, rb_from))
        target_file_path = self.cfg_folder + '/rb8_instruments.csv'
        rb8_ticker_list.insert(0, title)
        with open(target_file_path, 'w') as fr:
            fr.write('\n'.join(rb8_ticker_list))

        target_file_path = self.cfg_folder + '/mg1_pre_bind_map_file.csv'
        title = 'TICKER,EXCHANGE_ID,RECEIVE_FROM,'
        mg1_content_list.insert(0, title)
        with open(target_file_path, 'w') as fr:
            fr.write('\n'.join(mg1_content_list))

        target_file_path = self.cfg_folder + '/mg2_pre_bind_map_file.csv'
        title = 'TICKER,EXCHANGE_ID,RECEIVE_FROM,'
        mg2_content_list.insert(0, title)
        with open(target_file_path, 'w') as fr:
            fr.write('\n'.join(mg2_content_list))


    # mg1_pre_bind_map_file.csv，添加深圳映射，目前都是从rb7过来的
    def __modify_mg_sz(self):
        mg_file_name = 'mg1_pre_bind_map_file.csv'

        server_model = server_constant.get_server_model(self.server_name)
        source_file_path = '%s/cfg/%s' % (server_model.server_path_dict['tradeplat_project_folder'], mg_file_name)
        target_file_path = self.cfg_folder + '/' + mg_file_name
        server_model.download_file(source_file_path, target_file_path)

        with open(target_file_path) as fr:
            line_items = fr.readlines()
            temp_ticker_dict = dict()
            for i in range(0, len(line_items)):
                if i == 0:
                    title = line_items[i].replace('\n', '')
                else:
                    ticker = line_items[i].replace('\n', '').split(',')[0]
                    temp_ticker_dict[ticker] = line_items[i].replace('\n', '')

        for algo_parameter in self.multifactor_parameter_list + self.leadLag_parameter_list:
            instrument = self.instrument_dict[algo_parameter.ticker]
            if instrument.exchange_id == 19:
                temp_ticker_dict[algo_parameter.ticker] = '%s,19,rb4,' % algo_parameter.ticker

        message_content_list = [title]
        for (ticker, ticker_str) in temp_ticker_dict.items():
            message_content_list.append(ticker_str)

        with open(target_file_path, 'w') as fr:
            fr.write('\n'.join(message_content_list))

    # instruments_only_recv_ext_quote.csv修改
    def __modify_mktdtcenter_config(self):
        local_file_path = self.cfg_folder + '/' + 'instruments_only_recv_ext_quote.csv'
        dependencys_list = list(self.dependencys_set)

        messaget_content_list = ['TICKER,EXCHANGE_ID,']
        for dependencys_ticker in dependencys_list:
            instrument_db = self.instrument_dict[dependencys_ticker]
            messaget_content_list.append('%s,%s,' % (dependencys_ticker, instrument_db.exchange_id))
        with open(local_file_path, 'w') as fr:
            fr.write('\n'.join(messaget_content_list))

    def __modify_database(self):
        self.__save_algo_parameter(self.multifactor_parameter_list, 'StkIntraDayStrategy')
        self.__save_algo_parameter(self.leadLag_parameter_list, 'StkIntraDayLeadLagStrategy')

    def __save_algo_parameter(self, parameter_list, list_type):
        server_model = server_constant.get_server_model(self.server_name)
        session_strategy = server_model.get_db_session('strategy')

        split_parameter_list = self.__splist(parameter_list, SPLIT_LIST_SIZE)
        for i in range(0, len(split_parameter_list)):
            algo_parameter_list = split_parameter_list[i]
            parameter_value_dict = dict()
            parameter_value_dict['Account'] = 'steady_return'
            for algo_parameter in algo_parameter_list:
                parameter_items = self.parameter_dict[algo_parameter.parameter].split(',')
                for j in range(0, len(parameter_items)):
                    key = '%s_%s' % (algo_parameter.ticker, self.parameter_title_list[j])
                    parameter_value_dict[key] = parameter_items[j]

            strategy_parameter = StrategyParameter()
            strategy_parameter.time = date_utils.get_now()

            if list_type == 'StkIntraDayStrategy':
                strategy_parameter.name = 'StkIntraDayStrategy.container%s' % (i + 1)
            elif list_type == 'StkIntraDayLeadLagStrategy':
                strategy_parameter.name = 'StkIntraDayLeadLagStrategy.container%s' % (i + 1)
            strategy_parameter.value = json.dumps(parameter_value_dict)
            session_strategy.merge(strategy_parameter)
        session_strategy.commit()

    def __save_strategy_intraday_parameter(self):
        account_position_dict = dict()
        server_model = server_constant.get_server_model(self.server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(AccountPosition)
        for account_position_db in query.filter(AccountPosition.date == self.date_str, AccountPosition.id == 4):
            account_position_dict[account_position_db.symbol] = account_position_db.long_avail

        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for parameter_info in self.multifactor_parameter_list +  self.leadLag_parameter_list:
            if parameter_info.ticker in account_position_dict:
                parameter_info.initial_share = account_position_dict[parameter_info.ticker]
            else:
                self.email_content_list.append('ticker:%s not in account_position' % parameter_info.ticker)
                parameter_info.initial_share = 0
            session_jobs.add(parameter_info)
        session_jobs.commit()
        server_host.close()

    def __send_email(self):
        email_utils7.send_email_group_all('StkIntraDay File Build Over_%s' % self.server_name, ''.join(self.email_content_list), 'html')

    def __backup_files(self):
        tradeplat_file_name = 'tradeplat_%s.tar.gz' % date_utils.get_today_str('%Y-%m-%d')
        shutil.copy(self.tradeplat_file_folder + '/' + tradeplat_file_name,
                    self.local_backup_folder + '/' + tradeplat_file_name)

    @ staticmethod
    def __splist(l, s):
        return [l[i:i+s] for i in range(len(l)) if i % s == 0]


if __name__ == '__main__':
    stkintraday_mode = Stkintraday_Mode('citics')
    stkintraday_mode.start_work()
