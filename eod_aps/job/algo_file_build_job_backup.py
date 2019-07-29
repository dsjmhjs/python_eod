# -*- coding: utf-8 -*-
# 生成多因子策略的股票购买清单
import os
import shutil
import pandas as pd
from decimal import Decimal
from itertools import islice
from eod_aps.model.schema_portfolio import RealAccount, PfAccount, PfPosition, AccountPosition
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.model.schema_jobs import StrategyAccountInfo, SpecialTickers
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.job.phone_trade_manager_job import phone_trade_by_change_file
from eod_aps.job import *

operation_enums = const.BASKET_FILE_OPERATION_ENUMS
STRATEGY_BASE_TITLE = '"Ticker","Prev_Close","Weight","Volume"'
SUM_FILE_TITLE = 'strategy_name,ticker,account_volume,target_volume,trade_volume,target_weight,\
correct_weight,diff_weight,real_prev_close,adj_prev_close,money,error_message'


def rounding_number(number_input):
    # 对股数进行四舍五入， 160--》200
    return int(round(float(number_input) / float(100), 0) * 100)


def round_down(number_input):
    # 对股数向下取整
    return int(int(float(number_input) / float(100)) * 100)


class StrategyBasketInfo(object):
    def __init__(self, server_name, operation_type, cutdown_ratio=0):
        self.__server_name = server_name
        self.__operation_type = operation_type
        self.__cutdown_ratio = cutdown_ratio
        self.__date_str = date_utils.get_today_str('%Y-%m-%d')
        self.__date_str2 = self.__date_str.replace('-', '')
        self.__last_date_str = date_utils.get_last_trading_day('%Y-%m-%d')
        self.__last_date_str2 = self.__last_date_str.replace('-', '')

        self.__error_message_list = []
        self.__future_main_contract_dict = dict()
        self.__instrument_dict = dict()
        self.__suspend_stock_list = []
        self.__st_stock_list = []
        self.__low_stop_stock_list = []
        self.__high_stop_stock_list = []

        # 更新strategyaccount_info表
        self.__update_strategy_account_info()
        self.__base_save_folder = ''

    def strategy_basket_file_build(self):
        custom_log.log_info_job('Server:%s Operation:%s Start.' % (self.__server_name, self.__operation_type))
        server_host = server_constant.get_server_model('host')
        self.__build_future_maincontract_dict(server_host)
        self.__build_db_dict(server_host)

        if self.__operation_type == operation_enums.Close_Bits:
            self.__basket_file_close_bits()
        else:
            self.__basket_file_build(server_host)
        custom_log.log_info_job('Server:%s Operation:%s Stop.' % (self.__server_name, self.__operation_type))
        return self.__error_message_list

    def __basket_file_close_bits(self):
        server_model = server_constant.get_server_model(self.__server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        real_account_list = const.EOD_CONFIG_DICT['server_account_dict'][self.__server_name]
        self.__make_base_folder()
        for real_account in real_account_list:
            if 'any,commonstock' not in real_account.allow_targets:
                continue

            file_content_list = []
            for item in session_portfolio.query(AccountPosition).filter(AccountPosition.date == self.__date_str,
                                                                        AccountPosition.id == real_account.accountid):
                if not (0 < item.long < 100):
                    continue
                if not item.symbol[0] in ('0', '3', '6'):
                    continue
                if item.symbol in self.__suspend_stock_list:
                    continue
                file_content_list.append('%s,%s' % (item.symbol, int(item.long)))

            if len(file_content_list) > 0:

                strategy_file_name = 'default-manual-%s@%s-Peg-.txt' % (real_account.fund_name, server_model.ip)
                save_file_path = '%s/%s' % (self.__base_save_folder, strategy_file_name)
                with open(save_file_path, 'w') as fr:
                    fr.write('\n'.join(file_content_list))
            else:
                custom_log.log_error_job('Fund:%s bits stock is Null!' % real_account.fund_name)

    def split_sigmavwap_ai(self):
        config_file_path = os.path.join(TRADEPLAT_FILE_FOLDER_TEMPLATE % self.__server_name, 'tf_config',
                                        'stock_ref.csv')
        with open(config_file_path, 'rb') as fr:
            ai_ticker_list = [line.split(',')[0] for line in islice(fr, 1, None)]

        change_files_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__date_str2, 'change')
        base_file_list = []
        for file_name in os.listdir(change_files_folder):
            if not file_name.endswith('.txt'):
                continue
            base_file_list.append(file_name)

        for file_name in base_file_list:
            sigmavwap_list = []
            sigmavwap_ai_list = []
            with open(os.path.join(change_files_folder, file_name), 'rb') as fr:
                for line in fr.readlines():
                    ticker, volume = line.replace('\r\n', '').split(',')
                    if ticker in ai_ticker_list:
                        sigmavwap_ai_list.append('%s,%s' % (ticker, volume))
                    else:
                        sigmavwap_list.append('%s,%s' % (ticker, volume))

            if len(sigmavwap_list) > 0:
                sigmavwap_file_name = file_name
                with open(os.path.join(change_files_folder, sigmavwap_file_name), 'w') as fr:
                    fr.write('\n'.join(sigmavwap_list))
            if len(sigmavwap_ai_list) > 0:
                if self.__server_name == 'guoxin':
                    sigmavwap_ai_file_name = file_name.replace('SigmaVWAP3', 'SigmaVWAP_AI')
                else:
                    sigmavwap_ai_file_name = file_name.replace('SigmaVWAP', 'SigmaVWAP_AI')
                with open(os.path.join(change_files_folder, sigmavwap_ai_file_name), 'w') as fr:
                    fr.write('\n'.join(sigmavwap_ai_list))

    def check_basket_file(self):
        check_result_list = []
        server_model = server_constant.get_server_model(self.__server_name)
        change_files_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__date_str2, 'change')
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for item in session_jobs.query(StrategyAccountInfo).filter(StrategyAccountInfo.server_name == self.__server_name):
            for temp_strategy_name in ['SigmaVWAP', 'SigmaVWAP_AI']:
                pf_account_name = '%s-%s-%s-' % (item.strategy_name, item.group_name, item.fund)
                check_file_name = '%s-%s-%s@%s-%s-%s-.txt' % (item.strategy_name, item.group_name, item.fund,
                                                              server_model.ip, temp_strategy_name, 'OppositeSide')
                check_file_path = os.path.join(change_files_folder, check_file_name)
                if not os.path.exists(check_file_path):
                    check_result_list.append(
                        (self.__server_name, pf_account_name, '', 'File:%s Missing!(Error)' % check_file_path))
        return check_result_list

    def __make_base_folder(self):
        if self.__operation_type == operation_enums.Close:
            folder_suffix = 'close'
        elif self.__operation_type == operation_enums.Cutdown:
            folder_suffix = 'cutdown'
        elif self.__operation_type == operation_enums.Add:
            folder_suffix = 'add'
        elif self.__operation_type == operation_enums.Change:
            folder_suffix = 'base'
        elif self.__operation_type == operation_enums.Close_Bits:
            folder_suffix = 'close_bits'
        else:
            raise Exception("Error operation_type:%s" % self.__operation_type)
        base_save_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__date_str2, folder_suffix)
        if os.path.exists(base_save_folder):
            shutil.rmtree(base_save_folder)
        os.mkdir(base_save_folder)
        self.__base_save_folder = base_save_folder

    def __update_strategy_account_info(self):
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for strategy_account_info in session_jobs.query(StrategyAccountInfo):
            if date_utils.datetime_toString(strategy_account_info.update_date) == self.__date_str:
                continue
            elif strategy_account_info.all_number is None:
                pass
            else:
                all_number_list = strategy_account_info.all_number.split(',')
                index_value = all_number_list.index(strategy_account_info.last_number) + 1
                if index_value > len(all_number_list) - 1:
                    last_number = all_number_list[0]
                else:
                    last_number = all_number_list[index_value]
                strategy_account_info.last_number = last_number
            strategy_account_info.update_date = self.__date_str
            session_jobs.merge(strategy_account_info)
        session_jobs.commit()

    def __query_strategy_file_path(self, pf_account_name):
        strategy_name, group_name, fund, temp = pf_account_name.split('-')
        if 'Institution' in strategy_name and 'Event_Real' == group_name:
            strategy_file_name = 'Institution_Investigation.csv'
        elif 'Earning' in strategy_name and 'Event_Real' == group_name:
            strategy_file_name = 'Earning_Estimate.csv'
        elif 'Inflow' in strategy_name and 'Event_Real' == group_name:
            strategy_file_name = 'Inflow.csv'
        elif 'ANN6031A_Stock' == strategy_name and 'MultiFactor' == group_name:
            strategy_file_name = 'model_60_3_1_a.csv'
        elif 'ANN6031B_Stock' == strategy_name and 'MultiFactor' == group_name:
            strategy_file_name = 'model_60_3_1_b.csv'
        elif 'ANN6061A_Stock' == strategy_name and 'MultiFactor' == group_name:
            strategy_file_name = 'model_60_6_1_a.csv'
        elif 'ANN6061B_Stock' == strategy_name and 'MultiFactor' == group_name:
            strategy_file_name = 'model_60_6_1_b.csv'
        else:
            raise Exception("Error strategy_name:%s,group_name:%s" % (strategy_name, group_name))

        strategy_file_path = '%s/%s/%s' % (STRATEGY_FILE_PATH_DICT[group_name], self.__date_str2, strategy_file_name)
        if not os.path.exists(strategy_file_path):
            email_utils8.send_email_group_all('[Error]Algo File Miss!', 'Miss File:%s' % strategy_file_path, 'html')
            raise Exception("[Error]Miss File:%s" % strategy_file_path)
        return strategy_file_path

    # 根据配置文件获取需购买的股票和权重
    def __read_target_ticker_list(self, pf_account_name):
        strategy_file_path = self.__query_strategy_file_path(pf_account_name)

        ticker_weight_list = []
        with open(strategy_file_path, 'rb') as strategy_file:
            for line in islice(strategy_file, 1, None):
                line_items = line.split(',')
                ticker_str = line_items[1]
                weight = float(line_items[2])
                ticker_full = str(filter(lambda x: x.isdigit(), ticker_str))
                ticker_full = ticker_full.zfill(6)
                if ticker_full not in self.__instrument_dict:
                    self.__error_message_list.append((self.__server_name, pf_account_name, ticker_full, 'Target Ticker Not In Table'))
                    continue
                instrument_db = self.__instrument_dict[ticker_full]
                ticker_weight_list.append((ticker_full, weight, instrument_db.prev_close))
        return ticker_weight_list

    def __format_ticker_weight_list(self, strategy_account_info, ticker_weight_list):
        exclude_ticker_list = []
        exclude_ticker_str = strategy_account_info.exclude_ticker
        if exclude_ticker_str is not None:
            exclude_ticker_list.extend(exclude_ticker_str.split(','))
        pf_account_name = '%s_%s-%s-%s-' % (strategy_account_info.strategy_name, strategy_account_info.last_number,
                                            strategy_account_info.group_name, strategy_account_info.fund)

        error_weight = 0
        format_list = []
        for (ticker, weight, prev_close) in ticker_weight_list:
            if ticker in self.__suspend_stock_list:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Target Ticker Is Suspend'))
                error_weight += weight
                continue
            elif ticker in self.__st_stock_list:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Target Ticker Is ST'))
                error_weight += weight
                continue
            elif ticker in exclude_ticker_list:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Target Ticker Is Exclude'))
                error_weight += weight
                continue
            format_list.append((ticker, prev_close, weight))

        # 如果购买清单中存在停牌股票，平分其权重
        if error_weight > 0:
            average_error_weight = error_weight / float(len(format_list))
            format_list = [(a, b, c + average_error_weight) for (a, b, c) in format_list]
        format_list.sort()
        return format_list

    def __query_pf_position(self, pf_account_name):
        server_model = server_constant.get_server_model(self.__server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(PfAccount)
        pf_account_db = query.filter(PfAccount.fund_name == pf_account_name).first()

        pf_position_dict = dict()
        query_pf_position = session_portfolio.query(PfPosition)
        for pf_position_db in query_pf_position.filter(PfPosition.date == self.__date_str,
                                                       PfPosition.id == pf_account_db.id):
            ticker = pf_position_db.symbol
            if not ticker.isdigit():
                continue
            if ticker not in self.__instrument_dict:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Position Ticker Is Not In Table'))
                continue
            instrument_db = self.__instrument_dict[ticker]
            if instrument_db.prev_close is None:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Position Ticker Prev_Close is Null'))
                continue
            if pf_position_db.symbol in self.__suspend_stock_list:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Position Ticker is Suspend'))
                continue
            if pf_position_db.long < 100:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Position Ticker Volume Less Than 100'))
                continue
            pf_position_dict[ticker] = pf_position_db.long
        server_model.close()
        return pf_position_dict

    def __basket_file_build(self, server_host):
        self.__make_base_folder()

        sum_ticker_trade_list = []
        strategy_account_info_dict = self.__query_strategy_account_info_dict(server_host)
        for (item, pf_account_list) in strategy_account_info_dict.items():
            for pf_account_name in pf_account_list:
                if self.__operation_type == operation_enums.Close:
                    # 清仓
                    pf_position_dict = self.__query_pf_position(pf_account_name)
                    target_position_dict = dict()
                elif self.__operation_type == operation_enums.Cutdown:
                    # 减仓
                    pf_position_dict = self.__query_pf_position(pf_account_name)
                    target_position_dict = self.__build_target_file_cutdown(pf_position_dict, self.__cutdown_ratio)
                else:
                    ticker_weight_list = self.__read_target_ticker_list(pf_account_name)
                    format_ticker_weight_list = self.__format_ticker_weight_list(item, ticker_weight_list)
                    if format_ticker_weight_list is None:
                        return []
                    if self.__operation_type == operation_enums.Add:
                        # 开仓
                        pf_position_dict = dict()
                        trade_money = self.__calculation_add_money(item)
                    elif self.__operation_type == operation_enums.Change:
                        # 调仓
                        pf_position_dict = self.__query_pf_position(pf_account_name)
                        trade_money = self.__calculation_change_money(pf_account_name, pf_position_dict, ticker_weight_list)
                    target_position_dict = self.__build_target_position_dict(ticker_weight_list, trade_money)
                ticker_trade_list = self.__build_basket_file(pf_account_name, target_position_dict, pf_position_dict)

                filter_trade_list = self.__filter_by_real_position(pf_account_name, ticker_trade_list)
                filter_trade_list = sorted(filter_trade_list,
                                           cmp=lambda x, y: cmp(int(x.split(',')[1]), int(y.split(',')[1])))
                if len(filter_trade_list) > 0:
                    file_path = '%s/%s.txt' % (self.__base_save_folder, pf_account_name)
                    with open(file_path, 'w') as fr:
                        fr.write('\n'.join(filter_trade_list))
                sum_ticker_trade_list.append(ticker_trade_list)

        # 生成篮子的统计报告
        if len(sum_ticker_trade_list) > 0:
            self.__build_sum_file(sum_ticker_trade_list, self.__base_save_folder)

        # 调仓单需要更换生成目录，并分析是否生成自成交的PhoneTrade
        if self.__operation_type == operation_enums.Change:
            change_save_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name,
                                                  self.__date_str2, 'change')
            self.__build_phone_trade_file(self.__base_save_folder, change_save_folder)
            self.__base_save_folder = change_save_folder

        # 修改订单名称
        self.__rebuild_fle_name(self.__base_save_folder)
        server_host.close()

    def __query_strategy_account_info_dict(self, server_host):
        strategy_account_info_dict = dict()
        session_jobs = server_host.get_db_session('jobs')
        query = session_jobs.query(StrategyAccountInfo)
        for item in query.filter(StrategyAccountInfo.server_name == self.__server_name):
            pf_account_list = []
            # Close时包含default的仓位
            if self.__operation_type == operation_enums.Close:
                pf_account_list.append('default-manual-%s-' % item.fund)

            if item.last_number is None:
                pf_account_name = '%s-%s-%s-' % (item.strategy_name, item.group_name, item.fund)
                pf_account_list.append(pf_account_name)
            else:
                if self.__operation_type in (operation_enums.Close, operation_enums.Cutdown):
                    for number in item.all_number.split(','):
                        pf_account_name = '%s_%s-%s-%s-' % (item.strategy_name, number,
                                                            item.group_name, item.fund)
                        pf_account_list.append(pf_account_name)
                else:
                    pf_account_name = '%s_%s-%s-%s-' % (item.strategy_name, item.last_number,
                                                        item.group_name, item.fund)
                    pf_account_list.append(pf_account_name)
            strategy_account_info_dict[item] = list(set(pf_account_list))
        return strategy_account_info_dict

    def __build_phone_trade_file(self, source_folder, target_folder):
        if os.path.exists(target_folder):
            shutil.rmtree(target_folder)
        os.mkdir(target_folder)
        for file_name in os.listdir(source_folder):
            shutil.copyfile('%s/%s' % (source_folder, file_name),
                            '%s/%s' % (target_folder, file_name))
        phone_trade_by_change_file(self.__server_name, target_folder)

    def __build_basket_file(self, pf_account_name, target_position_dict, pf_position_dict):
        trade_ticker_list = target_position_dict.keys() + pf_position_dict.keys()
        trade_ticker_list = list(set(trade_ticker_list))
        trade_ticker_list.sort()

        ticker_trade_list = []
        for ticker in trade_ticker_list:
            pf_volume = int(pf_position_dict[ticker]) if ticker in pf_position_dict else 0
            if ticker in target_position_dict:
                target_volume, target_weight = target_position_dict[ticker]
                target_volume = int(target_volume)
            else:
                target_volume = 0
                target_weight = 0.0
            trade_volume = self.__ticker_volume_round(target_volume, pf_volume)

            instrument_db = self.__instrument_dict[ticker]
            ticker_trade_info = Ticker_Trade_Info()
            ticker_trade_info.strategy_name = pf_account_name
            ticker_trade_info.ticker = ticker
            ticker_trade_info.account_volume = pf_volume
            ticker_trade_info.target_volume = target_volume
            ticker_trade_info.volume = trade_volume
            ticker_trade_info.target_weight = '%.4f' % float(target_weight)
            if instrument_db.close is None:
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Ticker Close Is Null'))
                continue
            ticker_trade_info.real_prev_close = float(instrument_db.close)
            ticker_trade_info.adj_prev_close = float(instrument_db.prev_close)
            ticker_trade_list.append(ticker_trade_info)
        return ticker_trade_list

    def __build_sum_file(self, all_ticker_trade_list, save_folder):
        sum_file_list = []
        for ticker_trade_list in all_ticker_trade_list:
            if len(ticker_trade_list) == 0:
                continue

            total_position_money = 0.0
            for tti_item in ticker_trade_list:
                tti_item.position_money = (tti_item.account_volume + tti_item.volume) \
                                          * tti_item.adj_prev_close
                total_position_money += tti_item.position_money

            sum_file_list.append(SUM_FILE_TITLE)
            buy_money_list = []
            sell_money_list = []
            position_money = 0.0
            for tti_item in ticker_trade_list:
                tti_item.money = tti_item.adj_prev_close * tti_item.volume
                position_money += (tti_item.account_volume + tti_item.volume) * tti_item.adj_prev_close
                if tti_item.volume > 0:
                    buy_money_list.append(tti_item.money)
                else:
                    sell_money_list.append(tti_item.money)

                if total_position_money == 0:
                    correct_weight = 0
                else:
                    correct_weight = '%.4f' % (tti_item.position_money / total_position_money)
                diff_weight = '%.4f' % (float(tti_item.target_weight) - float(correct_weight))
                sum_file_list.append('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
                    tti_item.strategy_name, tti_item.ticker, str(tti_item.account_volume),
                    str(tti_item.target_volume), str(tti_item.volume), tti_item.target_weight,
                    correct_weight, diff_weight, str(tti_item.real_prev_close),
                    str(tti_item.adj_prev_close), str(tti_item.money),
                    str(tti_item.error_message)))
            sum_file_list.append('buy_money:,%s,sell_money:,%s' % (sum(buy_money_list), sum(sell_money_list)))
            sum_file_list.append('')
            sum_file_list.append('')
        with open('%s/sum_info_%s_%s.csv' % (save_folder, self.__server_name, self.__date_str2), 'w') as fr:
            fr.write('\n'.join(sum_file_list))

    def __filter_by_real_position(self, pf_account_name, ticker_trade_list):
        real_position_dict = dict()
        server_model = server_constant.get_server_model(self.__server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        strategy_name, group_name, fund, temp = pf_account_name.split('-')
        for account_position_db in session_portfolio.query(AccountPosition)\
                .join(RealAccount, RealAccount.accountid == AccountPosition.id) \
                .filter(RealAccount.fund_name == fund).filter(AccountPosition.date == self.__date_str):
            if not account_position_db.symbol.isdigit():
                continue
            real_position_dict[account_position_db.symbol] = account_position_db.long_avail

        validate_save_list = []
        for ticker_trade_info in ticker_trade_list:
            ticker = ticker_trade_info.ticker
            volume = ticker_trade_info.volume

            if volume == 0:
                continue
            elif volume > 0:
                validate_save_list.append('%s,%s' % (ticker, volume))
                continue

            if ticker not in real_position_dict:
                error_message = 'Ticker No Real Position'
                ticker_trade_info.volume = 0
                ticker_trade_info.error_message = error_message
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, error_message))
                continue

            real_account_volume = int(real_position_dict[ticker_trade_info.ticker])
            if abs(volume) > real_account_volume:
                error_message = 'Sell Volume:%s Bigger Than Real Position:%s' % (abs(volume), real_account_volume)
                ticker_trade_info.volume = -real_account_volume
                ticker_trade_info.error_message = error_message
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, error_message))
            elif int(volume) % 100 != 0 and abs(int(volume)) != real_account_volume:
                error_message = 'Sell Volume:%s Cannot Whole Sell.Real Position:%s' % (abs(volume), real_account_volume)
                ticker_trade_info.volume = round_down(volume)
                ticker_trade_info.error_message = error_message
                self.__error_message_list.append((self.__server_name, pf_account_name, ticker, error_message))

            if ticker_trade_info.volume != 0:
                validate_save_list.append('%s,%s' % (ticker, ticker_trade_info.volume))
        server_model.close()
        return validate_save_list

    def __ticker_volume_round(self, target_volume, pf_volume):
        ticker_round_volume = rounding_number(target_volume)
        if ticker_round_volume - pf_volume < 0:
            trade_volume = round_down(ticker_round_volume - pf_volume)
        else:
            trade_volume = rounding_number(ticker_round_volume - pf_volume)

        if target_volume == 0 and pf_volume == 0:
            trade_volume = 0
        elif trade_volume == 0 and pf_volume == 0:
            trade_volume = 100
        return trade_volume

    # 生成调整目标文件
    def __build_target_position_dict(self, ticker_weight_list, trade_money):
        target_position_dict = dict()
        for (ticker, weight, prev_close) in ticker_weight_list:
            volume = rounding_number(Decimal(trade_money) * Decimal(weight) / Decimal(prev_close))
            target_position_dict[ticker] = (volume, weight)
        return target_position_dict

    def __build_target_file_cutdown(self, pf_position_dict, cutdown_ratio):
        total_money = 0
        for (ticker, pf_volume) in pf_position_dict.items():
            instrument_db = self.__instrument_dict[ticker]
            prev_close = instrument_db.prev_close
            target_volume = rounding_number(Decimal(pf_volume) * Decimal(1 - cutdown_ratio))
            total_money += prev_close * target_volume

        target_position_dict = dict()
        for (ticker, pf_volume) in pf_position_dict.items():
            instrument_db = self.__instrument_dict[ticker]
            prev_close = instrument_db.prev_close
            target_volume = rounding_number(Decimal(pf_volume) * Decimal(1 - cutdown_ratio))
            weight = '%.4f' % ((prev_close * target_volume) / total_money,) if total_money > 0 else 0
            target_position_dict[ticker] = (target_volume, weight)
        return target_position_dict

    def __calculation_add_money(self, strategy_account_info):
        main_contract_db = self.__future_main_contract_dict[strategy_account_info.target_future]
        instrument_db = self.__instrument_dict[main_contract_db.main_symbol]
        add_money = float(instrument_db.prev_close) * float(instrument_db.fut_val_pt)
        return add_money

    def __calculation_change_money(self, pf_account_name, pf_position_dict, ticker_weight_list):
        last_change_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__last_date_str2, 'base')
        last_sell_tickers = []
        last_buy_tickers = []
        for file_name in os.listdir(last_change_folder):
            if not file_name.endswith('.txt') or pf_account_name not in file_name:
                continue
            with open('%s/%s' % (last_change_folder, file_name), 'rb') as fr:
                for x in fr.readlines():
                    ticker, volume = x.replace('\n', '').split(',')
                    if int(volume) > 0:
                        last_buy_tickers.append(ticker)
                    else:
                        last_sell_tickers.append(ticker)

        target_ticker_list = [x[0] for x in ticker_weight_list]
        account_money = 0.0
        for ticker in pf_position_dict.keys():
            # if ticker in last_sell_tickers and ticker not in target_ticker_list and ticker in self.__low_stop_stock_list:
            #     self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Ticker Is Low_Stop!'))
            #     continue
            #
            # if ticker in last_buy_tickers and ticker in target_ticker_list and ticker in self.__high_stop_stock_list:
            #     self.__error_message_list.append((self.__server_name, pf_account_name, ticker, 'Ticker Is High_Stop!'))
            #     continue

            instrument_db = self.__instrument_dict[ticker]
            volume = float(pf_position_dict[ticker])
            account_money += float(instrument_db.prev_close) * volume
        return account_money

    def __build_future_maincontract_dict(self, server_host):
        session_common = server_host.get_db_session('common')
        query = session_common.query(FutureMainContract)
        for main_contract_db in query:
            self.__future_main_contract_dict[main_contract_db.ticker_type] = main_contract_db

    def __build_db_dict(self, server_host):
        type_list = [Instrument_Type_Enums.Future, Instrument_Type_Enums.CommonStock]
        self.__instrument_dict = query_instrument_dict('host', type_list)

        special_ticker_list = []
        session_jobs = server_host.get_db_session('jobs')
        for special_ticker in session_jobs.query(SpecialTickers).filter(SpecialTickers.date == self.__date_str):
            special_ticker_list.append(special_ticker)

        self.__suspend_stock_list = [x.ticker for x in special_ticker_list if 'Suspend' in x.describe]
        custom_log.log_info_job('Suspend Stock List:%s' % ','.join(self.__suspend_stock_list))
        self.__st_stock_list = [x.ticker for x in special_ticker_list if 'ST' in x.describe]
        custom_log.log_info_job('ST Stock List:%s' % ','.join(self.__st_stock_list))
        self.__low_stop_stock_list = [x.ticker for x in special_ticker_list if 'Low_Stop' in x.describe]
        custom_log.log_info_job('Low_Stop Stock List:%s' % ','.join(self.__low_stop_stock_list))
        self.__high_stop_stock_list = [x.ticker for x in special_ticker_list if 'High_Stop' in x.describe]
        custom_log.log_info_job('High_Stop Stock List:%s' % ','.join(self.__high_stop_stock_list))

    def __rebuild_fle_name(self, base_folder_path):
        rename_config_dict = dict()
        with open(STOCK_SELECTION_CONFIG_FILE, 'r') as fr:
            for line in islice(fr, 1, None):
                _server_name, _group_name, _operation_type, _algo_type, _peg_level = line.replace('\n', '').split(',')
                dict_key = '%s|%s|%s' % (_server_name, _group_name, _operation_type)
                rename_config_dict[dict_key] = (_algo_type, _peg_level)

        server_model = server_constant.get_server_model(self.__server_name)
        for file_name in os.listdir(base_folder_path):
            if not file_name.endswith('.txt'):
                continue
            file_name_items = file_name.split('.')[0].split('-')
            group_name = file_name_items[1]

            algo_type = None
            peg_level = None
            dict_key = '%s|%s|%s' % (self.__server_name, group_name, self.__operation_type)
            if dict_key in rename_config_dict:
                (algo_type, peg_level) = rename_config_dict[dict_key]
            else:
                dict_key = '%s|%s|%s' % (self.__server_name, 'all', self.__operation_type)
                if dict_key in rename_config_dict:
                    (algo_type, peg_level) = rename_config_dict[dict_key]
            if algo_type is None or peg_level is None:
                continue

            rename_str = '%s-%s-%s@%s-%s-%s-.txt' % (file_name_items[0], file_name_items[1], file_name_items[2],
                                                     server_model.ip, algo_type, peg_level)
            os.rename('%s/%s' % (base_folder_path, file_name), '%s/%s' % (base_folder_path, rename_str))

    def ticker_index_report(self):
        server_host = server_constant.get_server_model('host')
        shsz300_ticker_list, sh000905_ticker_list = self.__query_index_ticker_list(server_host)

        pf_account_list = []
        session_jobs = server_host.get_db_session('jobs')
        for item in session_jobs.query(StrategyAccountInfo):
            if item.group_name != 'MultiFactor':
                continue
            pf_account_name = '%s-%s-%s-' % (item.strategy_name, item.group_name, '')
            pf_account_list.append(pf_account_name)

        strategy_ticker_list = []
        for pf_account_name in list(set(pf_account_list)):
            strategy_file_path = self.__query_strategy_file_path(pf_account_name)

            with open(strategy_file_path, 'rb') as fr:
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    strategy_ticker_list.append([strategy_file_path, line_items[1], float(line_items[2])])
        strategy_ticker_df = pd.DataFrame(strategy_ticker_list, columns=['File_Path', 'Ticker', 'Weight'])

        strategy_ticker_df['Include'] = ''
        strategy_ticker_df.loc[strategy_ticker_df['Ticker'].isin(shsz300_ticker_list), 'Include'] = 'SHSZ300'
        strategy_ticker_df.loc[strategy_ticker_df['Ticker'].isin(sh000905_ticker_list), 'Include'] = 'SH000905'
        strategy_ticker_df = strategy_ticker_df[['File_Path', 'Include', 'Weight']].groupby(['File_Path', 'Include']).sum().reset_index()

        output_file_path = '%s/%s/%s' % (STRATEGY_FILE_PATH_DICT['MultiFactor'], self.__date_str2,
                                         'index_structure_report.csv')
        strategy_ticker_df.to_csv(output_file_path, index=0)

    def __query_index_ticker_list(self, server_host):
        shsz300_ticker_list = []
        sh000905_ticker_list = []
        session_common = server_host.get_db_session('common')
        for index_instrument in session_common.query(Instrument).filter(Instrument.ticker.in_(('SHSZ300', 'SH000905'))):
            if index_instrument.ticker == 'SHSZ300':
                shsz300_ticker_list.extend(index_instrument.indx_members.split(';'))
            elif index_instrument.ticker == 'SH000905':
                sh000905_ticker_list.extend(index_instrument.indx_members.split(';'))
        return shsz300_ticker_list, sh000905_ticker_list


class Ticker_Trade_Info(object):
    """
        Ticker_Trade_Info
    """
    strategy_name = None
    ticker = None
    account_volume = 0
    target_volume = 0
    volume = 0
    target_weight = 0
    correct_weight = 0
    real_prev_close = None
    adj_prev_close = None
    money = 0.0
    position_money = 0.0
    error_message = ''

    def __init__(self):
        pass


if __name__ == '__main__':
    strategy_basket_info = StrategyBasketInfo('guosen', operation_enums.Close_Bits)
    print strategy_basket_info.strategy_basket_file_build()
