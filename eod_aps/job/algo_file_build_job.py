# -*- coding: utf-8 -*-
# 生成多因子策略的股票购买清单
import os
import shutil
import pandas as pd
import numpy as np
from itertools import islice
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.schema_portfolio import RealAccount, PfAccount, PfPosition, AccountPosition
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.model.schema_jobs import StrategyAccountInfo, SpecialTickers, StrategyTickerParameter
from eod_aps.job import *
from eod_aps.tools.aggregator_message_utils import AggregatorMessageUtils
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.phone_trade_tools import save_phone_trade_file

operation_enums = const.BASKET_FILE_OPERATION_ENUMS
common_utils = CommonUtils()
STRATEGY_BASE_TITLE = '"Ticker","Prev_Close","Weight","Volume"'
SUM_FILE_TITLE = 'strategy_name,ticker,account_volume,target_volume,trade_volume,target_weight,\
correct_weight,diff_weight,real_prev_close,adj_prev_close,money,error_message'


def format_trade_qty(row):
    market_value = row['Total_Market_Value']
    round_weight = row['Round_Weight']
    target_market_value = row['Target_Market_Value']
    target_round_weight = row['Target_Round_Weight']
    prev_close = row['Prev_Close']
    if prev_close == 0:
        return 0

    trade_qty = (target_market_value * target_round_weight - market_value * round_weight) / prev_close
    trade_qty = rounding_number(trade_qty)
    # 避免超卖
    if trade_qty < 0 and row['Qty'] < abs(trade_qty):
        trade_qty = -row['Qty']
    # 避免卖零股
    if trade_qty < 0 and row['Qty'] < 100:
        trade_qty = 0
    return trade_qty


def rounding_number(number_input):
    # 对股数进行四舍五入， 160--》200
    return int(round(float(number_input) / float(100), 0) * 100)


def round_down(number_input):
    # 对股数向下取整
    return int(int(float(number_input) / float(100)) * 100)


class StrategyBasketInfo(object):
    def __init__(self, server_name, operation_type):
        self.__server_name = server_name
        self.__operation_type = operation_type
        self.__cut_down_money = None
        self.__add_money = None

        self.__date_str = date_utils.get_today_str('%Y-%m-%d')
        self.__last_date_str = date_utils.get_last_trading_day('%Y-%m-%d', self.__date_str)
        self.__server_host = server_constant.get_server_model('host')

        self.__instrument_df = None
        self.__future_main_contract_dict = dict()

        self.__strategy_name_list = []
        self.__pf_position_df = None
        self.__target_position_df = None
        self.__market_value_df = None
        self.__real_position_df = None
        self.__real_trade_df = None
        self.__strategy_statistics_df = None
        self.__sum_info_df = None
        self.__phone_trade_df = None

        self.__suspend_stock_list = []
        self.__st_stock_list = []
        self.__low_stop_stock_list = []
        self.__high_stop_stock_list = []

        self.__base_save_folder = ''

    def strategy_basket_file_build(self, cut_down_money=0, add_money=0):
        custom_log.log_info_job('Server:%s Operation:%s Start.' % (self.__server_name, self.__operation_type))
        self.__make_base_folder()
        self.__build_db_dict()

        self.__cut_down_money = int(cut_down_money)
        if self.__operation_type == operation_enums.Cutdown and cut_down_money == 0:
            self.__cut_down_money = self.__query_ic_money()
        self.__add_money = int(add_money)
        if self.__operation_type == operation_enums.Add and add_money == 0:
            self.__add_money = self.__query_ic_money()

        self.__strategy_name_list = self.__query_strategy_basket_list()

        if self.__operation_type == operation_enums.Add:
            self.__pf_position_df = pd.DataFrame(columns=['Strategy_Name', 'Symbol', 'Qty'])
            self.__target_position_df = self.__query_target_position()
            self.__real_position_df = self.__query_real_position()
        elif self.__operation_type == operation_enums.Change:
            self.__pf_position_df = self.__query_pf_position()
            self.__target_position_df = self.__query_target_position()
            self.__real_position_df = self.__query_real_position()
        elif self.__operation_type == operation_enums.Close:
            self.__real_position_df, self.__pf_position_df = self.__query_actual_position()
            self.__target_position_df = pd.DataFrame(columns=['Strategy_Name', 'Symbol', 'Target_Weight'])
        elif self.__operation_type == operation_enums.Cutdown:
            self.__real_position_df, self.__pf_position_df = self.__query_actual_position()
            self.__target_position_df = pd.DataFrame(columns=['Strategy_Name', 'Symbol', 'Target_Weight'])
        elif self.__operation_type == operation_enums.Close_Bits:
            self.__pf_position_df = self.__query_pf_position()
            self.__target_position_df = pd.DataFrame(columns=['Strategy_Name', 'Symbol', 'Target_Weight'])
            self.__real_position_df = self.__query_real_position()
        self.__information_integration()
        self.__rebuild_basket_files()
        custom_log.log_info_job('Server:%s Operation:%s Stop.' % (self.__server_name, self.__operation_type))
        return self.__build_report_email()

    def split_sigmavwap_ai(self):
        """
            将change下文件切分为两个文件
        """
        deeplearning_ticker_list = []
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for x in session_jobs.query(StrategyTickerParameter)\
                             .filter(StrategyTickerParameter.server_name == self.__server_name,
                                     StrategyTickerParameter.strategy == 'Stock_DeepLearning'):
            deeplearning_ticker_list.append(x.ticker)

        change_files_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name,
                                               self.__date_str.replace('-', ''), 'change')
        base_file_list = [x for x in os.listdir(change_files_folder) if x.endswith('.txt')]

        for file_name in base_file_list:
            ticker_list = []
            ai_ticker_list = []
            with open(os.path.join(change_files_folder, file_name), 'rb') as fr:
                for line in fr.readlines():
                    ticker, volume = line.replace('\r\n', '').split(',')
                    if ticker in deeplearning_ticker_list:
                        ai_ticker_list.append('%s,%s' % (ticker, volume))
                    else:
                        ticker_list.append('%s,%s' % (ticker, volume))

            if len(ticker_list) > 0:
                rename_file_name = file_name
                with open(os.path.join(change_files_folder, rename_file_name), 'w') as fr:
                    fr.write('\n'.join(ticker_list))
            if len(ai_ticker_list) > 0:
                ai_rename_file_name = file_name.replace('SigmaVWAP', 'SigmaVWAP_AI')
                with open(os.path.join(change_files_folder, ai_rename_file_name), 'w') as fr:
                    fr.write('\n'.join(ai_ticker_list))

    def split_huabao_basket(self):
        source_folder_path = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, 'huabao',
                                              self.__date_str.replace('-', ''), 'change')
        target_folder_path1 = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, 'huabao',
                                               self.__date_str.replace('-', ''), 'change_special')
        target_folder_path2 = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, 'guosen',
                                               self.__date_str.replace('-', ''), 'change_special')
        for temp_path in (target_folder_path1, target_folder_path2):
            if not os.path.exists(temp_path):
                os.makedirs(temp_path)

        file_list = [file_name for file_name in os.listdir(source_folder_path) if file_name.endswith('.txt')]
        for file_name in file_list:
            target_info_list1, target_info_list2 = [], []
            with open(os.path.join(source_folder_path, file_name), 'rb') as fr:
                for line in fr.readlines():
                    ticker, volume = line.replace('\r\n', '').split(',')
                    if int(volume) < 0:
                        target_info_list1.append(line.replace('\r\n', ''))
                    else:
                        target_info_list2.append(line.replace('\r\n', ''))

            target_file_path1 = os.path.join(target_folder_path1, file_name)
            with open(target_file_path1, 'w') as fr:
                fr.write('\n'.join(target_info_list1))

            target_file_path2 = os.path.join(target_folder_path2, file_name.replace('172.16.10.120', '172.16.10.196'))
            with open(target_file_path2, 'w') as fr:
                fr.write('\n'.join(target_info_list2))

    def ticker_index_report(self):
        shsz300_ticker_list, sh000905_ticker_list = self.__query_index_ticker_list()

        pf_account_list = []
        session_jobs = self.__server_host.get_db_session('jobs')
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

        output_file_path = '%s/%s/%s' % (STRATEGY_FILE_PATH_DICT['MultiFactor'], self.__date_str.replace('-', ''),
                                         'index_structure_report.csv')
        strategy_ticker_df.to_csv(output_file_path, index=0)

    def check_basket_file(self):
        check_result_list = []
        server_model = server_constant.get_server_model(self.__server_name)
        change_files_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name,
                                               self.__date_str.replace('-', ''), 'change')
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for item in session_jobs.query(StrategyAccountInfo).filter(StrategyAccountInfo.server_name == self.__server_name):
            for temp_strategy_name in ['SigmaVWAP_AI', ]:
                pf_account_name = '%s-%s-%s-' % (item.strategy_name, item.group_name, item.fund)
                check_file_name = '%s-%s-%s@%s-%s-%s-.txt' % (item.strategy_name, item.group_name, item.fund,
                                                              server_model.ip, temp_strategy_name, 'OppositeSide')
                check_file_path = os.path.join(change_files_folder, check_file_name)
                if not os.path.exists(check_file_path):
                    check_result_list.append(
                        (self.__server_name, pf_account_name, 'File:%s Missing!(Error)' % check_file_path))
        return check_result_list

    def __query_index_ticker_list(self):
        shsz300_ticker_list = []
        sh000905_ticker_list = []
        session_common = self.__server_host.get_db_session('common')
        for index_instrument in session_common.query(Instrument).filter(Instrument.ticker.in_(('SHSZ300', 'SH000905'))):
            if index_instrument.ticker == 'SHSZ300':
                shsz300_ticker_list.extend(index_instrument.indx_members.split(';'))
            elif index_instrument.ticker == 'SH000905':
                sh000905_ticker_list.extend(index_instrument.indx_members.split(';'))
        return shsz300_ticker_list, sh000905_ticker_list

    def __query_ic_money(self):
        """
            获取当前主力IC合约一手的资金
        """
        session_common = self.__server_host.get_db_session('common')
        ic_item = session_common.query(FutureMainContract).filter(FutureMainContract.ticker_type == 'IC').first()
        ic_instrument = session_common.query(Instrument).filter(Instrument.ticker == ic_item.main_symbol).first()
        add_money = float(ic_instrument.prev_close) * float(ic_instrument.fut_val_pt)
        return add_money

    def __rebuild_basket_files(self):
        if self.__operation_type == operation_enums.Change:
            target_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name,
                                             self.__date_str.replace('-', ''), 'change')
            if os.path.exists(target_folder):
                shutil.rmtree(target_folder)
            os.mkdir(target_folder)
            for file_name in os.listdir(self.__base_save_folder):
                shutil.copyfile('%s/%s' % (self.__base_save_folder, file_name),
                                '%s/%s' % (target_folder, file_name))
            self.__base_save_folder = target_folder
            # 检查是否有phone trade
            self.__build_phone_trade_file()
        # 修改订单名称
        self.__rebuild_file_name()

    def __build_phone_trade_file(self):
        real_trade_df = self.__real_trade_df.copy()

        cross_trade_list = []
        for group_key, group in real_trade_df.groupby(['Fund_Name', 'Symbol']):
            buy_qty = 0
            sell_qty = 0

            group_list = [value for (key, value) in group.to_dict("index").items()]
            for group_item in group_list:
                if group_item['Trade_Qty'] > 0:
                    buy_qty += group_item['Trade_Qty']
                else:
                    sell_qty += group_item['Trade_Qty']
            if buy_qty == 0 or sell_qty == 0:
                continue

            cross_qty = min(buy_qty, abs(sell_qty))
            cross_buy_qty, cross_sell_qty = cross_qty, -cross_qty
            for group_item in group_list:
                if group_item['Trade_Qty'] > 0 and cross_buy_qty > 0:
                    cross_trade_qty = min(group_item['Trade_Qty'], cross_buy_qty)
                    cross_trade_list.append([group_item['Strategy_Name'], group_key[1], cross_trade_qty])
                    cross_buy_qty -= cross_trade_qty
                elif group_item['Trade_Qty'] < 0 and cross_sell_qty < 0:
                    cross_trade_qty = max(group_item['Trade_Qty'], cross_sell_qty)
                    cross_trade_list.append([group_item['Strategy_Name'], group_key[1], cross_trade_qty])
                    cross_sell_qty -= cross_trade_qty
        if len(cross_trade_list) == 0:
            return

        cross_trade_df = pd.DataFrame(cross_trade_list, columns=['Strategy_Name', 'Symbol', 'Cross_Qty'])
        real_trade_df = pd.merge(real_trade_df, cross_trade_df, how='left', on=['Strategy_Name', 'Symbol']).fillna(0)

        # 生成phone_trade文件
        phone_trade_df = real_trade_df[real_trade_df['Cross_Qty'] != 0]
        self.__phone_trade_df = phone_trade_df
        self.__build_phone_trade(phone_trade_df)

        # 重新生成扣除phone_trade数量后的调仓文件
        real_trade_df.loc[:, 'Trade_Qty'] = real_trade_df.Trade_Qty - real_trade_df.Cross_Qty
        real_trade_df = real_trade_df[real_trade_df['Trade_Qty'] != 0]
        for group_key, group in real_trade_df.groupby(['Strategy_Name']):
            strategy_trade_list = np.array(group[['Symbol', 'Trade_Qty']]).tolist()
            out_put_list = ['%s,%s' % (x[0], int(x[1])) for x in strategy_trade_list]
            file_path = '%s/%s.txt' % (self.__base_save_folder, group_key)
            with open(file_path, 'w') as fr:
                fr.write('\n'.join(out_put_list))

    def __build_phone_trade(self, phone_trade_df):
        phone_trade_list = []
        trade_info_list = [value for (key, value) in phone_trade_df.to_dict("index").items()]
        for phone_trade_item in trade_info_list:
            phone_trade_info = PhoneTradeInfo()
            phone_trade_info.fund = phone_trade_item['Fund_Name']

            strategy_name_item = phone_trade_item['Strategy_Name'].split('-')
            phone_trade_info.strategy1 = '%s.%s' % (strategy_name_item[1], strategy_name_item[0])
            phone_trade_info.symbol = phone_trade_item['Symbol']
            phone_trade_info.direction = Direction_Enums.Sell if phone_trade_item['Cross_Qty'] < 0 else Direction_Enums.Buy
            phone_trade_info.tradetype = Trade_Type_Enums.Normal
            phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
            phone_trade_info.exprice = phone_trade_item['Prev_Close']
            phone_trade_info.exqty = abs(phone_trade_item['Cross_Qty'])
            phone_trade_info.iotype = IO_Type_Enums.Inner1
            phone_trade_info.server_name = self.__server_name
            phone_trade_list.append(phone_trade_info)
        phone_trade_list.sort(key=lambda item: '%s|%s|%s' % (item.fund, item.symbol, item.exqty))

        phone_trade_file_name = 'phone_trade.csv'
        phone_trade_file_path = '%s/%s' % (self.__base_save_folder, phone_trade_file_name)
        save_phone_trade_file(phone_trade_file_path, phone_trade_list, False)

    def __rebuild_file_name(self):
        rename_config_dict = dict()
        with open(STOCK_SELECTION_CONFIG_FILE, 'r') as fr:
            for line in islice(fr, 1, None):
                _server_name, _group_name, _operation_type, _algo_type, _peg_level = line.replace('\n', '').split(',')
                dict_key = '%s|%s|%s' % (_server_name, _group_name, _operation_type)
                rename_config_dict[dict_key] = (_algo_type, _peg_level)

        server_model = server_constant.get_server_model(self.__server_name)
        for file_name in os.listdir(self.__base_save_folder):
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
            os.rename('%s/%s' % (self.__base_save_folder, file_name), '%s/%s' % (self.__base_save_folder, rename_str))

    def __query_strategy_basket_list(self):
        strategy_name_list = []
        session_jobs = self.__server_host.get_db_session('jobs')
        for item in session_jobs.query(StrategyAccountInfo)\
                .filter(StrategyAccountInfo.server_name == self.__server_name):
            # Close_Bits时只包含default的仓位
            if self.__operation_type == operation_enums.Close_Bits:
                strategy_name_list.append('default-manual-%s-' % item.fund)
                continue

            # Close时包含default的仓位
            if self.__operation_type == operation_enums.Close:
                strategy_name_list.append('default-manual-%s-' % item.fund)

            pf_account_name = '%s-%s-%s-' % (item.strategy_name, item.group_name, item.fund)
            strategy_name_list.append(pf_account_name)
        return list(set(strategy_name_list))

    def __build_db_dict(self):
        instrument_list = []
        session_common = self.__server_host.get_db_session('common')
        for x in session_common.query(Instrument).filter(Instrument.del_flag == 0,
                                                         Instrument.type_id == Instrument_Type_Enums.CommonStock):
            instrument_list.append([x.ticker, x.prev_close])
        self.__instrument_df = pd.DataFrame(instrument_list, columns=['Symbol', 'Prev_Close'])

        special_ticker_list = []
        session_jobs = self.__server_host.get_db_session('jobs')
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

    def __query_pf_position(self):
        # 从数据库获取策略仓位信息
        server_model = server_constant.get_server_model(self.__server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        pf_account_dict = {x.id: x.fund_name for x in session_portfolio.query(PfAccount)}

        pf_position_list = []
        for x in session_portfolio.query(PfPosition).filter(PfPosition.date == self.__date_str):
            if not x.symbol.isdigit():
                continue
            strategy_name = pf_account_dict[x.id]
            if strategy_name not in self.__strategy_name_list:
                continue
            pf_position_list.append([strategy_name, x.symbol, int(x.long)])
        pf_position_df = pd.DataFrame(pf_position_list, columns=['Strategy_Name', 'Symbol', 'Qty'])
        return pf_position_df

    def __query_actual_position(self):
        # 从aggregation获取实时的策略仓位信息
        aggregator_message_utils = AggregatorMessageUtils()
        aggregator_message_utils.login_aggregator()
        instrument_msg_dict = aggregator_message_utils.query_instrument_dict()
        position_risk_msg = aggregator_message_utils.query_position_risk_msg()

        real_position_list = []
        real_position_dict = dict()
        for holding_item in position_risk_msg.Holdings2:
            full_account_name = holding_item.Key
            (account_name, server_ip_str) = full_account_name.split('@')
            fund_name = account_name.split('-')[2]
            server_name = common_utils.get_server_name(server_ip_str)
            if server_name != self.__server_name:
                continue

            for position_msg_info in holding_item.Value:
                instrument_msg = instrument_msg_dict[int(position_msg_info.Key)]
                ticker = instrument_msg.ticker
                if not ticker.isdigit():
                    continue
                volume = position_msg_info.Value.PrevLongAvailable
                real_position_list.append([fund_name, ticker, volume])
                real_position_dict['%s|%s' % (fund_name, ticker)] = volume
        real_position_df = pd.DataFrame(real_position_list, columns=['Fund_Name', 'Symbol', 'Account_Qty'])

        pf_position_list = []
        for holding_item in position_risk_msg.Holdings:
            strategy_name = holding_item.Key
            (base_strategy_name, server_ip_str) = strategy_name.split('@')
            server_name = common_utils.get_server_name(server_ip_str)
            fund_name = base_strategy_name.split('-')[2]
            if server_name != self.__server_name:
                continue
            if base_strategy_name not in self.__strategy_name_list:
                continue

            for risk_msg_info in holding_item.Value:
                instrument_msg = instrument_msg_dict[int(risk_msg_info.Key)]
                ticker = instrument_msg.ticker
                volume = risk_msg_info.Value.YdLongRemain

                # 跟实际账户的可用仓位比对
                find_key = '%s|%s' % (fund_name, ticker)
                if find_key in real_position_dict:
                    volume = volume if volume < real_position_dict[find_key] else real_position_dict[find_key]
                pf_position_list.append([base_strategy_name, ticker, volume])
        pf_position_df = pd.DataFrame(pf_position_list, columns=['Strategy_Name', 'Symbol', 'Qty'])
        return real_position_df, pf_position_df

    # 根据配置文件获取需购买的股票和权重
    def __query_target_position(self):
        target_position_list = []
        for strategy_name in self.__strategy_name_list:
            strategy_file_path = self.__query_strategy_file_path(strategy_name)
            with open(strategy_file_path, 'rb') as strategy_file:
                for line in islice(strategy_file, 1, None):
                    line_items = line.split(',')
                    ticker_str = line_items[1]
                    weight = float(line_items[2])
                    ticker_full = str(filter(lambda x: x.isdigit(), ticker_str))
                    ticker_full = ticker_full.zfill(6)
                    target_position_list.append([strategy_name, ticker_full, weight])
        target_position_df = pd.DataFrame(target_position_list, columns=['Strategy_Name', 'Symbol', 'Target_Weight'])
        return target_position_df

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

        strategy_file_path = '%s/%s/%s' % (STRATEGY_FILE_PATH_DICT[group_name], self.__date_str.replace('-', ''),
                                           strategy_file_name)
        if not os.path.exists(strategy_file_path):
            email_utils2.send_email_group_all('[Error]Algo File Miss!', 'Miss File:%s' % strategy_file_path, 'html')
            raise Exception("[Error]Miss File:%s" % strategy_file_path)
        return strategy_file_path

    def __query_last_trade_info(self):
        filter_date_str = self.__last_date_str.replace('-', '')
        last_change_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name, filter_date_str, 'base')

        last_trade_list = []
        for strategy_name in self.__strategy_name_list:
            for file_name in os.listdir(last_change_folder):
                if not file_name.endswith('.txt') or strategy_name not in file_name:
                    continue

                with open('%s/%s' % (last_change_folder, file_name), 'rb') as fr:
                    for x in fr.readlines():
                        ticker, volume = x.replace('\n', '').split(',')
                        last_trade_list.append([strategy_name, ticker, int(volume)])
        last_trade_df = pd.DataFrame(last_trade_list, columns=['Strategy_Name', 'Symbol', 'Last_Trade_Qty'])
        return last_trade_df

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
        base_save_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name,
                                            self.__date_str.replace('-', ''), folder_suffix)
        if os.path.exists(base_save_folder):
            shutil.rmtree(base_save_folder)
        os.makedirs(base_save_folder)
        self.__base_save_folder = base_save_folder

    def __query_real_position(self):
        server_model = server_constant.get_server_model(self.__server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        real_account_dict = {x.accountid: x.fund_name for x in session_portfolio.query(RealAccount)}

        real_position_list = []
        for x in session_portfolio.query(AccountPosition).filter(AccountPosition.date == self.__date_str):
            if not x.symbol.isdigit():
                continue
            fund_name = real_account_dict[x.id]
            real_position_list.append([fund_name, x.symbol, x.long_avail])
        real_position_df = pd.DataFrame(real_position_list, columns=['Fund_Name', 'Symbol', 'Account_Qty'])
        return real_position_df

    def __information_integration(self):
        # 合并策略持仓和目标持仓信息
        pf_position_df = self.__pf_position_df.set_index(['Strategy_Name', 'Symbol'])
        target_position_df = self.__target_position_df.set_index(['Strategy_Name', 'Symbol'])
        trade_message_df = pd.concat([pf_position_df, target_position_df], axis=1).fillna(0)
        trade_message_df = trade_message_df.reset_index()

        # 计算当前市值，各股票所占比例，目标市值
        trade_message_df = pd.merge(trade_message_df, self.__instrument_df, how='left', on=['Symbol']).fillna(0)
        trade_message_df = trade_message_df[trade_message_df['Prev_Close'] != 0]
        trade_message_df.loc[:, 'Market_Value'] = trade_message_df['Qty'] * trade_message_df['Prev_Close']
        market_value_df = trade_message_df[['Strategy_Name', 'Market_Value']].groupby(['Strategy_Name']).sum().reset_index()

        total_market_value_df = market_value_df.copy()
        total_market_value_df.rename(columns={'Market_Value': 'Total_Market_Value'}, inplace=True)
        trade_message_df = pd.merge(trade_message_df, total_market_value_df, how='left', on=['Strategy_Name']).fillna(0)
        trade_message_df.loc[:, 'Weight'] = trade_message_df['Market_Value'] / trade_message_df['Total_Market_Value']

        # temp_trade_message_df = trade_message_df.copy()
        # temp_trade_message_df = temp_trade_message_df[-temp_trade_message_df['Symbol'].isin(['603828', ])]
        # temp_market_value_df = temp_trade_message_df[['Strategy_Name', 'Market_Value']].groupby(['Strategy_Name']).sum().reset_index()
        # target_market_value_df = temp_market_value_df.copy()

        target_market_value_df = market_value_df.copy()
        target_market_value_df.rename(columns={'Market_Value': 'Target_Market_Value'}, inplace=True)
        trade_message_df = pd.merge(trade_message_df, target_market_value_df, how='left', on=['Strategy_Name']).fillna(0)
        if self.__operation_type == operation_enums.Cutdown:
            trade_message_df.loc[:, 'Target_Market_Value'] = trade_message_df['Total_Market_Value'] - self.__cut_down_money
            trade_message_df.loc[:, 'Target_Weight'] = trade_message_df['Weight']
        elif self.__operation_type == operation_enums.Add:
            trade_message_df.loc[:, 'Target_Market_Value'] = self.__add_money
        elif self.__operation_type == operation_enums.Close:
            trade_message_df.loc[:, 'Target_Market_Value'] = 0
            trade_message_df.loc[:, 'Target_Weight'] = 0
        elif self.__operation_type == operation_enums.Close_Bits:
            trade_message_df.loc[:, 'Target_Market_Value'] = 0
            trade_message_df.loc[:, 'Target_Weight'] = 0

        # 过滤异常Symbol
        trade_message_df.loc[:, 'Error_Info'] = ''
        trade_message_df.loc[trade_message_df['Symbol'].isin(self.__suspend_stock_list), 'Error_Info'] = 'Ticker Suspend'
        trade_message_df.loc[trade_message_df['Symbol'].isin(self.__st_stock_list), 'Error_Info'] = 'Ticker ST'

        last_trade_df = self.__query_last_trade_info()
        trade_message_df = pd.merge(trade_message_df, last_trade_df, how='left', on=['Strategy_Name', 'Symbol']).fillna(0)
        trade_message_df.loc[:, 'Weight_Change'] = trade_message_df['Target_Weight'] - trade_message_df['Weight']
        trade_message_df.loc[(trade_message_df['Symbol'].isin(self.__high_stop_stock_list)) &
                             (trade_message_df['Weight_Change'] > 0) &
                             (trade_message_df['Last_Trade_Qty'] > 0), 'Error_Info'] = 'Ticker High Stop'
        trade_message_df.loc[(trade_message_df['Symbol'].isin(self.__low_stop_stock_list)) &
                             (trade_message_df['Weight_Change'] < 0) &
                             (trade_message_df['Last_Trade_Qty'] < 0), 'Error_Info'] = 'Ticker Low Stop'

        # 过滤掉异常后重新调整权重
        filter_trade_df = trade_message_df[trade_message_df['Error_Info'] == '']
        round_df = filter_trade_df[['Strategy_Name', 'Weight']].groupby(['Strategy_Name']).sum().reset_index()
        round_df.loc[:, 'Round_PCT'] = 1 / round_df['Weight']
        round_df['Round_PCT'][round_df['Weight'] == 0] = 1
        filter_trade_df = pd.merge(filter_trade_df, round_df[['Strategy_Name', 'Round_PCT']], how='left', on=['Strategy_Name']).fillna(0)
        filter_trade_df.loc[:, 'Round_Weight'] = filter_trade_df['Weight'] * filter_trade_df['Round_PCT']

        if self.__operation_type == operation_enums.Close or self.__operation_type == operation_enums.Close_Bits:
            filter_trade_df.loc[:, 'Target_Round_PCT'] = 1
            filter_trade_df.loc[:, 'Target_Round_Weight'] = 0
        else:
            target_round_df = filter_trade_df[['Strategy_Name', 'Target_Weight']].groupby(['Strategy_Name']).sum().reset_index()
            target_round_df.loc[:, 'Target_Round_PCT'] = 1 / target_round_df['Target_Weight']
            filter_trade_df = pd.merge(filter_trade_df, target_round_df[['Strategy_Name', 'Target_Round_PCT']], how='left', on=['Strategy_Name']).fillna(0)
            filter_trade_df.loc[:, 'Target_Round_Weight'] = filter_trade_df['Target_Weight'] * filter_trade_df['Target_Round_PCT']

        # 部分满足条件的一字涨停和一字跌停股票需要补买或补卖
        supplement_trade_df = trade_message_df[trade_message_df['Error_Info'].isin(('Ticker High Stop', 'Ticker Low Stop'))]
        if not supplement_trade_df.empty:
            supplement_trade_df.loc[:, 'Round_PCT'] = 1
            supplement_trade_df.loc[:, 'Round_Weight'] = supplement_trade_df['Weight']
            supplement_trade_df.loc[:, 'Target_Round_PCT'] = 1
            supplement_trade_df.loc[:, 'Target_Round_Weight'] = supplement_trade_df['Target_Weight']
            filter_trade_df = pd.concat([filter_trade_df, supplement_trade_df])

        # 计算具体交易量
        filter_trade_df.loc[:, 'Trade_Qty'] = filter_trade_df.apply(lambda row: format_trade_qty(row), axis=1)
        if self.__operation_type == operation_enums.Close_Bits:
            filter_trade_df.loc[:, 'Trade_Qty'] = filter_trade_df['Qty']

        filter_trade_df.loc[:, 'Fund_Name'] = filter_trade_df.apply(lambda row: row['Strategy_Name'].split('-')[2], axis=1)
        # 和真实仓位进行比对校验
        filter_trade_df = pd.merge(filter_trade_df, self.__real_position_df, how='left', on=['Fund_Name', 'Symbol']).fillna(0)
        filter_trade_df.loc[:, 'Error_Info_Attach'] = ''

        if self.__operation_type == operation_enums.Close_Bits:
            filter_trade_df.loc[filter_trade_df['Account_Qty'] >= 100, 'Error_Info_Attach'] = 'Cannot Close Bits(Warning)'
            filter_trade_df.loc[filter_trade_df['Account_Qty'] >= 100, 'Trade_Qty'] = 0
        else:
            # ----a.卖量超过真实仓位
            filter_trade_df.loc[(filter_trade_df['Trade_Qty'] < 0) &
                                (filter_trade_df['Trade_Qty'] + filter_trade_df['Account_Qty'] < 0), 'Error_Info_Attach'] = 'Sell More Than Real Position(Error)'
            filter_trade_df.loc[(filter_trade_df['Trade_Qty'] < 0) &
                                (filter_trade_df['Trade_Qty'] + filter_trade_df['Account_Qty'] < 0), 'Trade_Qty'] = -filter_trade_df['Account_Qty']
            # ----b.卖量非整百，且小于真实仓位
            filter_trade_df.loc[(filter_trade_df['Trade_Qty'] < 0) &
                                (filter_trade_df['Trade_Qty'] % 100 != 0) &
                                (filter_trade_df['Trade_Qty'] + filter_trade_df['Account_Qty'] != 0), 'Error_Info_Attach'] = 'Cannot Whole Sell(Error)'
            # ----c.持仓量小于100
            filter_trade_df.loc[(filter_trade_df['Qty'] < 100) &
                                (filter_trade_df['Qty'] > 0), 'Error_Info_Attach'] = 'Volume Less Than 100(Error)'
            filter_trade_df.loc[(filter_trade_df['Qty'] < 100) &
                                (filter_trade_df['Qty'] > 0), 'Trade_Qty'] = 0

        # 生成具体调仓文件
        filter_trade_df = filter_trade_df.sort_values(by=['Strategy_Name', 'Trade_Qty'])
        self.__real_trade_df = filter_trade_df[filter_trade_df['Trade_Qty'] != 0].copy()
        for group_key, group in self.__real_trade_df.groupby(['Strategy_Name']):
            strategy_trade_list = np.array(group[['Symbol', 'Trade_Qty']]).tolist()
            out_put_list = ['%s,%s' % (x[0], int(x[1])) for x in strategy_trade_list]
            file_path = '%s/%s.txt' % (self.__base_save_folder, group_key)
            with open(file_path, 'w') as fr:
                fr.write('\n'.join(out_put_list))

        # 生成统计文件
        filter_trade_df.loc[:, 'Direction'] = 'Buy'
        filter_trade_df.loc[filter_trade_df.Trade_Qty < 0, 'Direction'] = 'Sell'
        filter_trade_df.loc[:, 'Trade_Money'] = filter_trade_df['Trade_Qty'] * filter_trade_df['Prev_Close']
        strategy_statistics_list = []
        for group_key, group in filter_trade_df.groupby(['Strategy_Name', 'Direction']):
            strategy_statistics_list.append([group_key[0], group_key[1], group['Trade_Money'].sum()])
        strategy_statistics_df = pd.DataFrame(strategy_statistics_list, columns=['Strategy_Name', 'Direction', 'Money'])
        strategy_statistics_df = strategy_statistics_df.pivot_table(index='Strategy_Name', columns='Direction', values='Money', aggfunc=np.sum)

        if 'Buy' not in strategy_statistics_df.columns:
            strategy_statistics_df.loc[:, 'Buy'] = 0
        if 'Sell' not in strategy_statistics_df.columns:
            strategy_statistics_df.loc[:, 'Sell'] = 0
        strategy_statistics_df = strategy_statistics_df.fillna(0)
        strategy_statistics_df.loc[:, 'Diff'] = strategy_statistics_df['Buy'] + strategy_statistics_df['Sell']
        strategy_statistics_df.loc[:, 'Strategy_Name'] = strategy_statistics_df.index

        output_file_path = '%s/sum_info_%s_%s.csv' % (self.__base_save_folder, self.__server_name,
                                                      self.__date_str.replace('-', ''))
        strategy_statistics_df.to_csv(output_file_path, columns=['Strategy_Name', 'Buy', 'Sell', 'Diff'], index=0)
        self.__strategy_statistics_df = strategy_statistics_df

        filter_columns = ['Strategy_Name', 'Symbol', 'Account_Qty', 'Round_PCT', 'Round_Weight',
                          'Target_Round_PCT', 'Target_Round_Weight', 'Trade_Qty', 'Error_Info_Attach']
        sum_info_df = pd.merge(trade_message_df, filter_trade_df[filter_columns], how='left', on=['Strategy_Name', 'Symbol'])

        sum_info_df.loc[:, 'Error_Info'] = sum_info_df['Error_Info'].fillna('') + sum_info_df['Error_Info_Attach'].fillna('')
        sum_info_df = sum_info_df.sort_values(by=['Strategy_Name', 'Symbol'])
        trade_info_columns = ['Strategy_Name', 'Symbol', 'Account_Qty', 'Qty', 'Prev_Close', 'Weight', 'Round_PCT',
                              'Round_Weight', 'Total_Market_Value', 'Target_Weight', 'Target_Round_PCT',
                              'Target_Round_Weight', 'Target_Market_Value', 'Trade_Qty', 'Error_Info']
        sum_info_df.to_csv(output_file_path, columns=trade_info_columns, mode='a', index=0)
        self.__sum_info_df = sum_info_df

    def __build_report_email(self):
        # 生成邮件提醒信息
        email_trade_list = ['--------------Server:[%s] Operation:[%s]-----------------' % \
                            (self.__server_name, self.__operation_type)]
        error_info_df = self.__sum_info_df[self.__sum_info_df['Error_Info'] != ''][['Strategy_Name', 'Symbol', 'Error_Info']]
        if not error_info_df.empty:
            error_info_df['Server'] = self.__server_name
            error_info_title = 'Strategy_Name, Symbol, Error_Info, Server'
            error_info_list = np.array(error_info_df).tolist()
            email_trade_list.extend(email_utils2.list_to_html(error_info_title, error_info_list))

        strategy_statistics_title = 'Strategy_Name, Buy, Sell, Diff'
        strategy_statistics_list = np.array(self.__strategy_statistics_df[['Strategy_Name', 'Buy', 'Sell', 'Diff']]).tolist()
        email_trade_list.extend(email_utils2.list_to_html(strategy_statistics_title, strategy_statistics_list))
        email_trade_list.append('<br><br>')

        email_detail_list = []
        if self.__phone_trade_df is not None:
            self.__phone_trade_df = self.__phone_trade_df.sort_values(by=['Fund_Name', 'Symbol', 'Cross_Qty'])

            phone_trade_title = 'Strategy_Name, Symbol, Trade_Qty, Cross_Qty'
            phone_trade_list = np.array(self.__phone_trade_df[['Strategy_Name', 'Symbol', 'Trade_Qty', 'Cross_Qty']]).tolist()
            email_detail_list.extend(email_utils2.list_to_html(phone_trade_title, phone_trade_list))
        return email_trade_list, email_detail_list


# def huabao_close_special():
#     close_folder_path = 'Z:/dailyjob/StockSelection/huabao/20190329_close'
#     change_special_folder_path = 'Z:/dailyjob/StockSelection/huabao/20190329_change_special'
#
#     change_special_dict = dict()
#     for file_name in os.listdir(change_special_folder_path):
#         if not file_name.endswith('.txt'):
#             continue
#         info_items = file_name.split('@')[0].split('-')
#         strategy_name, fund_name = info_items[0], info_items[2]
#         with open(os.path.join(change_special_folder_path, file_name), 'r') as fr:
#             change_info_list = [x.replace('\n', '') for x in fr.readlines()]
#         dict_key = '%s|%s' % (strategy_name, fund_name)
#         change_special_dict.setdefault(dict_key, []).extend(change_info_list)
#
#     for file_name in os.listdir(close_folder_path):
#         if not file_name.endswith('.txt'):
#             continue
#
#         with open(os.path.join(close_folder_path, file_name), 'r') as fr:
#             close_info_list = [x.replace('\n', '') for x in fr.readlines()]
#
#         info_items = file_name.split('@')[0].split('-')
#         strategy_name, fund_name = info_items[0], info_items[2]
#         dict_key = '%s|%s' % (strategy_name, fund_name)
#         rebuild_close_list = []
#         if dict_key not in change_special_dict:
#             rebuild_close_list = close_info_list
#         else:
#             change_info_list = change_special_dict[dict_key]
#
#             for close_info in close_info_list:
#                 close_ticker, close_qty = close_info.split(',')
#                 rebuild_close_qty = int(close_qty)
#                 for change_info in change_info_list:
#                     change_ticker, change_qty = change_info.split(',')
#                     if change_ticker != close_ticker:
#                         continue
#                     rebuild_close_qty -= int(change_qty)
#                 if rebuild_close_qty == 0:
#                     continue
#                 rebuild_close_list.append('%s,%s' % (close_ticker, rebuild_close_qty))
#
#         with open('E:/test/huabao_close_special/%s' % file_name, 'w') as fr:
#             fr.write('\n'.join(rebuild_close_list))


if __name__ == '__main__':
    # strategy_basket_info = StrategyBasketInfo('citics', operation_enums.Change)
    # strategy_basket_info.strategy_basket_file_build()
    # strategy_basket_info.split_huabao_basket()
    # strategy_basket_info = StrategyBasketInfo('huabao', operation_enums.Close)
    # strategy_basket_info.strategy_basket_file_build()
    strategy_basket_info = StrategyBasketInfo('citics', operation_enums.Close_Bits)
    strategy_basket_info.strategy_basket_file_build()
