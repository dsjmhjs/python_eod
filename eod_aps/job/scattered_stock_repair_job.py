# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
from eod_aps.model.schema_portfolio import RealAccount, PfAccount, PfPosition
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.tools.aggregator_message_utils import AggregatorMessageUtils
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.phone_trade_tools import send_phone_trade, save_phone_trade_file, notify_phone_trade_list
from eod_aps.job import *

common_utils = CommonUtils()
instrument_type_inversion_dict = custom_enum_utils.enum_to_dict(const.INSTRUMENT_TYPE_ENUMS, True)


def calculation_target_volume(number_input):
    return int(int(float(number_input) / float(100) + 1) * 100)


def round_down(number_input):
    """
        对股数向下取整
    :param number_input:
    :return:
    """
    return int(int(float(number_input) / float(100)) * 100)


class ScatteredStockTools(object):
    def __init__(self, server_name):
        self.__server_name = server_name
        self.__server_model = server_constant.get_server_model(server_name)
        self.__filter_date_str = date_utils.get_today_str('%Y%m%d')
        self.__default_strategy_key = 'default-manual'
        self.__multifactor_strategy_key = 'MultiFactor'
        self.__intraday_strategy_key = 'StkIntraDayStrategy'
        self.__alarm_num = 50
        self.__pf_account_df = None
        self.__phone_trade_list = []

    def start_index(self):
        """
           入口函數
        """
        need_check_flag = self.abnormal_stock_repair_index()
        if not need_check_flag:
            time.sleep(30)
            self.default_strategy_repair_index()

            # 邮件通知
            if self.__phone_trade_list:
                notify_phone_trade_list(self.__phone_trade_list)

    def abnormal_stock_repair_index(self):
        """
             策略异常股票仓位处理
        """
        self.__load_data_from_aggregator()

        # 处理Multifactor零股至default
        multifactor_phone_trades = self.__multifactor_stock_repair()
        self.__phone_trade_list.extend(multifactor_phone_trades)
        # 处理日内策略剩余股票
        intraday_phone_trades = self.__intraday_strategy_repair()
        self.__phone_trade_list.extend(intraday_phone_trades)

        # 未超过设置的条目,直接发送phone_trade;超过了则保存文件,人工检查后再发送
        need_check_flag = False
        if len(self.__phone_trade_list) <= self.__alarm_num:
            send_phone_trade(self.__server_name, self.__phone_trade_list)
        else:
            server_save_path = os.path.join(PHONE_TRADE_FOLDER, self.__server_name)
            if not os.path.exists(server_save_path):
                os.mkdir(server_save_path)
            phone_trade_file_path = '%s/abnormal_stock_repair_%s.csv' % (server_save_path, self.__filter_date_str)
            save_phone_trade_file(phone_trade_file_path, self.__phone_trade_list)
            need_check_flag = True
        return need_check_flag

    def default_strategy_repair_index(self):
        """
           default策略的股票仓位处理
        """
        self.__load_data_from_aggregator()
        # 调整default策略的short仓位
        self.__default_short_volume_repair()
        # 调整default策略的long > 100仓位
        self.__default_long_volume_repair()

    def __load_data_from_aggregator(self):
        aggregator_message_utils = AggregatorMessageUtils()
        aggregator_message_utils.login_aggregator()

        instrument_msg_dict = aggregator_message_utils.query_instrument_dict()
        position_risk_msg = aggregator_message_utils.query_position_risk_msg()
        pf_position_list = []
        for holding_item in position_risk_msg.Holdings:
            strategy_name = holding_item.Key
            (base_strategy_name, server_ip_str) = strategy_name.split('@')
            server_name = common_utils.get_server_name(server_ip_str)
            if server_name != self.__server_name:
                continue

            for risk_msg_info in holding_item.Value:
                instrument_msg = instrument_msg_dict[int(risk_msg_info.Key)]
                ticker_type = instrument_type_inversion_dict[instrument_msg.TypeIDWire]
                if ticker_type != 'CommonStock':
                    continue

                ticker = instrument_msg.ticker
                volume = risk_msg_info.Value.Long - risk_msg_info.Value.Short
                prev_close = instrument_msg.prevCloseWired
                pf_position_list.append([base_strategy_name, ticker, volume, prev_close])
        self.pf_position_df = pd.DataFrame(pf_position_list, columns=['Strategy_Name', 'Symbol', 'Volume', 'Prev_Close'])

        pf_account_list = []
        session_portfolio = self.__server_model.get_db_session('portfolio')
        query_pf_account = session_portfolio.query(PfAccount)
        for x in query_pf_account:
            fund = x.fund_name.split('-')[2]
            if fund == '':
                continue
            pf_account_list.append([x.fund_name, x.name, x.group_name, fund])
        pf_account_df = pd.DataFrame(pf_account_list, columns=['Strategy_Name', 'Name', 'Group_Name', 'Fund'])
        default_pf_account_df = pf_account_df[pf_account_df['Strategy_Name'].str.find(self.__default_strategy_key) >= 0]
        default_pf_account_df.rename(columns={'Group_Name': 'Default_Group_Name', 'Name': 'Default_Name'}, inplace=True)
        self.__pf_account_df = pd.merge(pf_account_df, default_pf_account_df[['Default_Group_Name', 'Default_Name',
                                                                            'Fund']], how='left', on=['Fund'])

    def __multifactor_stock_repair(self):
        phone_trade_list = []
        pf_position_df = pd.merge(self.pf_position_df, self.__pf_account_df, how='left', on=['Strategy_Name'])
        filter_position_df = pf_position_df[(pf_position_df['Strategy_Name'].str.find(self.__multifactor_strategy_key) >= 0) &
                                            ((pf_position_df['Volume'] % 100 != 0) | (pf_position_df['Volume'] < 0))]
        if len(filter_position_df) == 0:
            return phone_trade_list

        for index, row in filter_position_df.iterrows():
            strategy_volume = row['Volume']
            if strategy_volume >= 0:
                trade_volume = strategy_volume % 100
                direction = Direction_Enums.Sell
            else:
                trade_volume = abs(strategy_volume)
                direction = Direction_Enums.Buy

            phone_trade_info = PhoneTradeInfo()
            phone_trade_info.fund = row['Fund']
            phone_trade_info.strategy1 = '%s.%s' % (row['Group_Name'], row['Name'])
            phone_trade_info.symbol = row['Symbol']
            phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
            phone_trade_info.tradetype = Trade_Type_Enums.Normal
            phone_trade_info.iotype = IO_Type_Enums.Inner2
            phone_trade_info.strategy2 = '%s.%s' % (row['Default_Group_Name'], row['Default_Name'])
            phone_trade_info.server_name = self.__server_name
            phone_trade_info.exprice = row['Prev_Close']
            phone_trade_info.direction = direction
            phone_trade_info.exqty = trade_volume
            phone_trade_list.append(phone_trade_info)
        return phone_trade_list

    def __intraday_strategy_repair(self):
        phone_trade_list = []
        pf_position_df = pd.merge(self.pf_position_df, self.__pf_account_df, how='left', on=['Strategy_Name'])
        filter_position_df = pf_position_df[(pf_position_df['Strategy_Name'].str.find(self.__intraday_strategy_key) >= 0) &
                                            (pf_position_df['Volume'] != 0)]
        if len(filter_position_df) == 0:
            return phone_trade_list

        for index, row in filter_position_df.iterrows():
            strategy_volume = row['Volume']
            if strategy_volume > 0:
                trade_volume = strategy_volume
                direction = Direction_Enums.Sell
            else:
                trade_volume = abs(strategy_volume)
                direction = Direction_Enums.Buy

            phone_trade_info = PhoneTradeInfo()
            phone_trade_info.fund = row['Fund']
            phone_trade_info.strategy1 = '%s.%s' % (row['Group_Name'], row['Name'])
            phone_trade_info.symbol = row['Symbol']
            phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
            phone_trade_info.tradetype = Trade_Type_Enums.Normal
            phone_trade_info.iotype = IO_Type_Enums.Inner2
            phone_trade_info.strategy2 = '%s.%s' % (row['Default_Group_Name'], row['Default_Name'])
            phone_trade_info.server_name = self.__server_name
            phone_trade_info.exprice = row['Prev_Close']
            phone_trade_info.direction = direction
            phone_trade_info.exqty = trade_volume
            phone_trade_list.append(phone_trade_info)
        return phone_trade_list

    def __default_short_volume_repair(self):
        phone_trade_list = []
        pf_position_df = pd.merge(self.pf_position_df, self.__pf_account_df, how='left', on=['Strategy_Name'])
        filter_position_df = pf_position_df[(pf_position_df['Strategy_Name'].str.find(self.__default_strategy_key) >= 0) &
                                            (pf_position_df['Volume'] < 0)]
        if len(filter_position_df) == 0:
            return

        for index, row in filter_position_df.iterrows():
            default_fund, default_symbol = row['Fund'], row['Symbol']
            default_need_volume = calculation_target_volume(abs(row['Volume']))
            symbol_position_df = pf_position_df[(pf_position_df['Strategy_Name'].str.find(self.__multifactor_strategy_key) >= 0) &
                                                (pf_position_df['Fund'] == default_fund) &
                                                (pf_position_df['Symbol'] == default_symbol) &
                                                (pf_position_df['Volume'] > 0)]
            for sub_index, sub_row in symbol_position_df.iterrows():
                if default_need_volume <= 0:
                    break

                trade_volume = default_need_volume if int(sub_row['Volume']) > default_need_volume else int(sub_row['Volume'])
                phone_trade_info = PhoneTradeInfo()
                phone_trade_info.fund = sub_row['Fund']
                phone_trade_info.strategy1 = '%s.%s' % (sub_row['Group_Name'], sub_row['Name'])
                phone_trade_info.symbol = sub_row['Symbol']
                phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
                phone_trade_info.tradetype = Trade_Type_Enums.Normal
                phone_trade_info.iotype = IO_Type_Enums.Inner2
                phone_trade_info.strategy2 = '%s.%s' % (sub_row['Default_Group_Name'], sub_row['Default_Name'])
                phone_trade_info.server_name = self.__server_name
                phone_trade_info.exprice = sub_row['Prev_Close']
                phone_trade_info.direction = Direction_Enums.Sell
                phone_trade_info.exqty = trade_volume
                phone_trade_list.append(phone_trade_info)

                default_need_volume -= trade_volume
        if phone_trade_list:
            send_phone_trade(self.__server_name, phone_trade_list)
            self.__phone_trade_list.extend(phone_trade_list)

    def __default_long_volume_repair(self):
        phone_trade_list = []
        pf_position_df = pd.merge(self.pf_position_df, self.__pf_account_df, how='left', on=['Strategy_Name'])
        filter_position_df = pf_position_df[(pf_position_df['Strategy_Name'].str.find(self.__default_strategy_key) >= 0) &
                                            (pf_position_df['Volume'] >= 100)]
        if len(filter_position_df) == 0:
            return

        for index, row in filter_position_df.iterrows():
            default_fund, default_symbol = row['Fund'], row['Symbol']
            symbol_position_df = pf_position_df[(pf_position_df['Fund'] == default_fund) &
                                                (pf_position_df['Strategy_Name'].str.find(self.__multifactor_strategy_key) >= 0) &
                                                (pf_position_df['Symbol'] == default_symbol)]
            if symbol_position_df.empty:
                continue

            default_sell_volume = round_down(row['Volume'])
            for sub_index, sub_row in symbol_position_df.iterrows():
                phone_trade_info = PhoneTradeInfo()
                phone_trade_info.fund = sub_row['Fund']
                phone_trade_info.strategy1 = '%s.%s' % (sub_row['Group_Name'], sub_row['Name'])
                phone_trade_info.symbol = sub_row['Symbol']
                phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
                phone_trade_info.tradetype = Trade_Type_Enums.Normal
                phone_trade_info.iotype = IO_Type_Enums.Inner2
                phone_trade_info.strategy2 = '%s.%s' % (sub_row['Default_Group_Name'], sub_row['Default_Name'])
                phone_trade_info.server_name = self.__server_name
                phone_trade_info.exprice = sub_row['Prev_Close']
                phone_trade_info.direction = Direction_Enums.Buy
                phone_trade_info.exqty = default_sell_volume
                phone_trade_list.append(phone_trade_info)
                break
        if phone_trade_list:
            send_phone_trade(self.__server_name, phone_trade_list)
            self.__phone_trade_list.extend(phone_trade_list)


if __name__ == '__main__':
    scattered_stock_tools = ScatteredStockTools('citics')
    scattered_stock_tools.start_index()
