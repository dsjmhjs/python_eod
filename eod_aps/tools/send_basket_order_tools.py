#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import os
import AllProtoMsg_pb2
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const
from eod_aps.tools.aggregator_message_utils import AggregatorMessageUtils
from eod_aps.tools.date_utils import DateUtils


date_utils = DateUtils()
STOCK_SELECTION_FOLDER = const.EOD_CONFIG_DICT['stock_selection_folder']
Msg_Typeid_Enums = const.MSG_TYPEID_ENUMS
operation_enums = const.BASKET_FILE_OPERATION_ENUMS
Order_Type_Enums = const.ORDER_TYPE_ENUMS
Trade_Type_Enums = const.TRADE_TYPE_ENUMS
Algo_Type_Enums = const.ALGO_TYPE_ENUMS
Algo_Status_Enums = const.ALGO_STATUS_ENUMS
Peg_Level_Enums = const.PEG_LEVEL_ENUMS
Direction_Enums = const.DIRECTION_ENUMS


class BasketOrderTools(object):
    def __init__(self, server_name, fund_name, operation_type):
        self.__server_name = server_name
        self.__fund_name = fund_name
        self.__operation_type = operation_type
        self.__date_str = date_utils.get_today_str('%Y-%m-%d')
        self.__date_str2 = self.__date_str.replace('-', '')

    def send_order(self):
        aggregator_message_utils = AggregatorMessageUtils()
        aggregator_message_utils.login_aggregator()

        aggregator_message_utils.query_instrument_dict()
        self.load_message_fils()

    def __build_algo_parameter(self, sub_algo_type):
        algopara = AllProtoMsg_pb2.AlgoParameter()
        algopara.AlgoNameWired = Algo_Type_Enums.MarketBasket
        algopara.Interval = 2000
        algopara.ParticipationRate = 0.3
        # algopara.CompleteRate = 0.5
        algopara.RoundLotPoint = 0
        algopara.IsShortSellFirst = True
        algopara.PeggedLevel = Peg_Level_Enums.OppositeSide
        algopara.ValidateLeg2Time = 1
        algopara.OperateChildOrderTime = 0

        algopara.SellAbove = 0.0
        algopara.BuyBelow = 100000.0

        # algopara.PriceParameter = PriceParameter

        if sub_algo_type == Algo_Type_Enums.SigmaVWAP:
            algopara.Interval2 = 1
            # algopara.StartTime = DateTime.Now
            # algopara.EndTime = DateTime.Parse(now.ToString("yyyy-MM-dd") + " 15:00:00")
        return algopara

    def __build_order(self, algo_strategy, fund_group, sub_algo_type, peg_level, symbol_content_list):
        server_model = server_constant.get_server_model(self.__server_name)

        algopara = self.__build_algo_parameter(sub_algo_type)

        basket_new_order_msg = AllProtoMsg_pb2.NewOrderMsg()
        basket_new_order_msg.Symbol = symbol_content_list[0].split(',')[0]
        basket_new_order_msg.TargetID = 0
        basket_new_order_msg.Location = server_model.connect_address.replace('tcp://', '')

        basket_order = AllProtoMsg_pb2.Order()
        basket_order.TradeTypeWire = Trade_Type_Enums.Normal
        basket_order.TypeWire = Order_Type_Enums.BasketAlgo
        basket_order.UserID = self.__fund_name
        basket_order.StrategyID = algo_strategy
        basket_order.AlgoStatus = Algo_Status_Enums.Running
        basket_order.Qty = 0
        # msg_child0.Price = self.get_target_pre_price(df.at[0, 'target'])
        basket_order.DirectionWire = Direction_Enums.Buy

        # msg_child_algo0 = AllProtoMsg_pb2.AlgoParameter()
        # msg_child_algo0.AlgoNameWired = Algo_Type_Enums.MarketBasket
        basket_order.Parameters.CopyFrom(algopara)
        basket_new_order_msg.Order.MergeFrom(basket_order)

        for symbo_content in symbol_content_list:
            ticker, qty = symbo_content.split(',')
            child_order = basket_new_order_msg.childOrdersWire.add()
            child_order.Symbol = ticker
            child_order.TargetID = 0
            child_order.Location = server_model.connect_address.replace('tcp://', '')

            msg_child0 = AllProtoMsg_pb2.Order()
            msg_child0.TradeTypeWire = Trade_Type_Enums.Normal
            msg_child0.TypeWire = Order_Type_Enums.BasketAlgo
            msg_child0.UserID = self.__fund_name
            msg_child0.StrategyID = algo_strategy
            msg_child0.AlgoStatus = Algo_Status_Enums.Running
            msg_child0.Qty = int(qty)
            # msg_child0.Price = self.get_target_pre_price(df.at[0, 'target'])
            msg_child0.DirectionWire = Direction_Enums.Buy

            msg_child_algo0 = AllProtoMsg_pb2.AlgoParameter()
            msg_child_algo0.AlgoNameWired = Algo_Type_Enums.SigmaVWAP
            msg_child0.Parameters.CopyFrom(msg_child_algo0)
            child_order.Order.MergeFrom(msg_child0)
        # basket_order.Parameters.CopyFrom(algopara)

        aggregator_message_utils = AggregatorMessageUtils()
        aggregator_message_utils.login_aggregator()
        aggregator_message_utils.send_new_order(basket_new_order_msg)

    def load_message_fils(self):
        if self.__operation_type == operation_enums.Close:
            folder_suffix = 'close'
        elif self.__operation_type == operation_enums.Add:
            folder_suffix = 'add'
        elif self.__operation_type == operation_enums.Change:
            folder_suffix = 'change'
        else:
            raise Exception("Error operation_type:%s" % self.__operation_type)

        server_model = server_constant.get_server_model(self.__server_name)
        file_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, self.__server_name, self.__date_str2, folder_suffix)
        for file_name in os.listdir(file_folder):
            if not file_name.endswith('.txt'):
                continue
            (strategy_name, group_name, fund_item, sub_algo_type, peg_level, temp) = file_name.replace('.txt', '').split('-')
            fund_name, server_ip = fund_item.split('@')
            algo_strategy = '%s.%s' % (group_name, strategy_name)
            fund_group = '%s@%s' % (fund_name, server_model.connect_address.replace('tcp://', ''))

            symbol_content_list = []
            with open(os.path.join(file_folder, file_name)) as fr:
                for line in fr.readlines():
                    symbol_content_list.append(line.replace('\n', ''))
            self.__build_order(algo_strategy, fund_group, sub_algo_type, peg_level, symbol_content_list)


if __name__ == '__main__':
    basket_order_tools = BasketOrderTools('huabao_test', 'LTS001', operation_enums.Change)
    basket_order_tools.send_order()
