# -*- coding: utf-8 -*-
import datetime
import os
import pickle
import threading
import time
import traceback
from threading import Timer
from eod_aps.tools.aggregator_message_utils import AggregatorMessageUtils
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.tradeplat_message_tools import *
from eod_aps.job import *
from eod_aps.tools.message_manage_tool import save_msg
import json

from eod_aps.tools.tradeplat_position_tools import InstrumentView, RiskView

email_utils16 = EmailUtils(const.EMAIL_DICT['group16'])
common_utils = CommonUtils()

market_latest_receive_time = None
order_latest_receive_time = None
trade_latest_receive_time = None

Algo_Status_Enums = const.ALGO_STATUS_ENUMS
Order_Status_Enums = const.ORDER_STATUS_ENUMS
Order_Type_Enums = const.ORDER_TYPE_ENUMS

custom_enum_utils = CustomEnumUtils()
order_type_inversion_dict = custom_enum_utils.enum_to_dict(const.ORDER_TYPE_ENUMS, True)
hedgeflag_type_inversion_dict = custom_enum_utils.enum_to_dict(const.HEDGEFLAG_TYPE_ENUMS, True)
order_status_inversion_dict = custom_enum_utils.enum_to_dict(const.ORDER_STATUS_ENUMS, True)
operation_status_inversion_dict = custom_enum_utils.enum_to_dict(const.OPERATION_STATUS_ENUMS, True)
trade_type_inversion_dict = custom_enum_utils.enum_to_dict(const.TRADE_TYPE_ENUMS, True)
algo_status_inversion_dict = custom_enum_utils.enum_to_dict(const.ALGO_STATUS_ENUMS, True)


def __validate_error_orders(order_dict):
    unusual_order_list = []

    format_str = '%Y-%m-%d %H:%M:%S'
    now_time_str = date_utils.get_today_str(format_str)
    for (order_id, order_msg) in order_dict.items():
        error_flag = False
        if order_msg.Order.StatusWire not in (const.ORDER_STATUS_ENUMS.Rejected, const.ORDER_STATUS_ENUMS.New):
            continue

        creation_time_str = common_utils.format_msg_time(order_msg.Order.CreationTime).strftime(format_str)
        if order_msg.Order.TypeWire == Order_Type_Enums.LimitOrder:
            transaction_time_str = common_utils.format_msg_time(order_msg.Order.TransactionTime).strftime(format_str)
            interval_seconds = date_utils.get_interval_seconds(creation_time_str, now_time_str)

            if order_msg.Order.StatusWire == Order_Status_Enums.Rejected and interval_seconds <= 300:
                error_flag = True
            elif order_msg.Order.StatusWire == Order_Status_Enums.New and interval_seconds >= 300:
                error_flag = True
        else:
            transaction_time_str = common_utils.format_msg_time(order_msg.Order.TransactionTime).strftime(format_str)
            interval_seconds = date_utils.get_interval_seconds(transaction_time_str, now_time_str)
            if interval_seconds >= 300:
                error_flag = True

        if error_flag:
            server_name = common_utils.get_server_name(order_msg.Location)
            unusual_order_list.append([server_name, order_msg.Order.OrderAccount, order_msg.Order.StrategyID,
                                       order_msg.Symbol, order_status_inversion_dict[order_msg.Order.StatusWire],
                                       creation_time_str, transaction_time_str, order_msg.Order.Note])
    const.EOD_POOL['unusual_order_list'] = unusual_order_list


def __save_orders_sub(orders, order_dict):
    for order_msg_info in orders:
        dict_key = '%s|%s' % (order_msg_info.Order.ID, order_msg_info.Location)
        order_dict[dict_key] = order_msg_info

        if len(order_msg_info.ChildOrder) > 0:
            print dict_key, len(order_msg_info.ChildOrder)

        if order_msg_info.ChildOrder:
            __save_orders_sub(order_msg_info.ChildOrder, order_dict)


def __build_order_dict(order_msg_info):
    order_item_dict = dict(
        Type=order_type_inversion_dict[order_msg_info.Order.TypeWire],
        Strategy=order_msg_info.Order.StrategyID,
        Symbol=order_msg_info.Symbol,
        HedgeType=hedgeflag_type_inversion_dict[order_msg_info.Order.HedgeTypeWire],
        Status=order_status_inversion_dict[order_msg_info.Order.StatusWire],
        OpStatus=operation_status_inversion_dict[order_msg_info.Order.OperationStatusWire],
        AlgoStatus=algo_status_inversion_dict[order_msg_info.Order.AlgoStatus],
        Price=order_msg_info.Order.Price,
        OrdVol=order_msg_info.Order.Qty,
        TradeVol=(order_msg_info.Order.ExQty / order_msg_info.Order.Qty * 100) if order_msg_info.Order.Qty > 0 else 0,
        ExPrice=order_msg_info.Order.ExAvgPrice,
        CreationT=common_utils.format_msg_time(order_msg_info.Order.CreationTime).strftime('%H:%M:%S')[:8],
        TransactionT=common_utils.format_msg_time(order_msg_info.Order.TransactionTime).strftime('%H:%M:%S')[:8],
        Note=order_msg_info.Order.Note,
        Account=order_msg_info.Order.OrderAccount,
        OrderID=order_msg_info.Order.ID,
        SysOrderID=order_msg_info.Order.SysID,
        TradeType=trade_type_inversion_dict[order_msg_info.Order.TradeTypeWire],
        NominalTradeType=trade_type_inversion_dict[order_msg_info.Order.NominalTradeTypeWire],
        ParentOrderID=order_msg_info.Order.ParentOrderID,
        Location=order_msg_info.Location,
        Server=common_utils.get_server_name(order_msg_info.Location),
    )
    return order_item_dict


def __clear_eod_pool():
    for pool_name in ('market_dict', 'instrument_view_dict', 'order_dict', 'order_view_tree_dict', 'trade_list',
                      'risk_dict', 'position_dict', 'position_update_time'):
        const.EOD_POOL[pool_name] = None


def __save_market(aggregator_message_utils):
    instrument_info_msg = aggregator_message_utils.query_instrument_info_msg2()
    market_dict = dict()
    for market_msg in instrument_info_msg.Infos:
        market_dict[market_msg.ID] = market_msg
    const.EOD_POOL['market_dict'] = market_dict

    instrument_view_dict = dict()
    instrument_symbol_dict = dict()
    instrument_dict = const.EOD_POOL['instrument_dict']
    for (key, market_msg) in market_dict.items():
        instrument_msg = instrument_dict[key]
        instrument_view = InstrumentView(instrument_msg, market_msg)
        instrument_view_dict[key] = instrument_view
        instrument_symbol_dict[instrument_view.Ticker] = instrument_view

    for (key, instrument_view) in instrument_view_dict.items():
        for underlying_ticker in instrument_view.UnderlyingTickers:
            if underlying_ticker.split(' ')[0] not in instrument_symbol_dict:
                continue
            instrument_view.Underlyings.append(instrument_symbol_dict[underlying_ticker.split(' ')[0]])
    const.EOD_POOL['instrument_view_dict'] = instrument_view_dict


# 重新组织order的树形结构
def __rebuild_order_tree(order_dict):
    order_view_dict = dict()
    for (dict_key, order_msg_info) in order_dict.items():
        order_view_dict[dict_key] = __build_order_dict(order_msg_info)

    order_view_tree_dict = dict()
    print len(order_view_dict)
    for (dict_key, order_item_dict) in order_view_dict.items():
        if order_item_dict['ParentOrderID'] == '':
            order_view_tree_dict[dict_key] = order_item_dict
        else:
            parent_dict_key = '%s|%s' % (order_item_dict['ParentOrderID'], order_item_dict['Location'])
            # TODO check why?
            if parent_dict_key not in order_view_dict:
                continue
            parent_item_dict = order_view_dict[parent_dict_key]
            parent_item_dict.setdefault('children', []).append(order_item_dict)
    const.EOD_POOL['order_view_tree_dict'] = order_view_tree_dict


def __save_orders(aggregator_message_utils):
    order_info_msg = aggregator_message_utils.query_order_msg()
    order_dict = const.EOD_POOL['order_dict'] if const.EOD_POOL['order_dict'] is not None else {}
    __save_orders_sub(order_info_msg.Orders, order_dict)
    const.EOD_POOL['order_dict'] = order_dict

    __rebuild_order_tree(order_dict)
    __validate_error_orders(order_dict)


def __save_trades(aggregator_message_utils):
    trade_info_msg = aggregator_message_utils.query_trade_msg()
    trade_list = const.EOD_POOL['trade_list'] if const.EOD_POOL['trade_list'] is not None else []

    for trade_msg_info in trade_info_msg.Trades:
        trade_time_str = common_utils.format_msg_time(trade_msg_info.Time).strftime('%Y-%m-%d %H:%M:%S')
        trade_list.append((trade_time_str, trade_msg_info))
    const.EOD_POOL['trade_list'] = trade_list


def __save_risk_and_position(aggregator_message_utils):
    position_risk_msg = aggregator_message_utils.query_position_risk_msg()
    risk_dict = dict()
    for holding_item in position_risk_msg.Holdings:
        strategy_name = holding_item.Key
        strategy_risk_dict = dict()
        for risk_msg_info in holding_item.Value:
            strategy_risk_dict[int(risk_msg_info.Key)] = risk_msg_info.Value
        risk_dict[strategy_name] = strategy_risk_dict
    const.EOD_POOL['risk_dict'] = risk_dict

    position_dict = dict()
    for holding_item in position_risk_msg.Holdings2:
        account_name = holding_item.Key
        account_position_dict = dict()
        for position_msg_info in holding_item.Value:
            account_position_dict[int(position_msg_info.Key)] = position_msg_info.Value
        position_dict[account_name] = account_position_dict
    const.EOD_POOL['position_dict'] = position_dict
    const.EOD_POOL['position_update_time'] = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')


def __save_msg_timer(aggregator_message_utils):
    try:
        __save_market(aggregator_message_utils)
        __save_orders(aggregator_message_utils)
        __save_trades(aggregator_message_utils)
        __save_risk_and_position(aggregator_message_utils)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__save_msg_timer.', error_msg)


def __query_aggregator_msg_thread():
    try:
        __clear_eod_pool()
        aggregator_message_utils = AggregatorMessageUtils()
        aggregator_message_utils.login_aggregator()

        instrument_dict = aggregator_message_utils.query_instrument_dict()
        const.EOD_POOL['instrument_dict'] = instrument_dict

        validate_number = int(date_utils.get_today_str('%H%M%S'))
        while 90000 <= validate_number <= 160000:
            Timer(5, __save_msg_timer, [aggregator_message_utils, ]).start()
            time.sleep(30)
            validate_number = int(date_utils.get_today_str('%H%M%S'))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__query_aggregator_msg_thread.', error_msg)


def aggregator_msg_load_job():
    t = threading.Thread(target=__query_aggregator_msg_thread, args=())
    t.start()


# 缓存数据至pickle文件，用于测试
def __pickle_aggregator_msg():
    __clear_eod_pool()
    aggregator_message_utils = AggregatorMessageUtils()
    aggregator_message_utils.login_aggregator()

    instrument_dict = aggregator_message_utils.query_instrument_dict()
    const.EOD_POOL['instrument_dict'] = instrument_dict
    __save_msg_timer(aggregator_message_utils)

    BASEPATH = os.path.dirname(os.path.abspath(__file__))
    fw = open(BASEPATH + '/../../cfg/aggregator_pickle_data.txt', 'wb')
    for pool_name in ('market_dict', 'instrument_view_dict', 'order_dict', 'order_view_tree_dict', 'trade_list',
                      'risk_dict', 'position_dict', 'position_update_time'):
        print pool_name
        pickle.dump(const.EOD_POOL[pool_name], fw)
    fw.close()


def __load_from_pickle_file():
    path = os.path.dirname(__file__)
    fr = open(path + '/../../cfg/aggregator_pickle_data.pickle', 'rb')
    instrument_dict = pickle.load(fr)
    market_dict = pickle.load(fr)
    instrument_view_dict = pickle.load(fr)
    order_dict = pickle.load(fr)
    order_view_tree_dict = pickle.load(fr)
    trade_list = pickle.load(fr)
    risk_dict = pickle.load(fr)
    position_dict = pickle.load(fr)
    position_update_time = pickle.load(fr)
    fr.close()

    # instrument_view_dict = dict()
    # for (instrument_key, instrument_msg) in instrument_dict.items():
    #     market_msg = market_dict[instrument_key]
    #     instrument_view = InstrumentView(instrument_msg, market_msg)
    #     instrument_view_dict[instrument_view.Ticker] = instrument_view
    #
    # for (ticker, instrument_view) in instrument_view_dict.items():
    #     for temp_ticker in instrument_view.UnderlyingTickers:
    #         instrument_view.Underlyings.append(instrument_view_dict[temp_ticker.split(' ')[0]])

    for (strategy_name, strategy_risk_dict) in risk_dict.items():
        for (instrument_key, position_msg) in strategy_risk_dict.items():
            (base_strategy_name, server_ip_str) = strategy_name.split('@')

            instrument_msg = instrument_dict[instrument_key]
            instrument_view = instrument_view_dict[instrument_msg.ticker]
            risk_view = RiskView(instrument_view, position_msg, base_strategy_name)

            if 'deriv_01' in base_strategy_name and 'SSE50' in base_strategy_name:
                print instrument_msg.ticker, risk_view.delta


if __name__ == '__main__':
    # aggregator_msg_load_job()
    __query_aggregator_msg_thread()
    # __load_from_pickle_file()

