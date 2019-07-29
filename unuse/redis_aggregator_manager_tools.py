# -*- coding: utf-8 -*-
import redis
import datetime
import bcl_pb2
import time
from threading import Timer
from eod_aps.tools.tradeplat_message_tools import *
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.tools.tradeplat_position_tools import RiskView
from eod_aps.tools.tradeplat_position_tools import InstrumentView

ip = const.EOD_CONFIG_DICT['redis_address'].split('|')[0]
port = const.EOD_CONFIG_DICT['redis_address'].split('|')[1]

r = redis.Redis(host=ip, port=port, db=3)
pipeline_redis = r.pipeline()

market_latest_receive_time = None
order_latest_receive_time = None
trade_latest_receive_time = None

custom_enum_utils = CustomEnumUtils()
order_type_dict = custom_enum_utils.enum_to_dict(const.ORDER_TYPE_ENUMS, inversion_flag=True)
hedgeflag_type_dict = custom_enum_utils.enum_to_dict(const.HEDGEFLAG_TYPE_ENUMS, True)
order_status_dict = custom_enum_utils.enum_to_dict(const.ORDER_STATUS_ENUMS, True)
operation_status_dict = custom_enum_utils.enum_to_dict(const.OPERATION_STATUS_ENUMS, True)
trade_type_dict = custom_enum_utils.enum_to_dict(const.TRADE_TYPE_ENUMS, True)


def __save_instrument_all(socket):
    # 往aggregator发送tradeserver的订阅消息
    trade_server_info_msg = send_tradeserverinfo_request_msg(socket)
    trade_server_info_list = []
    for trade_server_info in trade_server_info_msg.TradeServerInfo:
        r.lpush("Trade_Servers", trade_server_info)
        trade_server_info_list.append(trade_server_info)
    send_subscribetradeserverinfo_request_msg(socket, trade_server_info_list)

    instrument_dict, market_dict = send_instrument_info_request_msg(socket)
    for (target_id, instrument_message) in instrument_dict.items():
        r.hset("Instrument_all", target_id, instrument_message.SerializeToString())


def __save_market(socket):
    if market_latest_receive_time is None:
        instrument_info_msg = send_instrument_info_request_msg2(socket)
    else:
        instrument_info_msg = send_instrument_info_request_msg2(socket, market_latest_receive_time)
    global market_latest_receive_time
    market_latest_receive_time = instrument_info_msg.LatestReceiveTime

    for market_msg in instrument_info_msg.Infos:
        if not market_msg.ID:
            continue
        r.hset("Market", market_msg.ID, market_msg.SerializeToString())
    print 'Market LatestReceiveTime:%s, Len:%s' % (__GetDateTime(market_latest_receive_time),
                                                   len(instrument_info_msg.Infos))


def __save_orders_sub(orders, order_relation_tree_dict):
    for order_msg_info in orders:
        dict_key = '%s|%s' % (order_msg_info.Order.ID, order_msg_info.Location)
        if order_msg_info.ChildOrder:
            child_order_ids = [child_order_msg.Order.ID for child_order_msg in order_msg_info.ChildOrder]
            if order_msg_info.Order.ID in order_relation_tree_dict:
                order_relation_tree_dict[order_msg_info.Order.ID].extend(child_order_ids)
            else:
                order_relation_tree_dict[order_msg_info.Order.ID] = child_order_ids
            __save_orders_sub(order_msg_info.ChildOrder, order_relation_tree_dict)

        r.hset("Orders", dict_key, order_msg_info.SerializeToString())


def __save_orders(socket):
    if order_latest_receive_time is None:
        order_info_msg = send_order_info_request_msg2(socket)
    else:
        order_info_msg = send_order_info_request_msg2(socket, order_latest_receive_time)
    global order_latest_receive_time
    order_latest_receive_time = order_info_msg.LatestReceiveTime

    order_relation_tree_dict = dict()
    __save_orders_sub(order_info_msg.Orders, order_relation_tree_dict)

    for (order_id, sub_order_id_list) in order_relation_tree_dict.items():
        r.hset("Order_Relation_Tree", order_id, ','.join(sub_order_id_list))

    for cancelled_order_num_info in order_info_msg.CancelledOrderNumInfo:
        r.hset("CancelledOrderNum", cancelled_order_num_info.Account, cancelled_order_num_info.SerializeToString())
    print 'Order LatestReceiveTime:%s, Len:%s' % (__GetDateTime(order_latest_receive_time),
                                                  len(order_info_msg.Orders))


def __save_trades(socket):
    if trade_latest_receive_time is None:
        trade_info_msg = send_trade_info_request_msg2(socket)
    else:
        trade_info_msg = send_trade_info_request_msg2(socket, trade_latest_receive_time)
    global trade_latest_receive_time
    trade_latest_receive_time = trade_info_msg.LatestReceiveTime

    for trade_msg_info in trade_info_msg.Trades:
        r.hset("Trades", trade_msg_info.Time.value, trade_msg_info.SerializeToString())
    print 'Trade LatestReceiveTime:%s, Len:%s' % (__GetDateTime(trade_latest_receive_time),
                                                  len(trade_info_msg.Trades))


def __save_risk_and_position(socket):
    position_risk_msg = send_position_risk_request_msg2(socket)
    for holding_item in position_risk_msg.Holdings:
        r.lpush("Risks", holding_item.Key)
        for risk_msg_info in holding_item.Value:
            r.hset("RisksInfo:%s" % holding_item.Key, risk_msg_info.Key, risk_msg_info.Value.SerializeToString())

    for holding_item in position_risk_msg.Holdings2:
        r.lpush("Positions", holding_item.Key)
        for position_msg_info in holding_item.Value:
            r.hset("PositionsInfo:%s" % holding_item.Key, position_msg_info.Key,
                   position_msg_info.Value.SerializeToString())

    print 'Position LatestReceiveTime:%s, Len:%s' % (date_utils.get_today_str('%Y-%m-%d %H:%M:%S'),
                                                     len(position_risk_msg.Holdings2))


def __clear_redis():
    r.delete("Trade_Servers")
    r.delete("Instrument_all")
    r.delete("Market")
    r.delete("Order_Relation_Tree")
    r.delete("Orders")
    r.delete("Trades")
    for del_key in r.keys(pattern='Risks*'):
        r.delete(del_key)
    for del_key in r.keys(pattern='Positions*'):
        r.delete(del_key)


def __save_to_redis(socket):
    # __save_market(socket)
    # __save_orders(socket)
    # __save_trades(socket)
    __save_risk_and_position(socket)


def save_aggregator_to_redis(server_name):
    __clear_redis()

    socket = socket_init(server_name)
    # 新版aggregator需要先登陆
    send_login_msg(socket)
    __save_instrument_all(socket)

    validate_number = int(date_utils.get_today_str('%H%M%S'))
    while validate_number <= 180500:
        Timer(5, __save_to_redis, (socket,)).start()
        time.sleep(60)


def loader_market_from_redis():
    __loader_instrument_dict()
    __loader_market_dict()
    __update_eod_pool()

    # validate_number = int(date_utils.get_today_str('%H%M%S'))
    # while validate_number <= 180500:
    #     Timer(5, __update_eod_pool).start()
    #     time.sleep(60)


def __loader_instrument_dict():
    instrument_dict = dict()
    base_instrument_dict = r.hgetall("Instrument_all")
    for (target_id, instrument_info_str) in base_instrument_dict.items():
        instrument_msg = AllProtoMsg_pb2.Instrument()
        instrument_msg.ParseFromString(instrument_info_str)
        instrument_dict[int(target_id)] = instrument_msg
    const.EOD_POOL['instrument_dict'] = instrument_dict


def __loader_market_dict():
    market_dict = dict()
    base_market_dict = r.hgetall('Market')
    for (target_id, market_info_str) in base_market_dict.items():
        market_msg = AllProtoMsg_pb2.MarketDataResponseMsg()
        market_msg.ParseFromString(market_info_str)
        market_dict[int(target_id)] = market_msg
    const.EOD_POOL['market_dict'] = market_dict


def __update_eod_pool():
    instrument_dict = const.EOD_POOL['instrument_dict']
    market_dict = const.EOD_POOL['market_dict']

    instrument_index_dict = dict()
    for (key, instrument_msg) in instrument_dict.items():
        market_msg = market_dict[key]
        instrument = InstrumentView(instrument_msg, market_msg)
        instrument_index_dict[key] = instrument

    # order_dict = dict()
    # temp_order_dict = r.hgetall('Orders')
    # for (dict_key, order_info_str) in temp_order_dict.items():
    #     order_msg = AllProtoMsg_pb2.NewOrderMsg()
    #     order_msg.ParseFromString(order_info_str)
    #
    #     order_type = order_type_dict[order_msg.Order.TypeWire]
    #     if order_msg.TargetID not in instrument_dict:
    #         print order_msg.TargetID
    #         continue
    #     ticker = instrument_dict[order_msg.TargetID].ticker
    #     hedge_type = hedgeflag_type_dict[order_msg.Order.HedgeTypeWire]
    #     order_status = order_status_dict[order_msg.Order.StatusWire]
    #     operation_status = operation_status_dict[order_msg.Order.OperationStatusWire]
    #     trade_type = trade_type_dict[order_msg.Order.TradeTypeWire]
    #     nominal_trade_type = trade_type_dict[order_msg.Order.NominalTradeTypeWire]
    #
    #     order_item_dict = dict()
    #     order_item_dict['Type'] = order_type
    #     order_item_dict['Strategy'] = order_msg.Order.StrategyID
    #     order_item_dict['Symbol'] = ticker
    #     order_item_dict['Hedge'] = hedge_type
    #     order_item_dict['Status'] = order_status
    #     order_item_dict['Op_Status'] = operation_status
    #     order_item_dict['AlgoStatus'] = order_msg.Order.AlgoStatus
    #     order_item_dict['Price'] = order_msg.Order.Price
    #     order_item_dict['OrdVol'] = ''
    #     order_item_dict['TradeVol'] = ''
    #     order_item_dict['ExPrice'] = order_msg.Order.ExAvgPrice
    #     order_item_dict['CxlVol'] = order_msg.Order.ExQty
    #     order_item_dict["CreationT"] = __GetDateTime(order_msg.Order.CreationTime).strftime('%Y-%m-%d %H:%M:%S')
    #     order_item_dict["TransactionT"] = __GetDateTime(order_msg.Order.TransactionTime).strftime('%Y-%m-%d %H:%M:%S')
    #     order_item_dict['Note'] = order_msg.Order.Note
    #     order_item_dict['Undl'] = ''
    #     order_item_dict['c/p'] = ''
    #     order_item_dict['Expire'] = ''
    #     order_item_dict['Strike'] = ''
    #     order_item_dict['Account'] = order_msg.Order.OrderAccount
    #     order_item_dict['OrdID'] = order_msg.Order.ID
    #     order_item_dict['SysOrdID'] = order_msg.Order.SysID
    #     order_item_dict['TradeType'] = trade_type
    #     order_item_dict['NominalTradeType'] = nominal_trade_type
    #     order_dict[dict_key] = order_item_dict
    # const.EOD_POOL['order_dict'] = order_dict
    # print 'order_dict:%s' % len(order_dict)
    #
    # trade_dict = dict()
    # base_trade_dict = r.hgetall('Trades')
    # for (trade_time, trade_info_str) in base_trade_dict.items():
    #     trade_msg = AllProtoMsg_pb2.TimeValueBase()
    #     trade_msg.ParseFromString(trade_info_str)
    #
    #     trade_item_dict = dict()
    #     trade_item_dict['Time'] = __GetDateTime(trade_msg.Time).strftime('%Y-%m-%d %H:%M:%S')
    #     trade_item_dict['Symbol'] = trade_msg.Trade.symbol
    #     trade_item_dict['Qty'] = trade_msg.Trade.Qty
    #     trade_item_dict['Price'] = '%.2f' % trade_msg.Trade.Price
    #     trade_item_dict['Type'] = trade_msg.Trade.TradeTypeWired
    #     trade_item_dict['TradePL'] = trade_msg.Trade.TradePL.PL
    #     trade_item_dict['Fee'] = '%.2f' % trade_msg.Trade.TradeFee
    #     trade_item_dict['FairTradePL'] = trade_msg.Trade.TradePL.AccPL
    #     trade_item_dict["NetTradePL"] = trade_msg.Trade.TradePL.Commission
    #     trade_item_dict["OrderID"] = trade_msg.Trade.OrderID
    #     trade_item_dict["StrategyID"] = trade_msg.Trade.StrategyID
    #     trade_item_dict["AccountID"] = trade_msg.Trade.AccountID
    #     trade_dict[trade_time] = trade_item_dict
    # const.EOD_POOL['trade_dict'] = trade_dict
    # print 'trade_dict:%s' % len(trade_dict)

    pf_position_list = []
    strategy_list = r.lrange("Risks", 0, -1)
    for strategy_name in strategy_list:
        print '-------------%s--------------' % strategy_name
        base_strategy_dict = r.hgetall("RisksInfo:%s" % strategy_name)
        for (dict_key, strategy_risk_info_str) in base_strategy_dict.items():
            position_msg = AllProtoMsg_pb2.InstrumentPosition()
            position_msg.ParseFromString(strategy_risk_info_str)

            instrument_view = instrument_index_dict[int(dict_key)]
            risk_view = RiskView(instrument_view, position_msg, strategy_name)
            pf_position_list.append(risk_view)
    const.EOD_POOL['pf_position_list'] = pf_position_list
    print 'pf_position_list:%s' % len(pf_position_list)


def __GetDateTime(input_value):
    Jan1st1970 = date_utils.string_toDatetime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    value = input_value.value
    if input_value.scale == bcl_pb2.DateTime().TICKS:
        return Jan1st1970 + datetime.timedelta(microseconds=value / 10)
    elif input_value.scale == bcl_pb2.DateTime().MILLISECONDS:
        return Jan1st1970 + datetime.timedelta(milliseconds=value)
    elif input_value.scale == bcl_pb2.DateTime().SECONDS:
        return Jan1st1970 + datetime.timedelta(seconds=value)
    elif input_value.scale == bcl_pb2.DateTime().MINUTES:
        return Jan1st1970 + datetime.timedelta(minutes=value)
    elif input_value.scale == bcl_pb2.DateTime().HOURS:
        return Jan1st1970 + datetime.timedelta(hours=value)
    elif input_value.scale == bcl_pb2.DateTime().DAYS:
        return Jan1st1970 + datetime.timedelta(days=value)
    return Jan1st1970


# def __market_validate():
#     base_market_dict = r.hgetall('Market')
#     market_dict = dict()
#     for (target_id, market_info_str) in base_market_dict.items():
#         market_msg = AllProtoMsg_pb2.MarketDataResponseMsg()
#         market_msg.ParseFromString(market_info_str)
#         market_dict[target_id] = market_msg
#
#     base_instrument_dict = r.hgetall("Instrument_all")
#     instrument_dict = dict()
#     validate_dict = dict()
#     for (target_id, instrument_info_str) in base_instrument_dict.items():
#         instrument_msg = AllProtoMsg_pb2.Instrument()
#         instrument_msg.ParseFromString(instrument_info_str)
#         instrument_dict[target_id] = instrument_msg
#
#         dict_key = '%s|%s' % (instrument_msg.ExchangeIDWire, instrument_msg.TypeIDWire)
#         if dict_key in validate_dict:
#             validate_dict[dict_key].append(target_id)
#         else:
#             validate_dict[dict_key] = [target_id]
#
#     validate_date_str = date_utils.get_today_str('%Y-%m-%d')
#     validate_report_list = []
#     for (validate_key, target_id_list) in validate_dict.items():
#         if len(target_id_list) > 5:
#             validate_id_list = random.sample(target_id_list, 5)
#         else:
#             validate_id_list = target_id_list
#
#         for validate_id in validate_id_list:
#             if validate_id not in market_dict:
#                 print 'Error id:%s' % validate_id
#             else:
#                 market_info = market_dict[validate_id]
#                 instrument_msg = instrument_dict[validate_id]
#                 market_update_time = __GetDateTime(market_info.Args.UpdateTime)
#                 market_update_time_str = '%s' % market_update_time
#                 if validate_date_str in market_update_time_str:
#                     validate_report_list.append((validate_key, instrument_msg.ticker, market_update_time_str))
#                 else:
#                     validate_report_list.append((validate_key, instrument_msg.ticker,market_update_time_str + '(Error)'))
#
#     validate_report_list.sort()
#     email_utils = EmailUtils(EmailUtils.group1)
#     html_list = email_utils.list_to_html('Type,Ticker,Update_Time', validate_report_list)
#     email_utils.send_email_group_all(unicode('行情报告', 'utf-8'), ''.join(html_list), 'html')
#
#
def query_redis_order_list(query_params):
    query_account = query_params.get('account')
    query_ordertype = query_params.get('ordertype')
    query_ticker = query_params.get('ticker')
    query_status = query_params.get('status')

    order_list = []
    if 'order_dict' in const.EOD_POOL:
        order_dict = const.EOD_POOL['order_dict']
        account_set = set()
        for (dict_key, order_item_dict) in order_dict.items():
            account_set.add(order_item_dict['Account'])

            if query_account and order_item_dict['Account'] != query_account:
                continue

            if query_ordertype and order_item_dict['Type'] != query_ordertype:
                continue

            if query_ticker and order_item_dict['Symbol'] != query_ticker:
                continue

            if query_status and order_item_dict['Status'] not in query_status:
                continue

            order_list.append(order_item_dict)
        print account_set

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))

    total_number = len(order_list)
    return total_number, order_list[(query_page - 1) * query_size: query_page * query_size]


def query_redis_trade_list(query_params):
    query_ticker = query_params.get('ticker')
    query_tradetype = query_params.get('tradetype')

    trade_list = []
    if 'trade_dict' in const.EOD_POOL:
        trade_dict = const.EOD_POOL['trade_dict']
        for (trade_time, trade_item_dict) in trade_dict.items():
            if query_ticker and query_ticker not in trade_item_dict['Symbol']:
                continue

            trade_list.append(trade_item_dict)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))

    total_number = len(trade_list)
    return total_number, trade_list[(query_page - 1) * query_size: query_page * query_size]


def __query_aggregator_pf_position():
    socket = socket_init('aggregator')
    # 新版aggregator需要先登陆
    send_login_msg(socket)
    position_risk_msg = send_position_risk_request_msg2(socket)
    for holding_item in position_risk_msg.Holdings:
        r.lpush("Risks", holding_item.Key)
        for risk_msg_info in holding_item.Value:
            r.hset("RisksInfo:%s" % holding_item.Key, risk_msg_info.Key, risk_msg_info.Value.SerializeToString())

    print 'Position LatestReceiveTime:%s, Len:%s' % (date_utils.get_today_str('%Y-%m-%d %H:%M:%S'),
                                                     len(position_risk_msg.Holdings2))


def query_pf_position_list(query_params):
    query_ticker = query_params.get('ticker')
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')

    query_result_list = []
    if 'pf_position_list' in const.EOD_POOL:
        pf_position_list = const.EOD_POOL['pf_position_list']
        for pf_position_item in pf_position_list:
            if query_ticker and query_ticker != pf_position_item.Ticker:
                continue
            if query_server_name and query_server_name != pf_position_item.AccountName:
                continue
            if query_fund_name and query_fund_name != pf_position_item.AccountName:
                continue

            pf_position_item_dict = dict()
            pf_position_item_dict['Ticker'] = pf_position_item.Ticker
            pf_position_item_dict['Strategy'] = pf_position_item.AccountName
            pf_position_item_dict['Long'] = pf_position_item.long
            pf_position_item_dict['LongAvail'] = pf_position_item.long_available
            # pf_position_item_dict['LongCost'] = pf_position_item.LongCost
            pf_position_item_dict['Short'] = pf_position_item.short
            pf_position_item_dict['ShortAvail'] = pf_position_item.short_available
            # pf_position_item_dict['ShortCost'] = pf_position_item.ShortCost
            pf_position_item_dict['DayTradeFee'] = pf_position_item.fee
            query_result_list.append(pf_position_item_dict)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))

    total_number = len(query_result_list)
    return total_number, query_result_list[(query_page - 1) * query_size: query_page * query_size]


# def __risk_validate():
#     base_instrument_dict = r.hgetall("Instrument_all")
#     instrument_dict = dict()
#     for (target_id, instrument_info_str) in base_instrument_dict.items():
#         instrument_msg = AllProtoMsg_pb2.Instrument()
#         instrument_msg.ParseFromString(instrument_info_str)
#         instrument_dict[target_id] = instrument_msg
#
#     strategy_list = r.lrange("Risks", 0, -1)
#     for strategy_name in strategy_list:
#         print '-------------%s--------------' % strategy_name
#         base_strategy_dict = r.hgetall("RisksInfo:%s" % strategy_name)
#         for (dict_key, strategy_risk_info_str) in base_strategy_dict.items():
#             position_msg = AllProtoMsg_pb2.InstrumentPosition()
#             position_msg.ParseFromString(strategy_risk_info_str)
#
#             instrument_msg = instrument_dict[dict_key]
#             print instrument_msg.ticker, position_msg.Long, position_msg.Short
#
#
# def __position_validate():
#     base_instrument_dict = r.hgetall("Instrument_all")
#     instrument_dict = dict()
#     for (target_id, instrument_info_str) in base_instrument_dict.items():
#         instrument_msg = AllProtoMsg_pb2.Instrument()
#         instrument_msg.ParseFromString(instrument_info_str)
#         instrument_dict[target_id] = instrument_msg
#     account_list = r.lrange("Positions", 0, -1)
#     for account_name in account_list:
#         print '-------------%s--------------' % account_name
#         base_position_dict = r.hgetall("PositionsInfo:%s" % account_name)
#         for (dict_key, position_info_str) in base_position_dict.items():
#             position_msg = AllProtoMsg_pb2.InstrumentPosition()
#             position_msg.ParseFromString(position_info_str)
#
#             instrument_msg = instrument_dict[dict_key]
#             print instrument_msg.ticker, position_msg.Long, position_msg.Short


if __name__ == '__main__':
    save_aggregator_to_redis('aggregator')
    # loader_market_from_redis()

    # loader_market_from_redis()
    # __market_validate()
    # __trade_validate()
    # __position_validate()
    # __order_validate()
