# coding: utf-8
import calendar
import datetime
import os
import pandas as pd
import numpy as np
from eod_aps.model.schema_jobs import HardWareInfo, SpecialTickers, DailyVwapAnalyse
from eod_aps.model.schema_portfolio import RealAccount, PfAccount
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.schema_strategy import StrategyGrouping
from eod_aps.tools.tradeplat_position_tools import RiskView, InstrumentView
from eod_aps.tools.vwap_stream_cal_tools import VwapCalTools
from flask import render_template, request, current_app, flash, redirect, url_for, jsonify, make_response
from . import report
import json
from flask_login import login_required
from eod_aps.model.schema_history import ServerRisk
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.tools.phone_trade_tools import send_phone_trade
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.common_utils import CommonUtils

SEARCH_ATTRS = {'attr_names': ['server_name', ],
                'server_name': ['all', 'huabao', 'guoxin', 'nanhua', 'zhongxin', 'luzheng']}
date_utils = DateUtils()
common_utils = CommonUtils()

custom_enum_utils = CustomEnumUtils()
order_type_inversion_dict = custom_enum_utils.enum_to_dict(const.ORDER_TYPE_ENUMS, True)
hedgeflag_type_inversion_dict = custom_enum_utils.enum_to_dict(const.HEDGEFLAG_TYPE_ENUMS, True)
order_status_inversion_dict = custom_enum_utils.enum_to_dict(const.ORDER_STATUS_ENUMS, True)
operation_status_inversion_dict = custom_enum_utils.enum_to_dict(const.OPERATION_STATUS_ENUMS, True)
trade_type_inversion_dict = custom_enum_utils.enum_to_dict(const.TRADE_TYPE_ENUMS, True)
algo_status_inversion_dict = custom_enum_utils.enum_to_dict(const.ALGO_STATUS_ENUMS, True)

instrument_type_inversion_dict = custom_enum_utils.enum_to_dict(const.INSTRUMENT_TYPE_ENUMS, True)
market_status_inversion_dict = custom_enum_utils.enum_to_dict(const.MARKET_STATUS_ENUMS, True)
exchange_type_inversion_dict = custom_enum_utils.enum_to_dict(const.EXCHANGE_TYPE_ENUMS, True)

direction_dict = custom_enum_utils.enum_to_dict(const.DIRECTION_ENUMS)
trade_type_dict = custom_enum_utils.enum_to_dict(const.TRADE_TYPE_ENUMS)
io_type_dict = custom_enum_utils.enum_to_dict(const.IO_TYPE_ENUMS)


@report.route('/report_risks', methods=['GET', 'POST'])
@login_required
def report_risks():
    return render_template('report/report_risks.html', search_attrs=SEARCH_ATTRS, risk_list=[])


@report.route('/report_risks_func', methods=['GET', 'POST'])
@login_required
def report_risks_func():
    config = json.loads(request.form.get('config'))

    query_risk_list = []
    server_model = server_constant.get_server_model('host')
    session_history = server_model.get_db_session('history')
    for server_risk_db in session_history.query(ServerRisk).filter(
            ServerRisk.date.between(config['start_day'], config['end_day']),
            ServerRisk.server_name == config['server_name']):
        query_risk_list.append(server_risk_db)
    return json.dumps(query_risk_list)


@report.route('/info', methods=['GET'])
def basic_info():
    basic_item = {
        'general': 'test_value',
        'cpu': 'cpu'
    }
    return make_response(jsonify(code=200, data=basic_item), 200)


@report.route('/market_list', methods=['GET', 'POST'])
def query_market_list():
    query_params = request.json
    query_instrument_type = query_params.get('instrument_type')
    query_exchange = query_params.get('exchange')
    query_ticker = query_params.get('ticker')

    query_result_list = []
    if 'instrument_dict' not in const.EOD_POOL or 'market_dict' not in const.EOD_POOL:
        return make_response(jsonify(code=200, data={'data': [], 'total': 0}), 200)

    instrument_msg_dict = const.EOD_POOL['instrument_dict']
    for (market_id, market_msg) in const.EOD_POOL['market_dict'].items():
        market_item_dict = dict()
        instrument_msg = instrument_msg_dict[market_id]
        if query_ticker and query_ticker.lower() not in instrument_msg.ticker.lower():
            continue
        if query_instrument_type and query_instrument_type != instrument_type_inversion_dict[instrument_msg.TypeIDWire]:
            continue
        if query_exchange and query_exchange != exchange_type_inversion_dict[instrument_msg.ExchangeIDWire]:
            continue

        market_item_dict['Symbol'] = instrument_msg.ticker
        market_item_dict['Bid1'] = market_msg.Args.Bid1
        market_item_dict['Bid1Size'] = market_msg.Args.Bid1Size
        market_item_dict['Ask1'] = market_msg.Args.Ask1
        market_item_dict['Ask1Size'] = market_msg.Args.Ask1Size
        market_item_dict['YdVolume'] = market_msg.Args.VolumeTdy
        market_item_dict['Volume'] = market_msg.Args.Volume
        market_item_dict['UpdateTime'] = common_utils.format_msg_time(market_msg.Args.UpdateTime).strftime('%H:%M:%S')
        market_item_dict['PrevClose'] = instrument_msg.prevCloseWired
        market_item_dict['LastPrice'] = '%.2f' % market_msg.Args.LastPrice
        market_item_dict['NominalPrice'] = '%.2f' % market_msg.Args.NominalPrice
        market_item_dict['Status'] = market_status_inversion_dict[instrument_msg.marketStatusWired]

        if float(instrument_msg.prevCloseWired) == 0:
            chg_value = 0
        else:
            chg_value = '%.2f%%' % ((market_msg.Args.NominalPrice / instrument_msg.prevCloseWired - 1) * 100)
        market_item_dict['Chg'] = chg_value
        market_item_dict['BidAbnormal'] = market_msg.Args.BidAbnormal
        market_item_dict['AskAbnormal'] = market_msg.Args.AskAbnormal
        query_result_list.append(market_item_dict)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            query_result_list = sorted(query_result_list, key=lambda market_item: market_item[sort_prop], reverse=True)
        else:
            query_result_list = sorted(query_result_list, key=lambda market_item: market_item[sort_prop])
    else:
        query_result_list.sort(key=lambda obj: obj['Symbol'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(query_result_list)

    result_list = query_result_list[(query_page - 1) * query_size: query_page * query_size]

    stock_basic_data_dict = const.EOD_CONFIG_DICT['stock_basic_data_dict']
    for market_item_dict in result_list:
        tooltip_list = []
        temp_ticker = market_item_dict['Symbol']
        if temp_ticker in stock_basic_data_dict:
            stock_basic_data = stock_basic_data_dict[temp_ticker]
            tooltip_list.append('股票名:%s' % stock_basic_data['name'])
            tooltip_list.append('行业:%s' % stock_basic_data['industry'])
            tooltip_list.append('概念股:%s' % stock_basic_data['conception'])
            tooltip_list.append('预期pe:%s' % str(stock_basic_data['est_pe_fy1']))
            tooltip_list.append('市值(万元):%s' % str(stock_basic_data['market_value']))
        market_item_dict['tooltip'] = '<br/>'.join(tooltip_list)

    query_result = {'data': result_list,
                    'total': total_number
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/order_list', methods=['GET', 'POST'])
def query_order_list():
    query_params = request.json
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')
    query_strategy_type = query_params.get('strategy_type')
    query_strategy_list = []
    if query_strategy_type:
        strategy_grouping_dict = const.EOD_CONFIG_DICT['strategy_grouping_dict']
        if len(query_strategy_type) == 1:
            for (sub_group_name, strategy_list) in strategy_grouping_dict[query_strategy_type[0]].items():
                query_strategy_list.extend(strategy_list)
        elif len(query_strategy_type) == 2:
            query_strategy_list.extend(strategy_grouping_dict[query_strategy_type[0]][query_strategy_type[1]])
        else:
            query_strategy_list.append(query_strategy_type[2])
    query_strategy = query_params.get('strategy')
    query_ticker = query_params.get('ticker')
    query_order_id = query_params.get('order_id')
    query_order_status = query_params.get('order_status')
    query_algo_status = query_params.get('algo_status')

    if 'order_dict' not in const.EOD_POOL:
        return make_response(jsonify(code=200, data={'data': [], 'total': 0}), 200)

    query_result_list = []
    for (dict_key, order_item_dict) in const.EOD_POOL['order_view_tree_dict'].items():
        if query_fund_name and query_fund_name not in order_item_dict['Account']:
            continue
        if query_strategy and query_strategy.lower() not in order_item_dict['Strategy'].lower():
            continue
        strategy_name_item = order_item_dict['Strategy'].split('.')
        if query_strategy_list and strategy_name_item[0] not in query_strategy_list:
            continue
        if query_ticker and query_ticker.lower() not in order_item_dict['Symbol'].lower():
            continue
        if query_order_id and query_order_id not in order_item_dict['OrderID']:
            continue
        if query_server_name and query_server_name != order_item_dict['Server']:
            continue
        if query_order_status and query_order_status != order_item_dict['Status']:
            continue
        if query_algo_status and query_algo_status != order_item_dict['AlgoStatus']:
            continue

        query_result_list.append(order_item_dict)
    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(query_result_list)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            query_result_list = sorted(query_result_list, key=lambda market_item: market_item[sort_prop], reverse=True)
        else:
            query_result_list = sorted(query_result_list, key=lambda market_item: market_item[sort_prop])
    else:
        query_result_list.sort(key=lambda obj: obj['TransactionT'], reverse=True)

    query_result = {'data': query_result_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/trade_list', methods=['GET', 'POST'])
def query_trade_list():
    query_params = request.json
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')
    query_strategy_type = query_params.get('strategy_type')
    query_strategy_list = []
    if query_strategy_type:
        strategy_grouping_dict = const.EOD_CONFIG_DICT['strategy_grouping_dict']
        if len(query_strategy_type) == 1:
            for (sub_group_name, strategy_list) in strategy_grouping_dict[query_strategy_type[0]].items():
                query_strategy_list.extend(strategy_list)
        elif len(query_strategy_type) == 2:
            query_strategy_list.extend(strategy_grouping_dict[query_strategy_type[0]][query_strategy_type[1]])
        else:
            query_strategy_list.append(query_strategy_type[2])
    query_strategy = query_params.get('strategy')
    query_ticker = query_params.get('ticker')

    if 'trade_list' not in const.EOD_POOL:
        return make_response(jsonify(code=200, data={'data': [], 'total': 0}), 200)

    query_result_list = []
    for (trade_time, trade_msg) in const.EOD_POOL['trade_list']:
        if query_fund_name and query_fund_name not in trade_msg.Trade.AccountID:
            continue
        if query_strategy and query_strategy.lower() not in trade_msg.Trade.StrategyID.lower():
            continue
        strategy_name_item = trade_msg.Trade.StrategyID.split('.')
        if query_strategy_list and strategy_name_item[0] not in query_strategy_list:
            continue
        if query_ticker and query_ticker.lower() not in trade_msg.Trade.symbol.lower():
            continue

        server_name = common_utils.get_server_name(trade_msg.Location)
        if query_server_name and query_server_name != server_name:
            continue

        trade_item_dict = dict()
        trade_item_dict['Time'] = trade_time
        trade_item_dict['Symbol'] = trade_msg.Trade.symbol
        trade_item_dict['Qty'] = trade_msg.Trade.Qty
        trade_item_dict['Price'] = trade_msg.Trade.Price
        trade_item_dict['Type'] = trade_msg.Trade.TradeTypeWired
        trade_item_dict['TradePL'] = trade_msg.Trade.TradePL.PL
        trade_item_dict['Fee'] = trade_msg.Trade.TradeFee
        # trade_item_dict['FairTradePL'] = trade_msg
        # trade_item_dict['NetTradePL'] = trade_msg
        # trade_item_dict['Delta'] = trade_msg
        # trade_item_dict['Gamma'] = trade_msg
        # trade_item_dict['Vega'] = trade_msg
        # trade_item_dict['Theta'] = trade_msg
        # trade_item_dict['Undl'] = trade_msg
        # trade_item_dict['C/P'] = trade_msg
        #
        # trade_item_dict['Expire'] = trade_msg
        # trade_item_dict['Strike'] = trade_msg
        trade_item_dict['OrderID'] = trade_msg.Trade.OrderID
        trade_item_dict['Strategy'] = trade_msg.Trade.StrategyID
        trade_item_dict['Account'] = trade_msg.Trade.AccountID
        trade_item_dict['NominalTradeType'] = trade_msg.Trade.NominalTradeTypeWired
        trade_item_dict['TradeID'] = trade_msg.Trade.TradeID
        trade_item_dict['SelfCross'] = trade_msg.Trade.SelfCross
        trade_item_dict['Note'] = trade_msg.Trade.Note
        trade_item_dict['Server'] = server_name
        query_result_list.append(trade_item_dict)
    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(query_result_list)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            query_result_list = sorted(query_result_list,
                                       key=lambda pf_position_item: float(pf_position_item[sort_prop]), reverse=True)
        else:
            query_result_list = sorted(query_result_list,
                                       key=lambda pf_position_item: float(pf_position_item[sort_prop]))
    else:
        query_result_list.sort(key=lambda obj: obj['Time'], reverse=True)

    query_result = {'data': query_result_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_pf_position', methods=['GET', 'POST'])
def query_pf_position():
    query_params = request.json
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')
    query_strategy_type = query_params.get('strategy_type')
    query_strategy_list = []
    if query_strategy_type:
        strategy_grouping_dict = const.EOD_CONFIG_DICT['strategy_grouping_dict']
        if len(query_strategy_type) == 1:
            for (sub_group_name, strategy_list) in strategy_grouping_dict[query_strategy_type[0]].items():
                query_strategy_list.extend(strategy_list)
        elif len(query_strategy_type) == 2:
            query_strategy_list.extend(strategy_grouping_dict[query_strategy_type[0]][query_strategy_type[1]])
        else:
            query_strategy_list.append(query_strategy_type[2])
    query_strategy = query_params.get('strategy')
    query_ticker = query_params.get('ticker')

    if 'instrument_dict' not in const.EOD_POOL or 'market_dict' not in const.EOD_POOL or \
            'risk_dict' not in const.EOD_POOL:
        query_result = {'data': [],
                        'total': 0,
                        'sum_trading_pl': 0,
                        'sum_position_pl': 0,
                        'sum_total_pl': 0,
                        'update_time': ''
                        }
        return make_response(jsonify(code=200, data=query_result), 200)

    query_result_list = []
    instrument_msg_dict = const.EOD_POOL['instrument_dict']
    market_msg_dict = const.EOD_POOL['market_dict']

    sum_trading_pl = 0.0
    sum_position_pl = 0.0
    for (strategy_name, strategy_risk_dict) in const.EOD_POOL['risk_dict'].items():
        if query_server_name:
            server_model = server_constant.get_server_model(query_server_name)
            if server_model.ip not in strategy_name:
                continue
        if query_fund_name and query_fund_name not in strategy_name:
            continue
        if query_strategy and query_strategy.lower() not in strategy_name.lower():
            continue
        strategy_name_item = strategy_name.split('-')
        if query_strategy_list and strategy_name_item[1] not in query_strategy_list:
            continue

        for (instrument_key, position_msg) in strategy_risk_dict.items():
            if instrument_key not in instrument_msg_dict:
                print instrument_key
                continue

            instrument_msg = instrument_msg_dict[instrument_key]
            if query_ticker and query_ticker.lower() not in instrument_msg.ticker.lower():
                continue

            (base_strategy_name, server_ip_str) = strategy_name.split('@')
            market_msg = market_msg_dict[instrument_key]
            instrument_view = InstrumentView(instrument_msg, market_msg)
            risk_view = RiskView(instrument_view, position_msg, base_strategy_name)

            pf_position_item_dict = dict()
            pf_position_item_dict['Strategy'] = base_strategy_name
            pf_position_item_dict['Server'] = common_utils.get_server_name(server_ip_str)
            pf_position_item_dict['Ticker'] = instrument_msg.ticker
            pf_position_item_dict['HedgeFlag'] = position_msg.HedgeFlagWire
            pf_position_item_dict['YdLongRemain'] = position_msg.YdLongRemain
            pf_position_item_dict['PrevLong'] = position_msg.PrevLong
            pf_position_item_dict['Long'] = position_msg.Long
            pf_position_item_dict['LongCost'] = position_msg.LongCost
            pf_position_item_dict['DayLong'] = position_msg.DayLong
            pf_position_item_dict['DayLongCost'] = position_msg.DayLongCost
            pf_position_item_dict['PrevLongAvail'] = position_msg.PrevLongAvailable
            pf_position_item_dict['LongAvail'] = position_msg.LongAvailable
            pf_position_item_dict['YdShortRemain'] = position_msg.YdShortRemain
            pf_position_item_dict['PrevShort'] = position_msg.PrevShort
            pf_position_item_dict['Short'] = position_msg.Short
            pf_position_item_dict['ShortCost'] = position_msg.ShortCost
            pf_position_item_dict['DayShort'] = position_msg.DayShort
            pf_position_item_dict['DayShortCost'] = position_msg.DayShortCost
            # pf_position_item_dict['PrevShortAvail'] = position_msg.PrevShortAvailable
            pf_position_item_dict['ShortAvail'] = position_msg.ShortAvailable

            pf_position_item_dict['TradingPL'] = risk_view.trading_pl
            pf_position_item_dict['DayTradeFee'] = position_msg.DayTradeFee
            pf_position_item_dict['PositionPL'] = risk_view.position_pl
            pf_position_item_dict['TotalPL'] = risk_view.total_pl
            query_result_list.append(pf_position_item_dict)

            sum_trading_pl += risk_view.trading_pl
            sum_position_pl += risk_view.position_pl

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            query_result_list = sorted(query_result_list,
                                       key=lambda pf_position_item: __format_sort_str(pf_position_item[sort_prop]),
                                       reverse=True)
        else:
            query_result_list = sorted(query_result_list,
                                       key=lambda pf_position_item: __format_sort_str(pf_position_item[sort_prop]))
    else:
        query_result_list.sort(key=lambda obj: obj['Strategy'] + obj['Server'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(query_result_list)

    query_result = {'data': query_result_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number,
                    'sum_trading_pl': sum_trading_pl,
                    'sum_position_pl': sum_position_pl,
                    'sum_total_pl': sum_trading_pl + sum_position_pl,
                    'update_time': const.EOD_POOL['position_update_time']
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


def __format_sort_str(input_str):
    try:
        input_str = float(input_str)
    except ValueError:
        pass
    return input_str


@report.route('/query_real_position', methods=['GET', 'POST'])
def query_real_position():
    query_params = request.json
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')
    query_ticker = query_params.get('ticker')

    if 'instrument_dict' not in const.EOD_POOL:
        return make_response(jsonify(code=200, data={'data': [], 'total': 0, 'sum_trading_pl': 0,
                                                     'sum_position_pl': 0, 'sum_total_pl': 0}), 200)

    query_result_list = []
    instrument_msg_dict = const.EOD_POOL['instrument_dict']
    market_msg_dict = const.EOD_POOL['market_dict']

    sum_trading_pl = 0.0
    sum_position_pl = 0.0
    for (account_name, account_position_dict) in const.EOD_POOL['position_dict'].items():
        if query_server_name:
            server_model = server_constant.get_server_model(query_server_name)
            if server_model.ip not in account_name:
                continue
        if query_fund_name and query_fund_name not in account_name:
            continue

        for (instrument_key, position_msg) in account_position_dict.items():
            if instrument_key not in instrument_msg_dict:
                print instrument_key
                continue

            instrument_msg = instrument_msg_dict[instrument_key]
            if query_ticker and query_ticker.lower() not in instrument_msg.ticker.lower():
                continue

            (base_account_name, server_ip_str) = account_name.split('@')
            market_msg = market_msg_dict[instrument_key]
            instrument_view = InstrumentView(instrument_msg, market_msg)
            risk_view = RiskView(instrument_view, position_msg, account_name)

            position_item_dict = dict()
            position_item_dict['Server'] = common_utils.get_server_name(server_ip_str)
            position_item_dict['Account'] = base_account_name
            position_item_dict['Ticker'] = instrument_msg.ticker
            position_item_dict['HedgeFlag'] = position_msg.HedgeFlagWire
            position_item_dict['YdLongRemain'] = position_msg.YdLongRemain
            position_item_dict['PrevLong'] = position_msg.PrevLong
            position_item_dict['Long'] = position_msg.Long
            position_item_dict['LongCost'] = position_msg.LongCost
            position_item_dict['DayLong'] = position_msg.DayLong
            position_item_dict['DayLongCost'] = position_msg.DayLongCost
            position_item_dict['PrevLongAvail'] = position_msg.PrevLongAvailable
            position_item_dict['LongAvail'] = position_msg.LongAvailable
            position_item_dict['YdShortRemain'] = position_msg.YdShortRemain
            position_item_dict['PrevShort'] = position_msg.PrevShort
            position_item_dict['Short'] = position_msg.Short
            position_item_dict['ShortCost'] = position_msg.ShortCost
            position_item_dict['DayShort'] = position_msg.DayShort
            position_item_dict['DayShortCost'] = position_msg.DayShortCost
            # pf_position_item_dict['PrevShortAvail'] = position_msg.PrevShortAvailable
            position_item_dict['ShortAvail'] = position_msg.ShortAvailable

            position_item_dict['TradingPL'] = risk_view.trading_pl
            position_item_dict['DayTradeFee'] = position_msg.DayTradeFee
            position_item_dict['PositionPL'] = risk_view.position_pl
            position_item_dict['TotalPL'] = risk_view.total_pl
            query_result_list.append(position_item_dict)

            sum_trading_pl += risk_view.trading_pl
            sum_position_pl += risk_view.position_pl

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            query_result_list = sorted(query_result_list, key=lambda position_item: float(position_item[sort_prop]),
                                       reverse=True)
        else:
            query_result_list = sorted(query_result_list, key=lambda position_item: float(position_item[sort_prop]))
    else:
        query_result_list.sort(key=lambda obj: obj['Server'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(query_result_list)
    query_result = {'data': query_result_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number,
                    'sum_trading_pl': sum_trading_pl,
                    'sum_position_pl': sum_position_pl,
                    'sum_total_pl': (sum_trading_pl + sum_position_pl)
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_services', methods=['GET', 'POST'])
def query_services():
    params = request.json
    server_name = params.get('server_name')

    service_list = []
    server_model = server_constant.get_server_model('host')
    session_common = server_model.get_db_session('common')
    query_sql = "select app_name from common.server_info where server_name= '%s'" % server_name
    query_result = session_common.execute(query_sql)
    for result_item in query_result:
        server_item_dict = dict()
        server_item_dict['value'] = result_item[0]
        server_item_dict['label'] = result_item[0]
        service_list.append(server_item_dict)
    return make_response(jsonify(code=200, data=service_list), 200)


@report.route('/query_log_files', methods=['GET', 'POST'])
def query_log_files():
    params = request.json
    server_name = params.get('server_name')
    service_name = params.get('service_name')

    server_model = server_constant.get_server_model(server_name)
    cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                'ls *%s*.log' % service_name
                ]
    log_file_list = server_model.run_cmd_str2(';'.join(cmd_list))

    return_list = []
    for log_file_name in log_file_list:
        item_dict = dict()
        item_dict['value'] = log_file_name
        item_dict['label'] = log_file_name
        return_list.append(item_dict)
    return make_response(jsonify(code=200, data=return_list), 200)


@report.route('/query_ts_accounts', methods=['GET', 'POST'])
def query_ts_accounts():
    ts_account_list = []

    ts_server_list = server_constant.get_ts_servers()
    for server_name in ts_server_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(RealAccount)
        for account_db in query.filter(RealAccount.accounttype == 'TS', RealAccount.enable == 1):
            item_dict = dict()
            account_name = '%s-%s-%s-' % (account_db.accountname, account_db.accounttype, account_db.fund_name)
            item_dict['value'] = account_name
            item_dict['label'] = account_name
            ts_account_list.append(item_dict)
    return make_response(jsonify(code=200, data=ts_account_list), 200)


@report.route('/query_fund_by_server', methods=['GET', 'POST'])
def query_fund_by_server():
    params = request.json
    server_name = params.get('servername')

    fund_list = []
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_account = session_portfolio.query(RealAccount)
    for result_item in query_account.group_by(RealAccount.fund_name):
        fund_item_dict = dict()
        fund_item_dict['value'] = result_item.fund_name
        fund_item_dict['label'] = result_item.fund_name
        fund_list.append(fund_item_dict)
    fund_list.sort()

    query_result = {'data': fund_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_strategy_by_fund', methods=['GET', 'POST'])
def query_strategy_by_fund():
    params = request.json
    server_name = params.get('servername')
    fund = params.get('fund')
    print server_name, fund

    strategy_list = []
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.fund_name.like('%' + fund + '%')):
        strategy_item_dict = dict()
        strategy_item_dict['value'] = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        strategy_item_dict['label'] = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        strategy_list.append(strategy_item_dict)
    strategy_list.sort()

    query_result = {'data': strategy_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/phone_trade_list', methods=['GET', 'POST'])
def save_phone_trade_list():
    params = request.json
    print params

    phone_trade_list = []
    for param_item in params:
        phone_trade_info = PhoneTradeInfo()
        phone_trade_info.server_name = param_item.get('servername')
        phone_trade_info.fund = param_item.get('fundname')
        phone_trade_info.strategy1 = param_item.get('strategy1')

        phone_trade_info.symbol = param_item.get('ticker')
        phone_trade_info.exqty = param_item.get('volume')
        phone_trade_info.exprice = param_item.get('price')

        phone_trade_info.direction = direction_dict[param_item.get('direction')]
        phone_trade_info.tradetype = trade_type_dict[param_item.get('tradetype')]

        phone_trade_info.hedgeflag = const.HEDGEFLAG_TYPE_ENUMS.Speculation
        phone_trade_info.strategy2 = param_item.get('strategy2')
        phone_trade_info.iotype = io_type_dict[param_item.get('iotype')]
        phone_trade_list.append(phone_trade_info)
    server_save_path = os.path.join(const.EOD_CONFIG_DICT['phone_trade_folder'], param_item.get('servername'))
    if not os.path.exists(server_save_path):
        os.mkdir(server_save_path)
    send_phone_trade(param_item.get('servername'), phone_trade_list)

    query_result = {'data': "success"}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_risk_history', methods=['GET', 'POST'])
def query_risk_history():
    query_params = request.json
    user_name = query_params.get('user_name')
    search_date_item = query_params.get('search_date')
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')
    query_strategy_type = query_params.get('strategy_type')
    query_strategy_list = []
    strategy_grouping_dict = const.EOD_CONFIG_DICT['strategy_grouping_dict']
    if query_strategy_type:
        if len(query_strategy_type) == 1:
            for (sub_group_name, strategy_list) in strategy_grouping_dict[query_strategy_type[0]].items():
                query_strategy_list.extend(strategy_list)
        elif len(query_strategy_type) == 2:
            query_strategy_list.extend(strategy_grouping_dict[query_strategy_type[0]][query_strategy_type[1]])
        else:
            query_strategy_list.append(query_strategy_type[2])
    query_strategy_name = query_params.get('strategy_name')

    if search_date_item:
        [start_date, end_date] = search_date_item
        start_date = start_date[:10]
        end_date = end_date[:10]
    else:
        start_date = date_utils.get_today_str('%Y-%m-%d')
        end_date = start_date

    query_sql = "select strategy_group_list from jobs.user_list where user_id='%s'" % user_name
    server_model = server_constant.get_server_model('host')
    session_jobs = server_model.get_db_session('jobs')
    strategy_group_list_str = session_jobs.execute(query_sql).first()[0]
    filter_group_list = strategy_group_list_str.split(',') if strategy_group_list_str is not None else []

    strategy_group_name_dict = dict()
    for (group_name, temp_group_dict) in strategy_grouping_dict.items():
        for (sub_group_name, temp_strategy_list) in temp_group_dict.items():
            for temp_strategy_name in temp_strategy_list:
                strategy_group_name_dict[temp_strategy_name] = group_name

    server_risk_db_list = []
    server_host = server_constant.get_server_model('host')
    session_history = server_host.get_db_session('history')
    sum_total_pl = 0
    for server_risk_db in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
        item_dict = dict()
        if query_server_name and query_server_name != server_risk_db.server_name:
            continue
        if query_fund_name and query_fund_name not in server_risk_db.strategy_name:
            continue
        if query_strategy_name and query_strategy_name.lower() not in server_risk_db.strategy_name.lower():
            continue

        strategy_name_item = server_risk_db.strategy_name.split('-')
        # 根据账户权限过滤
        if len(filter_group_list) > 0 and strategy_group_name_dict[strategy_name_item[1]] not in filter_group_list:
            continue
        if query_strategy_list and strategy_name_item[1] not in query_strategy_list:
            continue

        item_dict['server_name'] = server_risk_db.server_name
        item_dict['date'] = date_utils.datetime_toString(server_risk_db.date)
        item_dict['strategy_name'] = server_risk_db.strategy_name
        item_dict['position_pl'] = server_risk_db.position_pl
        item_dict['trading_pl'] = server_risk_db.trading_pl
        item_dict['fee'] = server_risk_db.fee
        item_dict['stocks_pl'] = server_risk_db.stocks_pl
        item_dict['future_pl'] = server_risk_db.future_pl
        item_dict['total_pl'] = server_risk_db.total_pl
        item_dict['total_stocks_value'] = server_risk_db.total_stocks_value
        item_dict['total_future_value'] = server_risk_db.total_future_value
        server_risk_db_list.append(item_dict)
        sum_total_pl += float(server_risk_db.total_pl)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            server_risk_db_list = sorted(server_risk_db_list, key=lambda server_risk: int(server_risk[sort_prop]),
                                         reverse=True)
        else:
            server_risk_db_list = sorted(server_risk_db_list, key=lambda server_risk: int(server_risk[sort_prop]))

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(server_risk_db_list)
    result_list = server_risk_db_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': total_number, 'sum_total_pl': sum_total_pl}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/export_risk_history', methods=['GET', 'POST'])
def export_risk_history():
    query_params = request.json
    user_token = query_params.get('user_token')
    search_date_item = query_params.get('search_date')
    server_name = query_params.get('server_name')
    fund_name = query_params.get('fund_name')
    query_strategy_name = query_params.get('strategy_name')

    if search_date_item:
        [start_date, end_date] = search_date_item
        start_date = date_utils.get_last_trading_day('%Y-%m-%d', start_date[:10])
        end_date = end_date[:10]
    else:
        start_date = date_utils.get_today_str('%Y-%m-%d')
        end_date = start_date

    user_id = user_token.split('|')[0]
    query_sql = "select pf_account_list from jobs.user_list where user_id='%s'" % user_id
    server_model = server_constant.get_server_model('host')
    session_jobs = server_model.get_db_session('jobs')
    pf_account_list_str = session_jobs.execute(query_sql).first()[0]
    filter_pf_account_list = pf_account_list_str.split(',')

    index_return_rate_list = []
    query_sql = "select date, RETURN_RATE from jobs.daily_return_history where ticker = 'SH000905' and date >= '%s' \
    and date <= '%s'" % (start_date, end_date)
    session_jobs = server_model.get_db_session('jobs')
    for query_result_item in session_jobs.execute(query_sql):
        date_str = date_utils.datetime_toString(query_result_item[0])
        index_return_rate_list.append([date_str, query_result_item[1]])
    index_return_rate_df = pd.DataFrame(index_return_rate_list, columns=["Date", "Index_Rate"])

    server_risk_db_list = []
    server_host = server_constant.get_server_model('host')
    session_history = server_host.get_db_session('history')
    for server_risk_db in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
        if server_name and server_name != server_risk_db.server_name:
            continue
        if fund_name and fund_name not in server_risk_db.strategy_name:
            continue
        if query_strategy_name and query_strategy_name.lower() not in server_risk_db.strategy_name.lower():
            continue

        strategy_name_item = server_risk_db.strategy_name.split('-')
        if filter_pf_account_list and strategy_name_item[1] not in filter_pf_account_list:
            continue

        item_list = [date_utils.datetime_toString(server_risk_db.date), server_risk_db.total_stocks_value,
                     abs(server_risk_db.total_future_value), server_risk_db.position_pl, server_risk_db.future_pl,
                     server_risk_db.stocks_pl, server_risk_db.total_pl]
        server_risk_db_list.append(item_list)

    risk_view_df = pd.DataFrame(server_risk_db_list, columns=["Date", "Total_Stocks_Value", "Total_Future_Value",
                                                              "Position_PL", "Stocks_PL", "Future_PL", "Total_PL"])

    groupby_df1 = risk_view_df.groupby("Date").sum()[["Total_Stocks_Value", "Total_Future_Value", "Position_PL",
                                                      "Stocks_PL", "Future_PL", "Total_PL"]]
    groupby_df1['Date'] = groupby_df1.index.values
    groupby_df1.index = range(len(groupby_df1))
    groupby_df1 = pd.merge(groupby_df1, index_return_rate_df, on=['Date'], how='left')

    groupby_df1['Pre_Stocks_Value'] = groupby_df1.Total_Stocks_Value.shift(1)
    groupby_df1 = groupby_df1.sort_values('Date', ascending=True)
    groupby_df1 = groupby_df1.drop(0)

    groupby_df1['Alpha_Value'] = groupby_df1.Position_PL / groupby_df1.Pre_Stocks_Value - groupby_df1.Index_Rate
    groupby_df1['Net_Rate'] = groupby_df1.Total_PL / groupby_df1.Pre_Stocks_Value
    groupby_df1.index = groupby_df1.Date

    del groupby_df1['Position_PL']
    del groupby_df1['Total_Future_Value']
    groupby_df1 = groupby_df1.fillna(0)

    groupby_df1['Alpha_Value'] = groupby_df1['Alpha_Value'].apply(lambda x: '%.4f%%' % (x * 100))
    groupby_df1['Net_Rate'] = groupby_df1['Net_Rate'].apply(lambda x: '%.4f%%' % (x * 100))
    groupby_df1['Index_Rate'] = groupby_df1['Index_Rate'].apply(lambda x: '%.4f%%' % (x * 100))

    groupby_df1['Pre_Stocks_Value'] = groupby_df1['Pre_Stocks_Value'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Total_Stocks_Value'] = groupby_df1['Total_Stocks_Value'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Stocks_PL'] = groupby_df1['Stocks_PL'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Future_PL'] = groupby_df1['Future_PL'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Total_PL'] = groupby_df1['Total_PL'].apply(lambda x: '{:,}'.format(x))

    return_data_dict = groupby_df1.to_dict("index")
    return_data_list = [dict_value for (dict_key, dict_value) in return_data_dict.items()]
    return_data_list.sort(key=lambda obj: obj['Date'])
    query_result = {'data_list': return_data_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/refresh_risk_history', methods=['GET', 'POST'])
def refresh_risk_history():
    from eod_aps.job.server_risk_backup_job import server_risk_backup_job
    server_risk_backup_job()
    return make_response(jsonify(code=200, message=u"更新成功"), 200)


@report.route('/query_risk_history_detail', methods=['GET', 'POST'])
def query_risk_history_detail():
    query_params = request.json
    strategy_name = query_params.get('strategy_name')
    if 'Earning' in strategy_name:
        strategy_name = 'Earning'
    elif 'Inflow' in strategy_name:
        strategy_name = 'Inflow'

    date_list = date_utils.get_interval_trading_day_list(date_utils.get_now(), -20, '%Y-%m-%d')
    date_list.reverse()
    start_date = date_list[0]
    end_date = date_list[-1]

    query_result_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_history = server_host.get_db_session('history')

    for server_risk_db in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
        if strategy_name not in server_risk_db.strategy_name:
            continue

        item_date = date_utils.datetime_toString(server_risk_db.date)
        if item_date in query_result_dict:
            query_result_dict[item_date] += float(server_risk_db.total_pl)
        else:
            query_result_dict[item_date] = float(server_risk_db.total_pl)

    pl_list = []
    for date_item in date_list:
        if date_item in query_result_dict:
            pl_list.append(query_result_dict[date_item])
        else:
            pl_list.append(0)

    query_result = {'date_list': date_list, 'pl_list': pl_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/save_user', methods=['GET', 'POST'])
def save_user():
    params = request.json
    id = params.get('id')
    user_id = params.get('user_id')
    password = params.get('password')
    strategy_group_list = params.get('strategy_group_list')
    describe = params.get('describe')
    role_id = params.get('role_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    if id:
        sql_str = "UPDATE `jobs`.`user_list` SET user_id='%s', `password`='%s', `strategy_group_list`='%s', \
        `describe`='%s', role_id='%s' WHERE `id`=%s" % (user_id, password, ','.join(strategy_group_list),
                                                        describe, role_id, id)
    else:
        sql_str = "INSERT INTO `jobs`.`user_list` (user_id, `password`, `strategy_group_list`, `describe`, role_id) \
        VALUES ('%s','%s','%s','%s','%s')" % (user_id, password, ','.join(strategy_group_list), describe, role_id)
    session_job.execute(sql_str)
    session_job.commit()
    result_message = u"保存用户:%s成功" % user_id
    return make_response(jsonify(code=200, data=result_message), 200)


@report.route('/query_users', methods=['GET', 'POST'])
def query_users():
    query_sql = 'select a.`id`, a.`user_id`, a.`password`, a.`strategy_group_list`, a.`describe`, a.`role_id`, \
    b.`name` as role_name from user_list a left join role_list b on a.role_id = b.id'

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    user_list = []
    for user_info in session_job.execute(query_sql):
        user_dict = dict()
        user_dict['id'] = user_info[0]
        user_dict['user_id'] = user_info[1]
        user_dict['password'] = user_info[2]
        user_dict['strategy_group_list'] = user_info[3].split(',') if user_info[3] is not None else []
        user_dict['describe'] = user_info[4]
        user_dict['role_id'] = user_info[5]
        user_dict['role_name'] = user_info[6]
        user_list.append(user_dict)
    return make_response(jsonify(code=200, data=user_list), 200)


@report.route('/del_user', methods=['GET', 'POST'])
def del_user():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    del_sql = "delete from `jobs`.`user_list` where id=%s" % del_id
    session_job.execute(del_sql)
    session_job.commit()
    result_message = u"删除用户成功"
    return make_response(jsonify(code=200, data=result_message), 200)


@report.route('/save_role', methods=['GET', 'POST'])
def save_role():
    params = request.json
    print params

    id = params.get('id')
    name = params.get('name')
    menu_id_list = params.get('menu_id_list')
    describe = params.get('describe')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    if id:
        sql_str = "UPDATE `jobs`.`role_list` SET `name`='%s', `describe`='%s', `menu_id_list`='%s' WHERE `id`=%s" % \
                  (name, describe, ';'.join(menu_id_list), id)
    else:
        sql_str = "INSERT INTO `jobs`.`role_list` (`name`, `describe`, `menu_id_list`) VALUES ('%s', '%s', '%s')" % \
                  (name, describe, ';'.join(menu_id_list))
    session_job.execute(sql_str)
    session_job.commit()
    result_message = u"保存角色:%s成功" % name
    return make_response(jsonify(code=200, data=result_message), 200)


@report.route('/del_role', methods=['GET', 'POST'])
def del_role():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    del_sql = "delete from `jobs`.`role_list` where id=%s" % del_id
    session_job.execute(del_sql)
    session_job.commit()
    result_message = u"删除角色成功"
    return make_response(jsonify(code=200, data=result_message), 200)


@report.route('/query_roles', methods=['GET', 'POST'])
def query_roles():
    query_sql = 'select `id`, `name`, `describe`, `menu_id_list` from `jobs`.`role_list`'

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    role_list = []
    for role_info in session_job.execute(query_sql):
        role_dict = dict()
        role_dict['id'] = role_info[0]
        role_dict['name'] = role_info[1]
        role_dict['describe'] = role_info[2]
        role_dict['menu_id_list'] = role_info[3].split(';')
        role_list.append(role_dict)
    return make_response(jsonify(code=200, data=role_list), 200)


@report.route('/query_menus', methods=['GET', 'POST'])
def query_menus():
    query_sql = 'select `id`, `name`, `describe`, `url` from menu_list'
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    menu_list = []
    for menu_info in session_job.execute(query_sql):
        if menu_info[3]:
            menu_dict = dict()
            menu_dict['key'] = str(menu_info[0])
            menu_dict['label'] = menu_info[1]
            menu_list.append(menu_dict)
    return make_response(jsonify(code=200, data=menu_list), 200)


@report.route('/reset_password', methods=['GET', 'POST'])
def reset_password():
    params = request.json
    user_id = params.get('user_id')
    pre_password = params.get('pre_password')
    reset_pwd = params.get('reset_password')
    print user_id, pre_password

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    query_sql = "select password, role_id from `jobs`.`user_list` where user_id='%s'" % user_id
    user_info_item = session_job.execute(query_sql).first()
    if not user_info_item or user_info_item[0] != pre_password:
        return make_response(jsonify(code=404, message=u"原密码错误!"))
    else:
        update_sql = "UPDATE `jobs`.`user_list` SET `password`='%s' WHERE user_id='%s'" % (reset_pwd, user_id)
        session_job.execute(update_sql)
        session_job.commit()
        return make_response(jsonify(code=200, message=u"重置密码成功."), 200)


@report.route('/query_strategy1_list', methods=['GET', 'POST'])
def query_strategy1_list():
    params = request.json
    server_name = params.get('servername')
    if server_name == '':
        return make_response(jsonify(code=200, data={'data': []}), 200)

    strategy_list = []
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        strategy_item_dict = dict()
        strategy_item_dict['value'] = pf_account_db.fund_name
        strategy_item_dict['label'] = pf_account_db.fund_name
        strategy_list.append(strategy_item_dict)
    strategy_list.sort()

    query_result = {'data': strategy_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_strategy2_list', methods=['GET', 'POST'])
def query_strategy2_list():
    params = request.json
    server_name = params.get('servername')
    strategy1 = params.get('strategy1')
    if server_name == '' or strategy1 == '':
        return make_response(jsonify(code=200, data={'data': []}), 200)

    fund = strategy1.split('-')[2]
    print server_name, fund

    strategy_list = []
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.fund_name.like('%' + fund + '%')):
        strategy_item_dict = dict()
        strategy_item_dict['value'] = pf_account_db.fund_name
        strategy_item_dict['label'] = pf_account_db.fund_name
        strategy_list.append(strategy_item_dict)
    strategy_list.sort()

    query_result = {'data': strategy_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/send_phonetrade', methods=['GET', 'POST'])
def send_phonetrade():
    params = request.json

    phone_trade_info = PhoneTradeInfo()
    phone_trade_info.user = params.get('user')
    phone_trade_info.server_name = params.get('servername')

    strategy1_items = params.get('strategy1').split('-')
    phone_trade_info.fund = strategy1_items[2]
    phone_trade_info.strategy1 = '%s.%s' % (strategy1_items[1], strategy1_items[0])
    phone_trade_info.symbol = params.get('ticker')
    phone_trade_info.exqty = params.get('volume')
    phone_trade_info.exprice = params.get('price')

    phone_trade_info.direction = direction_dict[params.get('direction')]
    phone_trade_info.tradetype = trade_type_dict[params.get('tradetype')]
    phone_trade_info.hedgeflag = const.HEDGEFLAG_TYPE_ENUMS.Speculation
    phone_trade_info.iotype = io_type_dict[params.get('iotype')]
    if phone_trade_info.iotype == const.IO_TYPE_ENUMS.Inner2:
        strategy2_items = params.get('strategy2').split('-')
        phone_trade_info.strategy2 = '%s.%s' % (strategy2_items[1], strategy2_items[0])

    send_phone_trade(params.get('servername'), [phone_trade_info])
    query_result = {'message': "Send PhoneTrade Success!"}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/save_hardware', methods=['GET', 'POST'])
def save_hardware():
    params = request.json
    save_id = params.get('id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    if save_id:
        hardware_info_db = session_job.query(HardWareInfo).filter(HardWareInfo.id == save_id).first()
    else:
        hardware_info_db = HardWareInfo()

    hardware_info_db.location = params.get('location')
    hardware_info_db.type = params.get('type')
    hardware_info_db.ip = params.get('ip')
    hardware_info_db.user_name = params.get('user_name')
    hardware_info_db.operating_system = params.get('operating_system')
    hardware_info_db.marc = params.get('marc')
    hardware_info_db.asset_number = params.get('asset_number')
    hardware_info_db.describe = params.get('describe')
    hardware_info_db.enable = params.get('enable')
    session_job.merge(hardware_info_db)
    session_job.commit()
    result_message = u"保存:%s成功" % params.get('ip')
    return make_response(jsonify(code=200, data=result_message), 200)


@report.route('/query_hardware', methods=['GET', 'POST'])
def query_hardware():
    query_params = request.json
    query_type = query_params.get('type')
    query_ip = query_params.get('ip')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    hardware_list = []
    for hardware_info_db in session_job.query(HardWareInfo):
        if query_type and query_type != hardware_info_db.type:
            continue
        if query_ip and query_ip not in hardware_info_db.ip:
            continue
        hardware_dict = hardware_info_db.to_dict()
        hardware_list.append(hardware_dict)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_prop == 'ip':
            if sort_order == 'ascending':
                hardware_list = sorted(hardware_list, key=lambda hardware_info_item: ''.join(
                    [i.rjust(3, '0') for i in hardware_info_item[sort_prop].split('.')]), reverse=True)
            else:
                hardware_list = sorted(hardware_list, key=lambda hardware_info_item: ''.join(
                    [i.rjust(3, '0') for i in hardware_info_item[sort_prop].split('.')]))
        elif sort_order == 'ascending':
            hardware_list = sorted(hardware_list, key=lambda hardware_info_item: hardware_info_item[sort_prop],
                                   reverse=True)
        else:
            hardware_list = sorted(hardware_list, key=lambda hardware_info_item: hardware_info_item[sort_prop])
    else:
        hardware_list = sorted(hardware_list, key=lambda hardware_info_item: ''.join(
            [i.rjust(3, '0') for i in hardware_info_item['ip'].split('.')]))

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(hardware_list)
    query_result = {'data': hardware_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/del_hardware', methods=['GET', 'POST'])
def del_hardware():
    params = request.json
    del_id = params.get('del_id')
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    hardware_info_db = session_job.query(HardWareInfo).filter(HardWareInfo.id == del_id).first()
    session_job.delete(hardware_info_db)
    session_job.commit()
    result_message = u"删除成功"
    return make_response(jsonify(code=200, data=result_message), 200)


@report.route('/get_history_phone_trade', methods=['GET', 'POST'])
def get_history_phone_trade():
    query_column_list = ['server_name', 'fund', 'strategy1', 'strategy2', 'symbol', 'direction', 'tradetype',
                         'hedgeflag', 'exprice', 'exqty', 'iotype', 'update_time']
    server_model = server_constant.get_server_model('host')
    session_history = server_model.get_db_session('history')
    today_str = date_utils.get_today_str('%Y-%m-%d')
    q_today_str = '%' + today_str + '%'
    query_sql = "select %s from phone_trade_info where update_time like '%s'" % \
                (','.join(query_column_list), q_today_str)
    rst = session_history.execute(query_sql)
    data = []
    for line in rst.fetchall():
        temp_dict = {}
        for column_name in query_column_list:
            if column_name == 'update_time':
                temp_dict[column_name] = str(line[query_column_list.index(column_name)]).split(' ')[-1]
            elif column_name == 'direction':
                temp_dict[column_name] = direction_dict.keys()[
                    direction_dict.values().index(line[query_column_list.index(column_name)])]
            elif column_name == 'tradetype':
                temp_dict[column_name] = trade_type_dict.keys()[
                    trade_type_dict.values().index(line[query_column_list.index(column_name)])]
            elif column_name == 'iotype':
                temp_dict[column_name] = io_type_dict.keys()[
                    io_type_dict.values().index(line[query_column_list.index(column_name)])]
            elif column_name == 'hedgeflag':
                temp_dict[column_name] = hedgeflag_type_inversion_dict[line[query_column_list.index(column_name)]]
            else:
                temp_dict[column_name] = line[query_column_list.index(column_name)]
        data.append(temp_dict)
    response = {'data': data, }
    return make_response(jsonify(code=200, data=response), 200)


def build_week_df(server_host, strategy_grouping_dict, week_start, week_end):
    week_data_list = []
    session_history = server_host.get_db_session('history')
    for server_risk_db in session_history.query(ServerRisk).filter(ServerRisk.date.between(week_start, week_end)):
        strategy_name_item = server_risk_db.strategy_name.split('-')
        if strategy_name_item[1] not in strategy_grouping_dict:
            # print strategy_name_item[1]
            continue
        strategy_grouping_db = strategy_grouping_dict[strategy_name_item[1]]
        if strategy_grouping_db.group_name in ('Arbitrage', 'CTA'):
            continue
        group_name = strategy_grouping_db.group_name
        row_list = [group_name, strategy_name_item[1], server_risk_db.date.strftime('%Y-%m-%d'),
                    server_risk_db.total_pl, server_risk_db.total_stocks_value,
                    server_risk_db.total_future_value, server_risk_db.delta]
        week_data_list.append(row_list)

    title_list = ['Type', 'S/A', 'Date', 'Total Pnl', 'Total Stocks Value', 'Total Future Value', 'Delta']
    risk_df = pd.DataFrame(week_data_list, columns=title_list)
    pl_value_df = risk_df.groupby(['Type', 'S/A']).sum().reset_index()
    market_value_df = risk_df.groupby(['Type', 'S/A', 'Date']).sum().reset_index()
    market_value_df = market_value_df[market_value_df['Date'] == market_value_df['Date'].max()]

    del pl_value_df['Total Stocks Value']
    del pl_value_df['Total Future Value']
    del pl_value_df['Delta']
    del market_value_df['Date']
    del market_value_df['Total Pnl']
    combined_df = pd.merge(pl_value_df, market_value_df, how='left', on=['Type', 'S/A']).fillna(0)
    return combined_df


@report.route('/export_week_report', methods=['GET', 'POST'])
def export_week_report():
    params = request.json
    week_end = params.get('query_date')

    temp_date = date_utils.string_toDatetime(week_end)
    m1 = calendar.MONDAY
    while temp_date.weekday() != m1:
        temp_date = temp_date + datetime.timedelta(days=-1)
    week_start = temp_date.strftime("%Y-%m-%d")

    m5 = calendar.FRIDAY
    while temp_date.weekday() != m5:
        temp_date = temp_date + datetime.timedelta(days=-1)
    last_week_end = temp_date.strftime("%Y-%m-%d")
    last_week_start = last_week_end
    # print last_week_start, last_week_end, week_start, week_end

    server_host = server_constant.get_server_model('host')
    strategy_grouping_dict = dict()
    session_strategy = server_host.get_db_session('strategy')
    for strategy_grouping_db in session_strategy.query(StrategyGrouping):
        strategy_grouping_dict[strategy_grouping_db.strategy_name] = strategy_grouping_db

    group_week_df = build_week_df(server_host, strategy_grouping_dict, week_start, week_end)
    last_group_week_df = build_week_df(server_host, strategy_grouping_dict, last_week_start, last_week_end)
    group_week_df.set_index(['Type', 'S/A'], inplace=True)
    last_group_week_df.set_index(['Type', 'S/A'], inplace=True)
    summary_table = group_week_df.copy()
    summary_table['Last_TSV'] = last_group_week_df['Total Stocks Value']
    summary_table['Last_TFV'] = last_group_week_df['Total Future Value']
    summary_table['Abs_Total'] = (abs(summary_table['Total Stocks Value']) + abs(summary_table['Total Future Value']) +
                                  abs(summary_table['Last_TSV']) + abs(summary_table['Last_TFV'])) / 2.
    rate_of_return = (summary_table['Total Pnl'] / summary_table['Abs_Total'] * 100.).apply(
        lambda x: x if x != np.inf else 0)
    summary_table.insert(1, 'Rate of Return', rate_of_return)
    summary_table.reset_index(inplace=True)

    summary_table.loc["Total-Total"] = summary_table.sum(axis=0)
    summary_table.at["Total-Total", "Rate of Return"] = summary_table.at["Total-Total", "Total Pnl"] / \
                                                        summary_table.at["Total-Total", "Abs_Total"]
    summary_table.at["Total-Total", "Type"] = "Total"
    summary_table.at["Total-Total", "S/A"] = "Total"
    summary_table['Rate of Return'] = summary_table['Rate of Return'].apply(
        lambda x: '%.4f%%' % x if str(x).isdigit() else 0)

    return_data_list = []
    filter_title_list = ['Type', 'S/A', 'Total Pnl', 'Rate of Return', 'Total Stocks Value',
                         'Total Future Value', 'Delta']
    return_data_dict = summary_table[filter_title_list].to_dict("index")
    for (dict_key, dict_value) in return_data_dict.items():
        return_data_list.append(dict_value)
    query_result = {'data_list': return_data_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/export_strategy_risk_report', methods=['GET', 'POST'])
def export_strategy_risk_report():
    query_params = request.json
    [start_date, end_date] = query_params.get('query_dates')
    query_group_names = query_params.get('group_names')
    query_group_type = query_params.get('group_type')

    account_set = set()
    for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
        for real_account in account_list:
            account_set.add(real_account.fund_name)
    account_list = list(account_set)
    account_list.sort()

    server_host = server_constant.get_server_model('host')
    group_info_list = []
    session_strategy = server_host.get_db_session('strategy')
    for sg_db in session_strategy.query(StrategyGrouping):
        row_list = [sg_db.group_name, sg_db.sub_name, sg_db.strategy_name]
        group_info_list.append(row_list)
    title_list = ['GroupName', 'SubName', 'StrategyName']
    group_info_df = pd.DataFrame(group_info_list, columns=title_list)
    # 根据用户选择进行过滤
    group_info_df = group_info_df[group_info_df['GroupName'].isin(query_group_names)]
    filter_strategy_names = group_info_df['StrategyName'].values.tolist()

    server_risk_list = []
    session_history = server_host.get_db_session('history')
    for item in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
        strategy_name_items = item.strategy_name.split('-')
        if strategy_name_items[1] not in filter_strategy_names:
            continue
        item_list = [item.date, strategy_name_items[1], strategy_name_items[2], item.total_pl]
        server_risk_list.append(item_list)
    title_list = ['Date', 'StrategyName', 'Account', 'Total_PL']
    server_risk_df = pd.DataFrame(server_risk_list, columns=title_list)

    risk_report_df = server_risk_df.pivot_table(index=['Date', 'StrategyName'], columns='Account', values='Total_PL', aggfunc=np.sum).fillna(0)
    risk_report_df = risk_report_df.reset_index()

    full_title_list = ['Date', 'StrategyName', 'Total_PL'] + account_list
    risk_report_df = risk_report_df.ix[:, full_title_list].fillna(0)

    risk_total_df = pd.merge(risk_report_df, group_info_df, how='left', on=['StrategyName'])
    if query_group_type == 'GroupName':
        risk_total_df = risk_total_df.groupby(['Date', 'GroupName', ]).sum().reset_index()
        temp = risk_total_df[['Date', 'GroupName', ]]
        del risk_total_df['Date']
        del risk_total_df['GroupName']
    elif query_group_type == 'SubName':
        risk_total_df = risk_total_df.groupby(['Date', 'GroupName', 'SubName']).sum().reset_index()
        temp = risk_total_df[['Date', 'GroupName', 'SubName']]
        del risk_total_df['Date']
        del risk_total_df['GroupName']
        del risk_total_df['SubName']
    elif query_group_type == 'StrategyName':
        temp = risk_total_df[['Date', 'GroupName', 'SubName', 'StrategyName']]
        del risk_total_df['Date']
        del risk_total_df['GroupName']
        del risk_total_df['SubName']
        del risk_total_df['StrategyName']
    risk_total_df['Total'] = risk_total_df.apply(lambda x: x.sum(), axis=1)
    risk_total_df.loc['Total'] = risk_total_df.sum()
    for col in risk_total_df.columns.values:
        risk_total_df[col] = risk_total_df[col].apply(lambda x: '{:,}'.format(int(x)))
    risk_total_df = pd.merge(temp, risk_total_df, left_index=True, right_index=True, how='right')
    risk_total_df['Date'] = risk_total_df['Date'].astype(str)

    report_title = []
    if query_group_type == 'GroupName':
        risk_total_df['Date'].iloc[-1] = 'Z_Total'
        risk_total_df['GroupName'].iloc[-1] = ''
        report_title = ['Date', 'GroupName', ] + account_list + ['Total', ]
    elif query_group_type == 'SubName':
        risk_total_df['Date'].iloc[-1] = 'Z_Total'
        risk_total_df['GroupName'].iloc[-1] = ''
        risk_total_df['SubName'].iloc[-1] = ''
        report_title = ['Date', 'GroupName', 'SubName'] + account_list + ['Total', ]
    elif query_group_type == 'StrategyName':
        risk_total_df['Date'].iloc[-1] = 'Z_Total'
        risk_total_df['GroupName'].iloc[-1] = ''
        risk_total_df['SubName'].iloc[-1] = ''
        risk_total_df['StrategyName'].iloc[-1] = ''
        report_title = ['Date', 'GroupName', 'SubName', 'StrategyName'] + account_list + ['Total', ]

    return_data_list = []
    return_data_dict = risk_total_df.to_dict("index")
    for (dict_key, dict_value) in return_data_dict.items():
        return_data_list.append(dict_value)
    query_result = {'title_list': report_title, 'data_list': return_data_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_special_tickers', methods=['GET', 'POST'])
def query_special_tickers():
    query_param = request.json
    search_date_list = query_param.get('search_date')
    ticker = query_param.get('ticker')
    size = query_param.get('size')
    page = query_param.get('page')
    server_host = server_constant.get_server_model('host')
    session_job = server_host.get_db_session('jobs')
    describe_type = const.EOD_CONFIG_DICT['special_ticker_type'].split(',')
    data = []
    for obj in session_job.query(SpecialTickers):
        temp_dict = dict(date=str(obj.date), ticker=obj.ticker, describe=obj.describe)
        data.append(temp_dict)
    if search_date_list:
        start_date, end_date = search_date_list
        data = filter(lambda item: start_date <= item['date'] <= end_date, data)
    if ticker:
        print ticker
        data = filter(lambda item: item['ticker'] == ticker, data)
    start = (int(page) - 1) * int(size)
    end = int(page) * int(size)
    pagination = {'total': len(data), 'size': int(size), 'currentPage': int(page)}
    data = data[start:end]
    result = {'data': data, 'pagination': pagination, 'describe_type': describe_type}
    session_job.close()
    return make_response(jsonify(code=200, data=result), 200)


@report.route('/add_special_tickers', methods=['GET', 'POST'])
def add_special_tickers():
    server_host = server_constant.get_server_model('host')
    session_job = server_host.get_db_session('jobs')
    query_param = request.json
    for item in query_param:
        ticker_obj = SpecialTickers()
        ticker_obj.ticker = item['ticker']
        ticker_obj.date = str(item['date']).split('T')[0]
        ticker_obj.describe = ';'.join(item['describe'])
        session_job.add(ticker_obj)
    session_job.commit()
    session_job.close()
    return make_response(jsonify(code=200, data=''), 200)


@report.route('/unusual_order_list', methods=['GET', 'POST'])
def unusual_order_list():
    query_result_list = []
    for x in const.EOD_POOL['unusual_order_list']:
        order_item_dict = dict(server_name=x[0], account=x[1], strategy=x[2], symbol=x[3], status=x[4],
                               creation_time=x[5], transaction_time=x[6], note=x[7])
        query_result_list.append(order_item_dict)
    query_result_list.sort(key=lambda obj: obj['creation_time'])
    query_result = {'data': query_result_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/query_vwap_strategys', methods=['GET', 'POST'])
def query_vwap_strategys():
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    strategy_names = []
    for item in session.query(DailyVwapAnalyse.strategy).group_by(DailyVwapAnalyse.strategy):
        strategy_names.append(item[0])
    session.close()
    return make_response(jsonify(code=200, data=strategy_names), 200)


@report.route('/query_vwap_servers', methods=['GET', 'POST'])
def query_vwap_servers():
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    server_names = [x[0] for x in session.query(DailyVwapAnalyse.server).group_by(DailyVwapAnalyse.server)]
    session.close()
    return make_response(jsonify(code=200, data=server_names), 200)


@report.route('/query_vwap_accounts', methods=['GET', 'POST'])
def query_vwap_accounts():
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    account_names = [x[0] for x in session.query(DailyVwapAnalyse.account).group_by(DailyVwapAnalyse.account)]
    session.close()
    return make_response(jsonify(code=200, data=account_names), 200)


@report.route('/query_vwap_report', methods=['GET', 'POST'])
def query_vwap_report():
    query_params = request.json
    search_date_item = query_params.get('search_date')
    strategy_name = query_params.get('strategy_name')
    server_name = query_params.get('server_name')
    account_name = query_params.get('account_name')
    if search_date_item:
        [start_date, end_date] = search_date_item
        start_date = start_date[:10]
        end_date = end_date[:10]
    else:
        start_date = '1990-01-01'
        end_date = date_utils.get_today_str('%Y-%m-%d')

    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    data_list = []
    for item in session.query(DailyVwapAnalyse).filter(DailyVwapAnalyse.date.between(start_date, end_date)):
        if strategy_name and item.strategy != strategy_name:
            continue
        if server_name and item.server != server_name:
            continue
        if account_name and item.account != account_name:
            continue
        item_dict = dict()
        item_dict['date'] = date_utils.datetime_toString(item.date)
        item_dict['server'] = item.server
        item_dict['account'] = item.account
        item_dict['strategy'] = item.strategy
        item_dict['avg_buy_slippage'] = '%.6f' % item.avg_buy_slippage
        item_dict['avg_sell_slippage'] = '%.6f' % item.avg_sell_slippage
        item_dict['buy_amt'] = int(item.buy_amt)
        item_dict['sell_amt'] = int(item.sell_amt)
        item_dict['avg_slippage'] = '%.5f' % item.avg_slippage
        data_list.append(item_dict)
    session.close()
    data_list = sorted(data_list, key=lambda item: item['date'], reverse=True)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(data_list)
    query_result = {'data': data_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@report.route('/build_vwap_report', methods=['GET', 'POST'])
def build_vwap_report():
    date_str = date_utils.get_today_str('%Y-%m-%d')
    vwap_cal_tools = VwapCalTools(date_str)
    error_messages = vwap_cal_tools.start_index()
    if error_messages:
        return make_response(jsonify(code=100, message=';'.join(error_messages)), 200)
    else:
        return make_response(jsonify(code=200,), 200)