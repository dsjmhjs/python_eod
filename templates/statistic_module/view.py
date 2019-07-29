#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import time
from flask import request, jsonify, make_response
from eod_aps.model.eod_const import const
from . import statistic_module
from statistic_util import *
import redis
from eod_aps.tools.date_utils import DateUtils

date_utils = DateUtils()
# temp_data_dict = dict()


@statistic_module.route("/get_table_data", methods=["GET", "POST"])
def get_table_data():
    query_params = request.json
    query_ticker = query_params.get('ticker')
    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    sort_prop = query_params.get('sort_prop', '')
    sort_order = query_params.get('sort_order', '')
    symbol = query_params.get('symbol', '')
    data = []
    industry_list = []
    industry_bar_data = []
    index_pie_data = []
    industry_pie_data = []
    mv_pie_data = []
    pagination = {'total': len(data), 'size': int(query_size), 'currentPage': int(query_page)}
    rst = {'data': data, 'pagination': pagination, 'industry_bar_data': industry_bar_data,
           'index_pie_data': index_pie_data, 'industry_pie_data': industry_pie_data, 'mv_pie_data': mv_pie_data}
    base_info_dict = const.EOD_CONFIG_DICT['stock_basic_data_dict']
    market_value_list = []
    for ticker, item in base_info_dict.items():
        if 'market_value' in item and item['market_value']:
            market_value_list.append({'market_value': item['market_value'], 'ticker': ticker})
    mv_pie_data = get_market_value_pie_data('MultiFactor', market_value_list)
    index_pie_data, index_dict = get_index_pie_data(base_info_dict)

    host, port, db = const.EOD_CONFIG_DICT['redis_address'].split('|')
    r = redis.Redis(host=host, port=int(port), db=int(db))
    basket_name = 'MultiFactor'
    redis_name = 'market_ret:basket_weight:%s' % basket_name
    # print redis_name
    weigth_redis = r.get(redis_name)
    weigth_redis = weigth_redis.replace('nan', '-1')
    symbol_weight_info_dict = eval(weigth_redis)['weight']
    if 'instrument_dict' not in const.EOD_POOL or 'market_dict' not in const.EOD_POOL:
        return make_response(jsonify(code=200, data=rst), 200)

    instrument_msg_dict = const.EOD_POOL['instrument_dict']

    for (market_id, market_msg) in const.EOD_POOL['market_dict'].items():
        instrument_msg = instrument_msg_dict[market_id]
        if query_ticker and query_ticker.lower() not in instrument_msg.ticker.lower():
            continue
        if instrument_msg.ticker in symbol_weight_info_dict.keys():
            temp_dict = dict()
            if instrument_msg.ticker.isdigit():
                if 500000 <= int(instrument_msg.ticker) <= 600000:
                    continue
            temp_dict['symbol'] = instrument_msg.ticker
            temp_dict['name'] = base_info_dict[instrument_msg.ticker]['name']
            temp_dict['concept'] = base_info_dict[instrument_msg.ticker]['conception']
            temp_dict['industry'] = base_info_dict[instrument_msg.ticker]['industry']
            if base_info_dict[instrument_msg.ticker]['est_pe_fy1']:
                temp_dict['est_pe'] = base_info_dict[instrument_msg.ticker]['est_pe_fy1']
            else:
                temp_dict['est_pe'] = 0
            temp_dict['mv'] = int(base_info_dict[instrument_msg.ticker]['market_value'] / 10000)
            temp_dict['prev_close'] = instrument_msg.prevCloseWired
            temp_dict['last_prc'] = market_msg.Args.LastPrice

            if instrument_msg.ticker in index_dict['all_three_index']:
                temp_dict['tag'] = '中证500;沪深300;上证50'
            elif instrument_msg.ticker in index_dict['Both_ZZ500_SH50']:
                temp_dict['tag'] = '中证500;上证50'
            elif instrument_msg.ticker in index_dict['Both_CSI_ZZ500']:
                temp_dict['tag'] = '中证500;沪深300'
            elif instrument_msg.ticker in index_dict['Both_CSI300_SH50']:
                temp_dict['tag'] = '上证50;沪深300'
            elif instrument_msg.ticker in index_dict['Only_ZZ500']:
                temp_dict['tag'] = '中证500'
            elif instrument_msg.ticker in index_dict['Only_CSI300']:
                temp_dict['tag'] = '沪深300'
            elif instrument_msg.ticker in index_dict['Only_SH50']:
                temp_dict['tag'] = '上证50'
            elif instrument_msg.ticker in index_dict['Others']:
                temp_dict['tag'] = ''

            if float(instrument_msg.prevCloseWired) == 0:
                chg_value = 0
            else:
                chg_value = (market_msg.Args.NominalPrice / instrument_msg.prevCloseWired - 1) * 100
            temp_dict['ret'] = chg_value
            temp_dict['wei'] = float(symbol_weight_info_dict[instrument_msg.ticker]) * 100
            if temp_dict['industry'] not in industry_list:
                industry_list.append(temp_dict['industry'])
            data.append(temp_dict)
    industry_bar_data = get_industry_bar_data(basket_name, base_info_dict)
    for industry_name in industry_list:
        temp_industry_dict = filter(lambda item: item['industry'] == industry_name, data)
        industry_pie_data.append({'name': industry_name, 'y': len(temp_industry_dict) / float(len(data))})
    if symbol:
        data = filter(lambda data_item: data_item['symbol'] == symbol, data)
    if sort_prop:
        if sort_order == 'ascending':
            data = sorted(data, key=lambda data_item: data_item[sort_prop], reverse=True)
        else:
            data = sorted(data, key=lambda data_item: data_item[sort_prop])
    pagination = {'total': len(data), 'size': int(query_size), 'currentPage': int(query_page)}
    data = data[(query_page - 1) * query_size: query_page * query_size]

    for temp_dict in data:
        temp_dict['est_pe'] = '%.4f' % temp_dict['est_pe']
        temp_dict['prev_close'] = '%.2f' % temp_dict['prev_close']
        temp_dict['last_prc'] = '%.2f' % temp_dict['last_prc']
        temp_dict['ret'] = '%.2f' % temp_dict['ret']
        temp_dict['wei'] = '%.2f' % temp_dict['wei']

    time_str = time.strftime('%H:%M:%S', time.localtime())
    industry_bar_data = sorted(industry_bar_data, key=lambda item: item[1], reverse=True)
    rst = {'data': data, 'pagination': pagination, 'industry_bar_data': industry_bar_data, 'time_str': time_str,
           'index_pie_data': index_pie_data, 'industry_pie_data': industry_pie_data, 'mv_pie_data': mv_pie_data}
    return make_response(jsonify(code=200, data=rst), 200)
