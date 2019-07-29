# -*- coding: utf-8 -*-
import json
from datetime import datetime
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *
from eod_aps.model.obj_to_sql import to_many_sql
import pandas as pd
from collections import OrderedDict


now = datetime.now()
future_db_dict = dict()
option_db_dict = dict()
instrument_history_db_dict = dict()
future_insert_list = []
instrument_history_list = []
option_insert_list = []
instrument_type_enums = const.INSTRUMENT_TYPE_ENUMS
exchange_type_enums = const.EXCHANGE_TYPE_ENUMS

filter_exchange_list = [exchange_type_enums.CG, exchange_type_enums.CS, exchange_type_enums.HK]


def __format_exchange_id(exchange_name):
    exchange_id = None
    if 'SSE' == exchange_name:
        exchange_id = exchange_type_enums.CG
    elif 'SZE' == exchange_name:
        exchange_id = exchange_type_enums.CS
    elif 'HGE' == exchange_name:
        exchange_id = exchange_type_enums.HK
    return exchange_id


def __format_undl_ticker(product_id):
    undl_ticker = product_id
    if product_id == 'IF':
        undl_ticker = 'SHSZ300'
    elif product_id == 'IC':
        undl_ticker = 'SH000905'
    elif product_id == 'IH':
        undl_ticker = 'SSE50'
    return undl_ticker


def __format_commission_rate_type(underlying_instrid):
    ticker_type = filter(lambda x: not x.isdigit(), underlying_instrid)
    commission_rate_type = 'option_%s' % ticker_type
    return commission_rate_type


def __format_type_id(product_id):
    type_id = None
    if product_id in ('SHEOP', 'SHAOP'):
        type_id = instrument_type_enums.Option
    elif product_id in ('SZA', 'SHA', 'HKA', 'CY'):
        type_id = instrument_type_enums.CommonStock
    elif product_id in ('SHCB', 'SZCB'):
        type_id = instrument_type_enums.ConvertableBond
    elif product_id == 'SHFUNDETF':
        type_id = instrument_type_enums.MMF
    return type_id


def __format_session_ticker_type(type_id, product_id, underlying_instrid):
    if type_id == instrument_type_enums.Future:
        return product_id.upper()
    elif type_id == instrument_type_enums.Option:
        return filter(lambda x: not x.isdigit(), underlying_instrid).upper()


def __format_put_call(exchange_instid):
    put_call = None
    if 'P' in exchange_instid:
        put_call = 0
    elif 'C' in exchange_instid:
        put_call = 1
    return put_call


def __format_contract_adjustment(exchange_instid):
    contract_adjustment = None
    if 'M' in exchange_instid:
        contract_adjustment = 0
    elif 'A' in exchange_instid:
        contract_adjustment = 1
    elif 'B' in exchange_instid:
        contract_adjustment = 2
    elif 'C' in exchange_instid:
        contract_adjustment = 3
    return contract_adjustment


def __read_lts_file(lts_file_path):
    # 合约信息
    instrument_list = []
    sf_instrument_list = []
    of_instrument_list = []
    # 行情信息
    market_list = []
    index_market_list = []
    with open(lts_file_path) as fr:
        for line in fr.readlines():
            if len(line.strip()) == 0:
                continue

            line_content = line.replace('\n', '').split('|')[1]
            temp_dict = dict()
            for x in line_content.split(','):
                dict_key, dict_value = x.split(':', 1)
                temp_dict[dict_key.strip()] = dict_value

            if 'OnRspQryInstrument' in line:
                instrument_list.append(temp_dict)
            elif 'OnRspQrySFInstrument' in line:
                sf_instrument_list.append(temp_dict)
            elif 'OnRspQryOFInstrument' in line:
                of_instrument_list.append(temp_dict)
            elif 'OnRtnDepthMarketData' in line:
                market_list.append(temp_dict)
            elif 'OnRtnL2Index' in line:
                index_market_list.append(temp_dict)
    return instrument_list, sf_instrument_list, of_instrument_list, market_list, index_market_list


def __query_instrument_dict(session_common):
    instrument_dict = dict()
    for x in session_common.query(Instrument).filter(Instrument.exchange_id.in_((exchange_type_enums.CG,
                                                                                 exchange_type_enums.CS,
                                                                                 exchange_type_enums.HK))):
        instrument_dict[x.ticker] = x
    return instrument_dict


def __format_common_stock_df(common_stock_df):
    common_stock_df.loc[:, 'market_status_id'] = 2
    common_stock_df.loc[:, 'market_sector_id'] = 4
    common_stock_df.loc[:, 'round_lot_size'] = 100
    common_stock_df.loc[:, 'tick_size_table'] = '0:0.01'
    common_stock_df.loc[:, 'fut_val_pt'] = 1

    common_stock_df.loc[:, 'max_market_order_vol'] = 0
    common_stock_df.loc[:, 'min_market_order_vol'] = 0
    common_stock_df.loc[:, 'max_limit_order_vol'] = 1000000
    common_stock_df.loc[:, 'min_limit_order_vol'] = 100

    common_stock_df.loc[:, 'longmarginratio'] = 0
    common_stock_df.loc[:, 'shortmarginratio'] = 999

    common_stock_df.loc[:, 'multiplier'] = 1
    common_stock_df.loc[:, 'strike'] = 0

    common_stock_df.loc[:, 'is_settle_instantly'] = 0
    common_stock_df.loc[:, 'is_purchase_to_redemption_instantly'] = 0
    common_stock_df.loc[:, 'is_buy_to_redpur_instantly'] = 1
    common_stock_df.loc[:, 'is_redpur_to_sell_instantly'] = 1
    return common_stock_df


def __format_convertible_bond_df(convertible_bond_df):
    convertible_bond_df.loc[:, 'market_status_id'] = 2
    convertible_bond_df.loc[:, 'market_sector_id'] = 2
    convertible_bond_df.loc[:, 'round_lot_size'] = 10
    convertible_bond_df.loc[:, 'tick_size_table'] = '0:0.001'
    convertible_bond_df.loc[:, 'fut_val_pt'] = convertible_bond_df['VolumeMultiple']

    convertible_bond_df.loc[:, 'max_market_order_vol'] = convertible_bond_df['MaxMarketOrderVolume']
    convertible_bond_df.loc[:, 'min_market_order_vol'] = convertible_bond_df['MinMarketOrderVolume']
    convertible_bond_df.loc[:, 'max_limit_order_vol'] = 100000
    convertible_bond_df.loc[:, 'min_limit_order_vol'] = convertible_bond_df['MinLimitOrderVolume']

    convertible_bond_df.loc[:, 'longmarginratio'] = 0
    convertible_bond_df.loc[:, 'shortmarginratio'] = 999

    convertible_bond_df.loc[:, 'multiplier'] = convertible_bond_df['VolumeMultiple']

    convertible_bond_df.loc[:, 'is_settle_instantly'] = 1
    convertible_bond_df.loc[:, 'is_purchase_to_redemption_instantly'] = 0
    convertible_bond_df.loc[:, 'is_buy_to_redpur_instantly'] = 0
    convertible_bond_df.loc[:, 'is_redpur_to_sell_instantly'] = 0
    return convertible_bond_df


def __format_option_df(option_df, filter_date_str):
    option_df.loc[:, 'market_status_id'] = 2
    option_df.loc[:, 'market_sector_id'] = 4
    option_df.loc[:, 'round_lot_size'] = 1
    option_df.loc[:, 'tick_size_table'] = '0:%s' % option_df['PriceTick'].str
    option_df.loc[:, 'fut_val_pt'] = option_df['VolumeMultiple']

    option_df.loc[:, 'max_market_order_vol'] = option_df['MaxMarketOrderVolume']
    option_df.loc[:, 'min_market_order_vol'] = option_df['MinMarketOrderVolume']
    option_df.loc[:, 'max_limit_order_vol'] = option_df['MaxLimitOrderVolume']
    option_df.loc[:, 'min_limit_order_vol'] = option_df['MinLimitOrderVolume']

    option_df.loc[:, 'longmarginratio'] = 0
    option_df.loc[:, 'shortmarginratio'] = 0.15

    option_df.loc[:, 'multiplier'] = 10000
    option_df.loc[:, 'strike'] = option_df['ExecPrice']

    option_df.loc[:, 'is_settle_instantly'] = 1
    option_df.loc[:, 'is_purchase_to_redemption_instantly'] = 0
    option_df.loc[:, 'is_buy_to_redpur_instantly'] = 0
    option_df.loc[:, 'is_redpur_to_sell_instantly'] = 0

    option_df.loc[:, 'name'] = option_df['ExchangeInstID']
    option_df.loc[:, 'put_call'] = option_df.apply(lambda row: __format_put_call(row['InstrumentID']), axis=1)
    option_df.loc[:, 'undl_tickers'] = option_df['ExchangeInstID'].str[:6]
    option_df.loc[:, 'track_undl_tickers'] = option_df['undl_tickers']
    option_df.loc[:, 'commission_rate_type'] = 'option_%s' % option_df['undl_tickers'].str
    option_df.loc[:, 'contract_adjustment'] = option_df.apply(lambda row: __format_contract_adjustment(row['ExchangeInstID']), axis=1)
    option_df.loc[:, 'effective_since'] = filter_date_str

    option_df.loc[:, 'option_margin_factor1'] = 0.15
    option_df.loc[:, 'option_margin_factor2'] = 0.07
    return option_df


def __format_mmf_df(mmf_df):
    mmf_df.loc[:, 'market_status_id'] = 2
    mmf_df.loc[:, 'market_sector_id'] = 4
    mmf_df.loc[:, 'round_lot_size'] = 100
    mmf_df.loc[:, 'tick_size_table'] = '0:0.001'
    mmf_df.loc[:, 'multiplier'] = mmf_df['VolumeMultiple']
    mmf_df.loc[:, 'longmarginratio'] = 0
    mmf_df.loc[:, 'shortmarginratio'] = 999

    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CG]['is_settle_instantly'] = 1
    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CG]['is_purchase_to_redemption_instantly'] = 0
    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CG]['is_buy_to_redpur_instantly'] = 0
    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CG]['is_redpur_to_sell_instantly'] = 0

    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CS]['is_settle_instantly'] = 1
    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CS]['is_purchase_to_redemption_instantly'] = 1
    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CS]['is_buy_to_redpur_instantly'] = 1
    mmf_df[mmf_df['exchange_id'] == exchange_type_enums.CS]['is_redpur_to_sell_instantly'] = 1
    return mmf_df


def __format_fund_pcf(row):
    pcf_dict = OrderedDict()
    pcf_dict['Ticker'] = row['InstrumentID']
    split_merge_status = row['SplitMergeStatus']
    if split_merge_status == '0':
        pcf_dict['Split'] = '1'
        pcf_dict['Merge'] = '1'
    elif split_merge_status == '1':
        pcf_dict['Split'] = '1'
        pcf_dict['Merge'] = '0'
    elif split_merge_status == '2':
        pcf_dict['Split'] = '0'
        pcf_dict['Merge'] = '1'
    else:
        pcf_dict['Split'] = '0'
        pcf_dict['Merge'] = '0'
    pcf_dict['SFInstrumentID'] = row['SFInstrumentID']
    pcf_dict['MinSplitVolume'] = row['MinSplitVolume']
    pcf_dict['MinMergeVolume'] = row['MinMergeVolume']
    pcf_dict['VolumeRatio'] = row['VolumeRatio']
    pcf_dict['TradingDay'] = date_utils.get_today_str('%Y%m%d')

    if row['Creationredemption'] != '':
        creation_redemption = row['Creationredemption']
        if creation_redemption == '0':
            pcf_dict['Creation'] = '0'
            pcf_dict['Redemption'] = '0'
        elif creation_redemption == '1':
            pcf_dict['Creation'] = '1'
            pcf_dict['Redemption'] = '1'
        elif creation_redemption == '2':
            pcf_dict['Creation'] = '1'
            pcf_dict['Redemption'] = '0'
        elif creation_redemption == '3':
            pcf_dict['Creation'] = '0'
            pcf_dict['Redemption'] = '1'
    return json.dumps(pcf_dict)


def __format_fund_instrument(sf_instrument_list, of_instrument_list):
    sf_instrument_df = pd.DataFrame(sf_instrument_list)
    of_instrument_df = pd.DataFrame(of_instrument_list)

    fund_instrument_df = pd.concat([sf_instrument_df, of_instrument_df], sort=False).fillna('')
    fund_instrument_df.loc[:, 'pcf'] = fund_instrument_df.apply(lambda row: __format_fund_pcf(row), axis=1)
    return fund_instrument_df[['InstrumentID', 'NetPrice', 'pcf']]


def lts_update_index(date_str):
    if date_str is None or date_str == '':
        filter_date_str = now.strftime('%Y-%m-%d')
    else:
        filter_date_str = date_str

    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')

    instrument_list = []
    sf_instrument_list = []
    of_instrument_list = []
    market_info_list = []
    index_market_list = []
    lts_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('HUABAO_INSTRUMENT', filter_date_str)
    for lts_file_name in lts_file_list:
        lts_file_path = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, lts_file_name)
        temp_instrument_list, temp_sf_list, temp_of_list, temp_market_list, temp_index_market_list = \
            __read_lts_file(lts_file_path)
        instrument_list.extend(temp_instrument_list)
        sf_instrument_list.extend(temp_sf_list)
        of_instrument_list.extend(temp_of_list)
        market_info_list.extend(temp_market_list)
        index_market_list.extend(temp_index_market_list)

    instrument_info_df = pd.DataFrame(instrument_list)

    exchange_id_df = instrument_info_df.apply(lambda row: __format_exchange_id(row['ExchangeID']), axis=1)
    instrument_info_df.insert(0, 'exchange_id', exchange_id_df)

    filter_instrument_df = instrument_info_df[instrument_info_df['exchange_id'].isin(filter_exchange_list)]
    filter_instrument_df = filter_instrument_df[filter_instrument_df['IsTrading'] == '1']
    filter_instrument_df.loc[:, 'type_id'] = filter_instrument_df.apply(lambda row: __format_type_id(row['ProductID']), axis=1)

    common_stock_df = filter_instrument_df[filter_instrument_df['type_id'] == instrument_type_enums.CommonStock].copy()
    common_stock_df = __format_common_stock_df(common_stock_df)

    option_df = filter_instrument_df[filter_instrument_df['type_id'] == instrument_type_enums.Option].copy()
    option_df = __format_option_df(option_df, filter_date_str)

    convertible_bond_df = filter_instrument_df[filter_instrument_df['type_id'] == instrument_type_enums.ConvertableBond].copy()
    convertible_bond_df = __format_convertible_bond_df(convertible_bond_df)

    mmf_df = filter_instrument_df[filter_instrument_df['type_id'] == instrument_type_enums.MMF].copy()
    mmf_df = __format_mmf_df(mmf_df)

    format_instrument_df = pd.concat([common_stock_df, convertible_bond_df, option_df, mmf_df], sort=False).fillna('')

    instrument_db_dict = __query_instrument_dict(session_common)
    __add_instrument_db(session_common, instrument_db_dict, format_instrument_df)

    market_info_df = pd.DataFrame(market_info_list, columns=['InstrumentID', 'PreClosePrice', 'PreSettlementPrice',
                                                             'UpperLimitPrice', 'LowerLimitPrice', 'ClosePrice',
                                                             'Volume'])
    index_market_df = pd.DataFrame(index_market_list, columns=['InstrumentID', 'PreClosePrice', 'ClosePrice', 'Volume'])
    market_info_df = pd.concat([market_info_df, index_market_df], sort=False).fillna('')
    fund_instrument_df = __format_fund_instrument(sf_instrument_list, of_instrument_list)

    __update_instrument(session_common, instrument_db_dict, market_info_df, fund_instrument_df)
    session_common.commit()


def __add_instrument_db(session_common, instrument_db_dict, instrument_df):
    instrument_db_list = []
    for index, row in instrument_df.iterrows():
        ticker = row['InstrumentID']
        if ticker in instrument_db_dict:
            instrument_db = instrument_db_dict[ticker]
        else:
            instrument_db = Instrument()
            instrument_db.ticker = ticker
            # instrument_db.name = ticker
            instrument_db.name = row['ExchangeInstID']
            instrument_db.ticker_exch = ticker
            instrument_db.ticker_exch_real = ticker

            instrument_db.create_date = row['CreateDate'] if row['CreateDate'] != '' else None
            instrument_db.effective_since = row['OpenDate'] if row['OpenDate'] != '' else None
            instrument_db.tick_size_table = '0:%f' % float(row['PriceTick'])

            instrument_db.exchange_id = row['exchange_id']
            instrument_db.type_id = row['type_id']
            instrument_db.market_status_id = row['market_status_id']
            instrument_db.multiplier = row['multiplier']
            instrument_db.crncy = 'CNY'
            instrument_db.market_sector_id = row['market_sector_id']
            instrument_db.round_lot_size = row['round_lot_size']

            instrument_db.undl_tickers = row['undl_tickers']
            instrument_db.commission_rate_type = row['commission_rate_type']

            instrument_db.is_settle_instantly = row['is_settle_instantly']
            instrument_db.is_purchase_to_redemption_instantly = row['is_purchase_to_redemption_instantly']
            instrument_db.is_buy_to_redpur_instantly = row['is_buy_to_redpur_instantly']
            instrument_db.is_redpur_to_sell_instantly = row['is_redpur_to_sell_instantly']

            instrument_db.put_call = row['put_call'] if row['put_call'] != '' else None
            instrument_db.strike = row['strike'] if row['strike'] != '' else None
            instrument_db.option_margin_factor1 = row['option_margin_factor1'] if row['option_margin_factor1'] != '' else None
            instrument_db.option_margin_factor2 = row['option_margin_factor2'] if row['option_margin_factor2'] != '' else None
        instrument_db.expire_date = row['ExpireDate'] if row['ExpireDate'] != '' else None
        instrument_db.fut_val_pt = row['VolumeMultiple']
        instrument_db.max_market_order_vol = row['max_market_order_vol'] if row['max_market_order_vol'] != '' else None
        instrument_db.min_market_order_vol = row['min_market_order_vol'] if row['min_market_order_vol'] != '' else None
        instrument_db.max_limit_order_vol = row['max_limit_order_vol'] if row['max_limit_order_vol'] != '' else None
        instrument_db.min_limit_order_vol = row['min_limit_order_vol'] if row['min_limit_order_vol'] != '' else None
        instrument_db.longmarginratio = row['longmarginratio']
        instrument_db.shortmarginratio = row['shortmarginratio']
        instrument_db.longmarginratio_speculation = instrument_db.longmarginratio
        instrument_db.shortmarginratio_speculation = instrument_db.shortmarginratio
        instrument_db.longmarginratio_hedge = instrument_db.longmarginratio
        instrument_db.shortmarginratio_hedge = instrument_db.shortmarginratio
        instrument_db.longmarginratio_arbitrage = instrument_db.longmarginratio
        instrument_db.shortmarginratio_arbitrage = instrument_db.shortmarginratio
        instrument_db.update_date = datetime.now()
        if instrument_db.type_id == instrument_type_enums.CommonStock:
            if row['IsTrading'] == '1':
                instrument_db.inactive_date = None
            else:
                instrument_db.inactive_date = now.strftime('%Y-%m-%d')
        instrument_db_list.append(instrument_db)
    sql_list = to_many_sql(Instrument, instrument_db_list, 'common.instrument')
    for sql in sql_list:
        session_common.execute(sql)


def __update_instrument(session_common, instrument_db_dict, market_info_df, fund_instrument_df):
    now_time = int(now.strftime('%H%M%S'))
    instrument_db_list = []

    market_info_dict = dict()
    for index, row in market_info_df.iterrows():
        market_info_dict[row['InstrumentID']] = row

    fund_instrument_dict = dict()
    for index, row in fund_instrument_df.iterrows():
        fund_instrument_dict[row['InstrumentID']] = row

    for (ticker, instrument_db) in instrument_db_dict.items():
        if ticker not in market_info_dict and ticker not in fund_instrument_dict:
            continue

        if ticker in market_info_dict:
            row = market_info_dict[ticker]
            if now_time > 150500:
                if row['ClosePrice'] != '' and float(row['ClosePrice']) > 0:
                    instrument_db.close = row['ClosePrice']
                    instrument_db.close_update_time = datetime.now()
                instrument_db.volume = row['Volume']
            else:
                if row['PreClosePrice'] != '' and float(row['PreClosePrice']) > 0:
                    instrument_db.prev_close = row['PreClosePrice']
                    instrument_db.prev_close_update_time = datetime.now()
                instrument_db.prev_settlementprice = row['PreSettlementPrice'] if row['PreSettlementPrice'] != '' else None
                instrument_db.uplimit = row['UpperLimitPrice'] if row['UpperLimitPrice'] != '' else None
                instrument_db.downlimit = row['LowerLimitPrice'] if row['LowerLimitPrice'] != '' else None

        if ticker in fund_instrument_dict:
            row = fund_instrument_dict[ticker]
            if instrument_db.tranche is None:
                # 只更新母基金的prev_nav
                instrument_db.prev_nav = row['NetPrice']
            instrument_db.pcf = row['pcf']
        instrument_db_list.append(instrument_db)
    sql_list = to_many_sql(Instrument, instrument_db_list, 'common.instrument')
    for sql in sql_list:
        session_common.execute(sql)


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    lts_update_index(date_str)

