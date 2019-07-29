# -*- coding: utf-8 -*-
from datetime import datetime
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *
from eod_aps.model.obj_to_sql import to_many_sql
import pandas as pd

now = datetime.now()
future_db_dict = dict()
option_db_dict = dict()
instrument_history_db_dict = dict()
future_insert_list = []
instrument_history_list = []
option_insert_list = []
instrument_type_enums = const.INSTRUMENT_TYPE_ENUMS
exchange_type_enums = const.EXCHANGE_TYPE_ENUMS

future_exchange_list = [exchange_type_enums.SHF, exchange_type_enums.DCE, exchange_type_enums.ZCE,
                        exchange_type_enums.CFF, exchange_type_enums.INE]

INSTRUMENT_NAME_DICT = {'中证': 'cs', '上证': 'SSE', '股指': 'IDX', '期货': 'Future', '国债': 'TF', '指数': 'Index',
                        '买权': 'Call', '卖权': 'Put', '白糖': 'SR'}


def __format_exchange_id(exchange_name):
    exchange_id = None
    if 'CFFEX' == exchange_name:
        exchange_id = exchange_type_enums.CFF
    elif 'SHFE' == exchange_name:
        exchange_id = exchange_type_enums.SHF
    elif 'DCE' == exchange_name:
        exchange_id = exchange_type_enums.DCE
    elif 'CZCE' == exchange_name:
        exchange_id = exchange_type_enums.ZCE
    elif 'INE' == exchange_name:
        exchange_id = exchange_type_enums.INE
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


def __format_type_id(product_class):
    type_id = None
    if product_class == '1':
        type_id = instrument_type_enums.Future
    elif product_class == '2':
        type_id = instrument_type_enums.Option
    return type_id


def __format_session_ticker_type(type_id, product_id, underlying_instrid):
    if type_id == instrument_type_enums.Future:
        return product_id.upper()
    elif type_id == instrument_type_enums.Option:
        return filter(lambda x: not x.isdigit(), underlying_instrid).upper()


def __format_instrument_name(instrument_name):
    for (dict_key, dict_value) in INSTRUMENT_NAME_DICT.items():
        instrument_name = instrument_name.replace(dict_key, dict_value)
    return instrument_name


def __format_put_call(instrument_name):
    put_call = None
    if 'P' in instrument_name:
        put_call = 0
    elif 'C' in instrument_name:
        put_call = 1
    return put_call


def __format_option_strike(put_call, ticker):
    if put_call == 0:
        strike = ticker.replace('-', '').split('P')[-1]
    else:
        strike = ticker.replace('-', '').split('C')[-1]
    return strike


def __read_ctp_file(ctp_file_path):
    instrument_info_list = []
    market_info_list = []
    with open(ctp_file_path) as fr:
        for line in fr.readlines():
            if len(line.strip()) == 0:
                continue

            line_content = line.replace('\n', '').split('|')[1]
            temp_dict = dict()
            for x in line_content.split(','):
                dict_key, dict_value = x.split(':', 1)
                temp_dict[dict_key] = dict_value
            if 'OnRspQryInstrument' in line:
                instrument_info_list.append(temp_dict)
            elif 'OnRspQryDepthMarketData' in line:
                market_info_list.append(temp_dict)
    return instrument_info_list, market_info_list


def __query_instrument_dict(session_common):
    instrument_dict = dict()
    for x in session_common.query(Instrument).filter(Instrument.type_id.in_([instrument_type_enums.Future,
                                                                             instrument_type_enums.Option])):
        instrument_dict[x.ticker] = x
    return instrument_dict


def __query_trading_df(session_basicinfo):
    trading_info_list = []
    query_sql = "select symbol,date,time from basic_info.trading_info"
    for x in session_basicinfo.execute(query_sql):
        trading_info_list.append([x[0], x[1], x[2]])
    trading_info_df = pd.DataFrame(trading_info_list, columns=['Tick_Type', 'Date', 'Time'])

    format_trading_list = []
    for group_key, group in trading_info_df.groupby('Tick_Type'):
        start_date = group.iloc[0, 1]
        end_date = '20991231'
        trading_time = group.iloc[-1, 2]
        format_trading_list.append([group_key, '(%s,%s)%s' % (start_date, end_date, trading_time)])
    format_trading_df = pd.DataFrame(format_trading_list, columns=['session_ticker_type', 'session'])
    return format_trading_df


def __format_future_df(future_df):
    future_df.loc[:, 'multiplier'] = 1

    future_df.loc[:, 'longmarginratio'] = future_df['LongMarginRatio']
    future_df.loc[:, 'shortmarginratio'] = future_df['ShortMarginRatio']

    future_df.loc[:, 'is_settle_instantly'] = 1
    future_df.loc[:, 'is_purchase_to_redemption_instantly'] = 0
    future_df.loc[:, 'is_buy_to_redpur_instantly'] = 0
    future_df.loc[:, 'is_redpur_to_sell_instantly'] = 0

    future_df.loc[future_df['ProductID'].isin(['T', 'TF', 'TS']), 'market_sector_id'] = 5
    future_df.loc[future_df['ProductID'].isin(['IC', 'IF', 'IH']), 'market_sector_id'] = 6

    future_df.loc[:, 'undl_tickers'] = future_df.apply(lambda row: __format_undl_ticker(row['ProductID']), axis=1)
    future_df.loc[:, 'commission_rate_type'] = future_df['undl_tickers']
    return future_df


def __format_option_df(option_df, future_df):
    option_df.loc[:, 'multiplier'] = 10

    # 期权的longmarginratio和shortmarginratio设置成与对应期货一致
    margin_ratio_df = future_df[['InstrumentID', 'longmarginratio', 'shortmarginratio']].copy()
    margin_ratio_df.rename(columns={'InstrumentID': 'UnderlyingInstrID'}, inplace=True)
    option_df = pd.merge(option_df, margin_ratio_df, how='left', on=['UnderlyingInstrID'])

    option_df.loc[:, 'is_settle_instantly'] = 1
    option_df.loc[:, 'is_purchase_to_redemption_instantly'] = 0
    option_df.loc[:, 'is_buy_to_redpur_instantly'] = 0
    option_df.loc[:, 'is_redpur_to_sell_instantly'] = 0

    option_df.loc[:, 'undl_tickers'] = option_df['UnderlyingInstrID']
    option_df.loc[:, 'commission_rate_type'] = option_df.apply(lambda row: __format_commission_rate_type(row['UnderlyingInstrID']), axis=1)

    option_df.loc[:, 'put_call'] = option_df.apply(lambda row: __format_put_call(row['InstrumentID']), axis=1)
    option_df.loc[:, 'strike'] = option_df.apply(lambda row: __format_option_strike(row['put_call'], row['InstrumentID']), axis=1)
    option_df.loc[:, 'option_margin_factor1'] = 0.5
    option_df.loc[:, 'option_margin_factor2'] = 0.5
    return option_df


def ctp_update_index(date_str):
    if date_str is None or date_str == '':
        filter_date_str = now.strftime('%Y-%m-%d')
    else:
        filter_date_str = date_str

    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    session_basicinfo = server_host.get_db_session('basic_info')

    trading_df = __query_trading_df(session_basicinfo)

    instrument_info_list = []
    market_info_list = []
    ctp_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('CTP_INSTRUMENT', filter_date_str)
    for ctp_file_name in ctp_file_list:
        ctp_file_path = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, ctp_file_name)
        temp_instrument_list, temp_market_list = __read_ctp_file(ctp_file_path)
        instrument_info_list.extend(temp_instrument_list)
        market_info_list.extend(temp_market_list)

    instrument_info_df = pd.DataFrame(instrument_info_list)
    market_info_df = pd.DataFrame(market_info_list, columns=['InstrumentID', 'PreClosePrice', 'PreSettlementPrice',
                                                             'UpperLimitPrice', 'LowerLimitPrice', 'ClosePrice',
                                                             'Volume'])

    filter_instrument_df = instrument_info_df[instrument_info_df['ProductClass'].isin(['1', '2'])]

    exchange_id_df = filter_instrument_df.apply(lambda row: __format_exchange_id(row['ExchangeID']), axis=1)
    filter_instrument_df.insert(0, 'exchange_id', exchange_id_df)
    filter_instrument_df = filter_instrument_df[filter_instrument_df['exchange_id'].isin(future_exchange_list)]
    filter_instrument_df.loc[:, 'type_id'] = filter_instrument_df.apply(lambda row: __format_type_id(row['ProductClass']), axis=1)
    filter_instrument_df.loc[:, 'market_sector_id'] = 1
    filter_instrument_df.loc[:, 'market_status_id'] = 2
    filter_instrument_df.loc[:, 'round_lot_size'] = 1
    filter_instrument_df.loc[:, 'crncy'] = 'CNY'
    # 临到期cu的round_lot_size单独设置
    filter_instrument_df.loc[(filter_instrument_df['ProductID'] == 'cu') &
                             (filter_instrument_df['ExpireDate'][:7] == now.strftime('%Y-%m')), 'round_lot_size'] = 5
    # filter_instrument_df.loc[filter_instrument_df['LongMarginRatio'] > 10000, 'LongMarginRatio'] = 0
    # filter_instrument_df.loc[filter_instrument_df['ShortMarginRatio'] > 10000, 'ShortMarginRatio'] = 0
    filter_instrument_df.loc[:, 'put_call'] = None
    filter_instrument_df.loc[:, 'strike'] = None
    filter_instrument_df.loc[:, 'option_margin_factor1'] = None
    filter_instrument_df.loc[:, 'option_margin_factor2'] = None

    filter_instrument_df.loc[:, 'session_ticker_type'] = filter_instrument_df.\
        apply(lambda row: __format_session_ticker_type(row['type_id'], row['ProductID'], row['UnderlyingInstrID']), axis=1)
    filter_instrument_df = pd.merge(filter_instrument_df, trading_df, how='left', on=['session_ticker_type'])
    filter_instrument_df = pd.merge(filter_instrument_df, market_info_df, how='left', on=['InstrumentID'])

    future_instrument_df = filter_instrument_df[filter_instrument_df['type_id'] == instrument_type_enums.Future].copy()
    future_instrument_df = __format_future_df(future_instrument_df)

    option_instrument_df = filter_instrument_df[filter_instrument_df['type_id'] == instrument_type_enums.Option].copy()
    option_instrument_df = __format_option_df(option_instrument_df, future_instrument_df)

    format_instrument_df = pd.concat([future_instrument_df, option_instrument_df])
    instrument_db_dict = __query_instrument_dict(session_common)
    __save_instrument(session_common, instrument_db_dict, format_instrument_df)
    # instrument_df['InstrumentID'] = instrument_df['InstrumentID'].astype(str)
    # instrument_index_df = instrument_df.set_index(['InstrumentID', ])

    session_common.commit()


def __save_instrument(session_common, instrument_db_dict, instrument_df):
    instrument_db_list = []
    now_time = int(now.strftime('%H%M%S'))
    for index, row in instrument_df.iterrows():
        ticker = row['InstrumentID']
        if ticker in instrument_db_dict:
            instrument_db = instrument_db_dict[ticker]
            if (now_time > 150500) and (now_time < 180000):
                instrument_db.close = row['ClosePrice']
                instrument_db.close_update_time = datetime.now()
                instrument_db.volume = row['Volume']
            else:
                instrument_db.prev_close = row['PreClosePrice']
                instrument_db.prev_settlementprice = row['PreSettlementPrice']
                instrument_db.uplimit = row['UpperLimitPrice']
                instrument_db.downlimit = row['LowerLimitPrice']
                instrument_db.prev_close_update_time = datetime.now()
        else:
            instrument_db = Instrument()
            instrument_db.ticker = ticker
            instrument_db.name = ticker
            instrument_db.ticker_exch = ticker
            instrument_db.ticker_exch_real = ticker

            instrument_db.create_date = row['CreateDate']
            instrument_db.effective_since = row['OpenDate']
            instrument_db.tick_size_table = '0:%f' % float(row['PriceTick'])

            instrument_db.exchange_id = row['exchange_id']
            instrument_db.type_id = row['type_id']
            instrument_db.market_status_id = row['market_status_id']
            instrument_db.multiplier = row['multiplier']
            instrument_db.crncy = row['crncy']
            instrument_db.market_sector_id = row['market_sector_id']
            instrument_db.round_lot_size = row['round_lot_size']

            instrument_db.undl_tickers = row['undl_tickers']
            instrument_db.commission_rate_type = row['commission_rate_type']

            instrument_db.is_settle_instantly = row['is_settle_instantly']
            instrument_db.is_purchase_to_redemption_instantly = row['is_purchase_to_redemption_instantly']
            instrument_db.is_buy_to_redpur_instantly = row['is_buy_to_redpur_instantly']
            instrument_db.is_redpur_to_sell_instantly = row['is_redpur_to_sell_instantly']

            instrument_db.put_call = row['put_call']
            instrument_db.strike = row['strike']
            instrument_db.option_margin_factor1 = row['option_margin_factor1']
            instrument_db.option_margin_factor2 = row['option_margin_factor2']
            instrument_db.session = row['session']
        instrument_db.expire_date = row['ExpireDate']
        instrument_db.fut_val_pt = row['VolumeMultiple']
        instrument_db.max_market_order_vol = row['MaxMarketOrderVolume']
        instrument_db.min_market_order_vol = row['MinMarketOrderVolume']
        instrument_db.max_limit_order_vol = row['MaxLimitOrderVolume']
        instrument_db.min_limit_order_vol = row['MinLimitOrderVolume']
        instrument_db.longmarginratio = row['longmarginratio']
        instrument_db.shortmarginratio = row['shortmarginratio']
        instrument_db.longmarginratio_speculation = instrument_db.longmarginratio
        instrument_db.shortmarginratio_speculation = instrument_db.shortmarginratio
        instrument_db.longmarginratio_hedge = instrument_db.longmarginratio
        instrument_db.shortmarginratio_hedge = instrument_db.shortmarginratio
        instrument_db.longmarginratio_arbitrage = instrument_db.longmarginratio
        instrument_db.shortmarginratio_arbitrage = instrument_db.shortmarginratio
        instrument_db.update_date = datetime.now()
        instrument_db_list.append(instrument_db)
    sql_list = to_many_sql(Instrument, instrument_db_list, 'common.instrument')
    print len(sql_list)
    for sql in sql_list:
        session_common.execute(sql)


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    ctp_update_index(date_str)
