# -*- coding: utf-8 -*-
import codecs
import json
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.model.BaseModel import *
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *
from eod_aps.model.obj_to_sql import to_many_sql

instrument_exchange_db_dict = dict()
fund_db_dict = dict()
pre_structured_fund_dict = dict()
pre_index_db_dict = dict()


def build_instrument_db_dict():
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.exchange_id.in_((18, 19))):
        if instrument_db.type_id == 6:
            dict_key = '%s|%s' % (instrument_db.ticker_exch_real, instrument_db.exchange_id)
        else:
            dict_key = '%s|%s' % (instrument_db.ticker, instrument_db.exchange_id)
        instrument_exchange_db_dict[dict_key] = instrument_db

        if instrument_db.type_id in (7, 15, 16):
            fund_db_dict[instrument_db.ticker] = instrument_db
        elif instrument_db.type_id == 6:
            pre_index_db_dict[instrument_db.ticker] = instrument_db

        if instrument_db.type_id == 16:
            pre_structured_fund_dict[instrument_db.ticker] = instrument_db


def read_position_file_lts(lts_file_path):
    print 'Start read file:' + lts_file_path
    with codecs.open(lts_file_path, 'r', 'gbk') as fr:
        sf_instrument_array = []
        of_instrument_array = []
        instrument_array = []
        market_array = []
        index_array = []
        for line in fr.xreadlines():
            base_model = BaseModel()
            for temp_str in line.split('|')[1].split(','):
                temp_array = temp_str.replace('\n', '').split(':', 1)
                setattr(base_model, temp_array[0].strip(), temp_array[1])
            if 'OnRspQrySFInstrument' in line:
                sf_instrument_array.append(base_model)
            if 'OnRspQryOFInstrument' in line:
                of_instrument_array.append(base_model)
            elif 'OnRspQryInstrument' in line:
                instrument_array.append(base_model)
            elif 'OnRtnDepthMarketData' in line:
                market_array.append(base_model)
            elif 'OnRtnL2Index' in line:
                index_array.append(base_model)

    update_fund(sf_instrument_array, of_instrument_array)  # 更新分级基金的prev_nav
    update_instrument_base_info(instrument_array)
    update_market(market_array)  # 根据md行情数据更新prev_close
    update_market_index(index_array)  # 根据md的L2行情数据更新指数的prev_close


# 更新分级基金pcf中PREDAYRATIO属性值
def update_structured_fund():
    now_time = long(date_utils.get_today_str('%H%M%S'))
    if now_time > 150500:
        return

    for (ticker, pre_structured_fund) in pre_structured_fund_dict.items():
        # 筛选出分级基金母基金
        if pre_structured_fund.tranche is not None:
            continue
        (sub_ticker1, sub_ticker2) = pre_structured_fund.undl_tickers.split(';')
        sub_structured_fund = pre_structured_fund_dict[sub_ticker1]
        if sub_structured_fund.undl_tickers is None or sub_structured_fund.undl_tickers == '':
            # print 'sub structured fund:%s not point to index' % (sub_ticker1,)
            continue

        if sub_structured_fund.undl_tickers not in pre_index_db_dict:
            print 'unfind index,ticker:%s' % (sub_structured_fund.undl_tickers,)

        pre_index_db = pre_index_db_dict[sub_structured_fund.undl_tickers]
        dict_key = '%s|%s' % (pre_index_db.ticker_exch_real, pre_index_db.exchange_id)
        index_db = instrument_exchange_db_dict[dict_key]
        dict_key = '%s|%s' % (pre_structured_fund.ticker, pre_structured_fund.exchange_id)
        structured_fund = instrument_exchange_db_dict[dict_key]

        if index_db.prev_close == pre_index_db.prev_close:
            preday_ratio = 0
        else:
            preday_ratio = ((structured_fund.prev_nav - pre_structured_fund.prev_nav) * index_db.prev_close) \
                           / ((index_db.prev_close - pre_index_db.prev_close) * structured_fund.prev_nav)
            preday_ratio = '%.6f' % preday_ratio

        for structured_fund_ticker in [pre_structured_fund.ticker, sub_ticker1, sub_ticker2]:
            structured_fund_temp = pre_structured_fund_dict[structured_fund_ticker]
            dict_key = '%s|%s' % (structured_fund_temp.ticker, structured_fund_temp.exchange_id)
            structured_fund_db = instrument_exchange_db_dict[dict_key]
            structured_fund_pcf = json.loads(structured_fund_db.pcf)
            structured_fund_pcf['PREDAYRATIO'] = preday_ratio

            structured_fund_db.pcf = json.dumps(structured_fund_pcf)


def update_fund(sf_instrument_array, of_instrument_array):
    instrument_cr_dict = dict()
    for messageInfo in of_instrument_array:
        instrument_id = getattr(messageInfo, 'InstrumentID', '')
        creation_redemption = getattr(messageInfo, 'Creationredemption', '')
        instrument_cr_dict[instrument_id] = creation_redemption

    for messageInfo in sf_instrument_array:
        instrument_id = getattr(messageInfo, 'InstrumentID', '')
        if instrument_id not in fund_db_dict:
            continue
        fund_db = fund_db_dict[instrument_id]
        if fund_db.tranche is None:
            # 只更新母基金的prev_nav
            fund_db.prev_nav = getattr(messageInfo, 'NetPrice', '')

        pcf_dict = dict()
        pcf_dict['Ticker'] = instrument_id
        split_merge_status = getattr(messageInfo, 'SplitMergeStatus', '')
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
        sf_instrument_id = getattr(messageInfo, 'SFInstrumentID', '')
        pcf_dict['SFInstrumentID'] = sf_instrument_id
        min_split_volume = getattr(messageInfo, 'MinSplitVolume', '')
        pcf_dict['MinSplitVolume'] = min_split_volume
        min_merge_volume = getattr(messageInfo, 'MinMergeVolume', '')
        pcf_dict['MinMergeVolume'] = min_merge_volume
        volume_ratio = getattr(messageInfo, 'VolumeRatio', '')
        pcf_dict['VolumeRatio'] = volume_ratio
        pcf_dict['TradingDay'] = date_utils.get_today_str('%Y%m%d')

        if instrument_id in instrument_cr_dict:
            creation_redemption = instrument_cr_dict[instrument_id]
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
        fund_db.pcf = json.dumps(pcf_dict)


def update_instrument_base_info(message_array):
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in instrument_exchange_db_dict:
            continue

        instrument_db = instrument_exchange_db_dict[dict_key]
        instrument_db.fut_val_pt = getattr(messageInfo, 'VolumeMultiple', '')
        instrument_db.max_market_order_vol = getattr(messageInfo, 'MaxMarketOrderVolume', '')
        instrument_db.min_market_order_vol = getattr(messageInfo, 'MinMarketOrderVolume', '')
        instrument_db.max_limit_order_vol = getattr(messageInfo, 'MaxLimitOrderVolume', '')
        instrument_db.min_limit_order_vol = getattr(messageInfo, 'MinLimitOrderVolume', '')
        instrument_db.strike = getattr(messageInfo, 'ExecPrice', '')

        # 判断股票是否停牌
        if instrument_db.type_id != 4:
            continue
        is_trading = getattr(messageInfo, 'IsTrading', '')
        if is_trading == '1':
            instrument_db.inactive_date = None
        elif instrument_db.inactive_date is None:
            instrument_db.inactive_date = filter_date_str


def update_market(message_array):
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in instrument_exchange_db_dict:
            print 'error instrument_info key:', dict_key
            continue

        instrument_db = instrument_exchange_db_dict[dict_key]
        now_time = long(date_utils.get_today_str('%H%M%S'))
        if now_time < 150500:
            prev_close = getattr(messageInfo, 'PreClosePrice', '')
            if float(prev_close) > 0:
                instrument_db.prev_close = prev_close
            instrument_db.prev_settlementprice = getattr(messageInfo, 'PreSettlementPrice', '')
            instrument_db.uplimit = getattr(messageInfo, 'UpperLimitPrice', '')
            instrument_db.downlimit = getattr(messageInfo, 'LowerLimitPrice', '')
            instrument_db.prev_close_update_time = date_utils.get_now()
        else:
            close = getattr(messageInfo, 'ClosePrice', 0)
            if float(close) > 0:
                instrument_db.close = close
            instrument_db.volume = getattr(messageInfo, 'Volume', '')
            instrument_db.close_update_time = date_utils.get_now()
        instrument_db.update_date = date_utils.get_now()


def update_market_index(message_array):
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in instrument_exchange_db_dict:
            print 'error index_info key:', dict_key
            continue

        index_db = instrument_exchange_db_dict[dict_key]
        now_time = long(date_utils.get_today_str('%H%M%S'))
        if now_time < 150500:
            index_db.prev_close = getattr(messageInfo, 'PreClosePrice', '')
            index_db.prev_close_update_time = date_utils.get_now()
        else:
            index_db.close = getattr(messageInfo, 'ClosePrice', '')
            volume_str = getattr(messageInfo, 'Volume', '')
            if volume_str.isdigit():
                index_db.volume = volume_str
            index_db.close_update_time = date_utils.get_now()
        index_db.update_date = date_utils.get_now()


def update_db():
    sql_list = to_many_sql(Instrument, instrument_exchange_db_dict.values(), 'common.instrument')
    for sql in sql_list:
        session_common.execute(sql)
    now_time = long(date_utils.get_today_str('%H%M%S'))
    # 000974,000823的prev_close早上行情中为0，用昨日close赋值。
    if now_time < 150500:
        update_sql = 'update common.instrument t set t.PREV_CLOSE = t.`CLOSE` where t.PREV_CLOSE = 0'
        session_common.execute(update_sql)


def lts_price_analysis(date_str):
    print 'Enter Lts_price_analysis.'
    global session_common
    global filter_date_str

    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    if date_str is None or date_str == '':
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    else:
        filter_date_str = date_str

    build_instrument_db_dict()

    instrument_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('HUABAO_INSTRUMENT', filter_date_str)
    market_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('HUABAO_MARKET', filter_date_str)

    lts_file_list = []
    lts_file_list.extend(instrument_file_list)
    lts_file_list.extend(market_file_list)

    for qd_file in lts_file_list:
        read_position_file_lts('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, qd_file))

    update_structured_fund()
    update_db()
    session_common.commit()
    server_host.close()
    print 'Exit Lts_price_analysis.'


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    lts_price_analysis(date_str)
    # lts_price_analysis('2017-11-27')
