# -*- coding: utf-8 -*-
from datetime import datetime
from eod_aps.model.BaseModel import *
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.model.schema_common import Instrument, FutureMainContract
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *
from eod_aps.model.obj_to_sql import to_many_sql

now = datetime.now()
future_db_dict = dict()
option_db_dict = dict()
instrument_history_db_dict = dict()
future_insert_list = []
instrument_history_list = []
option_insert_list = []
instrument_type_enums = const.INSTRUMENT_TYPE_ENUMS
exchange_type_enums = const.EXCHANGE_TYPE_ENUMS


def read_price_file_ctp(ctp_file_path):
    print 'Start read file:', ctp_file_path

    future_list = []
    option_list = []
    with open(ctp_file_path) as fr:
        for line in fr.readlines():
            base_model = BaseModel()
            if len(line.strip()) == 0:
                continue
            for tempStr in line.split('|')[1].split(','):
                temp_array = tempStr.replace('\n', '').split(':', 1)
                setattr(base_model, temp_array[0].strip(), temp_array[1])

            if 'OnRspQryInstrument' in line:
                product_class = getattr(base_model, 'ProductClass', '')
                if product_class == '1':
                    future_list.append(base_model)
                if product_class == '2':
                    option_list.append(base_model)
    pre_add_future(future_list)  # 新增期货
    pre_add_option(option_list)  # 新增期权


def pre_add_future(message_array):
    global max_instrument_id
    future_ticker_list = []
    for message_info in message_array:
        ticker = getattr(message_info, 'InstrumentID', '')
        exchange_name = getattr(message_info, 'ExchangeID', '')
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
        else:
            continue
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in future_db_dict:
            continue

        future_db = Instrument()
        max_instrument_id += 1
        future_db.id = max_instrument_id
        future_db.ticker = ticker
        future_db.exchange_id = exchange_id
        future_db.fut_val_pt = getattr(message_info, 'VolumeMultiple', '')
        future_db.max_market_order_vol = getattr(message_info, 'MaxMarketOrderVolume', '')
        future_db.min_market_order_vol = getattr(message_info, 'MinMarketOrderVolume', '')
        future_db.max_limit_order_vol = getattr(message_info, 'MaxLimitOrderVolume', '')
        future_db.min_limit_order_vol = getattr(message_info, 'MinLimitOrderVolume', '')
        future_db.longmarginratio = getattr(message_info, 'LongMarginRatio', '')
        future_db.shortmarginratio = getattr(message_info, 'ShortMarginRatio', '')
        future_db.longmarginratio_speculation = future_db.longmarginratio
        future_db.shortmarginratio_speculation = future_db.shortmarginratio
        future_db.longmarginratio_hedge = future_db.longmarginratio
        future_db.shortmarginratio_hedge = future_db.shortmarginratio
        future_db.longmarginratio_arbitrage = future_db.longmarginratio
        future_db.shortmarginratio_arbitrage = future_db.shortmarginratio

        future_db.create_date = getattr(message_info, 'CreateDate', '')
        future_db.expire_date = getattr(message_info, 'ExpireDate', '')

        future_db.ticker_exch = ticker
        future_db.ticker_exch_real = ticker
        future_db.type_id = 1  # future
        future_db.market_status_id = 2
        future_db.multiplier = 1
        future_db.crncy = 'CNY'
        future_db.effective_since = getattr(message_info, 'OpenDate', '')

        product_id = getattr(message_info, 'ProductID', '')
        name_message = getattr(message_info, 'InstrumentName', '')
        price_tick = getattr(message_info, 'PriceTick', '')

        if product_id in ('T', 'TF', 'TS'):
            future_db.market_sector_id = 5
            future_db.round_lot_size = 1
            future_db.tick_size_table = '0:%f' % (float(price_tick),)
            future_db.undl_tickers = product_id
            future_db.commission_rate_type = product_id
            future_db.name = ticker
        elif product_id in ('IC', 'IF', 'IH'):
            future_db.market_sector_id = 6
            future_db.round_lot_size = 1
            future_db.tick_size_table = '0:%f' % (float(price_tick),)

            future_db.longmarginratio_hedge = 0.2
            future_db.shortmarginratio_hedge = 0.2

            undl_ticker = ''
            if product_id == 'IF':
                undl_ticker = 'SHSZ300'
            elif product_id == 'IC':
                undl_ticker = 'SH000905'
            elif product_id == 'IH':
                undl_ticker = 'SSE50'
            future_db.undl_tickers = undl_ticker
            future_db.commission_rate_type = undl_ticker

            future_db.name = name_message.replace('中证', 'cs').replace('上证', 'SSE').replace('股指', 'IDX') \
                .replace('期货', 'Future').replace('国债', 'TF').replace('指数', 'Index').replace('沪深300', '').strip()
        else:
            future_db.market_sector_id = 1
            future_db.round_lot_size = 1
            future_db.tick_size_table = '0:%f' % (float(price_tick),)
            if product_id == 'cu':
                expire_date = getattr(message_info, 'ExpireDate', '')
                if now.strftime('%Y-%m') == expire_date[:7]:
                    future_db.round_lot_size = 5
            future_db.undl_tickers = product_id
            future_db.commission_rate_type = product_id
            future_db.name = ticker

        future_db.is_settle_instantly = 1
        future_db.is_purchase_to_redemption_instantly = 0
        future_db.is_buy_to_redpur_instantly = 0
        future_db.is_redpur_to_sell_instantly = 0

        ticker_type = filter(lambda x: not x.isdigit(), ticker)
        future_db.session = __get_trading_info_list(ticker_type)[-1]
        future_insert_list.append(future_db)
        future_ticker_list.append(ticker)

    if len(future_ticker_list) > 0:
        print 'Prepare Insert Future:', ','.join(future_ticker_list)


def __get_track_undl_tickers(ticker_type):
    query = session_common.query(FutureMainContract)
    future_maincontract_db = query.filter(FutureMainContract.ticker_type == ticker_type).first()
    return future_maincontract_db.main_symbol


def __get_trading_info_list(ticker_type):
    start_date = None
    end_date = None
    trading_info_list = []
    last_trading_time = None

    query_sql = "select * from basic_info.trading_info t where t.symbol = '%s' order by date" % ticker_type
    for trading_info in session_basicinfo.execute(query_sql):
        if start_date is None:
            start_date = trading_info[1]

        if str(trading_info[1]) in const.HOLIDAYS:
            continue

        if last_trading_time is None:
            last_trading_time = trading_info[2]

        if trading_info[2] != last_trading_time:
            trading_info_list.append('(%s,%s)%s' % (start_date, end_date, last_trading_time))
            start_date = trading_info[1]

        end_date = trading_info[1]
        last_trading_time = trading_info[2]
    end_date = '20991231'
    trading_info_list.append('(%s,%s)%s' % (start_date, end_date, last_trading_time))
    return trading_info_list


def pre_add_option(message_array):
    global max_instrument_id
    option_ticker_list = []
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
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
        else:
            continue

        if ticker in option_db_dict:
            continue

        option_db = Instrument()
        max_instrument_id += 1
        option_db.id = max_instrument_id
        option_db.ticker = ticker
        option_db.exchange_id = exchange_id
        option_db.fut_val_pt = getattr(messageInfo, 'VolumeMultiple', '')
        option_db.max_market_order_vol = getattr(messageInfo, 'MaxMarketOrderVolume', '')
        option_db.min_market_order_vol = getattr(messageInfo, 'MinMarketOrderVolume', '')
        option_db.max_limit_order_vol = getattr(messageInfo, 'MaxLimitOrderVolume', '')
        option_db.min_limit_order_vol = getattr(messageInfo, 'MinLimitOrderVolume', '')

        undl_tickers = getattr(messageInfo, 'UnderlyingInstrID', '')
        ticker_type = filter(lambda x: not x.isdigit(), undl_tickers)
        option_db.undl_tickers = undl_tickers
        option_db.commission_rate_type = 'option_%s' % ticker_type

        option_db.create_date = getattr(messageInfo, 'CreateDate', '')
        option_db.expire_date = getattr(messageInfo, 'ExpireDate', '')
        option_db.name = ticker

        instrument_name = getattr(messageInfo, 'InstrumentName', '').strip()
        instrument_name = instrument_name.replace('买权', 'Call').replace('卖权', 'Put').replace('白糖', 'SR')
        put_call = __get_put_call(instrument_name)
        option_db.put_call = put_call

        if put_call == 0:
            option_db.strike = ticker.replace('-', '').split('P')[-1]
        else:
            option_db.strike = ticker.replace('-', '').split('C')[-1]

        price_tick = getattr(messageInfo, 'PriceTick', '')
        option_db.tick_size_table = '0:%s' % (float(price_tick),)

        option_db.ticker_exch = ticker
        option_db.ticker_exch_real = ticker
        option_db.market_status_id = 2
        option_db.type_id = 10  # option
        option_db.market_sector_id = 1
        option_db.round_lot_size = 1
        option_db.longmarginratio = 0
        option_db.shortmarginratio = 0.15
        option_db.longmarginratio_speculation = option_db.longmarginratio
        option_db.shortmarginratio_speculation = option_db.shortmarginratio
        option_db.longmarginratio_hedge = option_db.longmarginratio
        option_db.shortmarginratio_hedge = option_db.shortmarginratio
        option_db.longmarginratio_arbitrage = option_db.longmarginratio
        option_db.shortmarginratio_arbitrage = option_db.shortmarginratio
        option_db.multiplier = 10
        option_db.crncy = 'CNY'

        option_db.effective_since = filter_date_str
        option_db.is_settle_instantly = 1
        option_db.is_purchase_to_redemption_instantly = 0
        option_db.is_buy_to_redpur_instantly = 0
        option_db.is_redpur_to_sell_instantly = 0
        option_db.option_margin_factor1 = 0.5
        option_db.option_margin_factor2 = 0.5

        option_db.session = __get_trading_info_list(ticker_type)[-1]
        option_db.track_undl_tickers = __get_track_undl_tickers(ticker_type)
        option_insert_list.append(option_db)
        option_ticker_list.append(ticker)

    if len(option_ticker_list) > 0:
         print 'Prepare Insert Option:', ','.join(option_ticker_list)


def insert_server_db():
    if len(future_insert_list) == 0 and len(option_insert_list) == 0:
        return

    server_model = server_constant_local.get_server_model('host')
    server_session = server_model.get_db_session('common')
    future_sql_list = to_many_sql(Instrument, future_insert_list, 'common.instrument', 1000)
    for future_sql in future_sql_list:
        server_session.execute(future_sql)
    option_sql_list = to_many_sql(Instrument, option_insert_list, 'common.instrument', 1000)
    for option_sql in option_sql_list:
        server_session.execute(option_sql)
    server_session.commit()
    server_model.close()


def build_future_db_dict():
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id.in_([instrument_type_enums.Future, instrument_type_enums.Option])):
        if instrument_db.type_id == instrument_type_enums.Future:
            dict_key = '%s|%s' % (instrument_db.ticker, instrument_db.exchange_id)
            future_db_dict[dict_key] = instrument_db
        elif instrument_db.type_id == instrument_type_enums.Option:
            option_db_dict[instrument_db.ticker] = instrument_db


def __get_put_call(line_str):
    if 'P' in line_str:
        return 0
    elif 'C' in line_str:
        return 1
    else:
        print 'unfind:', line_str
        return 0


def get_instrument_max_id():
    global max_instrument_id
    max_instrument_id = session_common.execute('select max(id) from common.instrument').first()[0]


def ctp_price_analysis_add(date_str):
    print 'Enter ctp_price_analysis_add.'
    global session_common
    global session_basicinfo
    global filter_date_str

    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    session_basicinfo = server_host.get_db_session('basic_info')
    if date_str is None or date_str == '':
        filter_date_str = now.strftime('%Y-%m-%d')
    else:
        filter_date_str = date_str

    instrument_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('CTP_INSTRUMENT', filter_date_str)

    build_future_db_dict()
    get_instrument_max_id()
    for ctp_file in instrument_file_list:
        read_price_file_ctp('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, ctp_file))
    server_host.close()

    insert_server_db()
    print 'Exit ctp_price_analysis_add.'


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    ctp_price_analysis_add(date_str)
