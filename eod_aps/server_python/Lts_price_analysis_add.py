# -*- coding: utf-8 -*-
# 对各数据库新增期权和股票数据
from datetime import datetime
from eod_aps.model.BaseModel import *
import codecs
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.file_utils import FileUtils
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.server_python import *
from eod_aps.model.obj_to_sql import to_many_sql
import sys

reload(sys)
sys.setdefaultencoding('utf8')

now = datetime.now()

instrument_db_dict = dict()
instrument_insert_list = []
undl_ticker_dict = dict()
structured_fund_dict = dict()


def read_position_file_lts(lts_file_path):
    print 'Start read file:' + lts_file_path
    with codecs.open(lts_file_path, 'r', 'gbk') as fr:
        sf_instrument_array = []
        option_array = []
        stock_array = []
        structured_fund_array = []
        mmf_fund_array = []  # 货币基金
        convertible_bond_array = []  # 可转债
        for line in fr.xreadlines():
            base_model = BaseModel()
            for tempStr in line.split('|')[1].split(','):
                temp_array = tempStr.replace('\n', '').split(':', 1)
                setattr(base_model, temp_array[0].strip(), temp_array[1])

            if 'OnRspQryInstrument' in line:
                product_id = getattr(base_model, 'ProductID', '')
                if product_id in ('SHEOP', 'SHAOP'):
                    option_array.append(base_model)
                elif product_id in ('SZA', 'SHA', 'HKA', 'CY'):
                    stock_array.append(base_model)
                elif product_id in ('SZOF', 'SHOF'):
                    structured_fund_array.append(base_model)
                elif product_id in ('SHCB', 'SZCB'):
                    convertible_bond_array.append(base_model)
                elif product_id == 'SHFUNDETF':
                    mmf_fund_array.append(base_model)
            elif 'OnRspQrySFInstrument' in line:
                sf_instrument_array.append(base_model)
    print len(option_array)
    # structured_fund_undl_ticker(sf_instrument_array)
    # add_structured_fund(structured_fund_array)
    add_convertible_bond(convertible_bond_array)  # 新增可转债
    add_option(option_array)  # 新增期权
    add_stock(stock_array)  # 更新股票停牌日期数据和新增股票
    add_mmf_fund(mmf_fund_array)  # 新增货币基金


def add_convertible_bond(message_array):
    global max_instrument_id
    ticker_list = []
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        # 过滤掉不可交易或者退市的合约
        is_trading = getattr(messageInfo, 'IsTrading', '')
        if is_trading != '1':
            continue

        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in instrument_db_dict:
            continue

        convertible_bond_db = Instrument()
        max_instrument_id += 1
        convertible_bond_db.id = max_instrument_id
        convertible_bond_db.ticker = ticker
        convertible_bond_db.exchange_id = exchange_id
        convertible_bond_db.name = ''

        convertible_bond_db.fut_val_pt = getattr(messageInfo, 'VolumeMultiple', '')
        convertible_bond_db.multiplier = convertible_bond_db.fut_val_pt
        convertible_bond_db.max_market_order_vol = getattr(messageInfo, 'MaxMarketOrderVolume', '')
        convertible_bond_db.min_market_order_vol = getattr(messageInfo, 'MinMarketOrderVolume', '')
        # convertible_bond_db.max_limit_order_vol = getattr(messageInfo, 'MaxLimitOrderVolume', '')
        convertible_bond_db.max_limit_order_vol = 100000
        convertible_bond_db.min_limit_order_vol = getattr(messageInfo, 'MinLimitOrderVolume', '')

        convertible_bond_db.ticker_exch = ticker
        convertible_bond_db.ticker_exch_real = ticker
        convertible_bond_db.market_status_id = 2
        convertible_bond_db.type_id = 19
        convertible_bond_db.market_sector_id = 2
        convertible_bond_db.round_lot_size = 10
        convertible_bond_db.tick_size_table = '0:0.001'
        convertible_bond_db.crncy = 'CNY'
        convertible_bond_db.longmarginratio = 0
        convertible_bond_db.shortmarginratio = 999
        convertible_bond_db.longmarginratio_speculation = convertible_bond_db.longmarginratio
        convertible_bond_db.shortmarginratio_speculation = convertible_bond_db.shortmarginratio
        convertible_bond_db.longmarginratio_hedge = convertible_bond_db.longmarginratio
        convertible_bond_db.shortmarginratio_hedge = convertible_bond_db.shortmarginratio
        convertible_bond_db.longmarginratio_arbitrage = convertible_bond_db.longmarginratio
        convertible_bond_db.shortmarginratio_arbitrage = convertible_bond_db.shortmarginratio

        convertible_bond_db.is_settle_instantly = 1
        convertible_bond_db.is_purchase_to_redemption_instantly = 0
        convertible_bond_db.is_buy_to_redpur_instantly = 0
        convertible_bond_db.is_redpur_to_sell_instantly = 0
        instrument_insert_list.append(convertible_bond_db)
        ticker_list.append(ticker)
    if len(ticker_list) > 0:
        print 'Prepare Insert ConvertibleBond:', ','.join(ticker_list)


def add_mmf_fund(message_array):
    global max_instrument_id
    etf_ticker_list = []
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in instrument_db_dict:
            continue

        mmf_fund_db = Instrument()
        max_instrument_id += 1
        mmf_fund_db.id = max_instrument_id
        mmf_fund_db.ticker = ticker
        mmf_fund_db.exchange_id = exchange_id
        mmf_fund_db.name = ''

        mmf_fund_db.fut_val_pt = getattr(messageInfo, 'VolumeMultiple', '')
        mmf_fund_db.multiplier = mmf_fund_db.fut_val_pt
        mmf_fund_db.max_market_order_vol = getattr(messageInfo, 'MaxMarketOrderVolume', '')
        mmf_fund_db.min_market_order_vol = getattr(messageInfo, 'MinMarketOrderVolume', '')
        mmf_fund_db.max_limit_order_vol = getattr(messageInfo, 'MaxLimitOrderVolume', '')
        mmf_fund_db.min_limit_order_vol = getattr(messageInfo, 'MinLimitOrderVolume', '')

        mmf_fund_db.ticker_exch = ticker
        mmf_fund_db.ticker_exch_real = ticker
        mmf_fund_db.market_status_id = 2
        mmf_fund_db.type_id = 15
        mmf_fund_db.market_sector_id = 4
        mmf_fund_db.round_lot_size = 100
        # price_tick = getattr(messageInfo, 'PriceTick', '')
        mmf_fund_db.tick_size_table = '0:0.001'
        mmf_fund_db.crncy = 'CNY'
        mmf_fund_db.longmarginratio = 0
        mmf_fund_db.shortmarginratio = 999
        mmf_fund_db.longmarginratio_speculation = mmf_fund_db.longmarginratio
        mmf_fund_db.shortmarginratio_speculation = mmf_fund_db.shortmarginratio
        mmf_fund_db.longmarginratio_hedge = mmf_fund_db.longmarginratio
        mmf_fund_db.shortmarginratio_hedge = mmf_fund_db.shortmarginratio
        mmf_fund_db.longmarginratio_arbitrage = mmf_fund_db.longmarginratio
        mmf_fund_db.shortmarginratio_arbitrage = mmf_fund_db.shortmarginratio

        if exchange_name == 'SSE':
            mmf_fund_db.is_settle_instantly = 1
            mmf_fund_db.is_purchase_to_redemption_instantly = 0
            mmf_fund_db.is_buy_to_redpur_instantly = 0
            mmf_fund_db.is_redpur_to_sell_instantly = 0
        elif exchange_name == 'SZE':
            mmf_fund_db.is_settle_instantly = 1
            mmf_fund_db.is_purchase_to_redemption_instantly = 1
            mmf_fund_db.is_buy_to_redpur_instantly = 1
            mmf_fund_db.is_redpur_to_sell_instantly = 1

        instrument_insert_list.append(mmf_fund_db)
        etf_ticker_list.append(ticker)

    if len(etf_ticker_list) > 0:
        print 'Prepare Insert ETF:', ','.join(etf_ticker_list)


def structured_fund_undl_ticker(sf_instrument_array):
    for messageInfo in sf_instrument_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        undl_ticker = getattr(messageInfo, 'SFInstrumentID', '')
        structured_fund_dict[ticker] = ''
        structured_fund_dict[undl_ticker] = ''

        if ticker == undl_ticker:
            continue
        if undl_ticker in undl_ticker_dict:
            undl_ticker_dict[undl_ticker].append(ticker)
        else:
            ticker_list = [ticker]
            undl_ticker_dict[undl_ticker] = ticker_list


def add_structured_fund(message_array):
    global max_instrument_id
    structuredfund_ticker_list = []
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        if ticker not in structured_fund_dict:
            continue

        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in instrument_db_dict:
            continue

        structured_fund_db = Instrument()
        max_instrument_id += 1
        structured_fund_db.id = max_instrument_id
        structured_fund_db.ticker = ticker
        structured_fund_db.exchange_id = exchange_id
        structured_fund_db.name = ''

        undl_ticker_str = ''
        if ticker in undl_ticker_dict:
            undl_ticker_list = undl_ticker_dict[ticker]
            undl_ticker_str = ';'.join(undl_ticker_list)
        structured_fund_db.undl_tickers = undl_ticker_str

        if exchange_id == 18:
            structured_fund_db.is_settle_instantly = 0
            structured_fund_db.is_purchase_to_redemption_instantly = 0
            structured_fund_db.is_buy_to_redpur_instantly = 1
            structured_fund_db.is_redpur_to_sell_instantly = 1
        elif exchange_id == 19:
            structured_fund_db.is_settle_instantly = 0
            structured_fund_db.is_purchase_to_redemption_instantly = 0
            structured_fund_db.is_buy_to_redpur_instantly = 0
            structured_fund_db.is_redpur_to_sell_instantly = 0

        structured_fund_db.ticker_exch = ticker
        structured_fund_db.ticker_exch_real = ticker
        structured_fund_db.market_sector_id = 4
        structured_fund_db.type_id = 16
        structured_fund_db.crncy = 'CNY'
        structured_fund_db.round_lot_size = 100
        structured_fund_db.tick_size_table = '0:0.001'
        structured_fund_db.market_status_id = 2
        structured_fund_db.fut_val_pt = 1
        structured_fund_db.max_limit_order_vol = 1000000
        structured_fund_db.min_limit_order_vol = 100
        structured_fund_db.max_market_order_vol = 0
        structured_fund_db.min_market_order_vol = 0
        structured_fund_db.longmarginratio = 0
        structured_fund_db.shortmarginratio = 999
        structured_fund_db.longmarginratio_speculation = structured_fund_db.longmarginratio
        structured_fund_db.shortmarginratio_speculation = structured_fund_db.shortmarginratio
        structured_fund_db.longmarginratio_hedge = structured_fund_db.longmarginratio
        structured_fund_db.shortmarginratio_hedge = structured_fund_db.shortmarginratio
        structured_fund_db.longmarginratio_arbitrage = structured_fund_db.longmarginratio
        structured_fund_db.shortmarginratio_arbitrage = structured_fund_db.shortmarginratio
        structured_fund_db.multiplier = 1
        instrument_insert_list.append(structured_fund_db)
        structuredfund_ticker_list.append(ticker)

    if len(structuredfund_ticker_list) > 0:
        print 'Prepare Insert StructuredFund:', ','.join(structuredfund_ticker_list)


def add_option(message_array):
    global max_instrument_id
    option_ticker_list = []
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in instrument_db_dict:
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
        option_db.strike = getattr(messageInfo, 'ExecPrice', '')

        exchange_inst_id = getattr(messageInfo, 'ExchangeInstID', '')
        undl_tickers = exchange_inst_id[0:6]
        option_db.undl_tickers = undl_tickers
        option_db.track_undl_tickers = undl_tickers
        option_db.commission_rate_type = 'option_%s' % undl_tickers
        if 'M' in exchange_inst_id:
            option_db.contract_adjustment = 0
        elif 'A' in exchange_inst_id:
            option_db.contract_adjustment = 1
        elif 'B' in exchange_inst_id:
            option_db.contract_adjustment = 2
        elif 'C' in exchange_inst_id:
            option_db.contract_adjustment = 3

        option_db.create_date = getattr(messageInfo, 'CreateDate', '')
        option_db.expire_date = getattr(messageInfo, 'ExpireDate', '')

        instrument_name = getattr(messageInfo, 'InstrumentName', '').strip()
        instrument_name = instrument_name.replace('购', 'Call').replace('沽', 'Put').replace('月', 'month') \
            .replace('中国平安', undl_tickers).replace('上汽集团', undl_tickers)
        if 'Call' in instrument_name:
            put_call = 1
        else:
            put_call = 0
        # option_db.name = instrument_name
        # if ticker.isdigit():
        option_db.name = exchange_inst_id
        option_db.put_call = put_call

        price_tick = getattr(messageInfo, 'PriceTick', '')
        option_db.tick_size_table = '0:%s' % (float(price_tick),)

        option_db.ticker_exch = ticker
        option_db.ticker_exch_real = ticker
        option_db.market_status_id = 2
        option_db.type_id = 10  # option
        option_db.market_sector_id = 4
        option_db.round_lot_size = 1
        option_db.longmarginratio = 0
        option_db.shortmarginratio = 0.15
        option_db.longmarginratio_speculation = option_db.longmarginratio
        option_db.shortmarginratio_speculation = option_db.shortmarginratio
        option_db.longmarginratio_hedge = option_db.longmarginratio
        option_db.shortmarginratio_hedge = option_db.shortmarginratio
        option_db.longmarginratio_arbitrage = option_db.longmarginratio
        option_db.shortmarginratio_arbitrage = option_db.shortmarginratio
        option_db.multiplier = 10000
        option_db.crncy = 'CNY'
        option_db.effective_since = filter_date_str
        option_db.is_settle_instantly = 1
        option_db.is_purchase_to_redemption_instantly = 0
        option_db.is_buy_to_redpur_instantly = 0
        option_db.is_redpur_to_sell_instantly = 0
        option_db.option_margin_factor1 = 0.15
        option_db.option_margin_factor2 = 0.07
        instrument_insert_list.append(option_db)
        option_ticker_list.append(ticker)

    if len(option_ticker_list) > 0:
        print 'Prepare Insert Option:', ','.join(option_ticker_list)


def add_stock(message_array):
    global max_instrument_id
    stock_ticker_list = []
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'SSE':
            exchange_id = 18
        elif exchange_name == 'SZE':
            exchange_id = 19
        elif exchange_name == 'HGE':
            exchange_id = 13
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in instrument_db_dict:
            continue
        # instrument_name = getattr(messageInfo, 'InstrumentName', '')

        stock_db = Instrument()
        max_instrument_id += 1
        stock_db.id = max_instrument_id
        stock_db.ticker = ticker
        stock_db.exchange_id = exchange_id
        stock_db.name = ''

        stock_db.ticker_exch = ticker
        stock_db.ticker_exch_real = ticker
        stock_db.market_status_id = 2
        stock_db.market_sector_id = 4
        stock_db.type_id = 4
        stock_db.crncy = 'CNY'
        stock_db.round_lot_size = 100
        stock_db.tick_size_table = '0:0.01'
        stock_db.fut_val_pt = 1
        stock_db.max_market_order_vol = 0
        stock_db.min_market_order_vol = 0
        stock_db.max_limit_order_vol = 1000000
        stock_db.min_limit_order_vol = 100
        stock_db.longmarginratio = 0
        stock_db.shortmarginratio = 999
        stock_db.longmarginratio_speculation = stock_db.longmarginratio
        stock_db.shortmarginratio_speculation = stock_db.shortmarginratio
        stock_db.longmarginratio_hedge = stock_db.longmarginratio
        stock_db.shortmarginratio_hedge = stock_db.shortmarginratio
        stock_db.longmarginratio_arbitrage = stock_db.longmarginratio
        stock_db.shortmarginratio_arbitrage = stock_db.shortmarginratio
        stock_db.multiplier = 1
        stock_db.strike = 0
        stock_db.is_settle_instantly = 0
        stock_db.is_purchase_to_redemption_instantly = 0
        stock_db.is_buy_to_redpur_instantly = 1
        stock_db.is_redpur_to_sell_instantly = 1
        instrument_insert_list.append(stock_db)
        stock_ticker_list.append(ticker)

    if len(stock_ticker_list) > 0:
        print 'Prepare Insert Stock:', ','.join(stock_ticker_list)


def build_instrument_db_dict():
    query = session_common.query(Instrument)
    for future in query.filter(Instrument.exchange_id.in_((18, 19, 13))):
        dict_key = '%s|%s' % (future.ticker, future.exchange_id)
        instrument_db_dict[dict_key] = future


def get_instrument_max_id():
    global max_instrument_id
    max_instrument_id = session_common.execute('select max(id) from common.instrument').first()[0]


def insert_server_db():
    if len(instrument_insert_list) == 0:
        return

    server_model = server_constant_local.get_server_model('host')
    server_session = server_model.get_db_session('common')
    sql_list = to_many_sql(Instrument, instrument_insert_list, 'common.instrument')
    for sql in sql_list:
        server_session.execute(sql)
    server_session.commit()
    server_model.close()


def lts_price_analysis_add(date_str):
    print 'Enter Lts_price_analysis_add.'
    global session_common
    global filter_date_str

    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    if date_str is None or date_str == '':
        filter_date_str = now.strftime('%Y-%m-%d')
    else:
        filter_date_str = date_str

    build_instrument_db_dict()
    get_instrument_max_id()

    instrument_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('HUABAO_INSTRUMENT', filter_date_str)

    for qd_file in instrument_file_list:
        read_position_file_lts('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, qd_file))
    session_common.commit()
    server_host.close()

    insert_server_db()
    print 'Exit Lts_price_analysis_add.'


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    lts_price_analysis_add(date_str)
