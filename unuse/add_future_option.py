from eod_aps.model.instrument import Instrument
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.eod_const import const

input_file = 'E:/dailyFiles/future_option.csv'
option_insert_list = []



def add_future_option():
    fr = open(input_file)
    for line_item in fr.readlines():
        (ticker, uplimit, downlimit, multiplier, tick_size, effective_since, expire_date) = line_item.split(',')
        effective_since = '%s-%s-%s' % (effective_since[:4], effective_since[4:6], effective_since[6:8])
        expire_date = '%s-%s-%s' % (expire_date[:4], expire_date[4:6], expire_date[6:8])

        option_db = Instrument()
        option_db.ticker = ticker
        option_db.exchange_id = 21
        option_db.uplimit = uplimit
        option_db.downlimit = downlimit

        option_db.fut_val_pt = multiplier
        option_db.max_market_order_vol = 100
        option_db.min_market_order_vol = 1
        option_db.max_limit_order_vol = 100
        option_db.min_limit_order_vol = 1

        option_db.undl_tickers = ticker.split('-')[0]

        option_db.create_date = effective_since
        option_db.expire_date = expire_date

        option_db.name = ticker
        # option_db.strike = getattr(messageInfo, 'PriceTick', '')

        put_call = __get_put_call(ticker)
        option_db.put_call = put_call

        if put_call == 0:
            option_db.strike = ticker.replace('-', '').split('P')[-1]
        else:
            option_db.strike = ticker.replace('-', '').split('C')[-1]

        option_db.tick_size_table = '0:%s' % (float(tick_size),)

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
        option_db.multiplier = multiplier
        option_db.crncy = 'CNY'

        option_db.effective_since = effective_since
        option_db.is_settle_instantly = 1
        option_db.is_purchase_to_redemption_instantly = 0
        option_db.is_buy_to_redpur_instantly = 0
        option_db.is_redpur_to_sell_instantly = 0
        option_db.option_margin_factor1 = 0.5
        option_db.option_margin_factor2 = 0.5

        ticker_type = filter(lambda x: not x.isdigit(), option_db.undl_tickers)
        option_db.session = __get_trading_info_list(ticker_type)[-1]
        option_insert_list.append(option_db)
        print 'prepare insert option:', ticker

    for option_info in option_insert_list:
        session_common.add(option_info)
    session_common.commit()


def __get_put_call(line_str):
    if 'P' in line_str:
        return 0
    elif 'C' in line_str:
        return 1
    else:
        print 'unfind:', line_str
        return 0


def __get_trading_info_list(ticker_type):
    session_basicinfo = host_server_model.get_db_session('basic_info')
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

if __name__ == '__main__':
    server_constant = ServerConstant()
    host_server_model = server_constant.get_server_model('host')
    session_common = host_server_model.get_db_session('common')

    add_future_option()

    # session.commit()
