# -*- coding: utf-8 -*-
from eod_aps.model.instrument import Instrument
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.eod_const import const

server_constant = ServerConstant()


def __get_future_type(server_model):
    # session_common = server_model.get_db_session('common')
    # future_type_list = []
    #
    # query_sql = "select a.UNDL_TICKERS from common.instrument_history a group by a.UNDL_TICKERS"
    # for future_type in session_common.execute(query_sql):
    #     future_type_list.append(future_type[0])
    # return future_type_list
    future_type_set = set()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        ticker_type = filter(lambda x: not x.isdigit(), instrument_db.ticker)
        future_type_set.add(ticker_type)
    return list(future_type_set)


def __get_trading_info_list(server_model, ticker_type):
    session_basicinfo = server_model.get_db_session('basic_info')

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

def __update_db(server_model, trading_info_dict, include_history_flag):
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        ticker_type = filter(lambda x: not x.isdigit(), instrument_db.ticker)
        if ticker_type not in trading_info_dict:
            print 'unfind ticker', instrument_db.ticker
            continue
        trading_info_list = trading_info_dict[ticker_type]
        if include_history_flag:
            instrument_db.session = '+'.join(trading_info_list)
        else:
            instrument_db.session = trading_info_list[-1]
        session_common.merge(instrument_db)
    session_common.commit()


def update_trading_time(server_name, include_history_flag):
    server_model = ServerConstant().get_server_model(server_name)

    trading_info_dict = dict()
    future_type_list = __get_future_type(server_model)
    for future_type in future_type_list:
        trading_info_list = __get_trading_info_list(server_model, future_type)
        if len(trading_info_list) == 0:
            print 'Error future_type:', future_type
            continue
        trading_info_dict[future_type] = trading_info_list

    __update_db(server_model, trading_info_dict, include_history_flag)

    for type_info in trading_info_dict.keys():
        print type_info + ':------------------------'
        print '\n'.join(trading_info_dict[type_info])


if __name__ == '__main__':
    include_history_flag = False
    update_trading_time('zhongxin', include_history_flag)