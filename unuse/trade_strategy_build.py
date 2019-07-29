# -*- coding: utf-8 -*-
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.date_utils import DateUtils


date_utils = DateUtils()


def __find_strategy(order_id, order_dict):
    if order_id not in order_dict:
        return None
    order_db = order_dict[order_id]
    if order_db[2] != '':
        strategy_name = __find_strategy(str(order_db[2]), order_dict)
    else:
        strategy_name = order_db[1]
    return strategy_name


def __trade_strategy_build(server_model, trading_day):
    session_om = server_model.get_db_session('om')

    trade_list = []
    query_sql = "select id,order_id from om.trade2 where strategy_id = '' and time like '%s'" % ('%' + trading_day + '%',)
    for trade_db in session_om.execute(query_sql):
        trade_list.append((str(trade_db[0]), str(trade_db[1])))

    order_dict = dict()
    query_sql = "select id,strategy_id,parent_ord_id from om.order where create_time like '%s'" % ('%' + trading_day + '%',)
    for order_db in session_om.execute(query_sql):
        order_dict[str(order_db[0])] = order_db

    for (trade_id, order_id) in trade_list:
        strategy_name = __find_strategy(order_id, order_dict)
        if strategy_name is None or strategy_name == '':
            continue

        update_sql = "update om.trade2 set strategy_id='%s' where id='%s'" % (strategy_name, trade_id)
        session_om.execute(update_sql)
    session_om.commit()


def start():
    server_model = ServerConstant().get_server_model('nanhua')
    trading_day_list = date_utils.get_trading_day_list(date_utils.string_toDatetime('2016-09-23'),
                                                       date_utils.string_toDatetime('2016-10-14'))
    for trading_day in trading_day_list:
        __trade_strategy_build(server_model, trading_day.strftime("%Y-%m-%d"))
    

if __name__ == '__main__':
    start()
