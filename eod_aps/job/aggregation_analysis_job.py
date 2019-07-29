# -*- coding: utf-8 -*-
# 从各服务器同步持仓数据
from eod_aps.model.schema_om import OrderHistory, Trade2History
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.job import *


local_server_name = 'local118'


def aggregation_analysis_job(server_name):
    date_filter_str1 = date_utils.get_today_str('%Y-%m-%d')
    date_filter_str2 = date_utils.get_next_trading_day('%Y-%m-%d')
    custom_log.log_info_job('Aggregation From Server:%s, Date:%s Start.' % (server_name, date_filter_str1))

    server_model = server_constant.get_server_model(server_name)
    global execute_sql_list
    execute_sql_list = []

    __pf_account_analysis(server_model)
    __pf_position_analysis(server_model, date_filter_str2)
    __om_order_analysis(server_model, date_filter_str1)
    __om_trader_analysis(server_model, date_filter_str1)
    __update_local_db(local_server_name)
    server_model.close()
    custom_log.log_info_job('Aggregation From Server:%s, Date:%s Stop.' % (server_name, date_filter_str1))


def __pf_account_analysis(server_model):
    del_sql = "delete from `pf_account` where server_name = '%s'" % server_model.name
    execute_sql_list.append(del_sql)

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    insert_sql_base = """INSERT INTO `pf_account` (`ID`, `SERVER_NAME`, `NAME`, `FUND_NAME`, `GROUP_NAME`) VALUES """

    row_value_list = []
    for i, pf_account_db in enumerate(query_pf_account):
        value_sql = "(%s,'%s','%s','%s','%s')" % (pf_account_db.id, server_model.name, pf_account_db.name,
                                                  pf_account_db.fund_name, pf_account_db.group_name)
        row_value_list.append(value_sql)
        if i % 1000 == 0:
            execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))
            row_value_list = []
    if len(row_value_list) > 0:
        execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))


def __pf_position_analysis(server_model, date_filter_str):
    del_sql = "delete from `pf_position` where date = '%s' and server_name = '%s'" % (date_filter_str, server_model.name)
    execute_sql_list.append(del_sql)

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)

    insert_sql_base = "INSERT INTO `pf_position` (`DATE`, `SERVER_NAME`, `ID`, `SYMBOL`, `HEDGEFLAG`, \
`LONG`, `LONG_COST`, `LONG_AVAIL`, `SHORT`, `SHORT_COST`, `SHORT_AVAIL`, `FEE`, `CLOSE_PRICE`, `NOTE`, `DELTA`, `GAMMA`, \
`THETA`, `VEGA`, `RHO`, `YD_POSITION_LONG`, `YD_POSITION_SHORT`, `YD_LONG_REMAIN`, `YD_SHORT_REMAIN`, `PREV_NET`, \
`PURCHASE_AVAIL`) VALUES "

    pf_position_list = []
    for pf_position_db in query_pf_position.filter(PfPosition.date == date_filter_str):
        pf_position_list.append(pf_position_db)

    row_value_list = []
    for i, pf_position_db in enumerate(pf_position_list):
        value_sql = "('%s','%s','%s','%s','%s','%s','%s','%s','%s',\
'%s','%s','%s',NULL,NULL,'1','0','0','0','0','%s','%s','%s','%s','%s','%s')" % \
                       (pf_position_db.date, server_model.name, pf_position_db.id,
                        pf_position_db.symbol, pf_position_db.hedgeflag, pf_position_db.long, pf_position_db.long_cost,
                        pf_position_db.long_avail, pf_position_db.short, pf_position_db.short_cost,
                        pf_position_db.short_avail, pf_position_db.fee, pf_position_db.yd_position_long,
                        pf_position_db.yd_position_short, pf_position_db.yd_long_remain, pf_position_db.yd_short_remain,
                        pf_position_db.prev_net, pf_position_db.purchase_avail)
        row_value_list.append(value_sql)
        if i % 1000 == 0:
            execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))
            row_value_list = []
    if len(row_value_list) > 0:
        execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))


def __om_trader_analysis(server_model, date_filter_str):
    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
    start_date = '%s 21:00:00' % last_trading_day
    end_date = '%s 16:00:00' % date_filter_str

    session_om = server_model.get_db_session('om')
    query_om_trader = session_om.query(Trade2History)

    insert_sql_base = "REPLACE INTO `trade2` (`ID`, `SERVER_NAME`, `TIME`, `SYMBOL`, `QTY`, `PRICE`, `TRADE_TYPE`, \
`STRATEGY_ID`, `ACCOUNT`, `ORDER_ID`, `SELF_CROSS`, `TRADE_ID`) VALUES "

    om_trader_list = []
    for om_trader_db in query_om_trader.filter(Trade2History.time.between(start_date, end_date)):
        om_trader_list.append(om_trader_db)

    row_value_list = []
    for i, om_trader_db in enumerate(om_trader_list):
        value_sql = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                    (om_trader_db.id, server_model.name, om_trader_db.time, om_trader_db.symbol, om_trader_db.qty,
                     om_trader_db.price, om_trader_db.trade_type, om_trader_db.strategy_id, om_trader_db.account,
                     om_trader_db.order_id, om_trader_db.self_cross, om_trader_db.trade_id)
        row_value_list.append(value_sql)
        if i % 1000 == 0:
            execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))
            row_value_list = []
    if len(row_value_list) > 0:
        execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))


def __om_order_analysis(server_model, date_filter_str):
    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
    start_date = '%s 21:00:00' % last_trading_day
    end_date = '%s 16:00:00' % date_filter_str

    session_om = server_model.get_db_session('om')
    query_om_order = session_om.query(OrderHistory)

    insert_sql_base = "REPLACE INTO `order` (`ID`, `SERVER_NAME`, `SYS_ID`,`ACCOUNT`,`HEDGEFLAG`,`SYMBOL`,`DIRECTION`,`TYPE`,\
    `TRADE_TYPE`,`STATUS`,`OP_STATUS`,`PROPERTY`,`CREATE_TIME`,`TRANSACTION_TIME`,`USER_ID`,`STRATEGY_ID`,`PARENT_ORD_ID`\
    ,`QTY`,`PRICE`,`EX_QTY`,`EX_PRICE`,`ALGO_TYPE`) VALUES "

    om_order_list = []
    for om_order_db in query_om_order.filter(OrderHistory.create_time.between(start_date, end_date)):
        om_order_list.append(om_order_db)

    row_value_list = []
    for i, om_order_db in enumerate(om_order_list):
        value_sql = "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',\
'%s','%s','%s','%s','%s','%s','%s')" % (om_order_db.id, server_model.name, om_order_db.sys_id,
                         om_order_db.account, om_order_db.hedgeflag, om_order_db.symbol,
                         om_order_db.direction, om_order_db.type, om_order_db.trade_type, om_order_db.status,
                         om_order_db.op_status, om_order_db.property, om_order_db.create_time,
                         om_order_db.transaction_time, om_order_db.user_id, om_order_db.strategy_id,
                         om_order_db.parent_ord_id, om_order_db.qty, om_order_db.price, om_order_db.ex_qty,
                         om_order_db.ex_price, om_order_db.algo_type)
        row_value_list.append(value_sql)
        if i % 1000 == 0:
            execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))
            row_value_list = []
    if len(row_value_list) > 0:
        execute_sql_list.append('%s %s' % (insert_sql_base, ','.join(row_value_list)))


def __update_local_db(server_name):
    server_model = server_constant.get_server_model(server_name)
    session_aggregation = server_model.get_db_session('aggregation')

    for sql in execute_sql_list:
        session_aggregation.execute(sql)
    session_aggregation.commit()
    server_model.close()


if __name__ == '__main__':
    # all_trade_servers_list = server_constant.get_all_trade_servers()
    # for server_name in all_trade_servers_list:
    #     aggregation_analysis_job(server_name)
    aggregation_analysis_job('guosen')
