# -*- coding: utf-8 -*-
import datetime
from eod_aps.model.server_constans import ServerConstant

# config
server_name = 'local125'
account_id = '060000006185'
account_name = 'absolute_return'
ip = '172.16.10.120'
port = '17103'
trade_list_file_name = 'trade_list.csv'
pf_position_file_name = 'risk.csv'
origin_pf_position_datetime_str = '2017-09-27 08:00:00'

# constant info
csv_folder = './emergency_csv_file/'


def get_trade_info_dict():
    global ticker_full_portfolio_name_map
    trade_list_file_path = csv_folder + trade_list_file_name
    fr = open(trade_list_file_path)
    first_line_flag = True
    target_trade_info_list = []
    target_trade_value_dict = dict()
    for line in fr.readlines():
        if first_line_flag:
            title_list = line.strip().split(',')
            time_index = title_list.index('Time')
            symbol_index = title_list.index('Symbol')
            qty_index = title_list.index('Qty')
            price_index = title_list.index('Price')
            strategy_index = title_list.index('Strategy')
            account_index = title_list.index('Account')
            first_line_flag = False
        if line.strip() == '':
            continue
        trade_info_list = line.strip().split(',')
        trade_account_info = trade_info_list[account_index]
        if account_id not in trade_account_info or account_name not in trade_account_info:
            continue
        trade_time_info = trade_info_list[time_index].replace('?', ' ').replace('/', '-')
        trade_time_info_datetime = datetime.datetime.strptime(trade_time_info, '%m-%d-%Y %H:%M:%S')
        origin_pf_position_datetime = datetime.datetime.strptime(origin_pf_position_datetime_str, '%Y-%m-%d %H:%M:%S')
        if trade_time_info_datetime < origin_pf_position_datetime:
            continue
        trade_symbol_info = trade_info_list[symbol_index].replace('?', ' ')
        trade_symbol_info_portfolio = ticker_full_portfolio_name_map[trade_symbol_info]
        trade_qty_info = float(trade_info_list[qty_index])
        trade_price_info = float(trade_info_list[price_index])
        trade_strategy_info = trade_info_list[strategy_index]
        if trade_strategy_info == 'default':
            trade_strategy_info = 'manual.default'
        target_trade_info_list.append([trade_strategy_info, trade_time_info, trade_symbol_info, trade_qty_info,
                                       trade_price_info, trade_account_info])

        if trade_strategy_info not in target_trade_value_dict:
            target_trade_value_dict[trade_strategy_info] = dict()
            if trade_qty_info >= 0:
                target_trade_value_dict[trade_strategy_info][trade_symbol_info_portfolio] = [float(trade_qty_info), float(trade_qty_info), 0]
            else:
                target_trade_value_dict[trade_strategy_info][trade_symbol_info_portfolio] = [float(trade_qty_info), 0, -1 * float(trade_qty_info)]
        else:
            if trade_symbol_info_portfolio not in target_trade_value_dict[trade_strategy_info]:
                if trade_qty_info >= 0:
                    target_trade_value_dict[trade_strategy_info][trade_symbol_info_portfolio] = [float(trade_qty_info), float(trade_qty_info), 0]
                else:
                    target_trade_value_dict[trade_strategy_info][trade_symbol_info_portfolio] = [float(trade_qty_info), 0, -1 * float(trade_qty_info)]
            else:
                list_temp = target_trade_value_dict[trade_strategy_info][trade_symbol_info_portfolio]
                list_temp[0] += float(trade_qty_info)
                if float(trade_qty_info) >= 0:
                    list_temp[1] += float(trade_qty_info)
                else:
                    list_temp[2] += -1 * float(trade_qty_info)
                target_trade_value_dict[trade_strategy_info][trade_symbol_info_portfolio] = list_temp
    return target_trade_info_list, target_trade_value_dict


def get_id_strategy_name_dict():
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = 'select id, group_name, name, fund_name from portfolio.pf_account;'
    query_result = session_portfolio.execute(query_sql)
    id_strategy_name_dict = dict()
    for query_line in query_result:
        fund_name = query_line[3]
        if account_name not in fund_name:
            continue
        account_id = query_line[0]
        strategy_name = query_line[1] + '.' + query_line[2]
        id_strategy_name_dict[account_id] = strategy_name
    server_model.close()
    return id_strategy_name_dict


def get_strategy_name_id_dict():
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = 'select id, group_name, name, fund_name from portfolio.pf_account;'
    query_result = session_portfolio.execute(query_sql)
    strategy_name_id_dict = dict()
    for query_line in query_result:
        fund_name = query_line[3]
        if account_name not in fund_name:
            continue
        account_id = query_line[0]
        strategy_name = query_line[1] + '.' + query_line[2]
        strategy_name_id_dict[strategy_name] = account_id
    server_model.close()
    return strategy_name_id_dict


def get_max_query_date():
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql1 = 'select max(date) from portfolio.pf_position;'
    query_result1 = session_portfolio.execute(query_sql1)
    for query_line in query_result1:
        query_date = query_line[0]
    return query_date


def get_origin_pf_position(max_query_date):
    id_strategy_name_dict = get_id_strategy_name_dict()

    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    origin_pf_position_dict = dict()
    query_sql2 = "select id, symbol, `Long`, Short, Day_Long, Day_Short from portfolio.pf_position \
where date = '%s' order by id asc" % max_query_date
    query_result2 = session_portfolio.execute(query_sql2)
    for query_line in query_result2:
        account_id = query_line[0]
        if account_id not in id_strategy_name_dict:
            continue
        strategy_name = id_strategy_name_dict[account_id]
        symbol_info = query_line[1]
        long_value = query_line[2]
        short_value = query_line[3]
        day_long_value = query_line[4]
        day_short_value = query_line[5]
        if strategy_name not in origin_pf_position_dict:
            origin_pf_position_dict[strategy_name] = dict()
            origin_pf_position_dict[strategy_name][symbol_info] = [long_value - short_value, day_long_value, day_short_value]
        else:
            origin_pf_position_dict[strategy_name][symbol_info] = [long_value - short_value, day_long_value, day_short_value]
    server_model.close()
    return origin_pf_position_dict


def get_monitor_pf_position():
    global ticker_full_portfolio_name_map
    monitor_pf_position_file_path = csv_folder + pf_position_file_name
    fr = open(monitor_pf_position_file_path)
    monitor_pf_position_dict = dict()
    monitor_pf_position_value_dict = dict()
    first_line_flag = True
    for line in fr.readlines():
        if first_line_flag:
            title_list = line.strip().split(',')
            symbol_index = title_list.index('Symbol')
            long_index = title_list.index('Long')
            long_avail_index = title_list.index('LongAvail')
            long_cost_index = title_list.index('LongCost')
            day_long_index = title_list.index('DayLong')
            short_index = title_list.index('Short')
            short_avail_index = title_list.index('ShortAvail')
            short_cost_index = title_list.index('ShortCost')
            day_short_index = title_list.index('DayShort')
            # strategy_group_index = title_list.index('StrategyGroup')
            # strategy_index = title_list.index('Strategy')
            sub_strategy_index = title_list.index('SubStrategy')
            account_index = title_list.index('Account')
            first_line_flag = False
        if line.strip() == '':
            continue
        pf_position_info_list = line.strip().split(',')
        account_info = pf_position_info_list[account_index]
        if account_name not in account_info:
            continue
        sub_strategy_info = pf_position_info_list[sub_strategy_index]
        if ip not in sub_strategy_info or port not in sub_strategy_info:
            continue
        # strategy_name = pf_position_info_list[strategy_group_index] + '.' + pf_position_info_list[strategy_index]
        strategy_name_1 = pf_position_info_list[sub_strategy_index].split('-')[1]
        strategy_name_2 = pf_position_info_list[sub_strategy_index].split('-')[0]
        strategy_name = strategy_name_1 + '.' + strategy_name_2
        symbol_info_temp = pf_position_info_list[symbol_index].replace('?', ' ')
        if symbol_info_temp in ticker_full_portfolio_name_map:
            symbol_info = ticker_full_portfolio_name_map[symbol_info_temp]
        else:
            symbol_info = symbol_info_temp
            print symbol_info_temp
            print 'error instrument!'
        long_info = pf_position_info_list[long_index]
        long_avail_info = pf_position_info_list[long_avail_index]
        long_cost_info = pf_position_info_list[long_cost_index]
        day_long_info = pf_position_info_list[day_long_index]
        short_info = pf_position_info_list[short_index]
        short_avail_info = pf_position_info_list[short_avail_index]
        short_cost_info = pf_position_info_list[short_cost_index]
        day_short_info = pf_position_info_list[day_short_index]
        if strategy_name not in monitor_pf_position_dict:
            monitor_pf_position_dict[strategy_name] = dict()
            monitor_pf_position_dict[strategy_name][symbol_info] = [long_info, long_avail_info, long_cost_info,
                                                                    day_long_info, short_info, short_avail_info,
                                                                    short_cost_info, day_short_info]
        else:
            monitor_pf_position_dict[strategy_name][symbol_info] = [long_info, long_avail_info, long_cost_info,
                                                                    day_long_info, short_info, short_avail_info,
                                                                    short_cost_info, day_short_info]

        if strategy_name not in monitor_pf_position_value_dict:
            monitor_pf_position_value_dict[strategy_name] = dict()
            monitor_pf_position_value_dict[strategy_name][symbol_info] = [float(long_info) - float(short_info),
                                                                          float(day_long_info), float(day_short_info)]
        else:
            monitor_pf_position_value_dict[strategy_name][symbol_info] = [float(long_info) - float(short_info),
                                                                          float(day_long_info), float(day_short_info)]

    return monitor_pf_position_dict, monitor_pf_position_value_dict


def check_position(target_trade_value_dict, origin_pf_position_dict, monitor_pf_position_value_dict):
    check_flag = True
    for strategy in sorted(monitor_pf_position_value_dict.keys()):
        for symbol in monitor_pf_position_value_dict[strategy].keys():
            monitor_pf_position_value = monitor_pf_position_value_dict[strategy][symbol][0]
            monitor_day_long_value = monitor_pf_position_value_dict[strategy][symbol][1]
            monitor_day_short_value = monitor_pf_position_value_dict[strategy][symbol][2]
            if strategy not in origin_pf_position_dict:
                origin_pf_position_value = 0
                origin_pf_position_day_long = 0
                origin_pf_position_day_short = 0
            elif symbol not in origin_pf_position_dict[strategy]:
                origin_pf_position_value = 0
                origin_pf_position_day_long = 0
                origin_pf_position_day_short = 0
            else:
                origin_pf_position_value = origin_pf_position_dict[strategy][symbol][0]
                origin_pf_position_day_long = origin_pf_position_dict[strategy][symbol][1]
                origin_pf_position_day_short = origin_pf_position_dict[strategy][symbol][2]

            if strategy not in target_trade_value_dict:
                trade_value = 0
                trade_day_long = 0
                trade_day_short = 0
            elif symbol not in target_trade_value_dict[strategy]:
                trade_value = 0
                trade_day_long = 0
                trade_day_short = 0
            else:
                trade_value = target_trade_value_dict[strategy][symbol][0]
                trade_day_long = target_trade_value_dict[strategy][symbol][1]
                trade_day_short = target_trade_value_dict[strategy][symbol][2]

            if float(origin_pf_position_value) + trade_value != float(monitor_pf_position_value):
                check_flag = False
                print strategy, symbol, 'Long Short Error!'
                print origin_pf_position_value, trade_value, monitor_pf_position_value
            elif float(origin_pf_position_day_long) + trade_day_long != float(monitor_day_long_value):
                check_flag = False
                print strategy, symbol, 'Day Long Error!'
                print origin_pf_position_day_long, trade_day_long, monitor_day_long_value
            elif float(origin_pf_position_day_short) + trade_day_short != float(monitor_day_short_value):
                check_flag = False
                print strategy, symbol, 'Day Short Error!'
                print origin_pf_position_day_short, trade_day_short, monitor_day_short_value
    return check_flag


def get_day_long_day_short_value(strategy_name, target_trade_info_list, instrument_val_per_point_dict,
                                 strategy_name_id_dict, origin_day_long_day_short_dict, max_query_date):
    global ticker_full_portfolio_name_map
    trade_symbol_list = []
    for trade_info in target_trade_info_list:
        if trade_info[0] == strategy_name:
            trade_symbol_portfolio = ticker_full_portfolio_name_map[trade_info[2]]
            if trade_symbol_portfolio not in trade_symbol_list:
                trade_symbol_list.append(trade_symbol_portfolio)

    strategy_id = strategy_name_id_dict[strategy_name]
    fr = open(csv_folder + 'result.csv', 'a+')
    for symbol in trade_symbol_list:
        if strategy_id not in origin_day_long_day_short_dict:
            day_long_cost_value = 0
            day_short_cost_value = 0
        elif symbol not in origin_day_long_day_short_dict[strategy_id]:
            day_long_cost_value = 0
            day_short_cost_value = 0
        else:
            day_long_cost_value = origin_day_long_day_short_dict[strategy_id][symbol][0]
            day_short_cost_value = origin_day_long_day_short_dict[strategy_id][symbol][1]

        for trade_info in target_trade_info_list:
            trade_symbol_portfolio = ticker_full_portfolio_name_map[trade_info[2]]
            if trade_info[0] == strategy_name and trade_symbol_portfolio == symbol:
                trade_symbol = trade_symbol_portfolio
                fu_val_pt = float(instrument_val_per_point_dict[trade_symbol.split(' ')[0]])
                trade_qty = trade_info[3]
                trade_price = trade_info[4]

                if trade_qty >= 0:
                    day_long_cost_value += trade_qty * trade_price * fu_val_pt
                else:
                    day_short_cost_value += abs(trade_qty) * trade_price * fu_val_pt
        update_sql_base = "update portfolio.pf_position set DAY_LONG_COST = %s, DAY_SHORT_COST = %s where date = '%s' " \
                          "and ID = %s and symbol = '%s';"
        update_sql = update_sql_base % (day_long_cost_value, day_short_cost_value, max_query_date, strategy_id, trade_symbol)
        fr.write(update_sql + '\n')
        print update_sql
    fr.close()


def update_pf_position(monitor_pf_position_dict, origin_pf_position_dict):
    global max_query_date
    strategy_name_id_dict = get_strategy_name_id_dict()

    # update or insert
    fr = open(csv_folder + 'result.csv', 'w+')
    for strategy_name in sorted(monitor_pf_position_dict.keys()):
        strategy_id = strategy_name_id_dict[strategy_name]
        if strategy_name not in origin_pf_position_dict:
            for symbol in monitor_pf_position_dict[strategy_name].keys():
                pf_position_info = monitor_pf_position_dict[strategy_name][symbol]
                insert_sql_base = "insert into portfolio.pf_position (DATE, ID, SYMBOL, `LONG`, LONG_COST, LONG_AVAIL, " \
                                  "DAY_LONG, SHORT, SHORT_COST, SHORT_AVAIL, DAY_SHORT, PREV_NET) VALUES('%s', %s, '%s', " \
                                  "%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                insert_sql = insert_sql_base % (max_query_date, strategy_id, symbol, pf_position_info[0],
                                                pf_position_info[2], pf_position_info[1], pf_position_info[3]
                                                , pf_position_info[4], pf_position_info[6], pf_position_info[5]
                                                , pf_position_info[7],
                                                float(pf_position_info[0]) - float(pf_position_info[4]))
                print insert_sql
                fr.write(insert_sql + '\n')
        else:
            for symbol in monitor_pf_position_dict[strategy_name].keys():
                if symbol not in origin_pf_position_dict[strategy_name]:
                    pf_position_info = monitor_pf_position_dict[strategy_name][symbol]
                    insert_sql_base = "insert into portfolio.pf_position (DATE, ID, SYMBOL, `LONG`, LONG_COST, LONG_AVAIL, " \
                                      "DAY_LONG, SHORT, SHORT_COST, SHORT_AVAIL, DAY_SHORT, PREV_NET) VALUES('%s', %s, '%s', " \
                                      "%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    insert_sql = insert_sql_base % (max_query_date, strategy_id, symbol, pf_position_info[0],
                                                    pf_position_info[2], pf_position_info[1], pf_position_info[3]
                                                    , pf_position_info[4], pf_position_info[6], pf_position_info[5]
                                                    , pf_position_info[7],
                                                    float(pf_position_info[0]) - float(pf_position_info[4]))
                    print insert_sql
                    fr.write(insert_sql + '\n')
                else:
                    pf_position_info = monitor_pf_position_dict[strategy_name][symbol]
                    update_sql_base = "update portfolio.pf_position set `LONG` = %s, LONG_COST = %s, LONG_AVAIL = %s, " \
                                      "DAY_LONG = %s, SHORT = %s, SHORT_COST = %s, SHORT_AVAIL = %s, DAY_SHORT = %s, " \
                                      "PREV_NET = %s where date = '%s' and ID = %s and symbol = '%s';"
                    update_sql = update_sql_base % (pf_position_info[0], pf_position_info[2], pf_position_info[1],
                                                    pf_position_info[3], pf_position_info[4], pf_position_info[6],
                                                    pf_position_info[5], pf_position_info[7],
                                                    float(pf_position_info[0]) - float(pf_position_info[4]),
                                                    max_query_date, strategy_id, symbol)
                    print update_sql
                    fr.write(update_sql + '\n')
    fr.close()


def insert_trade_list(target_trade_info_list):
    fr = open(csv_folder + 'result.csv', 'a+')
    for trade_info in target_trade_info_list:
        datetime_temp = datetime.datetime.strptime(trade_info[1], '%m-%d-%Y %H:%M:%S')
        datetime_str = datetime.datetime.strftime(datetime_temp, '%Y-%m-%d %H:%M:%S')
        insert_sql_base = "insert into om.trade2_history (ID, TIME, SYMBOL, QTY, PRICE, TRADE_TYPE, STRATEGY_ID, ACCOUNT) " \
                          "VALUES (default, '%s', '%s', %s, %s, 2, '%s', '%s');"
        insert_sql = insert_sql_base % (datetime_str, trade_info[2], trade_info[3], trade_info[4], trade_info[0],
                                        trade_info[5])
        print insert_sql
        fr.write(insert_sql + '\n')
    fr.close()

def get_instrument_val_per_point_dict():
    instrument_val_per_point_dict = dict()
    server_model = ServerConstant().get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    query_sql = "select ticker, FUT_VAL_PT from common.instrument;"
    query_result = session_common.execute(query_sql)
    for query_line in query_result:
        instrument_name = query_line[0]
        val_per_point = query_line[1]
        instrument_val_per_point_dict[instrument_name] = val_per_point
    return instrument_val_per_point_dict


def get_origin_day_long_day_short_dict():
    global max_query_date
    origin_day_long_day_short_dict = dict()
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = "select id, symbol, day_long_cost, day_short_cost from portfolio.pf_position where date = '%s'" \
                % max_query_date
    query_result = session_portfolio.execute(query_sql)
    for query_line in query_result:
        account_id = query_line[0]
        symbol = query_line[1]
        day_long_cost = float(query_line[2])
        day_short_cost = float(query_line[3])
        if account_id not in origin_day_long_day_short_dict:
            origin_day_long_day_short_dict[account_id] = dict()
            origin_day_long_day_short_dict[account_id][symbol] = [day_long_cost, day_short_cost]
        else:
            origin_day_long_day_short_dict[account_id][symbol] = [day_long_cost, day_short_cost]
    return origin_day_long_day_short_dict


def ticker_name_without_exchange_dict():
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('om')

    # get exchange id and name map
    query_sql1 = 'select ID, EXCHANGE from common.exchange;'
    query_result = session_portfolio.execute(query_sql1)
    exchange_id_name_map = dict()
    exchange_id_filter_list = []
    for query_line in query_result:
        exchange_id = query_line[0]
        exchange_name = query_line[1]
        exchange_id_name_map[exchange_id] = exchange_name
        if exchange_name in ['CFF', 'CS', 'CG']:
            exchange_id_filter_list.append(exchange_id)

    query_sql = "select TICKER, EXCHANGE_ID from common.instrument;"
    query_result = session_portfolio.execute(query_sql)
    ticker_full_portfolio_name_map = dict()
    for query_line in query_result:
        ticker_name = query_line[0]
        exchange_id = query_line[1]
        if exchange_id in exchange_id_filter_list:
            ticker_name_portfolio = ticker_name
        else:
            ticker_name_portfolio = ticker_name + ' %s' % exchange_id_name_map[exchange_id]
        ticker_full_name = ticker_name + ' %s' % exchange_id_name_map[exchange_id]
        ticker_full_portfolio_name_map[ticker_full_name] = ticker_name_portfolio
    return ticker_full_portfolio_name_map


def build_pf_position_emergency_job():
    # build ticker name dict with portfolio ticker name
    global ticker_full_portfolio_name_map
    ticker_full_portfolio_name_map = ticker_name_without_exchange_dict()

    # get max date in portfolio
    global max_query_date
    max_query_date = get_max_query_date()

    # get trade list
    target_trade_info_list, target_trade_value_dict = get_trade_info_dict()

    # get monitor pf_position and sql pf_position
    origin_pf_position_dict = get_origin_pf_position(max_query_date)
    monitor_pf_position_dict, monitor_pf_position_value_dict = get_monitor_pf_position()

    # check if trade match pf_position
    check_flag = check_position(target_trade_value_dict, origin_pf_position_dict, monitor_pf_position_value_dict)

    if check_flag:
        # update pf_position and trade list
        update_pf_position(monitor_pf_position_dict, origin_pf_position_dict)
        insert_trade_list(target_trade_info_list)

        # update day long and day short of pf_position
        origin_day_long_day_short_dict = get_origin_day_long_day_short_dict()
        instrument_val_per_point_dict = get_instrument_val_per_point_dict()
        strategy_name_id_dict = get_strategy_name_id_dict()
        for strategy_name in target_trade_value_dict.keys():
            get_day_long_day_short_value(strategy_name, target_trade_info_list, instrument_val_per_point_dict,
                                         strategy_name_id_dict, origin_day_long_day_short_dict, max_query_date)


if __name__ == '__main__':
    build_pf_position_emergency_job()
