# coding=utf-8

import json
from eod_aps.job import *


def get_ticker_future_name(ticker_name):
    future_name = ''
    for i in ticker_name.split(' ')[0]:
        if i.isalpha():
            future_name += i
    return future_name


def build_calendarma_transfer_parameter_job(server_name_list):
    email_error_str = ''
    error_flag = False
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_strategy = server_model.get_db_session('portfolio')

        # get cta real account
        query_sql = "select distinct(Fund_name) from portfolio.real_account where accounttype = 'CTP' \
and allow_targets like '%shf%'"
        query_result = session_strategy.execute(query_sql)
        cta_account_name_list = []
        for query_line in query_result:
            cta_account_name_list.append(query_line[0])

        # get calendarma transfer pf_account id assemble
        calendarma_transfer_id_list = []
        for cta_acccount_name in cta_account_name_list:
            query_sql2 = "select id from portfolio.pf_account where `group_name` = 'CalendarMA' and \
`name` = 'transfer' and `fund_name` like '%s'" % ('%' + cta_acccount_name + '%')
            query_result2 = session_strategy.execute(query_sql2)
            for query_line in query_result2:
                calendarma_transfer_id_list.append(query_line[0])

        # get pf_position max date
        max_date = DateUtils().get_next_trading_day()
        query_sql3 = "select max(date) from portfolio.pf_position;"
        query_result3 = session_strategy.execute(query_sql3)
        for query_line in query_result3:
            max_date = query_line[0]

        # get future-ticker assembly
        server_future_ticker_dict = dict()
        for calendarma_transfer_id in calendarma_transfer_id_list:
            future_ticker_dict = dict()
            query_sql4 = "select `symbol`, `long`, `short` from portfolio.pf_position where id = %s and date = '%s'"\
                         % (calendarma_transfer_id, max_date)
            query_result4 = session_strategy.execute(query_sql4)
            for query_line in query_result4:
                future_name = get_ticker_future_name(query_line[0])
                ticker_name = query_line[0].split(' ')[0]
                ticker_position = query_line[1] - query_line[2]

                if float(ticker_position) == 0:
                    continue

                future_ticker_dict.setdefault(future_name, []).append([ticker_name, ticker_position])

                if future_name not in server_future_ticker_dict:
                    server_future_ticker_dict[future_name] = [ticker_name, ]
                else:
                    if ticker_name not in server_future_ticker_dict[future_name]:
                        server_future_ticker_dict[future_name].append(ticker_name)

            # check ticker, posiiton
            for [future_name, ticker_info] in sorted(future_ticker_dict.items()):
                if len(ticker_info) != 2:
                    error_flag = True
                    email_error_str += u'服务器:%s --ID:%s --期货:%s --需要平仓标的数量不为2，无法用Calendar平仓，请手动处理!' % \
                                       (server_name, calendarma_transfer_id, future_name)
                    continue

                if ticker_info[0][1] + ticker_info[1][1] != 0:
                    error_flag = True
                    email_error_str += u'服务器:%s -- ID:%s -- 期货:%s -- 前后主力合约仓位不匹配，无法用Calendar平仓，请手动处理!' % \
                                               (server_name, calendarma_transfer_id, future_name)
                    continue

        # get parameter future assemble
        query_sql5 = "select `value` from strategy.strategy_parameter where `name` = 'CalendarMA.Transfer'" \
                     " order by time desc limit 1"
        query_result5 = session_strategy.execute(query_sql5)
        strategy_parameter_dict = dict()
        for query_line in query_result5:
            strategy_parameter_str = query_line[0]
            strategy_parameter_dict = json.loads(strategy_parameter_str)
        future_name_list = []
        for parameter_key in sorted(strategy_parameter_dict.keys()):
            if '.' in parameter_key:
                future_name = parameter_key.split('.')[0]
                if future_name not in future_name_list and future_name != '000':
                    future_name_list.append(future_name)

        # build account
        strategy_parameter_dict['Account'] = ';'.join(cta_account_name_list)

        # build parameter dict
        for future_name in future_name_list:
            if future_name not in server_future_ticker_dict:
                strategy_parameter_dict['%s.BackFuture' % future_name] = ''
                strategy_parameter_dict['%s.FrontFuture' % future_name] = ''
                strategy_parameter_dict['%s.TickBuffer' % future_name] = 1
                strategy_parameter_dict['%s.closeL' % future_name] = 1
                strategy_parameter_dict['%s.enable' % future_name] = 0
                strategy_parameter_dict['%s.mean_window_size' % future_name] = 1500
                strategy_parameter_dict['%s.openL' % future_name] = 100
                strategy_parameter_dict['%s.profit_tick_num' % future_name] = 5
                strategy_parameter_dict['%s.slippage' % future_name] = 5
                strategy_parameter_dict['%s.var_window_size' % future_name] = 5
                for cta_acccount_name in cta_account_name_list:
                    strategy_parameter_dict['%s.tq.%s.max_open' % (future_name, cta_acccount_name)] = 0
                    strategy_parameter_dict['%s.tq.%s.max_outstanding' % (future_name, cta_acccount_name)] = 1
                    strategy_parameter_dict['%s.tq.%s.qty_per_trade' % (future_name, cta_acccount_name)] = 5
            else:
                if len(server_future_ticker_dict[future_name]) != 2:
                    error_flag = True
                    email_error_str += u'服务器:%s -- 期货:%s -- 换月标的数量不为2，无法用Calendar平仓，请手动处理！' % \
                                       (server_name, future_name)
                    strategy_parameter_dict['%s.BackFuture' % future_name] = ''
                    strategy_parameter_dict['%s.FrontFuture' % future_name] = ''
                    strategy_parameter_dict['%s.TickBuffer' % future_name] = 1
                    strategy_parameter_dict['%s.closeL' % future_name] = 1
                    strategy_parameter_dict['%s.enable' % future_name] = 0
                    strategy_parameter_dict['%s.mean_window_size' % future_name] = 1500
                    strategy_parameter_dict['%s.openL' % future_name] = 100
                    strategy_parameter_dict['%s.profit_tick_num' % future_name] = 5
                    strategy_parameter_dict['%s.slippage' % future_name] = 5
                    strategy_parameter_dict['%s.var_window_size' % future_name] = 5
                    for cta_acccount_name in cta_account_name_list:
                        strategy_parameter_dict['%s.tq.%s.max_open' % (future_name, cta_acccount_name)] = 0
                        strategy_parameter_dict['%s.tq.%s.max_outstanding' % (future_name, cta_acccount_name)] = 1
                        strategy_parameter_dict['%s.tq.%s.qty_per_trade' % (future_name, cta_acccount_name)] = 5
                else:
                    ticker_list = list()
                    ticker_list.append(server_future_ticker_dict[future_name][0])
                    ticker_list.append(server_future_ticker_dict[future_name][1])
                    ticker_list = sorted(ticker_list)
                    strategy_parameter_dict['%s.BackFuture' % future_name] = ticker_list[1]
                    strategy_parameter_dict['%s.FrontFuture' % future_name] = ticker_list[0]
                    strategy_parameter_dict['%s.TickBuffer' % future_name] = 1
                    strategy_parameter_dict['%s.closeL' % future_name] = 1
                    strategy_parameter_dict['%s.enable' % future_name] = 1
                    strategy_parameter_dict['%s.mean_window_size' % future_name] = 1500
                    strategy_parameter_dict['%s.openL' % future_name] = 100
                    strategy_parameter_dict['%s.profit_tick_num' % future_name] = 5
                    strategy_parameter_dict['%s.slippage' % future_name] = 5
                    strategy_parameter_dict['%s.var_window_size' % future_name] = 1500
                    for cta_acccount_name in cta_account_name_list:
                        strategy_parameter_dict['%s.tq.%s.max_open' % (future_name, cta_acccount_name)] = 0
                        strategy_parameter_dict['%s.tq.%s.max_outstanding' % (future_name, cta_acccount_name)] = 1
                        if future_name in ['IC', 'IF', 'IH']:
                            strategy_parameter_dict['%s.tq.%s.qty_per_trade' % (future_name, cta_acccount_name)] = 1
                        else:
                            strategy_parameter_dict['%s.tq.%s.qty_per_trade' % (future_name, cta_acccount_name)] = 5

        # insert strategy parameter sql
        new_strategy_parameter_str = json.dumps(strategy_parameter_dict)
        parameter_insert_sql = "insert into strategy.strategy_parameter (`TIME`, `NAME`, `VALUE`) VALUES(sysdate(), \
'CalendarMA.Transfer', '%s')" % new_strategy_parameter_str
        session_strategy.execute(parameter_insert_sql)
        session_strategy.commit()
        server_model.close()

    if error_flag:
        email_utils4.send_email_group_all(unicode('Calendar换月平仓报错', 'utf-8'), email_error_str, 'html')


if __name__ == "__main__":
    cta_server_list = server_constant.get_cta_servers()
    build_calendarma_transfer_parameter_job(cta_server_list)
