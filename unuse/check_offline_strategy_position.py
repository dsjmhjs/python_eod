# -*- coding: utf-8 -*-
from eod_aps.job import *

date_utils = DateUtils()
server_constant = ServerConstant()

server_name_list = ['nanhua_web', 'zhongxin']

def get_target_date():
    today_str = date_utils.get_today_str('%Y-%m-%d')
    if date_utils.is_trading_day(today_str):
        target_date_str = date_utils.get_next_trading_day('%Y-%m-%d')
        return target_date_str
    else:
        return today_str


def check_offline_strategy_position():
    date_str = get_target_date()
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    query_sql = 'select `NAME` from strategy.strategy_online where strategy_type = "CTA" and `ENABLE` = 0;'
    query_result = session_strategy.execute(query_sql)
    for query_line in query_result:
        strategy_name = query_line[0]
        print strategy_name
        for server_name in server_name_list:
            server_model_server = server_constant.get_server_model(server_name)
            session_portfolio = server_model_server.get_db_session('portfolio')
            query_sql2 = 'select id from portfolio.pf_account where group_name = "%s" and `NAME` = "%s";' \
                         % (strategy_name.split('.')[0], strategy_name.split('.')[1])
            query_result2 = session_portfolio.execute(query_sql2)
            id_list = []
            id_flag = False
            for query_line2 in query_result2:
                id_list.append(str(query_line2[0]))
                id_flag = True

            if id_flag:
                query_sql3 = 'select id, `LONG`, `SHORT` from portfolio.pf_position where id in (%s) and date = "%s";' \
                         % (','.join(id_list), date_str)
                query_result3 = session_portfolio.execute(query_sql3)
                for query_line3 in query_result3:
                    if query_line3[1] - query_line3[2] != 0:
                        print 'error!'
                        print query_line3[0], query_line3[1], query_line3[2]
            server_model_server.close()

if __name__ == '__main__':
    check_offline_strategy_position()