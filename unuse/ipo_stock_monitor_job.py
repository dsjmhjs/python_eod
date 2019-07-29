# -*- coding: cp936 -*-
import sys
import datetime
from eod_aps.job import *
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.wind_local_tools import w_ys, w_ys_close

reload(sys)
sys.setdefaultencoding('utf-8')

server_constant = ServerConstant()
email_utils = EmailUtils(EmailUtils.group8)


def __wind_login():
    global w
    w = w_ys()


def __wind_close():
    w_ys_close()


def ipo_stock_monitor():
    __wind_close()
    __wind_login()
    stock_server_list = server_constant.get_stock_servers()
    email_list = []
    for server_name in stock_server_list:
        print server_name

        server_model = ServerConstant().get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')

        # query ipo id
        query_sql1 = "select `ID` from portfolio.pf_account where FUND_NAME like 'container-Iposub%'"
        query_result1 = session_portfolio.execute(query_sql1)
        ipo_account_list = []
        for query_line in query_result1:
            ipo_account_list.append(str(query_line[0]))

        if len(ipo_account_list) == 0:
            continue

        # query max date
        query_sql2 = "select MAX(DATE) from portfolio.pf_position;"
        query_result2 = session_portfolio.execute(query_sql2)
        max_date = datetime.datetime.now().strftime('%Y-%m-%d')
        for query_line in query_result2:
            max_date = datetime.datetime.strftime(query_line[0], '%Y-%m-%d')

        # query ipo id
        query_sql3 = "select `SYMBOL` from portfolio.pf_position where id in (%s) and date = '%s';" % \
                     (','.join(ipo_account_list), max_date)
        query_result3 = session_portfolio.execute(query_sql3)
        ipo_id_list = []
        for query_line in query_result3:
            ipo_id_list.append(query_line[0])

        # rebuild stock ticker
        query_sql4 = "select ticker, exchange_id from common.instrument where ticker in (%s) and type_id = 4" % ','.join(ipo_id_list)
        query_result4 = session_portfolio.execute(query_sql4)
        rebuild_ipo_id_list = []
        for query_line in query_result4:
            if query_line[1] == 18:
                rebuild_ipo_id_list.append(query_line[0] + '.SH')
            elif query_line[1] == 19:
                rebuild_ipo_id_list.append(query_line[0] + '.SZ')
            else:
                continue

        # rebuild date
        date_str_1 = date_utils.get_last_trading_day('%Y-%m-%d', max_date)
        date_str_2 = date_utils.get_last_trading_day('%Y-%m-%d', date_str_1)

        # get wind price
        wind_price_dict = dict()
        wind_data = w.wsd(rebuild_ipo_id_list, "close", date_str_2, date_str_1, "")
        for i in range(len(wind_data['Codes'])):
            wind_price_dict[wind_data['Codes'][i]] = wind_data['Data'][i]

        # build table
        html_title = 'Ticker,Close_%s,Close_%s' % (date_str_2, date_str_1)
        table_list = []
        for (wind_ticker, wind_price_list) in sorted(wind_price_dict.items()):
            tr_list = []
            if wind_price_list[1] / wind_price_list[0] - 1 > 0.095:
                tr_list.append(str(wind_ticker))
                tr_list.append(str(wind_price_list[0]))
                tr_list.append(str(wind_price_list[1]))
            else:
                tr_list.append(str(wind_ticker))
                tr_list.append(str(wind_price_list[0]) + '(Error)')
                tr_list.append(str(wind_price_list[1]) + '(Error)')
            table_list.append(tr_list)
        html_list = email_utils.list_to_html(html_title, table_list)
        email_list.append(server_name + ':<br>')
        email_list.append(''.join(html_list))
        server_model.close()

    email_utils.send_email_group_all(unicode('打新股票开板校验', 'gbk'), '\n'.join(email_list), 'html')
    __wind_close()


if __name__ == '__main__':
    ipo_stock_monitor()

