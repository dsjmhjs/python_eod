# -*- coding: utf-8 -*-
# 检查是否存在今日复牌股票
import codecs
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.tools.stock_wind_utils import StockWindUtils
from eod_aps.job import *


stock_utils = StockUtils()


def special_ticker_report_job(server_list):
    html_list = []
    # 今日复牌股票信息
    resume_html_list = query_resume_ticker_info(server_list)
    html_list.extend(resume_html_list)
    html_list.append('<hr/>')

    # IPO股票信息
    ipo_html_list = query_ipo_ticker_info(server_list)
    html_list.extend(ipo_html_list)
    title_str = u'今日需关注股票信息_%s' % date_utils.get_today_str('%Y-%m-%d')
    email_utils9.send_email_group_all(title_str, ''.join(html_list), 'html')


def query_resume_ticker_info(server_list):
    resume_message_list = []
    resume_stock_list = stock_utils.get_resume_stocks()
    if len(resume_stock_list) == 0:
        return []

    resume_stock_list.sort()
    ticker_dict = stock_utils.get_ticker_dict()

    html_list = [u'<li>今日复牌股票列表:</li>']
    for stock_info in resume_stock_list:
        ticker_name = ticker_dict[stock_info][0]
        resume_message_list.append((stock_info, ticker_name))
    html_list.extend(email_utils11.list_to_html('Ticker,Name', resume_message_list))

    html_list.append(u'<br><br><li>今日复牌股票持仓情况:</li>')
    position_message_list = []
    for server_name in server_list:
        position_message_list.extend(__pf_position_check(server_name, resume_stock_list))
    html_list.extend(email_utils11.list_to_html('ServerName,FundName,Ticker,Qty', position_message_list))
    return html_list


def query_ipo_ticker_info(server_list):
    yzz_stock_list = stock_utils.get_yzz_stocks()
    new_stock_list = stock_utils.get_first_trading_stocks()

    date_str = date_utils.get_today_str('%Y-%m-%d')
    last_date_str = date_utils.get_last_trading_day('%Y-%m-%d')

    html_list = [u'<br><br><li>IPO股票列表(红色为打开涨停板):</li>']
    ipo_message_list = []
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')

        query_sql = "select `SYMBOL`,`LONG` from portfolio.pf_position where id in (select id from portfolio.pf_account \
    where FUND_NAME like '%s') and date = '%s'" % ('container-Iposub%', date_str)
        query_result = session_portfolio.execute(query_sql)
        for result_item in query_result:
            ticker = result_item[0]
            volume = result_item[1]
            if ticker.startswith('0') or ticker.startswith('3') or ticker.startswith('6'):
                if ticker in yzz_stock_list:
                    ipo_message_list.append([server_name, ticker, str(int(volume))])
                elif ticker in new_stock_list:
                    ipo_message_list.append([server_name, ticker, str(int(volume))])
                else:
                    prev_close = stock_utils.get_prev_close(last_date_str, ticker)
                    if prev_close is not None and float(prev_close) > 0:
                        ipo_message_list.append([server_name, ticker + '(Error)', str(int(volume)) + '(Error)'])
                    else:
                        ipo_message_list.append([server_name, ticker, str(int(volume))])
            else:
                ipo_message_list.append([server_name, ticker, str(int(volume))])

    html_list.extend(email_utils11.list_to_html('Server,Ticker,Volume', ipo_message_list))
    return html_list


def __pf_position_check(server_name, stock_list):
    position_message_list = []
    server_model = server_constant.get_server_model(server_name)
    pf_position_dict = __query_pf_position(server_model, stock_list)
    if len(pf_position_dict) == 0:
        return position_message_list

    pf_account_dict = __query_pf_account_dict(server_model)
    for (account_id, pf_position_list) in pf_position_dict.items():
        pf_account_db = pf_account_dict[account_id]
        for pf_position_db in pf_position_list:
            position_message_list.append((server_name, pf_account_db.fund_name,
                                          pf_position_db.symbol, -int(pf_position_db.long)))
    server_model.close()
    return position_message_list


def __query_pf_account_dict(server_model):
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    pf_account_dict = dict()
    for pf_account_db in query_pf_account:
        pf_account_dict[pf_account_db.id] = pf_account_db
    return pf_account_dict


def __query_pf_position(server_model, stock_list):
    now_date_str = date_utils.get_today_str('%Y-%m-%d')

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)

    pf_position_dict = dict()
    for pf_position_db in query_pf_position.filter(PfPosition.date == now_date_str,
                                                   PfPosition.symbol.in_(tuple(stock_list), )):
        if pf_position_db.id in pf_position_dict:
            pf_position_dict[pf_position_db.id].append(pf_position_db)
        else:
            pf_position_dict[pf_position_db.id] = [pf_position_db]
    return pf_position_dict


if __name__ == '__main__':
    pass
