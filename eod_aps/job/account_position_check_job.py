# -*- coding: utf-8 -*-
# 对真实仓位和策略仓位进行比较
import traceback
import pandas as pd
import numpy as np
from sqlalchemy.sql import or_
from eod_aps.model.schema_portfolio import RealAccount, PfAccount, PfPosition, AccountPosition
from eod_aps.model.schema_om import Trade2History, TradeBroker
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.job import *

# 过滤掉一些无需展示项，如:204001
filter_ticker_list = ['204001', ]
common_utils = CommonUtils()


def query_real_position_data(session_portfolio):
    account_dict = dict()
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        account_dict[account_db.accountid] = account_db

    query_sql = 'select max(DATE) from portfolio.account_position'
    filter_date_str = session_portfolio.execute(query_sql).first()[0]

    position_list = []
    account_id_list = account_dict.keys()
    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id.in_(tuple(account_id_list), ),
                                             AccountPosition.date == filter_date_str):
        if 'CNY' in position_db.symbol:
            continue
        elif '&' in position_db.symbol:
            continue
        elif position_db.long == 0 and position_db.short == 0:
            continue

        if ' ' in position_db.symbol:
            ticker = position_db.symbol.split(' ')[0]
        else:
            ticker = position_db.symbol

        account_db = account_dict[position_db.id]
        fund_name = account_db.fund_name
        position_list.append([position_db.id, fund_name, ticker, int(position_db.long), int(position_db.short)])
    return filter_date_str, account_dict, position_list


def query_pf_position_data(session_portfolio):
    pf_account_dict = dict()
    query = session_portfolio.query(PfAccount)
    for pf_account_db in query:
        if pf_account_db.fund_name.count('-') != 3:
            continue
        pf_account_dict[pf_account_db.id] = pf_account_db

    query_sql = 'select max(DATE) from portfolio.pf_position'
    pf_date_filter_str = session_portfolio.execute(query_sql).first()[0]

    pf_position_list = []
    pf_account_id_list = pf_account_dict.keys()
    query_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_position.filter(PfPosition.id.in_(tuple(pf_account_id_list), ),
                                                PfPosition.date == pf_date_filter_str):
        if pf_position_db.symbol == 'CNY':
            continue
        elif pf_position_db.long == 0 and pf_position_db.short == 0:
            continue

        if ' ' in pf_position_db.symbol:
            ticker = pf_position_db.symbol.split(' ')[0]
        else:
            ticker = pf_position_db.symbol

        pf_account_db = pf_account_dict[pf_position_db.id]
        fund_name = pf_account_db.fund_name.split('-')[2]
        pf_position_list.append([pf_position_db.id, fund_name, ticker,
                                 int(pf_position_db.long), int(pf_position_db.short)])
    return pf_date_filter_str, pf_account_dict, pf_position_list


def compare_position(real_position_dataframe, pf_position_dataframe):
    real_df = pd.DataFrame(real_position_dataframe, columns=['Account_id', 'Fund', 'Ticker', 'Long', 'Short'])
    pf_df = pd.DataFrame(pf_position_dataframe, columns=['PF_Account_id', 'Fund', 'Ticker', 'PF_Long', 'PF_Short'])

    real_df = real_df[['Fund', 'Ticker', 'Long', 'Short']]
    pf_df = pf_df[['Fund', 'Ticker', 'PF_Long', 'PF_Short']]
    grouped_real_df = real_df.groupby(['Fund', 'Ticker']).sum().reset_index()
    grouped_pf_df = pf_df.groupby(['Fund', 'Ticker']).sum().reset_index()

    merge_df = grouped_real_df.merge(grouped_pf_df, how="outer").fillna(0)
    merge_df['Diff'] = merge_df['Long'] - merge_df['PF_Long'] - (merge_df['Short'] - merge_df['PF_Short'])
    diff_df = merge_df[merge_df['Diff'] != 0]

    for filter_ticker in filter_ticker_list:
        diff_df = diff_df[diff_df['Ticker'] != filter_ticker]

    compare_indexs = ['Fund', 'Ticker', 'Long', 'Short', 'PF_Long', 'PF_Short', 'Diff']
    compare_result_list = np.array(diff_df[compare_indexs]).tolist()
    compare_result_list.sort()
    return compare_result_list


def __build_email_content_list(server_name, position_date, pf_position_date, compare_result_list):
    validate_date = int(date_utils.get_today_str('%Y%m%d'))
    validate_time = int(date_utils.get_today_str('%H%M%S'))

    position_date_int = int(date_utils.datetime_toString(position_date, '%Y%m%d'))
    pf_position_date_int = int(date_utils.datetime_toString(pf_position_date, '%Y%m%d'))

    date_check_flag = False
    if validate_time <= 163000:
        if position_date_int == validate_date and pf_position_date_int == validate_date:
            date_check_flag = True
    elif 163000 < validate_time <= 170000:
        if position_date_int == validate_date and pf_position_date_int >= validate_date:
            date_check_flag = True
    else:
        if position_date_int == validate_date and pf_position_date_int > validate_date:
            date_check_flag = True

    html_content_list = []
    position_date_str = date_utils.datetime_toString(position_date, '%Y-%m-%d')
    pf_position_date_str = date_utils.datetime_toString(pf_position_date, '%Y-%m-%d')
    if date_check_flag:
        email_content = 'server_name[%s]<br/>account_position max date:%s,pf_position max date:%s' \
                        % (server_name, position_date_str, pf_position_date_str)
    else:
        email_content = 'server_name[%s]<br/><font color="red">Error!account_position max date:%s,' \
                        'pf_position max date:%s</font>' % (server_name, position_date_str, pf_position_date)
    html_content_list.insert(0, email_content)

    if compare_result_list:
        html_title = 'Fund,Ticker,Real_Position_Long,Real_Position_Short,' \
                     'Pf_Position_Long,Pf_Position_Short,Diff'
        ticker_list = map(lambda item: item[1], compare_result_list)
        server_host = server_constant.get_server_model('host')
        session_host = server_host.get_db_session('common')
        exist_ticker_list = []
        for obj in session_host.query(Instrument).filter(Instrument.ticker.in_(ticker_list)):
            if obj.type_id not in (const.INSTRUMENT_TYPE_ENUMS.CommonStock, const.INSTRUMENT_TYPE_ENUMS.Future):
                continue
            exist_ticker_list.append(obj.ticker)
        for compare_item in compare_result_list:
            ticker = compare_item[1]
            if ticker in exist_ticker_list:
                compare_item[1] = '%s(Error)' % ticker
        html_table_list = email_utils4.list_to_html(html_title, compare_result_list)
        html_content_list.extend(html_table_list)
    return html_content_list


# 比较真实仓位和策略仓位
def pf_real_position_check(server_name):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    position_date, account_dict, real_position_list = query_real_position_data(session_portfolio)
    pf_position_date, pf_account_dict, pf_position_list = query_pf_position_data(session_portfolio)
    server_model.close()

    # 兼容真实仓位或策略仓位为空的情况，list为空会导致后续判断抛异常
    if not real_position_list:
        real_position_list.append(['0', '0', '0', 0, 0])
    if not pf_position_list:
        pf_position_list.append(['0', '0', '0', 0, 0])

    compare_result_list = compare_position(real_position_list, pf_position_list)
    return position_date, pf_position_date, compare_result_list


# 校验account_position表中是否存在LONG < YD_LONG_REMAIN或者SHORT < YD_SHORT_REMAIN的异常情况
def account_position_check_job(server_name):
    date_filter_str = date_utils.get_today_str('%Y-%m-%d')

    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    accountid_list = []
    query = session_portfolio.query(RealAccount)
    for account_db in query.filter(RealAccount.enable == 1):
        accountid_list.append(account_db.accountid)

    query_position = session_portfolio.query(AccountPosition)
    error_message_list = []
    for position_db in query_position.filter(AccountPosition.date == date_filter_str,
                                             AccountPosition.id.in_(tuple(accountid_list))):
        if position_db.long < position_db.yd_long_remain:
            error_message = 'account:%s, symbol:%s position error!long:%s, yd_long_remain:%s' % \
                            (position_db.id, position_db.symbol, position_db.long, position_db.yd_long_remain)
            error_message_list.append(error_message)
        if position_db.short < position_db.yd_short_remain:
            error_message = 'account:%s, symbol:%s position error!short:%s, yd_short_remain:%s' % \
                            (position_db.id, position_db.symbol, position_db.short, position_db.yd_short_remain)
            error_message_list.append(error_message)

    server_model.close()
    if len(error_message_list) > 0:
        email_utils4.send_email_group_all('[Error]Account Position_%s_%s' % (server_name, date_filter_str),
                                          '\n'.join(error_message_list))
        return False
    return True


def pf_real_trade_check_job(server_name, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')
    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
    start_date = last_trading_day + ' 21:00:00'

    fund_real_trade_dict = dict()
    real_account_fund_name_dict = dict()
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        fund_name = account_db.fund_name
        real_account_fund_name_dict.setdefault(fund_name, []).append(account_db.accountid)

    for (key, account_id_list) in real_account_fund_name_dict.items():
        query_trade = session_om.query(TradeBroker)
        fund_trade_dict = dict()
        for trade_broker_db in query_trade.filter(TradeBroker.account.in_(tuple(account_id_list), ),
                                                  TradeBroker.time >= start_date):
            trade_direction = 'Long' if trade_broker_db.qty > 0 else 'Short'
            trade_key = '%s|%s|%s' % (trade_broker_db.symbol, trade_broker_db.type, trade_direction)
            fund_trade_dict.setdefault(trade_key, []).append(trade_broker_db)

        real_trade_dict = dict()
        for (trade_key, trade_broker_list) in fund_trade_dict.items():
            total_qty = 0
            for trade_broker_db in trade_broker_list:
                total_qty += trade_broker_db.qty
            real_trade_dict[trade_key] = total_qty
        fund_real_trade_dict[key] = real_trade_dict

    fund_pf_trade_dict = dict()
    pf_account_fund_name_dict = dict()
    query_sql = 'select id, fund_name from portfolio.pf_account'
    r = session_portfolio.execute(query_sql)
    for item in r.fetchall():
        pf_account_id = item[0]
        try:
            fund_name = item[1].split('-')[2]
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_job('Error Fund_name:%s, Error_msg:%s' % (item[1], error_msg))
            continue

        if fund_name == '':
            continue
        pf_account_fund_name_dict.setdefault(fund_name, []).append(pf_account_id)

    for (key, account_id_list) in pf_account_fund_name_dict.items():
        query_trade2 = session_om.query(Trade2History)
        fund_trade2_dict = dict()
        for trade2_db in query_trade2.filter(Trade2History.account.like('%' + key + '%'),
                                             Trade2History.self_cross == 0,
                                             Trade2History.time >= start_date):
            if trade2_db.trade_type == 2:
                trade_type = 'OPEN'
            elif trade2_db.trade_type == 3:
                trade_type = 'CLOSE'
            elif trade2_db.trade_type == 4:
                trade_type = 'CLOSE_YESTERDAY'
            else:
                custom_log.log_error_job('Error Trade_Type:%s', trade2_db.trade_type)
                continue

            trade_direction = 'Long' if trade2_db.qty > 0 else 'Short'
            trade_key = '%s|%s|%s' % (trade2_db.symbol.split(' ')[0], trade_type, trade_direction)
            fund_trade2_dict.setdefault(trade_key, []).append(trade2_db)

        pf_trade_dict = dict()
        for (trade_key, trade_broker_list) in fund_trade2_dict.items():
            total_qty = 0
            for trade_broker_db in trade_broker_list:
                total_qty += trade_broker_db.qty
            pf_trade_dict[trade_key] = total_qty
        fund_pf_trade_dict[key] = pf_trade_dict

    result_email_message = []
    for (fund_name, real_trade_dict) in fund_real_trade_dict.items():
        pf_trade_dict = fund_pf_trade_dict[fund_name]
        trade_key_set = set()
        trade_key_set.update(real_trade_dict.keys())
        trade_key_set.update(pf_trade_dict.keys())
        for trade_key in trade_key_set:
            real_qty = real_trade_dict[trade_key] if trade_key in real_trade_dict else 0
            pf_qty = pf_trade_dict[trade_key] if trade_key in pf_trade_dict else 0
            if real_qty != pf_qty:
                result_email_message.append([fund_name, trade_key, real_qty, pf_qty])
    result_email_message.sort()

    if len(result_email_message) > 0:
        html_title = 'FUND_NAME,KEY,Exchange_QTY,ATP_QTY'
        html_list = email_utils4.list_to_html(html_title, result_email_message)
        email_utils4.send_email_group_all('[Error]Exchange And ATP Trade Compare_%s_%s' %
                                          (server_name, date_filter_str), ''.join(html_list), 'html')
    server_model.close()


def pf_real_position_check_job(server_name_tuple):
    check_message_list = []
    for server_name in server_name_tuple:
        position_date, pf_position_date, compare_result_list = pf_real_position_check(server_name)
        server_email_content = __build_email_content_list(server_name, position_date,
                                                          pf_position_date, compare_result_list)
        check_message_list.extend(server_email_content)
        check_message_list.append('<br/><br/><hr/>')
    email_utils4.send_email_group_all('Position Check Report', ''.join(check_message_list), 'html')


# 检查当前有持仓的策略仓位是否界面显示
def pf_account_check(server_name_tuple):
    server_name_dict = dict()
    if 'risk_dict' not in const.EOD_POOL:
        return

    for (strategy_name, strategy_risk_dict) in const.EOD_POOL['risk_dict'].items():
        (base_strategy_name, server_ip_str) = strategy_name.split('@')
        server_name = common_utils.get_server_name(server_ip_str)
        server_name_dict.setdefault(server_name, []).append(base_strategy_name)

    error_account_message = []
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    for server_name in server_name_tuple:
        if server_name not in server_name_dict:
            continue

        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        view_account_list = server_name_dict[server_name]
        for query_item in session_portfolio.query(PfAccount.fund_name) \
                .join(PfPosition, PfPosition.id == PfAccount.id).filter(PfPosition.date == filter_date_str) \
                .filter(or_(PfPosition.long > 0, PfPosition.short > 0)).group_by(PfAccount.fund_name):
            if query_item[0] not in view_account_list:
                error_account_message.append('%s,%s' % (server_name, query_item[0]))

    if server_name_dict and len(error_account_message) > 0:
        email_utils4.send_email_group_all('[Error]Pf Account Status Error!', '<br>'.join(error_account_message), 'html')


if __name__ == '__main__':
    all_trade_servers_list = server_constant.get_all_trade_servers()
    pf_real_position_check_job(all_trade_servers_list)
