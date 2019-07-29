# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import datetime
from eod_aps.model.schema_portfolio import RealAccount
from eod_aps.model.schema_history import ServerRisk
from eod_aps.job import *
from eod_aps.model.schema_strategy import StrategyGrouping


def __query_account_list(server_list):
    account_set = set()
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        for result_item in session_portfolio.query(RealAccount.fund_name).group_by(RealAccount.fund_name):
            account_set.add(result_item[0])
        server_model.close()
    account_list = list(account_set)
    account_list.sort()
    return account_list


def __query_strategy_list(strategy_type):
    strategy_list = []
    cross_strategy_list = []
    if strategy_type == 'stock':
        # session_jobs = server_host.get_db_session('jobs')
        # query_sql = 'select group_name, strategy_name from strategyaccount_info group by strategy_name'
        # for result_item in session_jobs.execute(query_sql):
        #     strategy_list.append('%s|%s' % (result_item[0], result_item[1]))
        pass
    elif strategy_type == 'cta':
        session_history = server_host.get_db_session('strategy')
        query_sql = 'select strategy_name from strategy_online where `enable` = 1 group by strategy_name'
        for result_item in session_history.execute(query_sql):
            strategy_list.append(result_item[0])
    else:
        raise Exception("Error strategy_type:%s" % strategy_type)
    strategy_list.sort()

    if strategy_type == 'stock':
        cross_strategy_list.append('MarketMaking1')
        cross_strategy_list.append('PutCallParity')
        cross_strategy_list.append('Covered_Call')
        cross_strategy_list.append('FundaMental')
        cross_strategy_list.append('manual')
        cross_strategy_list.append('StkIntraDayStrategy')
        cross_strategy_list.append('FutIntraDayStrategy')
        cross_strategy_list.append('MultiFactor')
    return strategy_list, cross_strategy_list


def __get_strategy_type(strategy_grouping_dict, base_type, strategy_name):
    strategy_type = None
    if strategy_name in strategy_grouping_dict:
        if base_type == 'stock':
            strategy_type = strategy_grouping_dict[strategy_name].group_name
        elif base_type == 'cta':
            strategy_type = strategy_grouping_dict[strategy_name].sub_name
    return strategy_type


def __server_risk_report(server_list, base_type, start_date, end_date):
    account_list = __query_account_list(server_list)
    strategy_list, cross_strategy_list = __query_strategy_list(base_type)

    session_history = server_host.get_db_session('history')
    server_risk_db_list = []
    for server_risk_db in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
        server_risk_db_list.append(server_risk_db)

    session_strategy = server_host.get_db_session('strategy')
    strategy_grouping_dict = dict()
    for strategy_grouping_db in session_strategy.query(StrategyGrouping):
        strategy_grouping_dict[strategy_grouping_db.strategy_name] = strategy_grouping_db

    table_list = []
    for strategy_name in strategy_list:
        if base_type == 'stock':
            strategy_type = __get_strategy_type(strategy_grouping_dict, base_type, strategy_name.split('|')[0])
            strategy_name = strategy_name.split('|')[1]
        elif base_type == 'cta':
            strategy_type = __get_strategy_type(strategy_grouping_dict, base_type, strategy_name)
        row_list = [strategy_type, strategy_name]
        for account_name in account_list:
            row_item_value = 0.0
            for server_risk_db in server_risk_db_list:
                # 过滤掉非本服务器的
                if server_risk_db.server_name not in server_list:
                    continue

                strategy_name_item = server_risk_db.strategy_name.split('-')
                if base_type == 'stock':
                    account_name_db = strategy_name_item[2]
                    if strategy_name not in server_risk_db.strategy_name or account_name != account_name_db:
                        continue
                    row_item_value += server_risk_db.total_pl
                elif base_type == 'cta' and strategy_name == 'default':
                    account_name_db = strategy_name_item[2]
                    if strategy_name not in server_risk_db.strategy_name or account_name != account_name_db:
                        continue
                    row_item_value += server_risk_db.total_pl
                elif base_type == 'cta':
                    strategy_name_db = strategy_name_item[1]
                    account_name_db = strategy_name_item[2]
                    if strategy_name != strategy_name_db or account_name != account_name_db:
                        continue
                    row_item_value += server_risk_db.total_pl
            row_list.append(int(row_item_value))
        table_list.append(row_list)

    for strategy_name in cross_strategy_list:
        strategy_type = __get_strategy_type(strategy_grouping_dict, base_type, strategy_name)
        row_list = [strategy_type, strategy_name]
        for account_name in account_list:
            row_item_value = 0.0
            for server_risk_db in server_risk_db_list:
                strategy_name_item = server_risk_db.strategy_name.split('-')
                if base_type == 'stock':
                    account_name_db = strategy_name_item[2]
                    if strategy_name not in server_risk_db.strategy_name or account_name != account_name_db:
                        continue
                    row_item_value += server_risk_db.total_pl
                elif base_type == 'cta':
                    strategy_name_db = strategy_name_item[1]
                    account_name_db = strategy_name_item[2]
                    if strategy_name != strategy_name_db or account_name != account_name_db:
                        continue
                    row_item_value += server_risk_db.total_pl
            row_list.append(int(row_item_value))
        table_list.append(row_list)

    risk_total_df = pd.DataFrame(table_list, columns=['Type', 'S\A'] + account_list)
    temp = risk_total_df[['Type', 'S\A']]
    del risk_total_df['Type']
    del risk_total_df['S\A']
    risk_total_df['Total'] = risk_total_df.apply(lambda x: x.sum(), axis=1)
    risk_total_df.loc['Total'] = risk_total_df.sum()
    for col in risk_total_df.columns.values:
        risk_total_df[col] = risk_total_df[col].apply(lambda x: '{:,}'.format(int(x)))
    risk_total_df = pd.merge(temp, risk_total_df, left_index=True, right_index=True, how='right')
    risk_total_df['Type'].iloc[-1] = 'Z_Total'
    risk_total_df['S\A'].iloc[-1] = 'Total'
    train_x_list = np.array(risk_total_df).tolist()
    train_x_list.sort()
    title = 'Type,S/A,%s,Total' % ','.join(account_list)
    return ''.join(email_utils8.list_to_html2(title, train_x_list))


def server_risk_report_job(start_date, end_date):
    base_info = '<div>Date:%s --> %s</div>' % (start_date, end_date)
    all_trade_servers = server_constant.get_all_trade_servers()
    html_info1 = __server_risk_report(all_trade_servers, 'stock', start_date, end_date)

    # cta_servers = server_constant.get_cta_servers()
    html_info2 = __server_risk_report(all_trade_servers, 'cta', start_date, end_date)

    aggregate_email_list = base_info + html_info1 + html_info2
    aggregate_email_list = aggregate_email_list.replace('<th align="center"', '<th align="center" width = "160" ')

    cta_email_list = base_info + html_info2
    cta_email_list = cta_email_list.replace('<th align="center"', '<th align="center" width = "160" ')
    return aggregate_email_list, cta_email_list


def server_risk_report_daily_job(start_date=None):
    global server_host
    server_host = server_constant.get_server_model('host')

    if start_date is None:
        start_date = date_utils.get_today_str('%Y-%m-%d')
    end_date = start_date
    aggregate_email_list, cta_email_list = server_risk_report_job(start_date, end_date)
    server_host.close()

    email_utils8.send_email_group_all('Aggregate P&L Report_Daily_%s' % start_date, aggregate_email_list, 'html')
    email_utils12.send_email_group_all('CTA P&L Report_Daily_%s' % start_date, cta_email_list, 'html')


def server_risk_report_week_job():
    global server_host
    server_host = server_constant.get_server_model('host')
    now_date = date_utils.get_now()
    trading_day_list = date_utils.get_interval_trading_day_list(now_date, -5, '%Y-%m-%d')
    start_date = trading_day_list[-1]
    end_date = trading_day_list[0]

    aggregate_email_list, cta_email_list = server_risk_report_job(start_date, end_date)
    server_host.close()

    email_utils8.send_email_group_all('Aggregate P&L Report_Weekly', aggregate_email_list, 'html')
    email_utils12.send_email_group_all('CTA P&L Report_Weekly', cta_email_list, 'html')


def server_risk_report_month_job():
    global server_host
    server_host = server_constant.get_server_model('host')

    today_date = date_utils.get_today()
    start_date = datetime.date(year=today_date.year, month=today_date.month, day=1).strftime('%Y-%m-%d')
    end_date = today_date.strftime('%Y-%m-%d')

    aggregate_email_list, cta_email_list = server_risk_report_job(start_date, end_date)
    server_host.close()
    email_utils8.send_email_group_all('Aggregate P&L Report_Month', aggregate_email_list, 'html')


def server_risk_report_quarter_job():
    global server_host
    server_host = server_constant.get_server_model('host')

    start_date = '2019-01-01'
    end_date = '2019-03-29'

    aggregate_email_list, cta_email_list = server_risk_report_job(start_date, end_date)
    server_host.close()
    email_utils8.send_email_group_all('Aggregate P&L Report_Quarter', aggregate_email_list, 'html')


def export_risk_history():
    user_token = 'admin'
    search_date_item = ['2018-03-01', '2018-03-05']
    server_name = None
    fund_name = None
    strategy_name = 'Earning'

    if search_date_item:
        [start_date, end_date] = search_date_item
        start_date = date_utils.get_last_trading_day('%Y-%m-%d', start_date[:10])
        end_date = end_date[:10]
    else:
        start_date = date_utils.get_today_str('%Y-%m-%d')
        end_date = start_date

    user_id = user_token.split('|')[0]
    query_sql = "select pf_account_list from jobs.user_list where user_id='%s'" % user_id
    server_model = server_constant.get_server_model('host')
    session_jobs = server_model.get_db_session('jobs')
    pf_account_list_str = session_jobs.execute(query_sql).first()[0]
    filter_pf_account_list = pf_account_list_str.split(',')

    index_return_rate_list = []
    query_sql = "select date, RETURN_RATE from jobs.daily_return_history where ticker = 'SH000905' and date >= '%s' \
    and date <= '%s'" % (start_date, end_date)
    session_jobs = server_model.get_db_session('jobs')
    for query_result_item in session_jobs.execute(query_sql):
        date_str = date_utils.datetime_toString(query_result_item[0], '%Y-%m-%d')
        index_return_rate_list.append([date_str, query_result_item[1]])
    index_return_rate_df = pd.DataFrame(index_return_rate_list, columns=["Date", "Index_Rate"])

    server_risk_db_list = []
    server_host = server_constant.get_server_model('host')
    session_history = server_host.get_db_session('history')
    for server_risk_db in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
        if server_name and server_name != server_risk_db.server_name:
            continue
        if fund_name and fund_name not in server_risk_db.strategy_name:
            continue
        if strategy_name and strategy_name not in server_risk_db.strategy_name:
            continue

        strategy_name_item = server_risk_db.strategy_name.split('-')
        if filter_pf_account_list and strategy_name_item[1] not in filter_pf_account_list:
            continue

        item_list = [date_utils.datetime_toString(server_risk_db.date, '%Y-%m-%d'), server_risk_db.total_stocks_value,
                     abs(server_risk_db.total_future_value), server_risk_db.position_pl, server_risk_db.future_pl,
                     server_risk_db.stocks_pl, server_risk_db.total_pl]
        server_risk_db_list.append(item_list)

    risk_view_df = pd.DataFrame(server_risk_db_list, columns=["Date", "Total_Stocks_Value", "Total_Future_Value",
                                                              "Position_PL", "Stocks_PL", "Future_PL", "Total_PL"])

    groupby_df1 = risk_view_df.groupby("Date").sum()[["Total_Stocks_Value", "Total_Future_Value", "Position_PL",
                                                      "Stocks_PL", "Future_PL", "Total_PL"]]
    groupby_df1['Date'] = groupby_df1.index.values
    groupby_df1.index = range(len(groupby_df1))
    groupby_df1 = pd.merge(groupby_df1, index_return_rate_df, on=['Date'], how='left')

    groupby_df1['Pre_Stocks_Value'] = groupby_df1.Total_Stocks_Value.shift(1)
    groupby_df1 = groupby_df1.sort_values('Date', ascending=True)
    groupby_df1 = groupby_df1.drop(0)

    groupby_df1['Alpha_Value'] = groupby_df1.Position_PL / groupby_df1.Pre_Stocks_Value - groupby_df1.Index_Rate
    groupby_df1['Net_Rate'] = groupby_df1.Total_PL / groupby_df1.Pre_Stocks_Value
    groupby_df1.index = groupby_df1.Date

    del groupby_df1['Position_PL']
    del groupby_df1['Total_Future_Value']
    groupby_df1 = groupby_df1.fillna(0)

    groupby_df1['Alpha_Value'] = groupby_df1['Alpha_Value'].apply(lambda x: '%.4f%%' % x)
    groupby_df1['Net_Rate'] = groupby_df1['Net_Rate'].apply(lambda x: '%.4f%%' % x)
    groupby_df1['Index_Rate'] = groupby_df1['Index_Rate'].apply(lambda x: '%.4f%%' % x)

    groupby_df1['Pre_Stocks_Value'] = groupby_df1['Pre_Stocks_Value'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Total_Stocks_Value'] = groupby_df1['Total_Stocks_Value'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Stocks_PL'] = groupby_df1['Stocks_PL'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Future_PL'] = groupby_df1['Future_PL'].apply(lambda x: '{:,}'.format(x))
    groupby_df1['Total_PL'] = groupby_df1['Total_PL'].apply(lambda x: '{:,}'.format(x))

    return_data_list = []
    return_data_dict = groupby_df1.to_dict("index")
    for (dict_key, dict_value) in return_data_dict.items():
        return_data_list.append(dict_value)
    return_data_list.sort(key=lambda obj: obj['Date'])
    query_result = {'data_list': return_data_list}
    print query_result


if __name__ == '__main__':
    server_risk_report_daily_job()
