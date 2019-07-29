# -*- coding: utf-8 -*-
# 每日统计各多因子策略的收益情况，并发送邮件通知
import pandas as pd
import os
from WindPy import *
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.trade2_history import Trade2History
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.job import *

date_stock_file_dict = dict()
date_future_file_dict = dict()
account_money_dict = dict()
instrument_db_dict = dict()
close_price_dict = dict()
pf_position_dict = dict()
trade_dict = dict()

sum_pl_dict = dict()
strategy_pnl_dict = dict()
date_set = set()
attachment_file_list = []

#  strategy_name_list = ['Long_Norm',]
strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'CSI300_MV10Norm']
base_save_folder_template = 'Z:/dailyjob/Report/StockSelection_Long_%s'
# base_save_folder_template = 'E:/dailyFiles/report/StockSelection_Long_%s'

filter_date_str = date_utils.get_today_str('%Y-%m-%d')
email_utils = EmailUtils(EmailUtils.group9)


def __build_close_price_dict(server_model, pf_account_id_list):
    w.start()
    session_portfolio = server_model.get_db_session('portfolio')
    date_info = session_portfolio.execute('select min(date) from portfolio.pf_position t where t.ID in (%s)' % \
                                          ','.join(pf_account_id_list)).first()
    start_date, end_date = date_info[0], date_utils.get_today()

    ticker_list = []
    ticker_wind_list = []
    query_sql = 'select symbol from portfolio.pf_position t where t.ID in (%s) group by symbol' % ','.join(pf_account_id_list)
    for ticker_items in session_portfolio.execute(query_sql):
        ticker_str = ticker_items[0]
        if ticker_str == 'CNY':
            continue

        ticker_wind_str = __get_wind_ticker(ticker_str)
        ticker_list.append(ticker_str)
        ticker_wind_list.append(ticker_wind_str)

    wind_data = w.wsd(ticker_wind_list, "close", start_date, end_date, "Fill=Previous")
    if wind_data.Data[0][0] == 'No Content':
        task_logger.error('Wind Query Result:No Content')
        return

    data_list = wind_data.Data
    for i in range(0, len(wind_data.Times)):
        date_item = wind_data.Times[i]
        for j in range(0, len(ticker_list)):
            ticker = ticker_list[j]
            ticker_close_price = data_list[j][i]
            dict_key = '%s|%s' % (date_item.strftime('%Y-%m-%d'), ticker)
            close_price_dict[dict_key] = ticker_close_price
    w.close()


def __build_pf_position_dict(server_model, pf_account_id_list):
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_pf_position.filter(PfPosition.id.in_(tuple(pf_account_id_list))):
        date_str = pf_position_db.date.strftime("%Y-%m-%d")
        dict_key = '%s|%s' % (date_str, pf_position_db.id)
        if dict_key in pf_position_dict:
            pf_position_dict[dict_key].append(pf_position_db)
        else:
            pf_position_dict[dict_key] = [pf_position_db]


def __build_trade_dict(server_model, pf_account_db_list):
    session_om = server_model.get_db_session('om')
    trade_list = session_om.query(Trade2History)

    strategy_list = []
    strategy_dict = dict()
    for pf_account_db in pf_account_db_list:
        strategy_name = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        strategy_list.append(strategy_name)
        strategy_dict[strategy_name] = pf_account_db.id

    for trade_db in trade_list.filter(Trade2History.strategy_id.in_(tuple(strategy_list))):
        date_str = trade_db.time.strftime("%Y-%m-%d")
        dict_key = '%s|%s' % (date_str, strategy_dict[trade_db.strategy_id])
        if dict_key in trade_dict:
            trade_dict[dict_key].append(trade_db)
        else:
            trade_dict[dict_key] = [trade_db]


def __build_account_money_dict(server_model):
    query_sql = "select date,id,long_money from pf_account_money"
    conn = server_model.get_db_connect('portfolio')
    df = pd.read_sql(query_sql, conn)
    df.index = df['date']
    group = df.groupby('id')

    for account_id, data in group:
        data = data.sort_values('date')
        start_date = data['date'].iloc[0]
        end_date = filter_date_str
        data = data.reindex(pd.date_range(start_date, end_date))
        data['date'] = data.index
        data['date'] = data['date'].astype('str')
        data = data.fillna(method='ffill')

        data['id'] = data['id'].astype('int').astype('str')
        data['long_money'] = data['long_money'].astype('str')
        data['dict_key'] = data['date'] + '|' + data['id']
        data.index = range(len(data))

        for ind in data.index.values:
            account_money_dict[data.at[ind, 'dict_key']] = data.at[ind, 'long_money']
    conn.close()


def pf_account_report(server_model, pf_account_db):
    global date_stock_file_dict
    global date_future_file_dict

    session_portfolio = server_model.get_db_session('portfolio')
    date_info = session_portfolio.execute('select min(date),max(date) from portfolio.pf_position where ID='\
                                          + str(pf_account_db.id)).first()
    start_date, end_date = date_info[0], date_utils.get_today()
    if start_date is None:
        return
    trading_day_list = date_utils.get_trading_day_list(start_date, end_date)

    future_open_dict = dict()
    prev_equity_total = None
    money_surplus_pool = 0.0

    report_result = []
    filter_date_str = ''
    for i in range(0, len(trading_day_list)):
        date_set.add(trading_day_list[i])
        filter_date_str = trading_day_list[i].strftime("%Y-%m-%d")
        pf_position_list = []

        dict_query_key = '%s|%s' % (filter_date_str, pf_account_db.id)
        if dict_query_key in pf_position_dict:
            pf_position_list = pf_position_dict[dict_query_key]

        total_buy_money = 0.0
        total_sell_money = 0.0
        if dict_query_key in trade_dict:
            trade_list = trade_dict[dict_query_key]
            for trade_db in trade_list:
                ticker = trade_db.symbol.split(' ')[0]
                if trade_db.trade_type == 0:
                    if trade_db.qty > 0:
                        total_buy_money += float(trade_db.price) * abs(trade_db.qty) * (1 + 0.00025)
                    else:
                        total_sell_money += float(trade_db.price) * abs(trade_db.qty) * (1 - 0.00125)

            total_money_change = total_buy_money - total_sell_money
        else:
            total_money_change = 0.0

        if total_money_change < 0:
            money_surplus_pool += abs(total_money_change)
        elif total_money_change > 0:
            money_surplus_pool -= min(total_money_change, money_surplus_pool)

        stock_equity = 0.0
        future_equity = 0.0
        for pf_position_db in pf_position_list:
            instrument_db = instrument_db_dict[pf_position_db.symbol]

            dict_query_key = '%s|%s' % (filter_date_str, instrument_db.ticker)
            ticker_close_price = float(close_price_dict[dict_query_key])

            if instrument_db.type_id == 4:
                stock_equity += float(pf_position_db.long) * ticker_close_price

        equity_total = stock_equity + future_equity
        if prev_equity_total is None:
            pnl = equity_total - total_money_change
            equity_base = total_money_change
        else:
            pnl = equity_total - total_money_change - prev_equity_total
            if total_money_change > 0:
                equity_base = prev_equity_total + money_surplus_pool + total_money_change
            elif total_money_change < 0:
                equity_base = prev_equity_total + money_surplus_pool + total_money_change
            else:
                equity_base = prev_equity_total + money_surplus_pool

        account_money_key = '%s|%s' % (filter_date_str, pf_account_db.id)
        account_money = account_money_dict[account_money_key]

        return_rate = pnl * 100 / float(equity_base)

        report_result.append('%s,%s,%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f%%' \
                             % (filter_date_str, total_buy_money, total_sell_money, total_money_change, money_surplus_pool
                        , prev_equity_total, stock_equity, future_equity, equity_total, pnl, equity_base, return_rate))

        sum_pl_dict['%s|%s' % (filter_date_str, 'return_rate_%s' % pf_account_db.name)] = return_rate

        strategy_pnl_key = '%s|%s' % (filter_date_str, pf_account_db.name)
        strategy_pnl_dict[strategy_pnl_key] = '%s|%s' % (pnl, account_money)

        prev_equity_total = equity_total

    save_folder = base_save_folder_template % filter_date_str
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)

    file_path = save_folder + '/strategy_report_%s_%s.csv' % (pf_account_db.name, filter_date_str)
    report_result.insert(0, 'date,total_buy_money,total_sell_money,total_money_change,money_surplus_pool,\
prev_equity_total,stock_equity, future_equity,equity_total,pnl,equity_base,return_rate')
    file_object = open(file_path, 'w+')
    file_object.write('\n'.join(report_result))
    file_object.close()
    attachment_file_list.append(file_path)
    server_model.close()

def __read_stock_daily_file(date_list):
    query_date_list = []
    for date_item in date_list:
        query_date_list.append("'" + date_item.strftime("%Y%m%d") + "'")

    server_model = server_constant.get_server_model('local118')
    session_portfolio = server_model.get_db_session('dump_wind')
    query_sql = "select t.TRADE_DT, t.S_INFO_WINDCODE, t.S_DQ_CLOSE from dump_wind.ASHAREEODPRICES t \
where t.TRADE_DT in (%s)" % ','.join(query_date_list)

    date_stock_file_dict = dict()
    for db_item in session_portfolio.execute(query_sql):
        date_str = db_item[0]
        ticker = db_item[1].split('.')[0]
        if date_str in date_stock_file_dict:
            date_stock_file_dict[date_str][ticker] = db_item[2]
        else:
            ticker_dict = dict()
            ticker_dict[ticker] = db_item[2]
            date_stock_file_dict[date_str] = ticker_dict
    server_model.close()
    return date_stock_file_dict


def __read_future_daily_file(date_list):
    query_date_list = []
    for date_item in date_list:
        query_date_list.append("'" + date_item.strftime("%Y%m%d") + "'")

    server_model = server_constant.get_server_model('local118')
    session_portfolio = server_model.get_db_session('dump_wind')
    query_sql = "select TRADE_DT,S_INFO_WINDCODE,S_DQ_CLOSE from dump_wind.CINDEXFUTURESEODPRICES \
where TRADE_DT in (%s)" % ','.join(query_date_list)

    date_future_file_dict = dict()
    for db_item in session_portfolio.execute(query_sql):
        date_str = db_item[0]
        ticker = db_item[1].split('.')[0]
        if date_str in date_future_file_dict:
            date_future_file_dict[date_str][ticker] = db_item[2]
        else:
            ticker_dict = dict()
            ticker_dict[ticker] = db_item[2]
            date_future_file_dict[date_str] = ticker_dict
    server_model.close()
    return date_future_file_dict


def __build_ticker_exchange(server_model):
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id.in_((1, 4))):
        instrument_db_dict[instrument_db.ticker] = instrument_db


def __email_algo_pf_position(pf_account_db_list):
    date_list = list(date_set)
    date_list.sort()

    daily_pnl_dict = dict()
    for strategy_base_name in strategy_name_list:
        csv_pnl_contentlist = []
        title_list = ['Pf_Account']
        for date_str in date_list:
            title_list.append(date_str.strftime("%Y-%m-%d"))
        title_list.append('Pnl_Total')
        csv_pnl_contentlist.append(','.join(title_list))

        for pf_account_db in pf_account_db_list:
            value_list = []
            if strategy_base_name not in pf_account_db.name:
                continue
            value_list.append(pf_account_db.name)
            strategy_pnl_total = 0.0
            for date_str in date_list:
                strategy_pnl_key = '%s|%s' % (date_str, pf_account_db.name)
                if strategy_pnl_key in strategy_pnl_dict:
                    (pnl_temp, equity_base) = strategy_pnl_dict[strategy_pnl_key].split('|')
                    value_list.append('%.3f' % float(pnl_temp))
                    strategy_pnl_total += float(pnl_temp)
                else:
                    value_list.append('0')
            value_list.append('%.3f' % strategy_pnl_total)
            csv_pnl_contentlist.append(','.join(value_list))

        total_value_list = [strategy_base_name]
        pnl_total = 0.0
        daily_pnl_list = []
        for date_str in date_list:
            daily_pnl_total = 0.0
            for pf_account_db in pf_account_db_list:
                if strategy_base_name not in pf_account_db.name:
                    continue
                strategy_pnl_key = '%s|%s' % (date_str, pf_account_db.name)
                if strategy_pnl_key in strategy_pnl_dict:
                    (pnl_temp, equity_base) = strategy_pnl_dict[strategy_pnl_key].split('|')
                    daily_pnl_total += float(pnl_temp)
                    pnl_total += float(pnl_temp)
            daily_pnl_list.append(daily_pnl_total)
            total_value_list.append('%.3f' % daily_pnl_total)
        total_value_list.append('%.3f' % pnl_total)
        daily_pnl_dict[strategy_base_name] = daily_pnl_list
        csv_pnl_contentlist.append(','.join(total_value_list))

        save_folder = base_save_folder_template % date_str
        if not os.path.exists(save_folder):
            os.mkdir(save_folder)

        file_path = save_folder + '/%s_pnl_report_%s.csv' % (strategy_base_name, filter_date_str)
        file_object = open(file_path, 'w+')
        file_object.write('\n'.join(csv_pnl_contentlist))
        file_object.close()

        df = pd.read_csv(file_path).T
        df.to_csv(file_path, header=False)
        attachment_file_list.append(file_path)

    daily_return_rate_dict = dict()
    for strategy_base_name in strategy_name_list:
        csv_ret_contentlist = []
        title_list = ['Pf_Account']
        for date_str in date_list:
            title_list.append(date_str.strftime("%Y-%m-%d"))
        title_list.append('Cum_Ret')
        csv_ret_contentlist.append(','.join(title_list))

        for pf_account_db in pf_account_db_list:
            value_list = []
            if strategy_base_name not in pf_account_db.name:
                continue
            value_list.append(pf_account_db.name)

            total_value = 1
            for date_str in date_list:
                key = '%s|%s' % (date_str, 'return_rate_%s' % pf_account_db.name)
                if key in sum_pl_dict:
                    value_list.append('%.3f%%' % sum_pl_dict[key])
                    total_value *= 1 + sum_pl_dict[key] / 100
                else:
                    value_list.append('')
            total_value -= 1
            value_list.append('%.3f%%' % (total_value * 100))
            csv_ret_contentlist.append(','.join(value_list))

        total_value_list = [strategy_base_name]
        total_value = 1
        daily_return_list = []
        for date_str in date_list:
            sum_pnl = 0.0
            sum_equity_base = 0.0
            for pf_account_db in pf_account_db_list:
                if strategy_base_name not in pf_account_db.name:
                    continue
                strategy_pnl_key = '%s|%s' % (date_str, pf_account_db.name)
                if strategy_pnl_key in strategy_pnl_dict:
                    (pnl_temp, equity_base) = strategy_pnl_dict[strategy_pnl_key].split('|')
                    sum_pnl += float(pnl_temp)
                    sum_equity_base += float(equity_base)
            return_rate = 0.0
            if sum_equity_base > 0:
                return_rate = sum_pnl * 100 / sum_equity_base
                total_value *= 1 + return_rate / 100
            daily_return_list.append(return_rate)
            total_value_list.append('%.3f%%' % return_rate)
        daily_return_rate_dict[strategy_base_name] = daily_return_list
        total_value -= 1
        total_value_list.append('%.3f%%' % (total_value * 100))
        csv_ret_contentlist.append(','.join(total_value_list))

        file_path = save_folder + '/%s_ret_report_%s.csv' % (strategy_base_name, filter_date_str)
        file_object = open(file_path, 'w+')
        file_object.write('\n'.join(csv_ret_contentlist))
        file_object.close()

        df = pd.read_csv(file_path).T
        df.to_csv(file_path, header=False)
        attachment_file_list.append(file_path)
    # email_utils.send_email_path('多因子策略收益统计(多头)', '', ','.join(attachment_file_list), 'html')


def get_pf_account_info_list(server_model):
    pf_account_db_list = []
    pf_account_id_list = []

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for strategy_name in strategy_name_list:
        for pf_account_db in query_pf_account.filter(PfAccount.fund_name.like('%' + strategy_name + '%'),
                                                     PfAccount.fund_name.like('%steady_return%')):
            pf_account_db_list.append(pf_account_db)
            pf_account_id_list.append(str(pf_account_db.id))
    return pf_account_db_list, pf_account_id_list


def __get_wind_ticker(ticker):
    instrument_db = instrument_db_dict[ticker]
    ticker_wind_str = ''
    if instrument_db.exchange_id == 18:
            ticker_wind_str = '%s.SH' % instrument_db.ticker
    elif instrument_db.exchange_id == 19:
        ticker_wind_str = '%s.SZ' % instrument_db.ticker
    elif instrument_db.exchange_id == 19:
        ticker_wind_str = '%s.SZ' % instrument_db.ticker
    elif instrument_db.exchange_id == 20:
        ticker_wind_str = '%s.SHF' % instrument_db.ticker
    elif instrument_db.exchange_id == 21:
        ticker_wind_str = '%s.DCE' % instrument_db.ticker
    elif instrument_db.exchange_id == 22:
        ticker_wind_str = '%s.CZC' % instrument_db.ticker
    elif instrument_db.exchange_id == 25:
        ticker_wind_str = '%s.CFE' % instrument_db.ticker
    return ticker_wind_str


def __integration_report_file(pf_account_db_list):
    save_folder = base_save_folder_template % filter_date_str
    strategy_file_dict = dict()
    for strategy_name in strategy_name_list:
        for pf_account_db in pf_account_db_list:
            if strategy_name in pf_account_db.name:
                if strategy_name in strategy_file_dict:
                    strategy_file_dict[strategy_name].append(pf_account_db)
                else:
                    strategy_file_dict[strategy_name] = [pf_account_db]

    for (strategy_name, pf_account_db_list) in strategy_file_dict.items():
        data = pd.DataFrame()

        exists_pf_account_list = []
        for pf_account_db in pf_account_db_list:
            report_file_name = 'strategy_report_%s_%s.csv' % (pf_account_db.name, filter_date_str)
            report_file_path = os.path.join(save_folder, report_file_name)
            if not os.path.exists(report_file_path):
                continue
            else:
                exists_pf_account_list.append(pf_account_db)
            df = pd.read_csv(report_file_path)

            df_pnl = df[['date', 'pnl', 'equity_base', 'return_rate']]
            df_pnl.columns = ['date', 'pnl_%s' % pf_account_db.name, 'equity_base_%s'% pf_account_db.name, 'return_rate_%s'% pf_account_db.name ]
            if len(data) == 0:
                data = df_pnl
            else:
                data = pd.merge(data, df_pnl, on='date', how='outer')

        data = data.fillna(0)
        data.index = data['date']
        data = data.drop('date', axis=1)

        pnl_cols = ['pnl_%s' % x.name for x in exists_pf_account_list]
        data_pnl = data[pnl_cols]
        data_pnl.columns = ['%s' % x.name for x in exists_pf_account_list]
        data_pnl[strategy_name] = data_pnl.apply(lambda x: x.sum(), axis=1)
        data_pnl.loc['Pnl_Total'] = data_pnl.apply(lambda x: x.sum())
        data_pnl.to_csv(os.path.join(save_folder, '%s_pnl_report_%s.csv' % (strategy_name, filter_date_str)), index=True)

        for field in ['pnl', 'equity_base']:
            data[field] = 0
            for pf_account_db in exists_pf_account_list:
                data[field] += data['%s_%s' % (field, pf_account_db.name)]
        data['return_rate'] = data['pnl'] * 100 / data['equity_base']
        data['return_rate'] = data['return_rate'].apply(lambda x: '%.3f%%' % x)

        ret_cols = ['return_rate_%s' % x.name for x in exists_pf_account_list]
        ret_cols.append('return_rate')
        data_ret = data[ret_cols]

        data_ret = data_ret.T
        fields = [x for x in data_ret.columns.values]
        data_ret['Cum_Ret'] = 1.0

        data_temp = pd.DataFrame()
        for filed in fields:
            data_temp[filed + '_temp'] = data_ret[filed].astype('str').apply(lambda x: x.replace('%', '')).astype('float')
            data_ret['Cum_Ret'] *= data_temp[filed + '_temp'] / 100 + 1
        data_ret['Cum_Ret'] = (data_ret['Cum_Ret'] - 1) * 100
        data_ret['Cum_Ret'] = data_ret['Cum_Ret'].apply(lambda x: '%.3f%%' % x)
        data_ret = data_ret.T

        save_column_list = ['%s' % x.name for x in exists_pf_account_list]
        save_column_list.append(strategy_name)
        data_ret.columns = save_column_list
        data_ret.to_csv(os.path.join(save_folder, '%s_ret_report_%s.csv' % (strategy_name, filter_date_str)), index=True)


def daily_return_long_report_job():
    global filter_date_str
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    server_name = 'huabao'
    server_model = server_constant.get_server_model(server_name)
    __build_account_money_dict(server_model)
    __build_ticker_exchange(server_model)

    pf_account_db_list, pf_account_id_list = get_pf_account_info_list(server_model)
    __build_close_price_dict(server_model, pf_account_id_list)

    __build_pf_position_dict(server_model, pf_account_id_list)
    __build_trade_dict(server_model, pf_account_db_list)

    for pf_account_db in pf_account_db_list:
        pf_account_report(server_model, pf_account_db)
    # __email_algo_pf_position(pf_account_db_list)
    __integration_report_file(pf_account_db_list)
    server_model.close()


if __name__ == '__main__':
    daily_return_long_report_job()