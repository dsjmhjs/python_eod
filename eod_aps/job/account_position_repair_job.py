# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from eod_aps.model.schema_portfolio import RealAccount, PfAccount, PfPosition, AccountPosition
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.tools.phone_trade_tools import save_phone_trade_file, send_phone_trade
from eod_aps.job.account_position_check_job import query_real_position_data, query_pf_position_data, compare_position
from eod_aps.job import *


def account_position_repair_job(server_name, alarm_num=5):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    position_date, account_dict, real_position_list = query_real_position_data(session_portfolio)
    pf_position_date, pf_account_dict, pf_position_list = query_pf_position_data(session_portfolio)
    server_model.close()

    if not real_position_list and not pf_position_list:
        return

    compare_result_list = compare_position(real_position_list, pf_position_list)
    phone_trade_list = __build_phone_trades(server_name, compare_result_list, pf_account_dict, pf_position_list)

    # 低于alarm_num直接发送至服务器，否则保存至文件
    if 0 < len(phone_trade_list) <= alarm_num:
        send_phone_trade(server_name, phone_trade_list)
    elif len(phone_trade_list) > alarm_num:
        server_save_path = os.path.join(PHONE_TRADE_FOLDER, server_name)
        if not os.path.exists(server_save_path):
            os.mkdir(server_save_path)
        phone_trade_file_path = '%s/position_repair_%s.csv' % (server_save_path, date_utils.get_today_str('%Y%m%d'))
        save_phone_trade_file(phone_trade_file_path, phone_trade_list)


# 根据差异生成PhoneTrade
def __build_phone_trades(server_name, compare_result_list, pf_account_dict, pf_position_list):
    pf_position_dict = dict()
    for pf_position_info in pf_position_list:
        pf_account_id, fund, ticker, pf_long, pf_short = pf_position_info
        dict_key = '%s|%s' % (fund, ticker)
        pf_position_dict.setdefault(dict_key, []).append(pf_position_info)
    default_account_dict = dict()
    for (account_id, pf_account_db) in pf_account_dict.items():
        if pf_account_db.group_name == 'manual':
            fund_name = pf_account_db.fund_name.split('-')[2]
            default_account_dict[fund_name] = pf_account_db

    instrument_dict = query_instrument_dict('host')

    phone_trade_list = []
    for compare_info_item in compare_result_list:
        fund, ticker, real_long, real_short, pf_long, pf_short, diff = compare_info_item
        if ticker not in instrument_dict:
            custom_log.log_error_job('Server:%s, UnFind Ticker:%s' % (server_name, ticker))
            continue
        instrument_db = instrument_dict[ticker]

        # *暂只处理股票的不一致
        if instrument_db.type_id != Instrument_Type_Enums.CommonStock:
            continue

        # 期货和期权需要单独设置tradetype
        future_flag = False
        if instrument_db.type_id in [Instrument_Type_Enums.Future, Instrument_Type_Enums.Option]:
            future_flag = True

        prev_close = instrument_db.prev_close
        if prev_close is None:
            continue

        if pf_long > real_long:
            diff_long = pf_long - real_long

            find_key = '%s|%s' % (fund, ticker)
            pf_position_list = pf_position_dict[find_key]
            for pf_position_info in pf_position_list:
                pf_account_id, fund, ticker, item_long, item_short = pf_position_info
                if item_long == 0:
                    continue

                pf_account_info = pf_account_dict[pf_account_id]
                phone_trade_info = PhoneTradeInfo()
                phone_trade_info.fund = fund
                phone_trade_info.strategy1 = '%s.%s' % (pf_account_info.group_name, pf_account_info.name)
                phone_trade_info.symbol = ticker
                phone_trade_info.direction = Direction_Enums.Sell
                phone_trade_info.tradetype = Trade_Type_Enums.Close if future_flag else Trade_Type_Enums.Normal
                phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
                phone_trade_info.exqty = min(item_long, diff_long)
                phone_trade_info.iotype = IO_Type_Enums.Inner1
                phone_trade_info.server_name = server_name
                phone_trade_info.exprice = prev_close
                phone_trade_list.append(phone_trade_info)

                diff_long = max(diff_long - item_long, 0)
                if diff_long == 0:
                    break

        if real_long > pf_long:
            default_pf_account = default_account_dict[fund]
            phone_trade_info = PhoneTradeInfo()
            phone_trade_info.fund = fund
            phone_trade_info.strategy1 = '%s.%s' % (default_pf_account.group_name, default_pf_account.name)
            phone_trade_info.symbol = ticker
            phone_trade_info.direction = Direction_Enums.Buy
            phone_trade_info.tradetype = Trade_Type_Enums.Open if future_flag else Trade_Type_Enums.Normal
            phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
            phone_trade_info.exqty = real_long - pf_long
            phone_trade_info.iotype = IO_Type_Enums.Inner1
            phone_trade_info.server_name = server_name
            phone_trade_info.exprice = prev_close
            phone_trade_list.append(phone_trade_info)

        if pf_short > real_short:
            diff_short = pf_short - real_short

            find_key = '%s|%s' % (fund, ticker)
            pf_position_list = pf_position_dict[find_key]
            for pf_position_info in pf_position_list:
                pf_account_id, fund, ticker, item_long, item_short = pf_position_info
                if item_short == 0:
                    continue
                pf_account_info = pf_account_dict[pf_account_id]

                phone_trade_info = PhoneTradeInfo()
                phone_trade_info.fund = fund
                phone_trade_info.strategy1 = '%s.%s' % (pf_account_info.group_name, pf_account_info.name)
                phone_trade_info.symbol = ticker
                phone_trade_info.direction = Direction_Enums.Buy
                phone_trade_info.tradetype = Trade_Type_Enums.Close if future_flag else Trade_Type_Enums.Normal
                phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
                phone_trade_info.exqty = min(item_short, diff_short)
                phone_trade_info.iotype = IO_Type_Enums.Inner1
                phone_trade_info.server_name = server_name
                phone_trade_info.exprice = prev_close
                phone_trade_list.append(phone_trade_info)

                diff_short = max(diff_short - item_short, 0)
                if diff_short == 0:
                    break

        if real_short > pf_short:
            default_pf_account = default_account_dict[fund]
            phone_trade_info = PhoneTradeInfo()
            phone_trade_info.fund = fund
            phone_trade_info.strategy1 = '%s.%s' % (default_pf_account.group_name, default_pf_account.name)
            phone_trade_info.symbol = ticker
            phone_trade_info.direction = Direction_Enums.Sell
            phone_trade_info.tradetype = Trade_Type_Enums.Open if future_flag else Trade_Type_Enums.Normal
            phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
            phone_trade_info.exqty = real_short - pf_short
            phone_trade_info.iotype = IO_Type_Enums.Inner1
            phone_trade_info.server_name = server_name
            phone_trade_info.exprice = prev_close
            phone_trade_list.append(phone_trade_info)
    return phone_trade_list


def __query_real_position_dataframe(session_portfolio):
    account_fund_dict = dict()
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        account_fund_dict[account_db.accountid] = account_db.fund_name

    query_sql = 'select max(DATE) from portfolio.account_position'
    filter_date_str = session_portfolio.execute(query_sql).first()[0]

    position_list = []
    account_id_list = account_fund_dict.keys()
    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id.in_(tuple(account_id_list), ),
                                             AccountPosition.date == filter_date_str):
        if position_db.symbol == 'CNY':
            continue
        elif '&' in position_db.symbol:
            continue
        elif position_db.long == 0 and position_db.short == 0:
            continue

        ticker = position_db.symbol.split(' ')[0] if ' ' in position_db.symbol else position_db.symbol
        fund_name = account_fund_dict[position_db.id]
        position_list.append([position_db.id, fund_name, ticker, int(position_db.long), int(position_db.short)])
    return filter_date_str, position_list


def __query_pf_position_dataframe(session_portfolio):
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

        ticker = pf_position_db.symbol.split(' ')[0] if ' ' in pf_position_db.symbol else pf_position_db.symbol
        pf_account_db = pf_account_dict[pf_position_db.id]
        fund_name = pf_account_db.fund_name.split('-')[2]
        pf_position_list.append([pf_position_db.id, fund_name, ticker,
                                 int(pf_position_db.long), int(pf_position_db.short)])
    return pf_date_filter_str, pf_account_dict, pf_position_list


def __compare_position(real_position_dataframe, pf_position_dataframe):
    real_df = pd.DataFrame(real_position_dataframe, columns=['Account_id', 'Fund', 'Ticker', 'Long', 'Short'])
    pf_df = pd.DataFrame(pf_position_dataframe, columns=['PF_Account_id', 'Fund', 'Ticker', 'PF_Long', 'PF_Short'])

    real_df = real_df[['Fund', 'Ticker', 'Long', 'Short']]
    pf_df = pf_df[['Fund', 'Ticker', 'PF_Long', 'PF_Short']]
    grouped_real_df = real_df.groupby(['Fund', 'Ticker']).sum().reset_index()
    grouped_pf_df = pf_df.groupby(['Fund', 'Ticker']).sum().reset_index()

    merge_df = grouped_real_df.merge(grouped_pf_df, how="outer").fillna(0)
    merge_df['Diff'] = merge_df['Long'] - merge_df['PF_Long'] - (merge_df['Short'] - merge_df['PF_Short'])
    diff_df = merge_df[merge_df['Diff'] != 0]

    # 过滤掉一些无需展示项，如:204001
    filter_ticker_list = ['204001', ]
    for filter_ticker in filter_ticker_list:
        diff_df = diff_df[diff_df['Ticker'] != filter_ticker]

    compare_indexs = ['Fund', 'Ticker', 'Long', 'Short', 'PF_Long', 'PF_Short', 'Diff']
    compare_result_list = np.array(diff_df[compare_indexs]).tolist()
    compare_result_list.sort()
    return compare_result_list


if __name__ == '__main__':
    account_position_repair_job('citics')
