# -*- coding: utf-8 -*-
import os
import pickle

import pandas as pd
from eod_aps.job import *
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.schema_portfolio import RealAccount, AccountPosition
from eod_aps.tools.phone_trade_tools import save_phone_trade_file


def __build_db_position_df(session_portfolio, account_id):
    account_position_list = []
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    query_position = session_portfolio.query(AccountPosition)
    for account_position in query_position.filter(AccountPosition.date == filter_date_str,
                                                  AccountPosition.id == account_id):
        account_position_list.append([account_position.symbol, account_position.long])

    title_list = ['Symbol', 'Long_db']
    db_position_df = pd.DataFrame(account_position_list, columns=title_list)
    return db_position_df


def __build_memory_position_df(server_name, real_account_db):
    instrument_msg_dict = const.EOD_POOL['instrument_dict']
    market_msg_dict = const.EOD_POOL['market_dict']
    position_msg_dict = const.EOD_POOL['position_dict']
    # instrument_msg_dict, market_msg_dict, position_msg_dict = __load_from_pickle_file()

    account_position_dict = dict()
    memory_account_name = '%s-%s-%s-' % \
                          (real_account_db.accountname, real_account_db.accounttype, real_account_db.fund_name)
    for (temp_account_name, temp_position_dict) in position_msg_dict.items():
        if temp_account_name.split('@')[0] == memory_account_name:
            account_position_dict = temp_position_dict
            break

    account_position_list = []
    for (instrument_key, position_msg) in account_position_dict.items():
        instrument_msg = instrument_msg_dict[instrument_key]
        market_msg = market_msg_dict[instrument_key]

        account_position_list.append([instrument_msg.ticker, server_name, real_account_db.fund_name,
                                      market_msg.Args.NominalPrice, position_msg.Long])

    title_list = ['Symbol', 'ServerName', 'FundName', 'Last_Price', 'Long']
    memory_position_df = pd.DataFrame(account_position_list, columns=title_list)
    return memory_position_df


def __build_phone_trade_file(server_name, pf_account_name, db_position_df, memory_position_df):
    position_compare_df = pd.merge(db_position_df, memory_position_df, how='outer', on=['Symbol']).fillna(0)
    position_diff_df = position_compare_df[position_compare_df['Long_db'] != position_compare_df['Long']]

    phone_trade_list = []
    position_diff_dict = position_diff_df.to_dict("index")
    for (dict_key, dict_values) in position_diff_dict.items():
        if dict_values['Symbol'] == 'CNY':
            continue
        phone_trade_info = PhoneTradeInfo()
        phone_trade_info.fund = dict_values['FundName']
        phone_trade_info.strategy1 = pf_account_name
        phone_trade_info.symbol = dict_values['Symbol']
        phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
        phone_trade_info.tradetype = Trade_Type_Enums.Normal
        phone_trade_info.iotype = IO_Type_Enums.Outer
        phone_trade_info.server_name = dict_values['ServerName']
        phone_trade_info.exprice = dict_values['Last_Price']

        if dict_values['Long_db'] < dict_values['Long']:
            phone_trade_info.direction = Direction_Enums.Buy
            phone_trade_info.exqty = dict_values['Long'] - dict_values['Long_db']
        else:
            phone_trade_info.direction = Direction_Enums.Sell
            phone_trade_info.exqty = dict_values['Long_db'] - dict_values['Long']
        phone_trade_list.append(phone_trade_info)

    server_save_path = os.path.join(PHONE_TRADE_FOLDER, server_name)
    if not os.path.exists(server_save_path):
        os.mkdir(server_save_path)
    phone_trade_file_path = '%s/position_repair_%s.csv' % (server_save_path, date_utils.get_today_str('%Y%m%d'))
    save_phone_trade_file(phone_trade_file_path, phone_trade_list)


def ts_position_revise_job(account_name, pf_account_name):
    ts_server_list = server_constant.get_ts_servers()
    for server_name in ts_server_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        real_account_db = session_portfolio.query(RealAccount).filter(RealAccount.accountname == account_name).first()

        if real_account_db is None:
            continue

        db_position_df = __build_db_position_df(session_portfolio, real_account_db.accountid)
        memory_position_df = __build_memory_position_df(server_name, real_account_db)
        __build_phone_trade_file(server_name, pf_account_name, db_position_df, memory_position_df)


def __load_from_pickle_file():
    path = os.path.dirname(__file__)
    fr = open(path + '/../../cfg/aggregator_pickle_data.txt', 'rb')
    instrument_dict = pickle.load(fr)
    market_dict = pickle.load(fr)
    order_dict = pickle.load(fr)
    order_view_tree_dict = pickle.load(fr)
    trade_list = pickle.load(fr)
    risk_dict = pickle.load(fr)
    position_dict = pickle.load(fr)
    position_update_time = pickle.load(fr)
    fr.close()
    return instrument_dict, market_dict, position_dict


if __name__ == "__main__":
    account_name = '198800888042'
    pf_account_name = 'Earning_05-Event_Real-balance01-'
    ts_position_revise_job(account_name, pf_account_name)


