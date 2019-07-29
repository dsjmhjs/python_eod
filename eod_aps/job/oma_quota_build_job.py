# -*- coding: utf-8 -*-
import os
from decimal import Decimal
from eod_aps.model.schema_portfolio import RealAccount, AccountPosition, OmaQuota
from eod_aps.job import *


class OmaView(object):
    """
        oma信息类
    """
    def __init__(self):
        pass

    investor_id = None
    filter_date_str = None
    real_account_list = []
    investor_money = 0
    include_ticker_list = []
    quota_ratio = 1


def __build_oma_view():
    today_str = date_utils.get_today_str('%Y-%m-%d')

    oma_view_list = []
    # oma_view1 = OmaView()
    # oma_view1.filter_date_str = today_str
    # oma_view1.investor_id = 'tangshang.ys01'
    # oma_view1.real_account_list.append('10')
    # oma_view1.investor_money = 1000000
    # oma_view1.include_ticker_list = ['002133', '002573', '300156', '300100',
    #                                  '600318', '600863', '600633', '601988', '600703', '600782']
    # oma_view1.quota_ratio = 0.3
    # oma_view_list.append(oma_view1)

    oma_view2 = OmaView()
    oma_view2.filter_date_str = today_str
    oma_view2.investor_id = 'MC.ys001'
    oma_view2.investor_money = 10000000
    oma_view_list.append(oma_view2)
    return oma_view_list


def __query_account_position(server_model, oma_view):
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = 'select max(DATE) from portfolio.account_position'
    filter_date_str = session_portfolio.execute(query_sql).first()[0]

    account_position_dict = dict()
    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id.in_(tuple(oma_view.real_account_list), ),
                                             AccountPosition.date == filter_date_str):
        # 过滤非股票
        if not position_db.symbol.isdigit():
            continue

        # 过滤非指定股票
        if position_db.symbol not in oma_view.include_ticker_list:
            continue

        if position_db.symbol in account_position_dict:
            account_position_dict[position_db.symbol] += position_db.long_avail * Decimal(oma_view.quota_ratio)
        else:
            account_position_dict[position_db.symbol] = position_db.long_avail * Decimal(oma_view.quota_ratio)
    return account_position_dict


# 根据调仓单过滤
def __filter_pf_position_transfer(server_model, oma_view, real_position_dict):
    session_portfolio = server_model.get_db_session('portfolio')

    fund_name_list = []
    query_account = session_portfolio.query(RealAccount)
    for real_account_db in query_account.filter(RealAccount.accountid.in_(tuple(oma_view.real_account_list), )):
        fund_name_list.append(real_account_db.fund_name)

    pf_position_dict = dict()
    transfer_file_folder = '%s/%s/%s' % (STOCK_SELECTION_FOLDER, server_model.name, oma_view.filter_date_str.replace('-', ''))
    if not os.path.exists(transfer_file_folder):
        custom_log.log_error_job('[Error]No Path:%s' % transfer_file_folder)
        return

    for file_name in os.listdir(transfer_file_folder):
        if '.txt' not in file_name:
            continue
        fund_name = file_name.split('-')[2]
        if fund_name not in fund_name_list:
            continue
        with open(transfer_file_folder + '/' + file_name) as fr:
            for line in fr.readlines():
                symbol, qty_value = line.replace('\n', '').split(',')
                if int(qty_value) > 0:
                    continue
                else:
                    if symbol in pf_position_dict:
                        pf_position_dict[symbol] += int(qty_value)
                    else:
                        pf_position_dict[symbol] = int(qty_value)

    for (symbol, sell_qty) in pf_position_dict.items():
        if symbol not in real_position_dict:
            custom_log.log_error_job('Error Ticker:%s' % symbol)
            continue
        real_position_dict[symbol] += sell_qty


def __save_oma_quota(server_model, oma_view, real_position_dict):
    session_portfolio = server_model.get_db_session('portfolio')
    oma_quota_del_sql = "delete from portfolio.oma_quota where investor_id='%s' and date='%s'" \
                        % (oma_view.investor_id, oma_view.filter_date_str)
    session_portfolio.execute(oma_quota_del_sql)

    for (symbol, qty_value) in real_position_dict.items():
        oma_quota = OmaQuota()
        oma_quota.date = oma_view.filter_date_str
        oma_quota.investor_id = oma_view.investor_id
        oma_quota.symbol = symbol
        qty_value = __round_down(qty_value)
        if qty_value > 0:
            oma_quota.sell_quota = __round_down(qty_value)
            oma_quota.buy_quota = __round_down(qty_value)
            session_portfolio.add(oma_quota)

    oma_quota = OmaQuota()
    oma_quota.date = oma_view.filter_date_str
    oma_quota.investor_id = oma_view.investor_id
    oma_quota.symbol = 'CNY'
    oma_quota.sell_quota = oma_view.investor_money
    oma_quota.buy_quota = oma_view.investor_money
    session_portfolio.add(oma_quota)
    session_portfolio.commit()


def __round_down(number_input):
    # 向下取整
    return int(int(float(number_input) / float(100)) * 100)


def oma_quota_build_job(server_name):
    oma_view_list = __build_oma_view()

    server_model = server_constant.get_server_model(server_name)
    for oma_view in oma_view_list:
        real_position_dict = __query_account_position(server_model, oma_view)
        if len(oma_view.include_ticker_list) > 0:
            __filter_pf_position_transfer(server_model, oma_view, real_position_dict)
        __save_oma_quota(server_model, oma_view, real_position_dict)
    server_model.close()


def oma_quota_rebuild_job(server_name):
    INVESTOR_ID = 'MC.ys001'
    FILTER_DATE_STR = '20171010'
    REBUILD_FILE_PATH = 'G:/rebuild_file_path'

    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    oma_quota_dict = dict()
    query_position = session_portfolio.query(OmaQuota)
    for oma_quota_db in query_position.filter(OmaQuota.investor_id == INVESTOR_ID, OmaQuota.date == FILTER_DATE_STR):
        if not oma_quota_db.symbol.isdigit():
            continue
        oma_quota_dict[oma_quota_db.symbol] = oma_quota_db

    for file_name in os.listdir(REBUILD_FILE_PATH):
        if '.txt' not in file_name:
            continue
        with open(REBUILD_FILE_PATH + '/' + file_name) as fr:
            for line in fr.readlines():
                symbol, qty_value = line.replace('\n', '').split(',')
                if int(qty_value) > 0:
                    continue

                if symbol in oma_quota_dict:
                    oma_quota_db = oma_quota_dict[symbol]
                    oma_quota_db.sell_quota += __round_down(qty_value)
                    oma_quota_db.buy_quota += __round_down(qty_value)
    for (symbol, oma_quota_db) in oma_quota_dict.items():
        session_portfolio.merge(oma_quota_db)
    session_portfolio.commit()
    server_model.close()


if __name__ == '__main__':
    oma_quota_build_job('guoxin')
    # oma_quota_rebuild_job('host')
