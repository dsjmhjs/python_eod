# -*- coding: utf-8 -*-
# 自动配平策略持仓和实际持仓间的差异（南华）
from eod_aps.model.schema_portfolio import RealAccount, PfPosition, AccountPosition
from eod_aps.job import *


def __close_position_automation_nanhua():
    # 真实账号和策略default账号对应关系
    account_correspondence_list = [(1, 3), (4, 4), (7, 5)]

    server_name = 'nanhua'
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    real_account_dict = dict()
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        fund_name = account_db.fund_name
        real_account_dict.setdefault(fund_name, []).append(account_db.accountid)

    query_sql = 'select max(DATE) from portfolio.account_position'
    account_date_filter_str = session_portfolio.execute(query_sql).first()[0]

    query_sql = 'select max(DATE) from portfolio.pf_position'
    pf_date_filter_str = session_portfolio.execute(query_sql).first()[0]

    modify_position_list = []
    for (account_id, pf_account_id) in account_correspondence_list:
        position_dict = dict()
        query_position = session_portfolio.query(AccountPosition)
        for position_db in query_position.filter(AccountPosition.id == account_id,
                                                 AccountPosition.date == account_date_filter_str):
            if not ('IC' in position_db.symbol or 'IF' in position_db.symbol or 'IH' in position_db.symbol):
                continue
            key = '%s|%s' % (position_db.symbol, position_db.hedgeflag)
            position_dict[key] = position_db

        pf_position_dict = dict()
        query_position = session_portfolio.query(PfPosition)
        for pf_position_db in query_position.filter(PfPosition.id == pf_account_id,
                                                 PfPosition.date == pf_date_filter_str):
            if not ('IC' in pf_position_db.symbol or 'IF' in pf_position_db.symbol or 'IH' in pf_position_db.symbol):
                continue
            key = '%s|%s' % (pf_position_db.symbol, pf_position_db.hedgeflag)
            pf_position_dict[key] = pf_position_db

        for (symbol, position_db) in position_dict.items():
            key = '%s|%s' % (symbol, position_db.hedgeflag)
            if key in pf_position_dict:
                pf_position_db = pf_position_dict[key]
                if position_db.long != pf_position_db.long:
                    pf_position_db.long = position_db.long
                    pf_position_db.long_cost = position_db.long_cost
                    pf_position_db.long_avail = position_db.long_avail
                    pf_position_db.yd_position_long = position_db.yd_position_long
                    pf_position_db.yd_long_remain = position_db.yd_long_remain
                if position_db.short != pf_position_db.short:
                    pf_position_db.short = position_db.short
                    pf_position_db.short_cost = position_db.short_cost
                    pf_position_db.short_avail = position_db.short_avail
                    pf_position_db.yd_position_short = position_db.yd_position_short
                    pf_position_db.yd_short_remain = position_db.yd_short_remain
                pf_position_db.prev_net = pf_position_db.yd_position_long - pf_position_db.yd_position_short

                pf_position_dict.pop(key)
                modify_position_list.append(pf_position_db)
            else:
                new_pf_position = PfPosition()
                new_pf_position.date = pf_date_filter_str
                new_pf_position.id = pf_account_id
                new_pf_position.symbol = position_db.symbol
                new_pf_position.hedgeflag = position_db.hedgeflag
                new_pf_position.long = position_db.long
                new_pf_position.long_cost = position_db.long_cost
                new_pf_position.long_avail = position_db.long_avail
                new_pf_position.yd_position_long = position_db.yd_position_long
                new_pf_position.yd_long_remain = position_db.yd_long_remain
                new_pf_position.short = position_db.short
                new_pf_position.short_cost = position_db.short_cost
                new_pf_position.short_avail = position_db.short_avail
                new_pf_position.yd_position_short = position_db.yd_position_short
                new_pf_position.yd_short_remain = position_db.yd_short_remain
                modify_position_list.append(new_pf_position)

        for (symbol, pf_position_db) in pf_position_dict.items():
            session_portfolio.delete(pf_position_db)

    for pf_position in modify_position_list:
        session_portfolio.merge(pf_position)
    session_portfolio.commit()
    server_model.close()


def close_position_automation_job():
    __close_position_automation_nanhua()


if __name__ == '__main__':
    close_position_automation_job()