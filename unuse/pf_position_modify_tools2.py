# -*- coding: utf-8 -*-
# 策略持仓修改工具
import xlrd
from eod_aps.model.pf_account import PfAccount
from sqlalchemy import desc

from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from datetime import datetime

server_constant = ServerConstant()
pf_account_dict = dict()


def pf_position_modify(symbol_key, account_id, long_volume, short_volume):
    server_model = server_constant.get_server_model('host')
    session_portfolio = server_model.get_db_session('portfolio')
    query_position = session_portfolio.query(PfPosition)
    pf_position_db = query_position.filter(PfPosition.symbol.like('%' + symbol_key + '%')).order_by(
        desc(PfPosition.date)).first()
    if pf_position_db.long_avail > 0:
        unit_price = pf_position_db.long_cost / pf_position_db.long_avail
    elif pf_position_db.short_avail > 0:
        unit_price = pf_position_db.short_cost / pf_position_db.short_avail

    pf_position = PfPosition()
    pf_position.date = datetime.now()
    pf_position.id = account_id
    if long_volume > 0:
        pf_position.long = long_volume
        pf_position.long_avail = long_volume
        pf_position.long_cost = unit_price * long_volume
    else:
        pf_position.long = 0
        pf_position.long_avail = 0
        pf_position.long_cost = 0

    if short_volume > 0:
        pf_position.short = short_volume
        pf_position.short_avail = short_volume
        pf_position.short_cost = unit_price * short_volume
    else:
        pf_position.short = 0
        pf_position.short_avail = 0
        pf_position.short_cost = 0

    pf_position.yd_position_long = 0
    pf_position.yd_position_short = 0
    pf_position.yd_long_remain = 0
    pf_position.yd_short_remain = 0

    session_portfolio.add(pf_position)
    session_portfolio.commit()


def __build_strategy_dict():
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        pf_account_dict[pf_account_db.fund_name] = pf_account_db


def __read_xls(file_path):
    data = xlrd.open_workbook(file_path)
    table = data.sheets()[0]  # 通过索引顺序获取
    nrows = table.nrows  # 行数
    for i in range(1, nrows, 1):
        ticker = table.cell(i, 0).value
        Long = table.cell(i, 2).value
        LongAvail = table.cell(i, 3).value
        LongCost = table.cell(i, 4).value

        Short = table.cell(i, 5).value
        ShortAvail = table.cell(i, 6).value
        ShortCost = table.cell(i, 7).value

        strategy = table.cell(i, 24).value

        pf_position = PfPosition()
        pf_position.date = datetime.now()
        pf_position.symbol = ticker.replace('CG', '').replace('CS', '').strip()

        pf_position.long = Long
        pf_position.long_avail = LongAvail
        pf_position.long_cost = LongCost

        pf_position.short = Short
        pf_position.short_avail = ShortAvail
        pf_position.short_cost = ShortCost

        strategy = strategy.split('@')[0]
        if strategy in pf_account_dict:
            pf_position.id = pf_account_dict[strategy].id
        else:
            print 'unfind:', strategy

        pf_position.yd_position_long = 0
        pf_position.yd_position_short = 0
        pf_position.yd_long_remain = 0
        pf_position.yd_short_remain = 0

        pf_position.delta = 1.000000
        pf_position.gamma = 0.000000
        pf_position.theta = 0.000000
        pf_position.vega = 0.000000
        pf_position.rho = 0.000000

        session_portfolio.add(pf_position)

    pf_position = PfPosition()
    pf_position.date = datetime.now()
    pf_position.symbol = 'CNY'

    pf_position.long = Long
    pf_position.long_avail = LongAvail
    pf_position.long_cost = LongCost

    pf_position.short = Short
    pf_position.short_avail = ShortAvail
    pf_position.short_cost = ShortCost

    pf_position.yd_position_long = 0
    pf_position.yd_position_short = 0
    pf_position.yd_long_remain = 0
    pf_position.yd_short_remain = 0

    pf_position.delta = 1.000000
    pf_position.gamma = 0.000000
    pf_position.theta = 0.000000
    pf_position.vega = 0.000000
    pf_position.rho = 0.000000
    pf_position.id = -1
    session_portfolio.add(pf_position)

    session_portfolio.commit()


if __name__ == '__main__':
    server_name = 'nanhua'
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    __build_strategy_dict()
    __read_xls('E:/gui_download/182.xlsx')
