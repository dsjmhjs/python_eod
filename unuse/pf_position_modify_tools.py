# -*- coding: utf-8 -*-
# 策略持仓修改工具
from sqlalchemy import desc

from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from datetime import datetime

server_constant = ServerConstant()

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

    pf_position = pf_position_db.copy()
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



if __name__ == '__main__':
    pf_position_modify('cu1608', 3, 1, 0)
