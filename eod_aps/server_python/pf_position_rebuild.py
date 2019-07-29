# -*- coding: utf-8 -*-
# 每日3点后更新pf_position表数据，重新创建明日数据，并进行赋值long->long_avail->yd_positong_long->yd_long_remain
from eod_aps.model.schema_portfolio import PfPosition
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.server_python import *


def pf_position_rebuild_index(start_date_str=None):
    print 'Enter pf_position_rebuild.py'
    if start_date_str is None:
        start_date_str = date_utils.get_today_str('%Y-%m-%d')
        next_trading_day_str = date_utils.get_next_trading_day('%Y-%m-%d')
    else:
        next_trading_day_str = date_utils.get_next_trading_day('%Y-%m-%d', start_date_str)

    server_host = server_constant_local.get_server_model('host')
    session_portfolio = server_host.get_db_session('portfolio')
    del_sql = "delete from pf_position where date = '%s'" % next_trading_day_str
    session_portfolio.execute(del_sql)

    i = 0
    query_pf_position = session_portfolio.query(PfPosition).filter(PfPosition.date == start_date_str)
    for pf_position_db in query_pf_position:
        if pf_position_db.symbol != 'CNY' and pf_position_db.long == pf_position_db.short:
            continue

        next_pf_position = PfPosition()
        next_pf_position.date = next_trading_day_str
        next_pf_position.id = pf_position_db.id
        next_pf_position.symbol = pf_position_db.symbol
        next_pf_position.hedgeflag = pf_position_db.hedgeflag

        next_pf_position.long = max(pf_position_db.long - pf_position_db.short, 0)
        next_pf_position.long_cost = max(pf_position_db.long_cost - pf_position_db.short_cost, 0)
        next_pf_position.long_avail = next_pf_position.long
        next_pf_position.yd_position_long = next_pf_position.long
        next_pf_position.yd_long_remain = next_pf_position.long

        next_pf_position.short = max(pf_position_db.short - pf_position_db.long, 0)
        next_pf_position.short_cost = max(pf_position_db.short_cost - pf_position_db.long_cost, 0)
        next_pf_position.short_avail = next_pf_position.short
        next_pf_position.yd_position_short = next_pf_position.short
        next_pf_position.yd_short_remain = next_pf_position.short

        next_pf_position.prev_net = next_pf_position.yd_position_long - next_pf_position.yd_position_short
        session_portfolio.add(next_pf_position)
        i += 1

    session_portfolio.commit()
    server_host.close()
    print 'Insert next_trading_day:%s pf_position:%s' % (next_trading_day_str, i)
    print 'Exit pf_position_rebuild.py'


if __name__ == '__main__':
    options = parse_arguments()
    date = options.date
    if date is None or date == '':
        pf_position_rebuild_index()
    else:
        pf_position_rebuild_index(date)
