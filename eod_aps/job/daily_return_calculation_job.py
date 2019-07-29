# -*- coding: utf-8 -*-
from eod_aps.model.schema_jobs import DailyReturnHistory
from decimal import Decimal
from eod_aps.job import *


index_dict = {'000016.SH': 'SSE50', '000905.SH': 'SH000905', '000300.SH': 'SHSZ300'}


def index_return_calculation_job(filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y%m%d')

    query_tickers = ["'%s'" % key_value for key_value in index_dict.keys()]
    server_model = server_constant.get_server_model('wind_db')
    session_dump_wind = server_model.get_db_session('dump_wind')
    query_sql = "select t.s_info_windcode,t.trade_dt,t.s_dq_preclose,t.s_dq_close from aindexeodprices t \
where t.s_info_windcode in (%s) and t.trade_dt = '%s'" % (','.join(query_tickers), filter_date_str)

    daily_return_history_list = []
    query = session_dump_wind.execute(query_sql)
    for db_item in query:
        wind_code = db_item[0]
        trade_dt = db_item[1]
        prev_close = Decimal(db_item[2])
        close = Decimal(db_item[3])

        daily_return_history = DailyReturnHistory()
        daily_return_history.ticker = index_dict[wind_code]
        daily_return_history.date = '%s-%s-%s' % (trade_dt[:4], trade_dt[4:6], trade_dt[6:8])
        daily_return_history.prev_close = prev_close
        daily_return_history.close = close
        daily_return_history.return_rate = (close - prev_close) / prev_close
        daily_return_history_list.append(daily_return_history)
    server_model.close()

    custom_log.log_info_job('Insert items:%s' % len(daily_return_history_list))
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for daily_return_history in daily_return_history_list:
        session_jobs.add(daily_return_history)
    session_jobs.commit()
    server_host.close()


if __name__ == '__main__':
    index_return_calculation_job('20180813')
