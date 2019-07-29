# -*- coding: utf-8 -*-
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.tools.instrument_tools import query_all_instrument_dict
from eod_aps.model.eod_const import const
import pandas as pd

stock_utils = StockUtils()


def pf_position_cal_index(server_name):
    type_list = [const.INSTRUMENT_TYPE_ENUMS.Future, ]
    instrument_dict = query_all_instrument_dict('host', type_list)

    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    pf_position_list = []
    query_sql = "select a.DATE, b.FUND_NAME, a.SYMBOL,a.`LONG`,a.SHORT from portfolio.pf_position a LEFT JOIN \
portfolio.pf_account b on a.id = b.id where a.DATE >= '2017-11-13' and a.DATE < '2018-03-01' and b.FUND_NAME is not Null"
    for pf_position_item in session_portfolio.execute(query_sql):
        date_str = pf_position_item[0].strftime('%Y-%m-%d')
        ticker = pf_position_item[2]
        long_value = float(pf_position_item[3])
        short_value = float(pf_position_item[4])
        if ' ' in ticker:
            ticker = ticker.split(' ')[0]

        ticker_close_price = stock_utils.get_close(date_str.replace('-', ''), ticker)
        if ticker_close_price is None:
            continue

        future_flag = False
        if not ticker.isdigit():
            future_flag = True

        total_stocks_value = 0
        total_future_value = 0
        if future_flag:
            instrument_db = instrument_dict[ticker]
            total_future_value = (long_value - short_value) * float(ticker_close_price) * float(instrument_db.fut_val_pt)
        else:
            total_stocks_value = (long_value - short_value) * float(ticker_close_price)

        row_list = [date_str, pf_position_item[1], total_stocks_value, total_future_value]
        pf_position_list.append(row_list)

    risk_total_df = pd.DataFrame(pf_position_list, columns=['Date', 'Strategy', 'stocks_value', 'future_value'])

    risk_view_df = risk_total_df.groupby(['Date', 'Strategy']).sum()[['stocks_value', 'future_value']]

    risk_dict = risk_view_df.to_dict("index")

    update_sql_list = []
    update_sql_base = "update history.server_risk t set t.Total_Stocks_Value = '%s', t.Total_Future_Value = '%s' where t.server_name = '%s' and t.date = '%s' and t.strategy_name = '%s';"
    for (dict_key, dict_values) in risk_dict.items():
        if dict_values['stocks_value'] == 0 and  dict_values['future_value'] == 0:
            continue

        update_sql_str = update_sql_base % (dict_values['stocks_value'], dict_values['future_value'], server_name, dict_key[0], dict_key[1])
        update_sql_list.append(update_sql_str)

    with open('change_sql.txt', 'w+') as fr:
        fr.write('\n'.join(update_sql_list))


if __name__ == '__main__':
    pf_position_cal_index('citics')
