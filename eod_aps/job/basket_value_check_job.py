# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.tools.stock_wind_utils import StockWindUtils
from eod_aps.job import *

stock_utils = StockUtils()
Instrument_Type_Enums = const.INSTRUMENT_TYPE_ENUMS
STRATEGY_FUND_NAME_LIST = ['MultiFactor', ]


def get_ticker_type(ticker):
    stock_flag = True
    for i in ticker:
        if i.isalpha():
            stock_flag = False
    if stock_flag:
        return 'stock'
    else:
        return 'future'


class BasketValueReport(object):
    def __init__(self, server_list, date_str=None):
        self.__server_list = server_list
        if date_str is None:
            date_str = date_utils.get_today_str('%Y-%m-%d')
        self.__date_str = date_str
        self.__pf_position_list = []
        self.__ticker_price_dict = dict()
        self.__email_content_list = []

    def check_index(self):
        self.__load_from_db()
        for server_name in self.__server_list:
            self.__query_pf_account_dict(server_name)
        self.__calculation_basket_value()

    def __load_from_db(self):
        self.__future_dict = query_instrument_dict('host', [Instrument_Type_Enums.Future, ])

        with StockWindUtils() as stock_wind_utils:
            ticker_type_list = [const.INSTRUMENT_TYPE_ENUMS.CommonStock, const.INSTRUMENT_TYPE_ENUMS.Future]
            common_ticker_list = stock_wind_utils.get_ticker_list(ticker_type_list)
            self.__ticker_price_dict = stock_wind_utils.get_close_dict(self.__date_str, common_ticker_list)

    def __query_pf_account_dict(self, server_name):
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')

        pf_account_dict = {x.id: x for x in session_portfolio.query(PfAccount)
                                                             .filter(PfAccount.group_name.in_(STRATEGY_FUND_NAME_LIST))}
        for pf_position_db in session_portfolio.query(PfPosition).filter(PfPosition.date == self.__date_str,
                                                                         PfPosition.id.in_(pf_account_dict.keys())):
            pf_account_db = pf_account_dict[pf_position_db.id]
            if pf_account_db.group_name not in STRATEGY_FUND_NAME_LIST:
                continue

            volume = pf_position_db.long - pf_position_db.short
            if volume == 0:
                continue

            fund = pf_account_db.fund_name.split('-')[2]
            stock_value, future_value = 0., 0.
            ticker_wind = stock_utils.get_find_key(pf_position_db.symbol)
            if ticker_wind.isdigit():
                if 500000 <= int(ticker_wind) <= 600000:
                    continue
            ticker_price = self.__ticker_price_dict[ticker_wind]
            ticker_type = get_ticker_type(pf_position_db.symbol)
            if ticker_type == 'stock':
                stock_value = float(volume) * float(ticker_price)
            else:
                fut_val_pt = self.__future_dict[pf_position_db.symbol].fut_val_pt
                future_value = float(volume) * float(ticker_price) * float(fut_val_pt)
            self.__pf_position_list.append([fund, pf_account_db.group_name, pf_account_db.fund_name,
                                            int(stock_value), int(future_value)])

    def __calculation_basket_value(self):
        html_title = 'Strategy_Name,Stock_Value,Future_Value,Value_Diff'
        pf_position_df = pd.DataFrame(self.__pf_position_list, columns=['Fund', 'Group_Name', 'Strategy_Name',
                                                                        'Stock_Value', 'Future_Value'])
        for group_key1, group1 in pf_position_df.groupby(['Fund', 'Group_Name']):
            self.__email_content_list.append('<font>Basket_Type: %s_%s<br>' % (group_key1[0], group_key1[1]))
            strategy_value_list = []
            for group_key2, group2 in group1.groupby(['Strategy_Name', ]):
                stock_value_sum = group2['Stock_Value'].sum()
                future_value_sum = group2['Future_Value'].sum()
                strategy_value_list.append([group_key2, stock_value_sum, future_value_sum])
            strategy_value_df = pd.DataFrame(strategy_value_list, columns=['Strategy_Name', 'Stock_Value',
                                                                           'Future_Value'])
            strategy_value_df['Value_Diff'] = strategy_value_df['Stock_Value'] + strategy_value_df['Future_Value']
            strategy_value_df.loc['Total'] = strategy_value_df.sum()
            strategy_value_df['Strategy_Name'].iloc[-1] = 'Total'
            strategy_value_df['Stock_Value'] = strategy_value_df['Stock_Value'].apply(lambda x: '{:,}'.format(x))
            strategy_value_df['Future_Value'] = strategy_value_df['Future_Value'].apply(lambda x: '{:,}'.format(x))
            strategy_value_df['Value_Diff'] = strategy_value_df['Value_Diff'].apply(lambda x: '{:,}'.format(x))
            strategy_value_df.loc['Total'][3:] = strategy_value_df.loc['Total'][3:] \
                .apply(lambda x: x if int(x.replace(',', '')) < 1000000 else x + '(Warning)')

            report_list = np.array(
                strategy_value_df[['Strategy_Name', 'Stock_Value', 'Future_Value', 'Value_Diff']]).tolist()
            html_table_list = email_utils4.list_to_html(html_title, report_list)
            self.__email_content_list.extend(html_table_list)
            self.__email_content_list.append('<br><br>')

        if len(self.__email_content_list) > 0:
            email_utils4.send_email_group_all('Basket Value Check Report', ''.join(self.__email_content_list), 'html')


if __name__ == "__main__":
    all_trade_servers = server_constant.get_all_trade_servers()
    basket_value_check = BasketValueReport(all_trade_servers, '2019-03-14')
    basket_value_check.check_index()
