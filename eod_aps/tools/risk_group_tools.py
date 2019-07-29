#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import pandas as pd
from eod_aps.model.eod_const import const
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.tradeplat_position_tools import RiskView


class RiskGroupTools(object):
    def __init__(self, summary_type):
        self.__summary_type = summary_type
        self.__instrument_view_dict = None
        self.__risk_dict = None
        self.__strategy_grouping_dict = None

    def risk_group_index(self):
        self.__instrument_view_dict = const.EOD_POOL['instrument_view_dict']
        self.__risk_dict = const.EOD_POOL['risk_dict']
        self.__strategy_grouping_dict = const.EOD_CONFIG_DICT['strategy_grouping_dict']

        group_dict = self.__strategy_group_dict()
        net_asset_df = self.__query_net_asset_df()

        group_risk_list = []
        for (strategy_name, strategy_risk_dict) in self.__risk_dict.items():
            if strategy_name not in group_dict:
                continue

            group_name = group_dict[strategy_name]
            for (instrument_key, position_msg) in strategy_risk_dict.items():
                instrument_view = self.__instrument_view_dict[instrument_key]

                (base_strategy_name, server_ip_str) = strategy_name.split('@')
                risk_view = RiskView(instrument_view, position_msg, base_strategy_name)
                group_risk_list.append([group_name, risk_view.total_stocks_value, risk_view.total_future_value,
                                        risk_view.trading_pl, position_msg.DayTradeFee, risk_view.position_pl,
                                        risk_view.total_pl, risk_view.delta, risk_view.gamma, risk_view.vega,
                                        risk_view.theta])

        df_title = ['Summary_Name', 'Total_Stocks_Value', 'Total_Future_Value', 'TradingPL', 'DayTradeFee',
                    'PositionPL', 'TotalPL', 'Delta', 'Gamma', 'Vega', 'Theta']
        base_group_risk_df = pd.DataFrame(group_risk_list, columns=df_title)
        group_risk_df = base_group_risk_df.groupby(['Summary_Name', ]).sum().reset_index()
        group_risk_df = group_risk_df.sort_values(by="Summary_Name")

        group_risk_df = pd.merge(group_risk_df, net_asset_df, how='left', on=['Summary_Name']).fillna(0)

        group_risk_df.loc['Total'] = group_risk_df.sum()
        group_risk_df['Summary_Name'].iloc[-1] = 'Z_Total'
        group_risk_df['Date_Str'].iloc[-1] = ''

        group_risk_df['Nav_Change'] = group_risk_df['TotalPL'] / group_risk_df['Net_value']
        group_risk_df['Nav_Change'] = group_risk_df['Nav_Change'].apply(lambda x: '%.2f%%' % (x * 100))

        group_init_title = ['Total_Stocks_Value', 'Total_Future_Value', 'TradingPL', 'DayTradeFee',
                            'PositionPL', 'TotalPL', 'Delta', 'Gamma', 'Vega', 'Theta', 'Net_value']
        group_risk_df[group_init_title] = group_risk_df[group_init_title].astype(int)
        group_risk_dict = group_risk_df.to_dict("index")
        return [y for (x, y) in group_risk_dict.items()]

    def __strategy_group_dict(self):
        strategy_group_dict = dict()
        if self.__summary_type in ('Strategy_Type', 'Undl_Tickers'):
            for (group_name, sub_grouping_dict) in self.__strategy_grouping_dict.items():
                for (sub_group_name, strategy_list) in sub_grouping_dict.items():
                    for strategy_name in strategy_list:
                        strategy_group_dict[strategy_name] = group_name

        group_dict = dict()
        for (strategy_name, strategy_risk_dict) in self.__risk_dict.items():
            group_name = None
            strategy_name_item = strategy_name.split('-')
            if self.__summary_type == 'Strategy_Type':
                group_name = strategy_group_dict[strategy_name_item[1]]
            elif self.__summary_type == 'Account':
                group_name = strategy_name_item[2]
            elif self.__summary_type == 'Undl_Tickers':
                strategy_group_name = strategy_group_dict[strategy_name_item[1]]
                if strategy_group_name != 'CTA':
                    continue
                for (instrument_key, position_msg) in strategy_risk_dict.items():
                    ticker = self.__instrument_view_dict[instrument_key].Ticker
                    group_name = filter(lambda x: not x.isdigit(), ticker)
                    break
            else:
                continue

            if group_name is not None:
                group_dict[strategy_name] = group_name
        return group_dict

    def __query_net_asset_df(self):
        net_asset_list = []
        if self.__summary_type == 'Account':
            server_host = server_constant.get_server_model('host')
            session = server_host.get_db_session('jobs')
            query_sql = 'select product_name, net_asset_value, date_str from \
(select * from asset_value_info order by date_str desc) as a group by product_name'
            for line in session.execute(query_sql):
                product_name, net_asset_value, date_str = line
                net_asset_list.append([product_name, float(net_asset_value), str(date_str)])
        net_asset_df = pd.DataFrame(net_asset_list, columns=['Summary_Name', 'Net_value', 'Date_Str'])
        return net_asset_df


if __name__ == '__main__':
    risk_group_tools = RiskGroupTools('Strategy_Type')
    risk_group_tools.risk_group_index()
