# -*- coding: utf-8 -*-
import json
import os
import pickle
import threading
import time
import traceback
from threading import Timer
from eod_aps.model.schema_history import ServerRisk
from eod_aps.model.schema_jobs import FundInfo, RiskManagement
from eod_aps.tools.tradeplat_message_tools import *
import pandas as pd
import numpy as np
from eod_aps.tools.tradeplat_position_tools import RiskView
from eod_aps.tools.ysquant_manager_tools import get_daily_data
from eod_aps.job import *

instrument_type_inversion_dict = custom_enum_utils.enum_to_dict(const.INSTRUMENT_TYPE_ENUMS, True)


class RiskCalculationJob(object):
    def __init__(self):
        self.base_fund_info_df = None
        self.base_adv_volume_df = None
        self.notify_flag = False

    def start_run(self):
        t = threading.Thread(target=self.__risk_calculation_thread, args=())
        t.start()

    def __risk_calculation_thread(self):
        try:
            self.__format_risk_management()
            self.base_fund_info_df = self.__query_fund_info_df()
            self.base_adv_volume_df = self.__query_adv_volume_df()

            validate_number = int(date_utils.get_today_str('%H%M%S'))
            if validate_number <= 93000:
                self.notify_flag = True

            while 90000 <= validate_number <= 183000:
                Timer(5, self.__calculation_by_message_timer, []).start()
                time.sleep(60)
                validate_number = int(date_utils.get_today_str('%H%M%S'))
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_job(error_msg)
            email_utils2.send_email_group_all('[Error]__risk_calculation_thread.', error_msg)

    def __format_risk_management(self):
        """
            更新有过期值的临时指标值
        """
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')

        today_str = date_utils.get_today_str('%Y-%m-%d')
        for item in session_jobs.query(RiskManagement):
            temp_fund_risk_list = []
            if item.fund_risk_list is None or item.fund_risk_list == '':
                continue

            for fund_risk_info in item.fund_risk_list.split(';'):
                [fund_name, warn_line, error_line,
                 temp_warn_line, temp_error_line, expiry_time] = fund_risk_info.split('|')
                if expiry_time != '' and date_utils.compare_date_str(today_str, expiry_time, '%Y-%m-%d'):
                    temp_warn_line, temp_error_line, expiry_time = '', '', ''
                temp_fund_risk_list.append('%s|%s|%s|%s|%s|%s' % (fund_name, warn_line, error_line,
                                                                  temp_warn_line, temp_error_line, expiry_time))
            item.fund_risk_list = ';'.join(temp_fund_risk_list)
            session_jobs.merge(item)
        session_jobs.commit()

    def __query_fund_info_df(self):
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        # 取三个交易日前的基金信息
        last_trading_day3 = date_utils.get_interval_trading_day_list(date_utils.get_today(), -4, '%Y-%m-%d')[-1]
        sql = "select product_name, net_asset_value, unit_net, sum_value, real_capital from jobs.asset_value_info \
    where date_str = '%s'" % last_trading_day3
        this_year_initial_val_dict = dict()
        sql_year = "select b.`product_name` AS `fund_name`,b.date_s AS `date`,b.`sum_value` AS `max_sum_value`,b.date_str AS \
`max_date`,d.`sum_value` AS `min_sum_value`,d.date_str AS `min_date`  from (select product_name,sum_value,date_str,date_format(`date_str`,'%Y')  AS `date_s` from (select * from asset_value_info order by date_str desc) as a group by product_name,date_format(a.`date_str`,'%Y')) as b  inner    join (select product_name,sum_value,date_str,date_format(`date_str`,'%Y')  AS `date_s` from (select * from asset_value_info order by date_str) as c group by product_name,date_format(c.`date_str`,'%Y')) as d on b.product_name=d.product_name and b.date_s=d.date_s"
        for line in session_jobs.execute(sql_year):
            if line[0] not in this_year_initial_val_dict:
                this_year_initial_val_dict[line[0]] = [line[2]]
            else:
                this_year_initial_val_dict[line[0]].append(line[2])
        last_fund_data_list = []
        for item in session_jobs.execute(sql):
            this_year_initial_val = 1
            if len(this_year_initial_val_dict[item[0]]) > 1:
                this_year_initial_val = this_year_initial_val_dict[item[0]][-2]
            last_fund_data_list.append(
                [item[0], float(item[1]), float(item[2]), float(item[3]), float(item[4]), float(this_year_initial_val)])
        title_list = ['FundName', 'Last_Net_Asset_Value', 'Last_Unit_Net', 'Last_Sum_Value', 'Real_Capital',
                      'This_Year_Initial_Val']
        last_fund_info_df = pd.DataFrame(last_fund_data_list, columns=title_list)

        # 最近两个交易日的收益信息
        server_risk_list = []
        start_date = date_utils.get_interval_trading_day_list(date_utils.get_today(), -3, '%Y-%m-%d')[-1]
        end_date = date_utils.get_last_trading_day('%Y-%m-%d')
        session_history = server_host.get_db_session('history')
        for item in session_history.query(ServerRisk).filter(ServerRisk.date.between(start_date, end_date)):
            fund_name = item.strategy_name.split('-')[2]
            server_risk_list.append([fund_name, item.total_pl])
        title_list = ['FundName', 'Last_TotalPnl']
        last_fund_risk_df = pd.DataFrame(server_risk_list, columns=title_list)
        # Last_TotalPnl存储近两个交易日的累计收益
        last_fund_risk_df = last_fund_risk_df.groupby(['FundName', ]).sum().reset_index()

        create_time_list = []
        today_str = date_utils.get_today_str('%Y-%m-%d')
        for fund_info_db in session_jobs.query(FundInfo):
            create_day_str = fund_info_db.create_time.strftime('%Y-%m-%d')
            interval_days = date_utils.get_interval_days(create_day_str, today_str)
            create_time_list.append([fund_info_db.name, interval_days])
        title_list = ['FundName', 'Create_Days']
        create_time_df = pd.DataFrame(create_time_list, columns=title_list)

        fund_report_df = pd.merge(last_fund_info_df, create_time_df, how='left', on=['FundName']).fillna(0)
        fund_report_df = pd.merge(fund_report_df, last_fund_risk_df, how='left', on=['FundName']).fillna(0)
        fund_report_df['Net_Asset_Value'] = fund_report_df['Last_Net_Asset_Value'] + fund_report_df['Last_TotalPnl']
        return fund_report_df

    def __query_adv_volume_df(self):
        last_trading_day = date_utils.get_last_trading_day('%Y%m%d')
        adv_volume_df = get_daily_data(last_trading_day, ["volume", ])
        adv_volume_df["volume_%s" % last_trading_day] = adv_volume_df["volume"].astype(str).replace('', 0).astype(float)

        adv_date_length = 20
        trading_day_list = date_utils.get_interval_trading_day_list(
            date_utils.string_toDatetime(last_trading_day, '%Y%m%d'), -1 * adv_date_length, '%Y%m%d')
        for filter_date_str in trading_day_list[1:]:
            temp_data_df = get_daily_data(filter_date_str, ["volume", ])
            adv_volume_df["volume_%s" % filter_date_str] = temp_data_df["volume"].astype(str).replace('', 0).astype(
                float)
        del adv_volume_df["volume"]
        trading_days_df = adv_volume_df.copy()
        trading_days_df[trading_days_df > 0.] = 1.

        adv_volume_df['volume_total'] = adv_volume_df.sum(axis=1).values
        adv_volume_df['trading_days'] = trading_days_df.sum(axis=1).values
        adv_volume_df["ADV_Volume"] = adv_volume_df["volume_total"] / adv_volume_df['trading_days']
        adv_volume_df["ADV_Volume"] = adv_volume_df["ADV_Volume"].fillna(0).astype(int)
        adv_volume_df['Ticker'] = adv_volume_df.index
        return adv_volume_df[['Ticker', 'ADV_Volume']]

    def __calculation_by_message_timer(self):
        risk_management_dict = dict()
        server_host = server_constant.get_server_model('host')
        session_jobs = server_host.get_db_session('jobs')
        for risk_management_db in session_jobs.query(RiskManagement):
            risk_management_dict[risk_management_db.monitor_index] = risk_management_db

        rm5_parameters_dict = json.loads(risk_management_dict['NAV_Change_ByUnderlying'].parameters)
        change_ratio = rm5_parameters_dict['change_ratio']

        fund_info_df = self.base_fund_info_df.copy()
        adv_volume_df = self.base_adv_volume_df.copy()

        ticker_info_df, risk_base_df = self.__query_risk_base_df()
        ticker_info_df = pd.merge(ticker_info_df, adv_volume_df, how='left', on=['Ticker']).fillna(0)

        fund_risk_df, underlying_risk_df, stock_risk_df = self.__format_risk_df(fund_info_df, ticker_info_df,
                                                                                risk_base_df, change_ratio)
        self.__filter_by_fund_name(risk_management_dict, fund_risk_df, underlying_risk_df, stock_risk_df)
        self.__build_risk_email(risk_management_dict, fund_risk_df, underlying_risk_df, stock_risk_df)
        self.__save_to_eod_pool(fund_risk_df, underlying_risk_df, stock_risk_df)

    def __query_risk_base_df(self):
        # total_dict = dict()
        # fr = open('../../cfg/aggregator_pickle_data.txt', 'rb')
        # for pool_name in ('market_dict', 'instrument_view_dict', 'order_dict', 'order_view_tree_dict', 'trade_list',
        #                   'risk_dict', 'position_dict', 'position_update_time'):
        #     total_dict[pool_name] = pickle.load(fr)
        # fr.close()
        # instrument_view_dict = total_dict['instrument_view_dict']
        # market_msg_dict = total_dict['market_dict']
        # risk_dict = total_dict['risk_dict']

        if 'instrument_view_dict' not in const.EOD_POOL or 'market_dict' not in const.EOD_POOL:
            return
        market_msg_dict = const.EOD_POOL['market_dict']
        instrument_view_dict = const.EOD_POOL['instrument_view_dict']
        risk_dict = const.EOD_POOL['risk_dict']
        ticker_info_df, risk_base_df = self.__format_risk_data(risk_dict, instrument_view_dict, market_msg_dict)
        return ticker_info_df, risk_base_df

    def __format_risk_df(self, fund_info_df, ticker_info_df, risk_base_df, change_ratio):
        fund_risk_df = self.__build_fund_risk_df(fund_info_df, risk_base_df)
        stock_risk_df, gem_risk_df = self.__build_ticker_risk_df(fund_info_df, ticker_info_df, risk_base_df)
        fund_risk_df = pd.merge(fund_risk_df, gem_risk_df, how='left', on=['FundName']).fillna(0)

        underlying_risk_df = self.__build_underlying_risk_df(fund_info_df, ticker_info_df, risk_base_df, change_ratio)
        return fund_risk_df, underlying_risk_df, stock_risk_df

    def __filter_by_fund_name(self, risk_management_dict, fund_risk_df, underlying_risk_df, stock_risk_df):
        for (monitor_index, risk_management_db) in risk_management_dict.items():
            include_fund_list = []
            for fund_risk_info in risk_management_db.fund_risk_list.split(';'):
                include_fund_list.append(fund_risk_info.split('|')[0])

            if monitor_index in ('NAV_Change_1D', 'NAV_Change_3D', 'NAV_Change_From_D1', 'Underlying_Count',
                                 'GEMPercent', 'NetDelta_Percentage'):
                fund_risk_df.loc[-fund_risk_df['FundName'].isin(include_fund_list), monitor_index] = None
            elif monitor_index in ('NAV_Change_ByUnderlying',):
                underlying_risk_df.loc[-underlying_risk_df['FundName'].isin(include_fund_list), monitor_index] = None
            elif monitor_index in ('NAVPercentageByStock', 'ADVPercentByStock'):
                stock_risk_df.loc[-stock_risk_df['FundName'].isin(include_fund_list), monitor_index] = None

    def __build_risk_email(self, risk_management_dict, fund_risk_df, underlying_risk_df, stock_risk_df):
        warning_message_list = []
        error_message_list = []
        for (monitor_index, risk_management_db) in risk_management_dict.items():
            temp_risk_list = []
            for fund_risk_info in risk_management_db.fund_risk_list.split(';'):
                [fund_name, warn_line, error_line,
                 temp_warn_line, temp_error_line, expiry_time] = fund_risk_info.split('|')
                if temp_warn_line is None or temp_warn_line == '':
                    warn_line_value = warn_line
                else:
                    warn_line_value = temp_warn_line

                if temp_error_line is None or temp_error_line == '':
                    error_line_value = error_line
                else:
                    error_line_value = temp_error_line
                temp_risk_list.append([fund_name, warn_line_value, error_line_value])
            single_risk_df = pd.DataFrame(temp_risk_list, columns=['FundName', 'Warn_Line', 'Error_Line'])
            single_risk_df['Warn_Line'] = single_risk_df['Warn_Line'].astype(float)
            single_risk_df['Error_Line'] = single_risk_df['Error_Line'].astype(float)

            if monitor_index in ('NAV_Change_1D', 'NAV_Change_3D', 'NAV_Change_From_D1', 'Underlying_Count'):
                temp_fund_risk_df = fund_risk_df.copy()
                temp_fund_risk_df = pd.merge(temp_fund_risk_df, single_risk_df, how='left', on=['FundName'])
                if monitor_index == 'Underlying_Count':
                    rm_parameters_dict = json.loads(risk_management_db.parameters)
                    check_days = float(rm_parameters_dict['check_days'])
                    temp_fund_risk_df = temp_fund_risk_df[temp_fund_risk_df['Create_Days'] > check_days]

                temp_filter_df = temp_fund_risk_df[
                    (temp_fund_risk_df[monitor_index] <= temp_fund_risk_df['Warn_Line']) &
                    (temp_fund_risk_df[monitor_index] > temp_fund_risk_df['Error_Line'])]
                filter_title_list = ['FundName', monitor_index]
                risk_warn_df = temp_filter_df.loc[:, filter_title_list]

                temp_filter_df = temp_fund_risk_df[temp_fund_risk_df[monitor_index] <= temp_fund_risk_df['Error_Line']]
                risk_error_df = temp_filter_df.loc[:, filter_title_list]
            elif monitor_index in ('GEMPercent',):
                temp_fund_risk_df = fund_risk_df.copy()
                temp_fund_risk_df = pd.merge(temp_fund_risk_df, single_risk_df, how='left', on=['FundName'])

                filter_title_list = ['FundName', monitor_index]
                risk_warn_df = temp_fund_risk_df[(temp_fund_risk_df[monitor_index] >= temp_fund_risk_df['Warn_Line']) &
                                                 (temp_fund_risk_df[monitor_index] < temp_fund_risk_df['Error_Line'])][
                    filter_title_list]

                risk_error_df = temp_fund_risk_df[temp_fund_risk_df[monitor_index] >= temp_fund_risk_df['Error_Line']][
                    filter_title_list]
            elif monitor_index in ('NAV_Change_ByUnderlying',):
                temp_underlying_risk_df = underlying_risk_df.copy()
                temp_underlying_risk_df = pd.merge(temp_underlying_risk_df, single_risk_df, how='left', on=['FundName'])

                filter_title_list = ['FundName', 'Ticker_Underlying', monitor_index]
                risk_warn_df = temp_underlying_risk_df[
                    (abs(temp_underlying_risk_df[monitor_index]) >= temp_underlying_risk_df['Warn_Line']) &
                    (abs(temp_underlying_risk_df[monitor_index]) < temp_underlying_risk_df['Error_Line'])][
                    filter_title_list]
                risk_error_df = temp_underlying_risk_df[
                    abs(temp_underlying_risk_df[monitor_index]) >= temp_underlying_risk_df['Error_Line']][
                    filter_title_list]
            elif monitor_index in ('NetDelta_Percentage',):
                temp_fund_risk_df = fund_risk_df.copy()
                temp_fund_risk_df = pd.merge(temp_fund_risk_df, single_risk_df, how='left', on=['FundName'])

                filter_title_list = ['FundName', monitor_index]
                risk_warn_df = \
                    temp_fund_risk_df[(abs(temp_fund_risk_df[monitor_index]) >= temp_fund_risk_df['Warn_Line']) &
                                      (abs(temp_fund_risk_df[monitor_index]) < temp_fund_risk_df['Error_Line'])][
                        filter_title_list]
                risk_error_df = \
                    temp_fund_risk_df[abs(temp_fund_risk_df[monitor_index]) >= temp_fund_risk_df['Error_Line']][
                        filter_title_list]
            elif monitor_index in ('NAVPercentageByStock',):
                temp_stock_risk_df = stock_risk_df.copy()
                temp_stock_risk_df = pd.merge(temp_stock_risk_df, single_risk_df, how='left', on=['FundName'])

                filter_title_list = ['FundName', 'Ticker', monitor_index]
                risk_warn_df = \
                    temp_stock_risk_df[(temp_stock_risk_df[monitor_index] >= temp_stock_risk_df['Warn_Line']) &
                                       (temp_stock_risk_df[monitor_index] < temp_stock_risk_df['Error_Line'])][
                        filter_title_list]
                risk_error_df = \
                    temp_stock_risk_df[temp_stock_risk_df[monitor_index] >= temp_stock_risk_df['Error_Line']][
                        filter_title_list]
            elif monitor_index in ('ADVPercentByStock',):
                temp_stock_risk_df = stock_risk_df.copy()
                temp_stock_risk_df = pd.merge(temp_stock_risk_df, single_risk_df, how='left', on=['FundName'])

                filter_title_list = ['FundName', 'Ticker', monitor_index]
                risk_warn_df = \
                    temp_stock_risk_df[(temp_stock_risk_df[monitor_index] >= temp_stock_risk_df['Warn_Line']) &
                                       (temp_stock_risk_df[monitor_index] < temp_stock_risk_df['Error_Line'])][
                        filter_title_list]
                risk_error_df = \
                    temp_stock_risk_df[temp_stock_risk_df[monitor_index] >= temp_stock_risk_df['Error_Line']][
                        filter_title_list]
            else:
                continue

            if risk_warn_df.size > 0:
                if monitor_index in ('NAV_Change_1D', 'NAV_Change_3D', 'NAV_Change_From_D1', 'GEMPercent',
                                     'NAVPercentageByStock', 'ADVPercentByStock', 'NetDelta_Percentage'):
                    risk_warn_df[monitor_index] = risk_warn_df[monitor_index].apply(lambda x: '%.2f%%' % (x * 100))
                elif monitor_index in ('NAV_Change_ByUnderlying',):
                    risk_warn_df[monitor_index] = risk_warn_df[monitor_index].apply(lambda x: '%.4f%%' % (x * 100))

                temp_warn_list = np.array(risk_warn_df).tolist()
                temp_html_list = email_utils2.list_to_html(','.join(filter_title_list), temp_warn_list)
                temp_html_list.insert(0, '<h>Risk Management:%s, Warning Cordon:%s</h>' % (
                    risk_management_db.monitor_index, ''))
                warning_message_list.extend(temp_html_list)
            if risk_error_df.size > 0:
                if monitor_index in ('NAV_Change_1D', 'NAV_Change_3D', 'NAV_Change_From_D1', 'GEMPercent',
                                     'NAVPercentageByStock', 'ADVPercentByStock', 'NetDelta_Percentage'):
                    risk_error_df[monitor_index] = risk_error_df[monitor_index].apply(lambda x: '%.2f%%' % (x * 100))
                elif monitor_index in ('NAV_Change_ByUnderlying',):
                    risk_error_df[monitor_index] = risk_error_df[monitor_index].apply(lambda x: '%.4f%%' % (x * 100))

                temp_error_list = np.array(risk_error_df).tolist()
                temp_html_list = email_utils2.list_to_html(','.join(filter_title_list), temp_error_list)
                temp_html_list.insert(0, '<h>Risk Management:%s, Error Cordon:%s</h>' % (
                    risk_management_db.monitor_index, ''))
                error_message_list.extend(temp_html_list)

        if (warning_message_list or error_message_list) and self.notify_flag:
            email_message_list = ['<h>--------------[Error Message]---------------------</h><br>']
            email_message_list.extend(error_message_list)
            email_message_list.append('<br><br><br><h>--------------[Warning Message]---------------------</h><br>')
            email_message_list.extend(warning_message_list)
            email_utils17.send_email_group_all(u'风控异常报告', ''.join(email_message_list), 'html')
            self.notify_flag = False

    def __build_fund_risk_df(self, fund_info_df, risk_base_df):
        temp_title_list = ['FundName', 'TotalPnl', 'Stocks_Value', 'Future_Value', 'Delta']
        fund_risk_df = risk_base_df[temp_title_list].groupby(['FundName', ]).sum().reset_index()

        risk_management_df = pd.merge(fund_info_df, fund_risk_df, how='left', on=['FundName']).fillna(0)
        # 风控1 一天亏损%
        risk_management_df['NAV_Change_1D'] = risk_management_df['TotalPnl'] / risk_management_df['Net_Asset_Value']
        # 风控2 三天亏损%
        risk_management_df['NAV_Change_3D'] = (risk_management_df['Last_TotalPnl'] + risk_management_df['TotalPnl']) / \
                                              risk_management_df['Last_Net_Asset_Value']
        # 风控3 累积亏损%
        risk_management_df['NAV_Change_From_D1'] = risk_management_df['Last_Sum_Value'] + risk_management_df[
            'NAV_Change_3D'] - 1

        # 风控4 标的总数
        fund_items_df = risk_base_df.groupby(['FundName'])['Ticker'].nunique().reset_index()
        fund_items_df.rename(columns={'Ticker': 'Underlying_Count'}, inplace=True)
        risk_management_df = pd.merge(risk_management_df, fund_items_df, how='left', on=['FundName']).fillna(0)
        # 风控9 方向风险敞口
        risk_management_df['NetDelta_Percentage'] = risk_management_df['Delta'] / risk_management_df['Net_Asset_Value']

        # 今年的收益
        # real_sum_unit_value = (risk_management_df['Last_TotalPnl'] + risk_management_df['TotalPnl'] +
        #                        risk_management_df['Last_Net_Asset_Value']) / risk_management_df['Real_Capital']
        risk_management_df['This_Year'] = (risk_management_df['Last_Sum_Value'] + risk_management_df['NAV_Change_3D'] - \
                                          risk_management_df['This_Year_Initial_Val'])/risk_management_df['This_Year_Initial_Val']

        return risk_management_df

    def __build_ticker_risk_df(self, fund_info_df, ticker_info_df, risk_base_df):
        temp_title_list = ['FundName', 'Ticker', 'Qty', 'TotalPnl']
        ticker_risk_df = risk_base_df[temp_title_list].groupby(['FundName', 'Ticker']).sum().reset_index()

        temp_title_list = ['Ticker', 'Type', 'LastPrice', 'ValPt', 'ADV_Volume']
        ticker_risk_df = pd.merge(ticker_risk_df, ticker_info_df[temp_title_list], how='left', on=['Ticker']).fillna(0)
        stock_risk_df = ticker_risk_df[ticker_risk_df['Type'] == 'CommonStock']

        temp_title_list = ['FundName', 'Net_Asset_Value']
        stock_risk_df = pd.merge(stock_risk_df, fund_info_df[temp_title_list], how='left', on=['FundName']).fillna(0)
        # 风控6 单票上限
        stock_risk_df['NAVPercentageByStock'] = stock_risk_df['Qty'] * stock_risk_df['LastPrice'] * \
                                                stock_risk_df['ValPt'] / stock_risk_df['Net_Asset_Value']
        # 风控8 个股%ADV上限
        stock_risk_df['ADVPercentByStock'] = stock_risk_df['Qty'] / stock_risk_df['ADV_Volume']
        stock_risk_df['ADVPercentByStock'][np.isinf(stock_risk_df['ADVPercentByStock'])] = 0
        # 风控7  创业板总比例
        stock_risk_df['Gem_Flag'] = stock_risk_df['Ticker'].apply(lambda x: 1 if x.startswith('3') else 0)
        gem_stock_risk_df = stock_risk_df[(stock_risk_df['Type'] == 'CommonStock') & (stock_risk_df['Gem_Flag'] == 1)]
        temp_title_list = ['FundName', 'NAVPercentageByStock']
        gem_risk_df = gem_stock_risk_df[temp_title_list].groupby(['FundName', ]).sum().reset_index()
        gem_risk_df.rename(columns={'NAVPercentageByStock': 'GEMPercent'}, inplace=True)
        return stock_risk_df, gem_risk_df

    def __build_underlying_risk_df(self, fund_info_df, ticker_info_df, risk_base_df, change_ratio):
        # 风控5  净值Chg%By标的
        temp_risk_df = pd.merge(risk_base_df, ticker_info_df, how='left', on=['Ticker']).fillna(0)

        option_risk_df = temp_risk_df[temp_risk_df['Type'] == 'Option']
        temp_risk_df['Change_Money_Option'] = option_risk_df['Qty'] * option_risk_df['Price_Underlying'] * 1 * \
                                              option_risk_df['ValPt'] * (change_ratio * option_risk_df['Ticker_Delta'] + \
                                                                         change_ratio * change_ratio * option_risk_df[
                                                                             'Ticker_Gamma'] * 0.5)

        other_risk_df = temp_risk_df[temp_risk_df['Type'] != 'Option']
        temp_risk_df['Change_Money_Other'] = other_risk_df['Qty'] * other_risk_df['LastPrice'] * other_risk_df[
            'ValPt'] * change_ratio

        temp_risk_df['Change_Money'] = temp_risk_df['Change_Money_Option'].fillna(0.) + temp_risk_df[
            'Change_Money_Other'].fillna(0.)
        underlying_risk_df = temp_risk_df[['FundName', 'Ticker_Underlying', 'Change_Money']].groupby(
            ['FundName', 'Ticker_Underlying']).sum().reset_index()
        underlying_risk_df = pd.merge(underlying_risk_df, fund_info_df[['FundName', 'Net_Asset_Value']], how='left',
                                      on=['FundName']).fillna(0)
        underlying_risk_df['NAV_Change_ByUnderlying'] = underlying_risk_df['Change_Money'] / underlying_risk_df[
            'Net_Asset_Value']
        return underlying_risk_df

    def __save_to_eod_pool(self, fund_risk_df, underlying_risk_df, stock_risk_df):
        risk_management_dict = dict()
        risk_management_dict['fund_risk_df'] = fund_risk_df
        risk_management_dict['underlying_risk_df'] = underlying_risk_df
        risk_management_dict['stock_risk_df'] = stock_risk_df
        risk_management_dict['update_time'] = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
        const.EOD_POOL['risk_management_dict'] = risk_management_dict

    def __format_risk_data(self, risk_dict, instrument_view_dict, market_msg_dict):
        ticker_info_list = []
        ticker_info_dict = dict()
        for (instrument_key, market_msg) in market_msg_dict.items():
            instrument_view = instrument_view_dict[instrument_key]
            ticker_info_dict[instrument_view.Ticker] = instrument_view
            ticker = instrument_view.Ticker
            ticker_type = instrument_type_inversion_dict[instrument_view.TypeID]
            if ticker_type == "Future":
                ticker_underlying = filter(lambda x: not x.isdigit(), ticker)
            elif ticker_type == "Option":
                ticker_underlying = instrument_view.TrackUnderlyingTickers[0]
            else:
                ticker_underlying = ticker

            last_price = instrument_view.NominalPrice
            ticker_delta = instrument_view.Delta
            ticker_gamma = instrument_view.Gamma
            val_pt = instrument_view.ValPT
            ticker_info_list.append(
                [ticker, ticker_type, ticker_underlying, last_price, ticker_delta, ticker_gamma, val_pt])
        title_list = ['Ticker', 'Type', 'Ticker_Underlying', 'LastPrice', 'Ticker_Delta', 'Ticker_Gamma', 'ValPt']
        ticker_info_df = pd.DataFrame(ticker_info_list, columns=title_list)

        table_list = []
        for (account_name, account_position_dict) in risk_dict.items():
            for (instrument_key, position_msg) in account_position_dict.items():
                (base_account_name, server_ip_str) = account_name.split('@')
                instrument_view = instrument_view_dict[instrument_key]
                risk_view = RiskView(instrument_view, position_msg, account_name)

                fund_name = base_account_name.split('-')[2]
                account = base_account_name
                ticker = instrument_view.Ticker

                price_underlying = 0
                ticker_type = instrument_type_inversion_dict[instrument_view.TypeID]
                if ticker_type == "Option":
                    ticker_underlying = instrument_view.TrackUnderlyingTickers[0].split(' ')[0]
                    underlying_instrument_view = ticker_info_dict[ticker_underlying]
                    price_underlying = underlying_instrument_view.NominalPrice

                qty = position_msg.Long - position_msg.Short
                total_pl = risk_view.total_pl
                total_stocks_value = risk_view.total_stocks_value
                total_future_value = risk_view.total_future_value

                delta = risk_view.delta
                gamma = risk_view.gamma
                row_list = [fund_name, account, ticker, price_underlying, qty, total_pl, total_stocks_value,
                            total_future_value, delta, gamma]
                table_list.append(row_list)
        title_list = ['FundName', 'Account', 'Ticker', 'Price_Underlying', 'Qty', 'TotalPnl', 'Stocks_Value',
                      'Future_Value', 'Delta', 'Gamma']
        risk_base_df = pd.DataFrame(table_list, columns=title_list)
        return ticker_info_df, risk_base_df


if __name__ == '__main__':
    risk_calculation_job = RiskCalculationJob()
    risk_calculation_job.start_run()
