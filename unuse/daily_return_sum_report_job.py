# -*- coding: utf-8 -*-
import pandas as pd
import os
from eod_aps.job import *

strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'CSI300_MV10Norm']
report_folder_list = ['StockSelection', 'StockSelection_Long']
sum_save_folder = 'AllStratPerformance'

# base_folder = 'E:/dailyFiles/report'
base_folder = 'Z:/dailyjob/Report'


def __build_equity_stratperformance():
    for report_type in ('ret_report', 'pnl_report'):
        data = pd.DataFrame()
        count = 1
        for report_folder_name in report_folder_list:
            day_report_folder_name = '%s_%s' % (report_folder_name, now_date_str)
            for strategy_name in strategy_name_list:
                day_report_file_name = '%s_%s_%s.csv' % (strategy_name, report_type, now_date_str)
                day_report_file_path = os.path.join(base_folder, day_report_folder_name, day_report_file_name)
                df = pd.read_csv(day_report_file_path)
                length = len(df.columns)
                df.columns = [str(x) for x in range(length)]
                df = df[['0', str(length-1)]]
                df.columns = ['date', 'return_%d' % count]
                if len(data) == 0:
                    data = df
                else:
                    data = pd.merge(data, df, on='date', how='outer')
                count += 1
        data.columns = ['Date', 'Long_IndNorm_Hedge', 'Long_MV10Norm_Hedge', 'Long_Norm_Hedge', 'Long_MV5Norm_Hedge', \
                        'ZZ500_Norm_Hedge', 'CSI300_MV10Norm_Hedge','Long_IndNorm_Long', 'Long_MV10Norm_Long', \
                        'Long_Norm_Long', 'Long_MV5Norm_Long', 'ZZ500_Norm_Long', 'CSI300_MV10Norm_Long']
        data = data.sort_values('Date', ascending=False)
        data.to_csv(os.path.join(base_folder, sum_save_folder, 'StockSelection_%s_latest.csv' % report_type), index=False)
        data.to_csv(os.path.join(base_folder, sum_save_folder, 'StockSelection_%s_%s.csv' % (report_type, now_date_str)), index=False)


def __build_all_stratperformance():
    for report_type in ('ret_report', 'pnl_report'):
        data = pd.DataFrame()
        count = 1
        for report_folder_name in report_folder_list:
            day_report_folder_name = '%s_%s' % (report_folder_name, now_date_str)
            for strategy_name in strategy_name_list:
                day_report_file_name = '%s_%s_%s.csv' % (strategy_name, report_type, now_date_str)
                day_report_file_path = os.path.join(base_folder, day_report_folder_name, day_report_file_name)
                df = pd.read_csv(day_report_file_path)
                length = len(df.columns)
                df.columns = [str(x) for x in range(length)]
                df = df[['0', str(length-1)]]
                df.columns = ['date', 'return_%d' % count]
                if len(data) == 0:
                    data = df
                else:
                    data = pd.merge(data, df, on='date', how='outer')
                count += 1
        data.columns = ['Date', 'Long_IndNorm_Hedge', 'Long_MV10Norm_Hedge', 'Long_Norm_Hedge', 'Long_MV5Norm_Hedge', \
                        'ZZ500_Norm_Hedge', 'CSI300_MV10Norm_Hedge','Long_IndNorm_Long', 'Long_MV10Norm_Long', \
                        'Long_Norm_Long', 'Long_MV5Norm_Long', 'ZZ500_Norm_Long', 'CSI300_MV10Norm_Long']
        data = data.sort_values('Date', ascending=False)
        data.to_csv(os.path.join(base_folder, sum_save_folder, 'StockSelection_%s_latest.csv' % report_type), index=False)
        data.to_csv(os.path.join(base_folder, sum_save_folder, 'StockSelection_%s_%s.csv' % (report_type, now_date_str)), index=False)


def daily_return_sum_report_job():
    global now_date_str
    now_date_str = date_utils.get_today_str('%Y-%m-%d')
    __build_all_stratperformance()


def sum_equity():
    base_folder = 'Z:/dailyjob/Report/StockSelection_Long_2017-03-06'

    data = pd.DataFrame()
    for index in ('01','02','03','04','05','06','07','08','09','10'):
        for strategy_name in ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm']:
            report_file_name = 'strategy_report_%s_%s_2017-03-06.csv' % (strategy_name, index)
            report_file_path = os.path.join(base_folder, report_file_name)

            df = pd.read_csv(report_file_path)

            df_pnl = df[['date', 'equity_base']]
            df_pnl.columns = ['date', '%s_%s' % (strategy_name, index)]
            if len(data) == 0:
                data = df_pnl
            else:
                data = pd.merge(data, df_pnl, on='date', how='outer')

    for index in ('01','02','03','04','05'):
        for strategy_name in ['Long_MV5Norm']:
            report_file_name = 'strategy_report_%s_%s_2017-03-06.csv' % (strategy_name, index)
            report_file_path = os.path.join(base_folder, report_file_name)

            df = pd.read_csv(report_file_path)

            df_pnl = df[['date', 'equity_base']]
            df_pnl.columns = ['date', '%s_%s' % (strategy_name, index)]
            if len(data) == 0:
                data = df_pnl
            else:
                data = pd.merge(data, df_pnl, on='date', how='outer')
    data = data.fillna(0)
    data.index = data['date']
    data = data.drop('date', axis=1)
    data.to_csv(os.path.join(base_folder, 'equity_base_sum.csv'), index=True)

if __name__ == '__main__':
    daily_return_sum_report_job()