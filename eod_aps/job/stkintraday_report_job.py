# -*- coding: utf-8 -*-
import codecs
import os
import re
import shutil
import tarfile
import pandas as pd
import numpy as np
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.job import *


server_constant = ServerConstant()
date_utils = DateUtils()
Instrument_Type_Enum = const.INSTRUMENT_TYPE_ENUMS
buy_commission = 0.00025
sell_commission = 0.00125
base_export_folder = 'Z:/strategy/intraday_deep_learning/report'
# base_export_folder = 'G:/report'


class StkintradayReport(object):
    def __init__(self, server_name, account_list, date_str=None):
        self.__server_name = server_name
        self.__account_list = account_list
        if date_str is None:
            date_str = date_utils.get_today_str()
        self.__date_str = date_str
        self.__target_file_path = None

        self.__log_backup_folder_path = const.EOD_CONFIG_DICT['log_backup_folder_template'] % self.__server_name
        self.__unzip_folder_path = '%s/unzip' % self.__log_backup_folder_path
        self.__total_volume_dict = dict()

    def __download_log_files(self):
        server_model = server_constant.get_server_model(self.__server_name)
        tar_file_name = 'stkintraday_log_%s.tar.gz' % date_utils.get_today_str('%Y%m%d%H%M%S')
        cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                    'tar -zcf %s *StrategyLoader_%s*.log  *TFCalculator_%s*.log *IdxCalculator_%s*.log' % \
                    (tar_file_name, self.__date_str, self.__date_str, self.__date_str)
                    ]
        server_model.run_cmd_str(';'.join(cmd_list))

        source_file_path = '%s/%s' % (server_model.server_path_dict['tradeplat_log_folder'], tar_file_name)
        target_folder_path = const.EOD_CONFIG_DICT['log_backup_folder_template'] % self.__server_name
        self.__target_file_path = '%s/%s' % (target_folder_path, tar_file_name)
        server_model.download_file(source_file_path, self.__target_file_path)

    def __unzip_log_files(self):
        if os.path.exists(self.__unzip_folder_path):
            shutil.rmtree(self.__unzip_folder_path)
        os.mkdir(self.__unzip_folder_path)

        if self.__target_file_path is None:
            for temp_file_name in os.listdir(self.__log_backup_folder_path):
                if 'stkintraday_log_' in str(temp_file_name) and '.tar.gz' in str(temp_file_name) \
                    and self.__date_str in str(temp_file_name):
                    self.__target_file_path = os.path.join(self.__log_backup_folder_path, temp_file_name)
                    break

        custom_log.log_info_job('UnZip File:%s' % self.__target_file_path)
        tar = tarfile.open(self.__target_file_path)
        names = tar.getnames()
        for name in names:
            tar.extract(name, path=self.__unzip_folder_path)
        tar.close()

    def __read_message_list(self, account_name, position_list, order_list, param_set):
        order_info_list = []
        for order_info in order_list:
            line_dict = self.__format_trade_line(order_info)
            ticker = line_dict['ticker_str'].split('_')[-1]
            if '.' in line_dict['trade_time']:
                trade_time = date_utils.string_toDatetime(line_dict['trade_time'], format_str='%Y-%b-%d %H:%M:%S.%f')
            else:
                trade_time = date_utils.string_toDatetime(line_dict['trade_time'], format_str='%Y-%b-%d %H:%M:%S')
            order_info_list.append([line_dict['OrdId'], ticker, line_dict['side'], line_dict['ex_qty'],
                                    line_dict['last_ex_price'], line_dict['status'], trade_time])
        order_df = pd.DataFrame(order_info_list,
                                columns=['OrdId', 'Ticker', 'Side', 'Qty', 'Price', 'Status', 'Trade_Time'])

        fill_trade_df = order_df[order_df['Status'].isin(['Filled', 'PartialFilled'])]
        fill_trade_df.loc[:, 'Qty'] = fill_trade_df["Qty"].astype(float)
        fill_trade_df = fill_trade_df.sort_values(by=["Trade_Time", "Qty"], ascending=[True, True])

        # 去重，避免问题1
        fill_trade_df = fill_trade_df.drop_duplicates('OrdId', keep='last')
        if len(fill_trade_df) == 0:
            return

        fill_trade_df.loc[:, 'Price'] = fill_trade_df["Price"].astype(float)
        fill_trade_df.loc[:, 'MarketValue'] = fill_trade_df['Qty'] * fill_trade_df['Price']

        fill_trade_df.loc[:, 'Commission'] = 0
        fill_trade_df.loc[fill_trade_df['Side'] == 'Buy', 'Commission'] = buy_commission
        fill_trade_df.loc[fill_trade_df['Side'] == 'Sell', 'Commission'] = sell_commission
        fill_trade_df.loc[:, 'Fee'] = fill_trade_df['MarketValue'] * fill_trade_df['Commission']

        fill_trade_dict = dict()
        for group_key, ticker_trade_df in fill_trade_df.groupby(['Ticker', ]):
            fill_trade_dict[group_key] = ticker_trade_df

        format_message_list = []
        for line_info in position_list:
            if 'StkIntraDayStrategy' not in line_info:
                continue

            if 'Info:' in line_info:
                line_dict = self.__format_position_line(line_info)
            elif 'Posi:' in line_info:
                line_dict = self.__format_position_line2(line_info)
            else:
                continue

            if '.' in line_dict['trade_time']:
                trade_time = date_utils.string_toDatetime(line_dict['trade_time'], format_str='%Y-%b-%d %H:%M:%S.%f')
            else:
                trade_time = date_utils.string_toDatetime(line_dict['trade_time'], format_str='%Y-%b-%d %H:%M:%S')

            ticker = line_dict['ticker_str'].split('_')[-1]
            buy_number = int(line_dict['buy_number'])
            sell_number = int(line_dict['sell_number'])
            volume = buy_number - sell_number

            format_message_list.append([trade_time, ticker, volume])
        position_df = pd.DataFrame(format_message_list, columns=['Trade_Time', 'Ticker', 'Volume'])

        export_folder_path = '%s/%s_%s_%s' % (base_export_folder, self.__date_str,
                                              self.__server_name, account_name)
        if not os.path.exists(export_folder_path):
            os.mkdir(export_folder_path)

        total_report_df_dict = dict()
        for ticker, ticker_position_df in position_df.groupby(['Ticker', ]):
            if ticker not in fill_trade_dict:
                continue
            ticker_trade_df = fill_trade_dict[ticker]

            report_message_list = []
            between_time_list = self.__query_time_list(ticker_position_df)
            if len(between_time_list) == 0:
                continue

            for (start_time, end_time) in between_time_list:
                filter_position_df = ticker_position_df[(ticker_position_df['Trade_Time'] >= start_time) &
                                                        (ticker_position_df['Trade_Time'] <= end_time)]
                filter_trade_df = ticker_trade_df[(ticker_trade_df['Trade_Time'] > start_time) &
                                                  (ticker_trade_df['Trade_Time'] <= end_time)]

                if start_time.strftime("%H:%M:%S") < '11:30:00' and end_time.strftime("%H:%M:%S") > '13:00:00':
                    interval_seconds = (end_time - start_time).seconds - 60 * 90
                else:
                    interval_seconds = (end_time - start_time).seconds

                buy_trade_df = filter_trade_df[filter_trade_df['Side'] == 'Buy']
                sell_trade_df = filter_trade_df[filter_trade_df['Side'] == 'Sell']
                total_buy_volume = int(buy_trade_df['Qty'].sum())
                total_sell_volume = int(sell_trade_df['Qty'].sum())

                buy_money = buy_trade_df['MarketValue'].sum()
                sell_money = sell_trade_df['MarketValue'].sum()
                if total_buy_volume > total_sell_volume and len(sell_trade_df) > 0:
                    temp_trade_price = sell_trade_df.iloc[-1]['Price']
                    margin = sell_money + (total_buy_volume - total_sell_volume) * temp_trade_price - buy_money
                elif total_buy_volume < total_sell_volume and len(buy_trade_df) > 0:
                    temp_trade_price = buy_trade_df.iloc[-1]['Price']
                    margin = sell_money + (total_buy_volume - total_sell_volume) * temp_trade_price - buy_money
                else:
                    margin = sell_money - buy_money

                fee = filter_trade_df['Fee'].sum()
                net_margin = margin - fee

                filter_position_df.loc[:, 'Abs_Volume'] = filter_position_df['Volume'].abs()
                max_volume = filter_position_df['Abs_Volume'].max()

                report_message_list.append(
                    [ticker, start_time, end_time, buy_money, sell_money, margin, fee, net_margin,
                     margin / buy_money, net_margin / buy_money,
                     max_volume, total_buy_volume, total_sell_volume, interval_seconds])
            report_message_title = [u'合约', u'开始时间', u'结束时间', u'总买入', u'总卖出', u'毛利',
                                    u'交易费用', u'净利', u'毛利率', u'净利率', u'最大持仓量',
                                    u'累计买入量', u'累计卖出量', u'持仓时间']
            ticker_interval_df = pd.DataFrame(report_message_list, columns=report_message_title)
            ticker_interval_df = ticker_interval_df[
                abs(ticker_interval_df[u'累计买入量'] - ticker_interval_df[u'累计卖出量']) < 100]
            if len(ticker_interval_df) == 0:
                continue

            buy_money = ticker_interval_df[u'总买入'].sum()
            sell_money = ticker_interval_df[u'总卖出'].sum()
            margin = ticker_interval_df[u'毛利'].sum()

            fee = ticker_interval_df[u'交易费用'].sum()
            net_margin = ticker_interval_df[u'净利'].sum()
            trade_volume = ticker_interval_df[u'累计买入量'].sum()
            total_report_list = []

            profit_net_return = 0
            loss_net_return = 0
            if len(ticker_interval_df.loc[ticker_interval_df[u'净利率'] >= 0, u'净利率']) > 0:
                profit_net_return = ticker_interval_df.loc[ticker_interval_df[u'净利率'] >= 0, u'净利率'].mean()
            if len(ticker_interval_df.loc[ticker_interval_df[u'净利率'] < 0, u'净利率']) > 0:
                loss_net_return = ticker_interval_df.loc[ticker_interval_df[u'净利率'] < 0, u'净利率'].mean()
            if profit_net_return == 0:
                profit_loss_ratio = 0
            elif loss_net_return == 0:
                profit_loss_ratio = 999
            else:
                profit_loss_ratio = abs(profit_net_return / loss_net_return)

            total_volume = self.__total_volume_dict['%s|%s' % (account_name, ticker)]
            prev_close = instrument_db_dict[ticker].prev_close
            total_report_list.append([ticker, buy_money, sell_money, margin, fee, net_margin, margin / buy_money,
                                      net_margin / buy_money, ticker_interval_df[u'最大持仓量'].max(),
                                      ticker_interval_df[u'最大持仓量'].min(), int(ticker_interval_df[u'最大持仓量'].mean()),
                                      int(trade_volume), trade_volume * 1. / total_volume,
                                      ticker_interval_df[u'持仓时间'].max(),
                                      ticker_interval_df[u'持仓时间'].min(), ticker_interval_df[u'持仓时间'].mean(),
                                      len(ticker_interval_df[ticker_interval_df[u'净利'] >= 0]) * 1. / len(
                                          ticker_interval_df),
                                      profit_loss_ratio, total_volume, total_volume * float(prev_close)
                                      ])

            report_message_title = [u'合约', u'总买入', u'总卖出', u'毛利', u'交易费用', u'净利', u'毛利率', u'净利率', u'最大持仓量',
                                    u'最小持仓量', u'平均持仓量', u'累计交易量', u'仓位使用率', u'最长持仓时间', u'最短持仓时间',
                                    u'平均持仓时间', u'胜率', u'盈亏比', u'初始仓位', u'仓位市值']
            total_report_df = pd.DataFrame(total_report_list, columns=report_message_title)

            check_df = ticker_interval_df[abs(ticker_interval_df[u'累计买入量'] - ticker_interval_df[u'累计卖出量']) >= 100]
            if len(check_df) > 0:
                email_utils2.send_email_group_all(u'[Error]日内仓位异常_%s' % account_name, check_df.to_html(index=False),
                                                  'html')
            else:
                total_report_df_dict[ticker] = total_report_df

            ticker_interval_df[u'毛利'] = ticker_interval_df[u'毛利'].apply(lambda x: '%.4f' % x)
            ticker_interval_df[u'交易费用'] = ticker_interval_df[u'交易费用'].apply(lambda x: '%.4f' % x)
            ticker_interval_df[u'净利'] = ticker_interval_df[u'净利'].apply(lambda x: '%.4f' % x)
            ticker_interval_df[u'毛利率'] = ticker_interval_df[u'毛利率'].apply(lambda x: '%.4f%%' % (x * 100))
            ticker_interval_df[u'净利率'] = ticker_interval_df[u'净利率'].apply(lambda x: '%.4f%%' % (x * 100))

            total_report_df[u'毛利'] = total_report_df[u'毛利'].apply(lambda x: '%.4f' % x)
            total_report_df[u'交易费用'] = total_report_df[u'交易费用'].apply(lambda x: '%.4f' % x)
            total_report_df[u'净利'] = total_report_df[u'净利'].apply(lambda x: '%.4f' % x)
            total_report_df[u'毛利率'] = total_report_df[u'毛利率'].apply(lambda x: '%.4f%%' % (x * 100))
            total_report_df[u'净利率'] = total_report_df[u'净利率'].apply(lambda x: '%.4f%%' % (x * 100))
            total_report_df[u'仓位使用率'] = total_report_df[u'仓位使用率'].apply(lambda x: '%.4f%%' % (x * 100))
            total_report_df[u'胜率'] = total_report_df[u'胜率'].apply(lambda x: '%.4f%%' % (x * 100))
            total_report_df[u'仓位市值'] = total_report_df[u'仓位市值'].apply(lambda x: '%.1f' % x)

            if end_time is not None:
                output_file_path = '%s/%s_%s_%s.html' % (export_folder_path, ticker, param_set, end_time.strftime("%Y-%m-%d"))
                with codecs.open(output_file_path, 'w+', 'utf-8') as fr:
                    fr.write(ticker_interval_df.to_html(index=False))
                    fr.write('<br><br><br><br>')
                    fr.write(total_report_df.to_html(index=False))
                    fr.write('<br><br><br><br>')
                    fr.write(ticker_trade_df.to_html(index=False))
                    fr.write('<br><br><br><br>')
                    fr.write(ticker_position_df.to_html(index=False))

        ticker_report_dict = dict()
        for (dict_key, total_volume) in self.__total_volume_dict.items():
            temp_account_name, group_key = dict_key.split('|')
            if temp_account_name != account_name:
                continue

            if group_key in total_report_df_dict:
                total_report_df = total_report_df_dict[group_key]
                ticker_report_dict.setdefault(param_set, []).append(total_report_df)
            else:
                total_volume = self.__total_volume_dict['%s|%s' % (account_name, group_key)]
                prev_close = instrument_db_dict[group_key].prev_close
                total_report_list = [[group_key, 0, 0, 0, 0, 0, '0', '0', 0, 0, 0, 0, '0', 0, 0, 0, '0', 0,
                                     total_volume, total_volume * float(prev_close)
                                      ]]
                report_message_title = [u'合约', u'总买入', u'总卖出', u'毛利', u'交易费用', u'净利', u'毛利率', u'净利率', u'最大持仓量',
                                        u'最小持仓量', u'平均持仓量', u'累计交易量', u'仓位使用率', u'最长持仓时间', u'最短持仓时间',
                                        u'平均持仓时间', u'胜率', u'盈亏比', u'初始仓位', u'仓位市值']
                total_report_df = pd.DataFrame(total_report_list, columns=report_message_title)
                ticker_report_dict.setdefault(param_set, []).append(total_report_df)
        self.__build_daily_report(ticker_report_dict, export_folder_path)

    def __build_daily_report(self, ticker_report_dict, export_folder_path):
        for param_set, df_list in ticker_report_dict.items():
            ticker_detailed_df = pd.concat(df_list)
            total_money = ticker_detailed_df[u'仓位市值'].astype(float).sum()

            ticker_number1 = len(ticker_detailed_df[ticker_detailed_df[u'初始仓位'] > 0])
            ticker_number2 = len(ticker_detailed_df)

            format_detailed_df = ticker_detailed_df[(ticker_detailed_df[u'总买入'] > 0) | (ticker_detailed_df[u'总卖出'] > 0)]
            format_detailed_df.loc[:, u'毛利'] = format_detailed_df[u'毛利'].astype(float)
            format_detailed_df.loc[:, u'交易费用'] = format_detailed_df[u'交易费用'].astype(float)
            format_detailed_df.loc[:, u'净利'] = format_detailed_df[u'净利'].astype(float)
            format_detailed_df.loc[:, u'毛利率'] = format_detailed_df[u'毛利率'].apply(lambda x: float(x.replace('%', '')))
            format_detailed_df.loc[:, u'净利率'] = format_detailed_df[u'净利率'].apply(lambda x: float(x.replace('%', '')))
            format_detailed_df.loc[:, u'仓位使用率'] = format_detailed_df[u'仓位使用率'].apply(lambda x: float(x.replace('%', '')))
            format_detailed_df.loc[:, u'胜率'] = format_detailed_df[u'胜率'].apply(lambda x: float(x.replace('%', '')))
            format_detailed_df.loc[:, u'仓位市值'] = format_detailed_df[u'仓位市值'].astype(float)

            buy_money = format_detailed_df[u'总买入'].sum()
            sell_money = format_detailed_df[u'总卖出'].sum()
            margin = format_detailed_df[u'毛利'].sum()
            fee = format_detailed_df[u'交易费用'].sum()
            net_margin = format_detailed_df[u'净利'].sum()
            total_user_money = format_detailed_df[u'仓位市值'].sum()

            maoli_avg = margin / buy_money
            jingli_avg = net_margin / buy_money
            ticker_use_number = len(format_detailed_df[format_detailed_df[u'累计交易量'] > 0])

            position_ratio_max = format_detailed_df[u'仓位使用率'].max()
            position_ratio_min = format_detailed_df[u'仓位使用率'].min()
            position_ratio_mean = format_detailed_df[u'仓位使用率'].mean()

            holding_time_max = format_detailed_df[u'平均持仓时间'].max()
            holding_time_min = format_detailed_df[u'平均持仓时间'].min()
            holding_time_mean = format_detailed_df[u'平均持仓时间'].mean()

            win_max = format_detailed_df[u'胜率'].max()
            win_min = format_detailed_df[u'胜率'].min()
            win_mean = format_detailed_df[u'胜率'].mean()

            profit_avg = format_detailed_df[format_detailed_df[u'净利率'] >= 0][u'净利率'].mean()
            loss_avg = abs(format_detailed_df[format_detailed_df[u'净利率'] < 0][u'净利率'].mean())

            if profit_avg == 0:
                profit_loss_ratio = 0
            elif loss_avg == 0:
                profit_loss_ratio = 999
            else:
                profit_loss_ratio = abs(profit_avg / loss_avg)

            daily_report_list = [
                [margin, fee, net_margin, maoli_avg, jingli_avg, ticker_use_number, ticker_number1, ticker_number2, position_ratio_mean,
                 position_ratio_max, position_ratio_min, holding_time_mean, holding_time_max,
                 holding_time_min, win_mean, win_max, win_min, profit_avg, loss_avg, profit_loss_ratio,
                 buy_money, total_user_money, buy_money/total_user_money, total_money, buy_money/total_money
                 ]]

            report_message_title = [u'毛利', u'交易费用', u'净利', u'平均毛利率', u'平均净利率', u'股票使用量', u'股票总量1', u'股票总量2', u'仓位使用率平均值',
                                    u'仓位使用率最大值', u'仓位使用率最小值', u'平均持仓时间平均值', u'平均持仓时间最大值', u'平均持仓时间最小值',
                                    u'胜率平均值', u'胜率最大值', u'胜率最小值', u'平均每笔净盈利', u'平均每笔净亏损', u'盈亏比',
                                    u'总买入', u'使用股票市值', u'资金使用率1', u'总仓位市值', u'资金使用率2']
            daily_report_df = pd.DataFrame(daily_report_list, columns=report_message_title)
            daily_report_df[u'平均毛利率'] = daily_report_df[u'平均毛利率'].apply(lambda x: '%.4f%%' % (x * 100))
            daily_report_df[u'平均净利率'] = daily_report_df[u'平均净利率'].apply(lambda x: '%.4f%%' % (x * 100))
            daily_report_df[u'仓位使用率平均值'] = daily_report_df[u'仓位使用率平均值'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'仓位使用率最大值'] = daily_report_df[u'仓位使用率最大值'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'仓位使用率最小值'] = daily_report_df[u'仓位使用率最小值'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'资金使用率1'] = daily_report_df[u'资金使用率1'].apply(lambda x: '%.4f%%' % (x * 100))
            daily_report_df[u'资金使用率2'] = daily_report_df[u'资金使用率2'].apply(lambda x: '%.4f%%' % (x * 100))

            daily_report_df[u'胜率平均值'] = daily_report_df[u'胜率平均值'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'胜率最大值'] = daily_report_df[u'胜率最大值'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'胜率最小值'] = daily_report_df[u'胜率最小值'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'平均每笔净盈利'] = daily_report_df[u'平均每笔净盈利'].apply(lambda x: '%.4f%%' % x)
            daily_report_df[u'平均每笔净亏损'] = daily_report_df[u'平均每笔净亏损'].apply(lambda x: '%.4f%%' % x)

            output_file_path = '%s/param%s_risk_total.csv' % (export_folder_path, param_set)
            daily_report_df.to_csv(output_file_path, index=False, encoding="gbk")

            output_file_path = '%s/param%s_risk_detail.csv' % (export_folder_path, param_set)
            ticker_detailed_df.to_csv(output_file_path, index=False, encoding="gbk")

            output_file_path = '%s/param%s_total.html' % (export_folder_path, param_set)
            with codecs.open(output_file_path, 'w+', 'utf-8') as fr:
                fr.write(daily_report_df.to_html(index=False))
                fr.write('<br><br><br><br>')
                fr.write(ticker_detailed_df.to_html(index=False))

    def __build_report_files(self):
        global instrument_db_dict
        instrument_db_dict = query_instrument_dict('host', [Instrument_Type_Enums.CommonStock, ])

        log_file_list = [x for x in os.listdir(self.__unzip_folder_path)]
        log_file_list.sort()

        position_list = []
        order_list = []
        for log_file_name in log_file_list:
            if 'StrategyLoader' not in log_file_name:
                continue
            log_file_path = '%s/%s' % (self.__unzip_folder_path, log_file_name)
            with open(log_file_path) as fr:
                for line in fr.readlines():
                    if 'OnStratStart' in line:
                        param_set_dict = self.__format_param_line(line)
                        if not param_set_dict:
                            continue
                        param_set = param_set_dict['param_set']
                    elif 'SIDSImpl' in line and 'Info:' in line:
                        position_list.append(line)
                    elif 'SIDSImpl' in line and 'Posi:' in line:
                        position_list.append(line)
                    elif 'SIDSImpl' in line and 'Event:' in line:
                        order_list.append(line)

        for account_name in self.__account_list:
            filter_position_list = [x for x in position_list if account_name in x]
            filter_order_list = [x for x in order_list if account_name in x]
            self.__read_message_list(account_name, filter_position_list, filter_order_list, param_set)

    def report_index(self):
        self.__download_log_files()
        self.__unzip_log_files()
        self.__build_report_files()

    def __format_param_line(self, line_info):
        reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] \
OnStratStart: param_set=(?P<param_set>.*), coefficient=(?P<coefficient>.*), max_place_ratio=(?P<max_place_ratio>.*), \
threshold_long_close=(?P<threshold_long_close>.*), threshold_short_close=(?P<threshold_short_close>.*), \
min_active_seconds=(?P<min_active_seconds>.*), unit_qty=(?P<unit_qty>.*), initial_shares=(?P<initial_shares>.*)'
        reg = re.compile(reg_line)
        reg_match = reg.match(line_info)
        try:
            param_set_dict = reg_match.groupdict()
        except AttributeError:
            return {}

        account_name = ''
        for x in self.__account_list:
            if x in param_set_dict['ticker_str']:
                account_name = x
        ticker = param_set_dict['ticker_str'].split('_')[-1]
        self.__total_volume_dict['%s|%s' % (account_name, ticker)] = float(param_set_dict['initial_shares'])
        return param_set_dict

    def __format_trade_line(self, line_info):
        reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] (?P<trade_time>.*) \
Event: OrdId=(?P<OrdId>.*), side=(?P<side>.*), price=(?P<price>.*), qty=(?P<qty>.*), ex_qty=(?P<ex_qty>.*), \
last_ex_price=(?P<last_ex_price>.*), status=(?P<status>.*), create=(?P<create>.*)'
        reg = re.compile(reg_line)
        reg_match = reg.match(line_info)
        line_dict = reg_match.groupdict()
        return line_dict

    def __format_position_line(self, line_info):
        reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] (?P<trade_time>.*) \
Info: tbuy=(?P<buy_number>.*), tsell=(?P<sell_number>.*), obuy=(?P<obuy>.*), osell=(?P<osell>.*), bvwap=(?P<bvwap>.*), \
svwap=(?P<svwap>.*), sig=(?P<sig>.*), sig_adj=(?P<sig_adj>.*), sig_Last=(?P<sig_Last>.*)'
        reg = re.compile(reg_line)
        reg_match = reg.match(line_info)
        line_dict = reg_match.groupdict()
        return line_dict

    def __format_position_line2(self, line_info):
        reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] (?P<trade_time>.*) \
Posi: tbuy=(?P<buy_number>.*), tsell=(?P<sell_number>.*), obuy=(?P<obuy>.*), osell=(?P<osell>.*), bvwap=(?P<bvwap>.*), \
svwap=(?P<svwap>.*)'
        reg = re.compile(reg_line)
        reg_match = reg.match(line_info)
        line_dict = reg_match.groupdict()
        return line_dict

    def __query_time_list(self, ticker_position_df):
        last_volume = 0
        start_time = None
        end_time = None

        between_time_list = []
        ticker_position_list = np.array(ticker_position_df).tolist()
        for i, temp_info in enumerate(ticker_position_list):
            trade_time = temp_info[0]
            if start_time is None:
                start_time = trade_time

            volume = temp_info[2]
            if abs(last_volume) < 100 and abs(volume) < 100:
                start_time = trade_time
            elif abs(last_volume) >= 100 and abs(volume) == 0:
                end_time = trade_time
                between_time_list.append([start_time, end_time])
                start_time = trade_time
            elif abs(last_volume) >= 100 and abs(volume) < 100:
                if (i + 1) < len(ticker_position_list):
                    next_temp_info = ticker_position_list[i + 1]
                    if 0 <= next_temp_info[2] < 100:
                        end_time = next_temp_info[0]
                        between_time_list.append([start_time, end_time])
                        start_time = next_temp_info[0]
                        continue
                end_time = trade_time
                between_time_list.append([start_time, end_time])
                start_time = trade_time
            last_volume = volume

        format_time_list = []
        for i, (start_time, end_time) in enumerate(between_time_list):
            if i == 0:
                temp_start_time = start_time
                temp_end_time = end_time
            else:
                if (start_time - temp_end_time).seconds > 1:
                    format_time_list.append([temp_start_time, temp_end_time])
                    temp_start_time = start_time
                    temp_end_time = end_time
                else:
                    temp_end_time = end_time

            if i == len(between_time_list) - 1:
                format_time_list.append([temp_start_time, temp_end_time])
        return format_time_list


if __name__ == '__main__':
    stkintraday_report_job = StkintradayReport('huabao', ['steady_return', 'absolute_return'])
    stkintraday_report_job.report_index()
