# -*- coding: utf-8 -*-

# 问题
# 1.单个成交trade存在多条数据的情况，如先PartialFilled再Filled, 需要根据OrdId覆盖(在分段内去重，整体去重因零股会有问题)
# 2.无成交记录


import codecs
import os
import pickle
import re
import traceback
import pandas as pd
import numpy as np
from eod_aps.tools.date_utils import DateUtils

base_file_path = 'Z:/temp/yangzhoujie/result_report/StrategyLoader_logs'
export_file_path = 'Z:/temp/yangzhoujie/result_report/Market_Report_20180920'
date_utils = DateUtils()
buy_commission = 0.00025
sell_commission = 0.00125
# buy_commission = 0.00027
# sell_commission = 0.00127
ticker_report_dict = dict()
total_volume_dict = dict()


def __format_param_line(line_info):
    reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] \
OnStratStart: param_set=(?P<param_set>.*), coefficient=(?P<coefficient>.*), max_place_ratio=(?P<max_place_ratio>.*), \
threshold_long_close=(?P<threshold_long_close>.*), threshold_short_close=(?P<threshold_short_close>.*), \
min_active_seconds=(?P<min_active_seconds>.*), unit_qty=(?P<unit_qty>.*), initial_shares=(?P<initial_shares>.*)'
    reg = re.compile(reg_line)
    reg_match = reg.match(line_info)
    param_set_dict = reg_match.groupdict()

    ticker = param_set_dict['ticker_str'].split('_')[-1]
    total_volume_dict[ticker] = int(param_set_dict['initial_shares'])
    return param_set_dict


def __format_trade_line(line_info):
    reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] (?P<trade_time>.*) \
Event: OrdId=(?P<OrdId>.*), side=(?P<side>.*), price=(?P<price>.*), qty=(?P<qty>.*), ex_qty=(?P<ex_qty>.*), \
last_ex_price=(?P<last_ex_price>.*), status=(?P<status>.*), create=(?P<create>.*)'
    reg = re.compile(reg_line)
    reg_match = reg.match(line_info)
    line_dict = reg_match.groupdict()
    return line_dict


def __format_position_line(line_info):
    reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] (?P<trade_time>.*) \
Info: tbuy=(?P<buy_number>.*), tsell=(?P<sell_number>.*), obuy=(?P<obuy>.*), osell=(?P<osell>.*), bvwap=(?P<bvwap>.*), \
svwap=(?P<svwap>.*), sig=(?P<sig>.*), sig_adj=(?P<sig_adj>.*), sig_Last=(?P<sig_Last>.*)'
    reg = re.compile(reg_line)
    reg_match = reg.match(line_info)
    line_dict = reg_match.groupdict()
    return line_dict


def __format_position_line2(line_info):
    reg_line = '^.*\[(?P<log_time>.*)\] \[(?P<ticker_str>.*)\] \[(?P<log_type>.*)\] (?P<trade_time>.*) \
Posi: tbuy=(?P<buy_number>.*), tsell=(?P<sell_number>.*), obuy=(?P<obuy>.*), osell=(?P<osell>.*), bvwap=(?P<bvwap>.*), \
svwap=(?P<svwap>.*)'
    reg = re.compile(reg_line)
    reg_match = reg.match(line_info)
    line_dict = reg_match.groupdict()
    return line_dict


def __query_time_list(ticker_position_df):
    last_volume = 0
    start_time = None
    end_time = None

    between_time_list = []
    for temp_info in np.array(ticker_position_df).tolist():
        trade_time = temp_info[0]
        volume = temp_info[2]
        if abs(last_volume) < 50 and abs(volume) < 50:
            start_time = trade_time
        elif abs(last_volume) >= 50 and abs(volume) < 50:
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


def __read_message_list(line_info_list, order_list, param_set, log_file_path):
    order_info_list = []
    for order_info in order_list:
        line_dict = __format_trade_line(order_info)
        ticker = line_dict['ticker_str'].split('_')[-1]
        if '.' in line_dict['trade_time']:
            trade_time = date_utils.string_toDatetime(line_dict['trade_time'], format_str='%Y-%b-%d %H:%M:%S.%f')
        else:
            trade_time = date_utils.string_toDatetime(line_dict['trade_time'], format_str='%Y-%b-%d %H:%M:%S')
        order_info_list.append([line_dict['OrdId'], ticker, line_dict['side'], line_dict['ex_qty'],
                                line_dict['last_ex_price'], line_dict['status'], trade_time])

    order_df = pd.DataFrame(order_info_list,
                            columns=['OrdId', 'Ticker', 'Side', 'Qty', 'Price', 'Status', 'Trade_Time'])
    order_df = order_df.sort_values(by="Trade_Time")

    fill_trade_df = order_df[order_df['Status'].isin(['Filled', 'PartialFilled'])]
    if len(fill_trade_df) == 0:
        return

    fill_trade_df.loc[:, 'Qty'] = fill_trade_df["Qty"].astype(float)
    fill_trade_df.loc[:, 'Price'] = fill_trade_df["Price"].astype(float)
    fill_trade_df.loc[:, 'MarketValue'] = fill_trade_df['Qty'] * fill_trade_df['Price']

    fill_trade_df['Commission'] = 0
    fill_trade_df.loc[fill_trade_df['Side'] == 'Buy', 'Commission'] = buy_commission
    fill_trade_df.loc[fill_trade_df['Side'] == 'Sell', 'Commission'] = sell_commission
    fill_trade_df.loc[:, 'Fee'] = fill_trade_df['MarketValue'] * fill_trade_df['Commission']

    fill_trade_dict = dict()
    for group_key, ticker_trade_df in fill_trade_df.groupby(['Ticker', ]):
        fill_trade_dict[group_key] = ticker_trade_df

    format_message_list = []
    for line_info in line_info_list:
        if 'StkIntraDayStrategy' not in line_info:
            continue

        if 'Info:' in line_info:
            line_dict = __format_position_line(line_info)
        elif 'Posi:' in line_info:
            line_dict = __format_position_line2(line_info)
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

    for group_key, ticker_position_df in position_df.groupby(['Ticker', ]):
        if group_key not in fill_trade_dict:
            print group_key
            continue
        # if group_key == '002035' and param_set == '1':
        #     print log_file_path

        ticker_trade_df = fill_trade_dict[group_key]

        report_message_list = []
        between_time_list = __query_time_list(ticker_position_df)

        for (start_time, end_time) in between_time_list:
            filter_position_df = ticker_position_df[(ticker_position_df['Trade_Time'] >= start_time) &
                                                    (ticker_position_df['Trade_Time'] <= end_time)]
            filter_trade_df = ticker_trade_df[(ticker_trade_df['Trade_Time'] > start_time) &
                                              (ticker_trade_df['Trade_Time'] <= end_time)]

            # 去重，避免问题1
            filter_trade_df.drop_duplicates('OrdId', keep='last', inplace=True)

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
            if total_buy_volume > total_sell_volume:
                temp_trade_price = sell_trade_df.iloc[-1]['Price']
                margin = sell_money + (total_buy_volume - total_sell_volume) * temp_trade_price - buy_money
            elif total_buy_volume < total_sell_volume:
                temp_trade_price = buy_trade_df.iloc[-1]['Price']
                margin = sell_money + (total_buy_volume - total_sell_volume) * temp_trade_price - buy_money
            else:
                margin = sell_money - buy_money

            fee = filter_trade_df['Fee'].sum()
            net_margin = margin - fee

            filter_position_df.loc[:, 'Abs_Volume'] = filter_position_df['Volume'].abs()
            max_volume = filter_position_df['Abs_Volume'].max()

            report_message_list.append([group_key, start_time, end_time, buy_money, sell_money, margin, fee, net_margin,
                                        margin / buy_money, net_margin / buy_money,
                                        max_volume, total_buy_volume, total_sell_volume, interval_seconds])

        report_message_title = [u'合约', u'开始时间', u'结束时间', u'总买入', u'总卖出', u'毛利',
                                u'交易费用', u'净利', u'毛利率', u'净利率', u'最大持仓量',
                                u'累计买入量', u'累计卖出量', u'持仓时间']
        ticker_interval_df = pd.DataFrame(report_message_list, columns=report_message_title)

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

        total_volume = total_volume_dict[group_key]
        total_report_list.append([group_key, buy_money, sell_money, margin, fee, net_margin, margin / buy_money,
                                  net_margin / buy_money, ticker_interval_df[u'最大持仓量'].max(),
                                  ticker_interval_df[u'最大持仓量'].min(), int(ticker_interval_df[u'最大持仓量'].mean()),
                                  int(trade_volume), trade_volume * 1. / total_volume,
                                  ticker_interval_df[u'持仓时间'].max(),
                                  ticker_interval_df[u'持仓时间'].min(), ticker_interval_df[u'持仓时间'].mean(),
                                  len(ticker_interval_df[ticker_interval_df[u'净利'] >= 0]) * 1. / len(
                                      ticker_interval_df),
                                  profit_loss_ratio
                                  ])

        report_message_title = [u'合约', u'总买入', u'总卖出', u'毛利', u'交易费用', u'净利', u'毛利率', u'净利率', u'最大持仓量',
                                u'最小持仓量', u'平均持仓量', u'累计交易量', u'仓位使用率', u'最长持仓时间', u'最短持仓时间',
                                u'平均持仓时间', u'胜率', u'盈亏比']
        total_report_df = pd.DataFrame(total_report_list, columns=report_message_title)
        ticker_report_dict.setdefault(param_set, []).append(total_report_df)

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

        if end_time is not None:
            output_file_path = '%s/%s_%s_%s.html' % (export_file_path, group_key, param_set, end_time.strftime("%Y-%m-%d"))
            with codecs.open(output_file_path, 'w+', 'utf-8') as fr:
                fr.write(ticker_interval_df.to_html(index=False))
                fr.write('<br><br><br><br>')
                fr.write(total_report_df.to_html(index=False))
                fr.write('<br><br><br><br>')
                fr.write(ticker_trade_df.to_html(index=False))
                fr.write('<br><br><br><br>')
                fr.write(ticker_position_df.to_html(index=False))


def __build_daily_report():
    for param_set, df_list in ticker_report_dict.items():
        ticker_detailed_df = pd.concat(df_list, sort=False)

        format_detailed_df = ticker_detailed_df.copy()
        format_detailed_df.loc[:, u'毛利'] = format_detailed_df[u'毛利'].astype(float)
        format_detailed_df.loc[:, u'交易费用'] = format_detailed_df[u'交易费用'].astype(float)
        format_detailed_df.loc[:, u'净利'] = format_detailed_df[u'净利'].astype(float)
        format_detailed_df.loc[:, u'毛利率'] = format_detailed_df[u'毛利率'].apply(lambda x: float(x.replace('%', '')))
        format_detailed_df.loc[:, u'净利率'] = format_detailed_df[u'净利率'].apply(lambda x: float(x.replace('%', '')))
        format_detailed_df.loc[:, u'仓位使用率'] = format_detailed_df[u'仓位使用率'].apply(lambda x: float(x.replace('%', '')))
        format_detailed_df.loc[:, u'胜率'] = format_detailed_df[u'胜率'].apply(lambda x: float(x.replace('%', '')))

        buy_money = format_detailed_df[u'总买入'].sum()
        sell_money = format_detailed_df[u'总卖出'].sum()
        margin = format_detailed_df[u'毛利'].sum()
        fee = format_detailed_df[u'交易费用'].sum()
        net_margin = format_detailed_df[u'净利'].sum()

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

        daily_report_list = [[margin, fee, net_margin, maoli_avg, jingli_avg, ticker_use_number, position_ratio_mean,
                              position_ratio_max, position_ratio_min, holding_time_mean, holding_time_max,
                              holding_time_min, win_mean, win_max, win_min, profit_avg, loss_avg, profit_loss_ratio
                              ]]

        report_message_title = [u'毛利', u'交易费用', u'净利', u'平均毛利率', u'平均净利率', u'股票使用量', u'仓位使用率平均值',
                                u'仓位使用率最大值', u'仓位使用率最小值', u'平均持仓时间平均值', u'平均持仓时间最大值', u'平均持仓时间最小值' ,
                                u'胜率平均值', u'胜率最大值', u'胜率最小值', u'平均每笔净盈利', u'平均每笔净亏损', u'盈亏比']
        daily_report_df = pd.DataFrame(daily_report_list, columns=report_message_title)
        daily_report_df[u'平均毛利率'] = daily_report_df[u'平均毛利率'].apply(lambda x: '%.4f%%' % (x * 100))
        daily_report_df[u'平均净利率'] = daily_report_df[u'平均净利率'].apply(lambda x: '%.4f%%' % (x * 100))
        daily_report_df[u'仓位使用率平均值'] = daily_report_df[u'仓位使用率平均值'].apply(lambda x: '%.4f%%' % x)
        daily_report_df[u'仓位使用率最大值'] = daily_report_df[u'仓位使用率最大值'].apply(lambda x: '%.4f%%' % x)
        daily_report_df[u'仓位使用率最小值'] = daily_report_df[u'仓位使用率最小值'].apply(lambda x: '%.4f%%' % x)

        daily_report_df[u'胜率平均值'] = daily_report_df[u'胜率平均值'].apply(lambda x: '%.4f%%' % x)
        daily_report_df[u'胜率最大值'] = daily_report_df[u'胜率最大值'].apply(lambda x: '%.4f%%' % x)
        daily_report_df[u'胜率最小值'] = daily_report_df[u'胜率最小值'].apply(lambda x: '%.4f%%' % x)
        daily_report_df[u'平均每笔净盈利'] = daily_report_df[u'平均每笔净盈利'].apply(lambda x: '%.4f%%' % x)
        daily_report_df[u'平均每笔净亏损'] = daily_report_df[u'平均每笔净亏损'].apply(lambda x: '%.4f%%' % x)

        output_file_path = '%s/param%s_total_report.html' % (export_file_path, param_set)
        with codecs.open(output_file_path, 'w+', 'utf-8') as fr:
            fr.write(daily_report_df.to_html(index=False))
            fr.write('<br><br><br><br>')
            fr.write(ticker_detailed_df.to_html(index=False))


def read_pnl_fils():
    log_file_list = []
    for log_file_name in os.listdir(base_file_path):
        # if 'test_' not in log_file_name:
        #     continue
        log_file_list.append(log_file_name)
    log_file_list.sort()

    message_list = []
    order_list = []
    for log_file_name in log_file_list:
        log_file_path = '%s/%s' % (base_file_path, log_file_name)
        with open(log_file_path) as fr:
            for line in fr.readlines():
                if 'OnStratStart' in line:
                    if len(message_list) > 0:
                        __read_message_list(message_list, order_list, param_set, log_file_path)
                    param_set_dict = __format_param_line(line)
                    param_set = param_set_dict['param_set']
                    message_list = []
                    order_list = []
                elif 'SIDSImpl' in line and 'Info:' in line:
                    message_list.append(line)
                elif 'SIDSImpl' in line and 'Posi:' in line:
                    message_list.append(line)
                elif 'SIDSImpl' in line and 'Event:' in line:
                    order_list.append(line)

    if len(message_list) > 0:
        __read_message_list(message_list, order_list, param_set, log_file_path)
    __build_daily_report()


if __name__ == '__main__':
    read_pnl_fils()
    # __build_daily_report()
