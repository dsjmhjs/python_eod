# -*- coding: utf-8 -*-
import os
import platform
import sys
from itertools import islice
from eod_aps.job import *


date_set = set()
result_value_dict = dict()

sysstr = platform.system()
if sysstr == "Windows":
    result_file_path = 'Z:/temp/longling/StkIntraDayStrategy/result/'
    result_rebuild_file_path = 'Z:/temp/longling/StkIntraDayStrategy/result_rebuild'
    result_report_file_path = 'Z:/temp/longling/StkIntraDayStrategy/result_report'
elif sysstr == "Linux":
    result_file_path = '/nas/longling/StkIntraDayStrategy/result/'
    result_rebuild_file_path = '/nas/longling/StkIntraDayStrategy/result_rebuild'
    result_report_file_path = '/nas/longling/StkIntraDayStrategy/result_report'
else:
    task_logger.info('Other System tasks')
    sys.exit()

ticker_str_sh = '600051,600112,600237,600708,600765,600816,601388,600238,600291,600359,600691,601009,600064,600077,\
600090,600665,600681,600683,600791,601668,600026,600048,600052,600060,600325,600503,600658,600751,600986,601166,600287,\
600295,600335,600757,600835,600970,603001'

ticker_str_sz = '000586,000597,000666,000789,000835,002045,002090,002566,002667,300106,300167,300313,300401,000159,\
000701,000900,002133,002321,002452,002519,300029,300341,000036,000066,000587,002016,002567,000002,000059,000521,000631,\
000863,002048,002157,002208,002454,300063,300240,000501,000667,000892,000897,002394,002401,002420,300282'


def result_file_analysis():
    ticker_dict = dict()
    for file_name in os.listdir(result_rebuild_file_path):
        if '.csv' not in file_name:
            continue
        ticker = file_name.split('_')[0]

        date_str = file_name.split('_')[1]
        date_set.add(date_str)

        if ticker_dict.has_key(ticker):
            ticker_dict[ticker].append(file_name)
        else:
            ticker_dict[ticker] = [file_name]

    date_list = list(date_set)
    date_list.sort()
    for (ticker, file_list) in ticker_dict.items():
        para_set = set()
        file_list.sort()
        for file_name in file_list:
            date_str = file_name.split('_')[1]
            input_file = open(result_rebuild_file_path + '/' + file_name)
            for line in islice(input_file, 1, None):
                line_item = line.strip().split(',')
                paralist = line_item[1]
                weight60s = line_item[3]
                result_value = line_item[-1]

                para_key = '%s|%s' % (paralist, weight60s)
                para_set.add(para_key)

                dict_key = '%s|%s' % (para_key, date_str)
                result_value_dict[dict_key] = result_value

        line_list = []
        para_list = list(para_set)
        para_list.sort()
        for para_item_str in para_list:
            line_items = []
            for para_str in para_item_str.split('|'):
                line_items.append(para_str)

            for date_str in date_list:
                find_key = '%s|%s' % (para_item_str, date_str)
                if find_key in result_value_dict:
                    line_items.append('%.6f' % float(result_value_dict[find_key]))
                else:
                    line_items.append('None')

            line_list.append(','.join(line_items))

        analysis_title = 'parameter1,parameter2'
        for date_str in date_list:
            analysis_title += ',' + date_str

        file_path = '%s/%s_report.csv' % (result_report_file_path, ticker)
        file_object = open(file_path, 'w+')
        file_object.write(analysis_title + '\n' + '\n'.join(line_list))
        file_object.close()


def profit_loss_ratio(x):
    if len(x[x > 0]) == 0:
        return 0
    elif len(x[x < 0]) == 0:
        return 100
    else:
        return (sum(x[x > 0]) * float(len(x[x < 0]))) / (abs((sum(x[x < 0])) * float(len(x[x > 0]))))

parameter_size = 2
def result_file_report():
    analytics = importr('PerformanceAnalytics')
    for file_name in os.listdir(result_report_file_path):
        base_df = pd.read_csv(result_report_file_path + '/' + file_name)
        need_columns = base_df.columns.values[2:]
        df_T = base_df.loc[:, need_columns].T

        avg_result = df_T.mean()
        avg_dataframe = pd.DataFrame(com._convert_DataFrame(avg_result).T)
        avg_dataframe.index = base_df.index

        r_dataframe = com.convert_to_r_matrix(df_T, True)
        sharpeRatio_result = analytics.SharpeRatio(r_dataframe, FUN='StdDev')
        sharpeRatio_dataframe = pd.DataFrame(com._convert_DataFrame(sharpeRatio_result).T)
        sharpeRatio_dataframe.index = base_df.index

        maxDrawdown_result = analytics.maxDrawdown(r_dataframe)
        maxDrawdown_dataframe = pd.DataFrame(com._convert_DataFrame(maxDrawdown_result).T)
        maxDrawdown_dataframe.index = base_df.index

        win_ratio_result = df_T.apply(lambda x: len(x[x > 0]) / float(len(x)))
        win_ratio_dataframe = pd.DataFrame(com._convert_DataFrame(win_ratio_result).T)
        win_ratio_dataframe.index = base_df.index

        profit_loss_result = df_T.apply(lambda x: profit_loss_ratio(x))
        profit_loss_dataframe = pd.DataFrame(com._convert_DataFrame(profit_loss_result).T)
        profit_loss_dataframe.index = base_df.index

        df_result = pd.concat([base_df, avg_dataframe], axis=1).concat([base_df, sharpeRatio_dataframe], axis=1)\
            .concat([base_df, maxDrawdown_dataframe], axis=1).concat([base_df, win_ratio_dataframe], axis=1)\
            .concat([base_df, profit_loss_dataframe], axis=1)
        df_result.cloumns = list(base_df.columns.values).append('avg_ret').append('sharpe_ratio')\
            .append('maxDrawdown').append('win_ratio').append('pnl_ratio')
        df_result.to_csv('/nas/yangzhoujie/rebuild.csv', index=False)

        # input_file = open(result_report_file_path + '/' + file_name)
        #
        # line_list = []
        # index = 0
        # for line in input_file.readlines():
        #     if index == 0:
        #         index += 1
        #         title = line.strip() + ',avg_ret,sharpe_ratio,win_ratio,pnl_ratio'
        #         continue
        #     line_item = line.strip().split(',')
        #     date_return_list = []
        #     for i in range(parameter_size, len(line_item), 1):
        #         if 'None' != line_item[i]:
        #             date_return_list.append(float(line_item[i]))
        #
        #     avg_value = __average(date_return_list)
        #     narray = np.array(date_return_list)
        #     stdev_value = np.std(narray)
        #     sharpe_ratio = avg_value / (stdev_value * (252 ** 0.5))
        #
        #     # max_drawdown_value = __max_drawdown(date_return_list)
        #     (win_rate, profit_loss_ratio) = __calculation_stat(date_return_list)
        #
        #     calculation_result_str = '%s,%.6f,%.6f,%.6f,%.6f' % (line.strip(), avg_value,  sharpe_ratio, \
        #                                                      win_rate, profit_loss_ratio)
        #     line_list.append(calculation_result_str)
        #
        # line_list = sorted(line_list, cmp=lambda x, y: cmp(float(x.split(',')[-4]), float(y.split(',')[-4])),
        #                      reverse=True)
        # line_list.insert(0, title)
        # file_path = '%s/%s' % (result_report_file_path, file_name)
        # file_object = open(file_path, 'w+')
        # file_object.write('\n'.join(line_list))
        # file_object.close()


def __average(seq):
    return float(sum(seq)) / float(len(seq))

def __max_drawdown(seq):
    drawdown_list = []

    min_value = seq[0]
    for i in range(1, len(seq), 1):
        if seq[i] <= min_value:
            min_value = seq[i]
        drawdown_list.append(seq[i] - min_value)
    return max(drawdown_list)


def __calculation_stat(seq):
    win_list = [elem for elem in seq if elem >= 0]
    loss_list = [elem for elem in seq if elem < 0]

    win_rate = float(len(win_list)) / float(len(seq))

    if len(win_list) == 0:
        profit_loss_ratio = 0
    elif len(loss_list) == 0:
        profit_loss_ratio = 100
    else:
        profit_loss_ratio = (sum(win_list) / len(win_list)) / (abs(sum(loss_list)) / len(loss_list))
    return win_rate, profit_loss_ratio



def filter_file(date_str):
    validate_file_list = []
    for ticker in ticker_str_sz.split(','):
        file_name = '%s_%s_pnl.csv' % (ticker, date_str)
        validate_file_list.append(file_name)

    for file_name in os.listdir(result_file_path):
        if date_str not in file_name:
            continue
        if file_name not in validate_file_list:
            # os.remove(result_file_path + '/' + file_name)
            task_logger.error('Error File:%s' % file_name)



if __name__ == '__main__':
    # result_file_analysis()
    # result_file_report()
    # filter_file('2016-09-19')

    result_file_analysis()
    # result_file_report()