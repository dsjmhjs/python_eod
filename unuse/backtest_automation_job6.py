# -*- coding: utf-8 -*-
import os
import subprocess
from itertools import islice
from pandas import merge

from eod_aps.model.backtest_param_config import BacktestParamConfig

ticker_file_base_path = '/mnt/ssd1/IntradayAlpha_RData/MultiFactorWeightData/%s'
server_file_path = '/home/yangzhoujie/StkIntraDayStrategy'
save_file_folder = server_file_path + '/config'
ticker_filename_base = '%s_%s_mid_quote_fwd_ret_%s.csv'

config_dataframe = None
backtest_result_folder = server_file_path + '/result'
backtest_result__rebuild_folder = server_file_path + '/result_rebuild'
cmd_template = 'BackTest --StratsName=StkIntraDay --WatchList=%s --ParaList=%s  --Parameter=[' \
               'Account]1:0:0;%s --StartTime=%s --EndTime=%s  ' \
               '--AssemblyName=libstk_intra_day_strategy --Parallel=16 '

run_cmd_template = './build64_release/BackTest/BackTestCpp --run_state=host --command_file=./config/%s \
--export_path=./result/ --Parallel=16'


def run_backtest(cmd_file_name):
    if cmd_file_name.split('_')[2] == '6':
        ticker_type = 'SH'
    else:
        ticker_type = 'SZ'
    __rebuild_backtest_config_file(ticker_type)

    print 'run backtest_cmd_file', cmd_file_name
    __run_backtest(cmd_file_name)


def __rebuild_backtest_config_file(exchange_type):
    if exchange_type == 'SH':
        ln_cmd = 'rm config.test.txt;ln -s config.test.sh.txt config.test.txt'
    elif exchange_type == 'SZ':
        ln_cmd = 'rm config.test.txt;ln -s config.test.sz.txt config.test.txt'
    ln_cmd_str = 'cd %s;%s' % (server_file_path, ln_cmd)
    return_code = subprocess.call(ln_cmd_str, shell=True)
    print return_code


def __run_backtest(cmd_file_name):
    ticker = cmd_file_name.split('_')[2]
    date = cmd_file_name.split('_')[3]
    result_file_name = '%s_%s-%s-%s_pnl.csv' % (ticker, date[:4], date[4:6], date[6:8])
    del_cmd_str = 'cd %s;rm -f %s' % (backtest_result_folder, result_file_name)
    return_code = subprocess.call(del_cmd_str, shell=True)
    print 'cmd:', del_cmd_str, return_code

    run_backtest_cmd = run_cmd_template % cmd_file_name
    cmd_str = 'cd %s;%s' % (server_file_path, run_backtest_cmd)
    return_code = subprocess.call(cmd_str, shell=True)
    print 'cmd:', cmd_str, return_code


def server_enter(ticker):
    ticker_file_path = ticker_file_base_path % ticker
    date_set = set()
    for file_name in os.listdir(ticker_file_path):
        date_set.add(file_name.split('_')[1])

    __build_config_dataframe()
    for date_str in date_set:
        cmd_str_list = __build_command_file(ticker_file_path, ticker, date_str)
        if len(cmd_str_list) == 0:
            continue

        cmd_file_name = 'command_file_%s_%s.txt' % (ticker, date_str)
        file_path = '%s/%s' % (save_file_folder, cmd_file_name)
        file_object = open(file_path, 'w+')
        file_object.write('\n'.join(cmd_str_list))
        file_object.close()

        run_backtest(cmd_file_name)


def __build_command_file(ticker_file_path, ticker, filter_date_str):
    cmd_str_list = []
    interval_list = ['30s', '60s', '120s', '300s']
    param_dict = dict()
    param_set = set()
    for interval_str in interval_list:
        interval_param_dict = dict()
        ticker_filename = ticker_filename_base % (ticker, filter_date_str, interval_str)
        input_file = open(ticker_file_path + '/' + ticker_filename)
        for line in islice(input_file, 1, None):
            line_items = line.replace('\n', '').replace('"', '').split(',')
            interval_param_dict[line_items[1]] = line_items[2]
            param_set.add(line_items[1])
        param_dict[interval_str] = interval_param_dict

    para_list = []
    parameter_info_list = []
    for interval_str in interval_list:
        interval_param_dict = param_dict[interval_str]
        for param in param_set:
            para_list.append('%s.%s' % (ticker, param))
            if param in interval_param_dict:
                interval_param_value = interval_param_dict[param]
            else:
                interval_param_value = 0
            parameter_info_list.append('[%s_%s_weight%s]%s' %
                                       (ticker, param, interval_str, interval_param_value))

    common_parameter_list = __build_common_parameter_list(ticker)
    format_date_str = '%s-%s-%s' % (filter_date_str[0:4], filter_date_str[4:6], filter_date_str[6:8])
    for common_parameter_array in common_parameter_list:
        temp_parameter_info_list = parameter_info_list + common_parameter_array
        cmd_str = cmd_template % (ticker, ';'.join(para_list), ';'.join(temp_parameter_info_list),
                                  format_date_str, format_date_str)
        cmd_str_list.append(cmd_str)
    return cmd_str_list


def __build_config_dataframe():
    param_config = BacktestParamConfig()
    config_dataframe = param_config.param_config_ls[0]
    for i in range(1, len(param_config.param_config_ls)):
        dataframe = param_config.param_config_ls[i]
        config_dataframe['key'] = 1
        dataframe['key'] = 1
        config_dataframe = merge(config_dataframe, dataframe, on="key")
        del config_dataframe['key']
    global config_dataframe


def __build_common_parameter_list(ticker):
    common_parameter_list = []
    title_list = config_dataframe.columns
    values_list = config_dataframe.values
    for line_values in values_list:
        output_str = []
        for i in range(0, len(line_values)):
            output_str.append('[%s_%s]%s' % (ticker, title_list[i], str(line_values[i])))
        common_parameter_list.append(output_str)
    return common_parameter_list


def result_file_analysis():
    ticker_message_dict = dict()
    parameter_dict = dict()
    parameter_index = 1

    for file_name in os.listdir(backtest_result_folder):
        output_list = []
        result_title = []

        (ticker, date_str, other_value) = file_name.split('_')

        input_file = open('%s/%s' % (backtest_result_folder, file_name))
        file_values = input_file.readlines()
        for i in range(0, len(file_values)):
            (parameter_str_all, report_str) = file_values[i].replace('\n', '').rsplit(',', 1)
            parameter_str = __rebuild_parameter_str(ticker, parameter_str_all)
            if parameter_str in parameter_dict:
                parameter_alias = parameter_dict[parameter_str]
            else:
                parameter_alias = 'parameter_' + str(parameter_index)
                parameter_dict[parameter_str] = parameter_alias
                parameter_index += 1

            line_values = []
            result_items = report_str.split('|')
            for k in range(1, len(result_items)):
                (title_item, value_item) = result_items[k].split('=')
                if i == 0:
                    result_title.append(title_item)
                line_values.append(value_item)
            output_list.append('%s,%s,%s' % (date_str, parameter_alias, ','.join(line_values)))

        if ticker in ticker_message_dict:
            ticker_message_dict[ticker] = ticker_message_dict[ticker] + output_list
        else:
            ticker_message_dict[ticker] = output_list

    for (ticker, save_message_list) in ticker_message_dict.items():
        file_object = open('%s/%s_report.csv' % (backtest_result__rebuild_folder, ticker), 'w')
        save_message_list.insert(0, 'DATE,PARAMETER,%s' % ','.join(result_title).upper())
        file_object.write('\n'.join(save_message_list))
        file_object.close()

    parameter_file_list = []
    parameter_title_list = ['PARAMETER']
    save_title_flag = True
    for (parameter_key, parameter_value) in parameter_dict.items():
        parameter_item_list = [parameter_value]
        for parameter_item in parameter_key.split(','):
            (sub_title, sub_value) = parameter_item.split(':')
            if save_title_flag:
                parameter_title_list.append(sub_title)
            parameter_item_list.append(sub_value)
        save_title_flag = False
        parameter_file_list.append(','.join(parameter_item_list))
    parameter_file_list = sorted(parameter_file_list, cmp=lambda x, y: cmp(int(x.split(',')[0].split('_')[1]), int(y.split(',')[0].split('_')[1])), reverse=False)
    parameter_file_list.insert(0, ','.join(parameter_title_list))
    file_object = open('%s/parameter_dict.csv' % backtest_result__rebuild_folder, 'w')
    file_object.write('\n'.join(parameter_file_list))
    file_object.close()


def __rebuild_parameter_str(ticker, parameter_str_all):
    parameter_items = []
    parameter_str = parameter_str_all.split('para=')[1]
    for parameter_item in parameter_str.split(','):
        if 'weight30s' in parameter_item:
            continue
        if 'weight60s' in parameter_item:
            continue
        if 'weight120s' in parameter_item:
            continue
        if 'weight300s' in parameter_item:
            continue
        parameter_items.append(parameter_item.replace(ticker + '_', ''))
    parameter_items.sort()
    return ','.join(parameter_items)

if __name__ == '__main__':
    __build_config_dataframe()
    server_enter('000001')
    result_file_analysis()





