# -*- coding: utf-8 -*-
import os
from itertools import islice
import shutil


file_name_template = '%s_%s_mid_quote_fwd_ret_%s.csv'
interval_list = ['30s', '60s', '120s', '300s']
file_rename_dict = {'30s': 'RET30.csv', '60s': 'RET60.csv', '120s': 'RET120.csv', '300s': 'RET300.csv'}


def build_factordata_cfg_file(factor_source_folder, factorgenerator_target_folder, ticker):
    ticker_save_path = os.path.join(factorgenerator_target_folder, ticker)

    if os.path.exists(ticker_save_path):
        shutil.rmtree(ticker_save_path, True)
    os.mkdir(ticker_save_path)

    date_set = set()
    for file_name in os.listdir(os.path.join(factor_source_folder, ticker)):
        temp_date_str = file_name.split('_')[1]
        date_set.add(temp_date_str)
    date_list = list(date_set)
    date_list.sort()
    date_str = date_list[-1]

    factor_dict = dict()
    for interval_str in interval_list:
        file_name = file_name_template % (ticker, date_str, interval_str)
        file_path = '%s/%s/%s' % (factor_source_folder, ticker, file_name)
        if not os.path.exists(file_path):
            return

        rebuild_content_list = []
        with open(file_path, 'rb') as fr:
            index = 0
            for line in islice(fr, 0, None):
                index += 1
                line_items = line.replace('\n', '').replace('"', '').split(',')
                rebuild_content_list.append(','.join(line_items))
                if index == 1:
                    continue

                factor = line_items[0]
                sub_factor = line_items[1]
                if factor in factor_dict:
                    factor_dict[factor].add(sub_factor)
                else:
                    sub_factor_set = set()
                    sub_factor_set.add(sub_factor)
                    factor_dict[factor] = sub_factor_set

        file_rename = file_rename_dict[interval_str]
        with open(ticker_save_path + '/' + file_rename, 'w+') as fr:
            fr.write('\n'.join(rebuild_content_list))

    for (factor, sub_factor_set) in factor_dict.items():
        factor_value_list = []
        title_list = []
        save_title_flag = True
        for sub_factor in sub_factor_set:
            sub_factor_value_list = []
            sub_factor_items = sub_factor.split('_')
            for i in range(1, len(sub_factor_items)):
                sub_factor_item = sub_factor_items[i]
                sub_factor_value = filter(lambda x: x.isdigit(), sub_factor_item)
                if sub_factor_value == '':
                    continue
                if save_title_flag:
                    title_list.append(filter(lambda x: not x.isdigit(), sub_factor_item))
                sub_factor_value_list.append(sub_factor_value)
            save_title_flag = False
            factor_value_list.append(','.join(sub_factor_value_list))
        factor_value_list.insert(0, ','.join(title_list))

        with open('%s/%s.csv' % (ticker_save_path, factor), 'w+') as fr:
            fr.write('\n'.join(factor_value_list))
    return date_str


if __name__ == '__main__':
    pass