# -*- coding: utf-8 -*-
# 篮子股票购买清单比对是否存在自成交风险
import os

save_folder_path = 'Z:/dailyjob/StockSelection'


def algo_file_compare(date_str):
    i = 0
    for file_name in os.listdir(save_folder_path + '/' + date_str):
        if 'txt' not in file_name:
            continue
        fr = open('%s/%s' % (save_folder_path + '/' + date_str, file_name))

        if i == 0:
            file_dict1 = dict()
        elif i == 1:
            file_dict2 = dict()

        for line in fr.readlines():
            (ticker, volume) = line.replace('\n', '').split(',')
            if i == 0:
                file_dict1[ticker] = volume
            elif i == 1:
                file_dict2[ticker] = volume
        i += 1

    for (ticker, volume1) in file_dict1.items():
        if ticker not in file_dict2:
            continue
        volume2 = file_dict2[ticker]

        if int(volume2) < 0 < int(volume1):
            print ticker
        elif int(volume1) < 0 < int(volume2):
            print ticker


if __name__ == '__main__':
    algo_file_compare('20161213')