# -*- coding: utf-8 -*-
import os
from itertools import islice
from eod_aps.job import *
from eod_aps.tools.file_utils import FileUtils

cfg_file_template = '%s_%s_mid_quote_fwd_ret_%s.csv'
vwap_interval_list = ['30s', '60s', '120s', '300s']
vwap_file_rename_dict = {'30s': 'RET30.csv', '60s': 'RET60.csv', '120s': 'RET120.csv', '300s': 'RET300.csv'}

# intraday_interval_list = ['15s', '30s', '60s', '120s']
# intraday_file_rename_dict = {'15s': 'RETA15.csv', '30s': 'RETA30.csv', '60s': 'RETA60.csv', '120s': 'RETA120.csv'}

volume_mean_file_template = VOLUME_MEAN_FILE_TEMPLATE
vwap_cfg_folder = VWAP_PARAMETER_FILE_FOLDER


class FactorFileRebuild(object):
    def __init__(self, target_folder):
        self.__date_str = date_utils.get_today_str('%Y%m%d')
        self.__source_folder = vwap_cfg_folder
        self.__target_folder = target_folder
        self.__check_date_list = date_utils.get_interval_trading_day_list(date_utils.get_today(), -5)
        self.__volume_mean_dict = self.__read_volume_mean()

    def rebuild_index(self):
        FileUtils(self.__target_folder).clear_folder()
        self.__rebuild_vwap_factor()

    def __read_volume_mean(self):
        volume_mean_file_path = volume_mean_file_template % self.__date_str
        volume_mean_dict = dict()
        with open(volume_mean_file_path, 'rb') as fr:
            for line_str in islice(fr, 1, None):
                line_items = line_str.replace('\n', '').split(',')
                if len(line_items) != 2:
                    continue
                volume_mean_dict[line_items[0]] = line_items[1]
        return volume_mean_dict

    def __rebuild_vwap_factor(self):
        for ticker in os.listdir(self.__source_folder):
            ticker_target_folder = os.path.join(self.__target_folder, ticker)
            os.mkdir(ticker_target_folder)
            ticker_factor_date = self.__find_ticker_factor_date(ticker)
            if ticker_factor_date is None:
                continue

            factor_dict = self.__rename_ticker_factor_file(ticker, ticker_factor_date)
            if len(factor_dict) > 0:
                self.__rebuild_ticker_factor_file(ticker, factor_dict)

    def __find_ticker_factor_date(self, ticker):
        test_interval = vwap_interval_list[0]

        find_date_str = None
        for val_date_str in self.__check_date_list:
            test_file_name = cfg_file_template % (ticker, val_date_str, test_interval)
            test_file_path = os.path.join(self.__source_folder, ticker, test_file_name)
            if os.path.exists(test_file_path):
                find_date_str = val_date_str
                break
        return find_date_str

    def __rename_ticker_factor_file(self, ticker, ticker_factor_date):
        factor_dict = dict()
        for interval_str in vwap_interval_list:
            cfg_file_name = cfg_file_template % (ticker, ticker_factor_date, interval_str)
            cfg_file_path = '%s/%s/%s' % (self.__source_folder, ticker, cfg_file_name)
            if not os.path.exists(cfg_file_path):
                custom_log.log_error_job('Miss Cfg File:%s!' % cfg_file_name)
                continue

            rebuild_content_list = []
            with open(cfg_file_path, 'rb') as fr:
                index = 0
                for line_str in islice(fr, 0, None):
                    index += 1
                    line_items = line_str.replace('\n', '').replace('"', '').split(',')
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

            rename_file = vwap_file_rename_dict[interval_str]
            rename_file_path = '%s/%s/%s' % (self.__target_folder, ticker, rename_file)
            with open(rename_file_path, 'w+') as fr:
                fr.write('\n'.join(rebuild_content_list))
        return factor_dict

    def __rebuild_ticker_factor_file(self, ticker, factor_dict):
        for (factor_name, sub_factor_set) in factor_dict.items():
            factor_value_list = []
            title_list = []
            save_title_flag = True
            for sub_factor in sub_factor_set:
                sub_factor_list = []
                sub_factor_items = sub_factor.split('_')
                for i in range(1, len(sub_factor_items)):
                    sub_factor_item = sub_factor_items[i]
                    sub_factor_value = filter(lambda x: x.isdigit(), sub_factor_item)
                    if sub_factor_value == '':
                        continue
                    if save_title_flag:
                        title_list.append(filter(lambda x: not x.isdigit(), sub_factor_item))
                    sub_factor_list.append(sub_factor_value)
                save_title_flag = False
                factor_value_list.append(','.join(sub_factor_list))

            if 'PACE' == factor_name:
                volume_mean = self.__volume_mean_dict[ticker] if ticker in self.__volume_mean_dict else 0
                factor_value_list = ['%s,%s' % (volume_mean, item) for item in factor_value_list]
                title_list.insert(0, 'VOLUME_MEAN')
                factor_value_list.insert(0, ','.join(title_list))
            else:
                factor_value_list.insert(0, ','.join(title_list))

            factor_save_path = '%s/%s/%s.csv' % (self.__target_folder, ticker, factor_name)
            with open(factor_save_path, 'w') as fr:
                fr.write('\n'.join(factor_value_list))


def factordata_file_rebuild_job():
    target_folder_path = VWAP_PARAMETER_PRODUCT_FOLDER
    factor_file_rebuild = FactorFileRebuild(target_folder_path)
    factor_file_rebuild.rebuild_index()


if __name__ == '__main__':
    factordata_file_rebuild_job()
