# -*- coding: utf-8 -*-
import requests
from eod_aps.job import *
import re
from bs4 import BeautifulSoup
import csv
import sys
import pandas as pd
from cfg import custom_log

trade_types = ['IF', 'IC', 'IH', 'TS', 'TF', 'T']


class SpiderCffInfo(object):
    def __init__(self, url_list, last_trading_day):
        self.url_list = url_list
        self.last_trading_day = int(last_trading_day.replace('/', ''))

    def __enter__(self):
        self.save_path = '%s/trade_position.csv' % CFF_INFO_FOLDER
        self.csv_file = open(self.save_path, 'ab')
        self.writer = csv.writer(self.csv_file)
        return self

    def writer_csv(self, trade_list):
        self.writer.writerow(trade_list)
        sys.stdout.write('#' + '->' + "\b\b")
        sys.stdout.flush()

    def spider_data(self):
        data_list = pd.read_csv(self.save_path)
        check_day = data_list[data_list['date'] == self.last_trading_day]['date'].values

        if len(check_day) != 0:
            custom_log.log_info_cmd('trading_day:[%s] data Already exist.' % check_day[0])
            return
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) \
Chrome/70.0.3538.77 Safari/537.36'}
        for url in self.url_list:
            try:
                xml = requests.get(url, headers=headers)
            except BaseException:
                continue
            text = xml.text
            soup = BeautifulSoup(text, 'lxml')
            if soup.find('tradingday') is not None:
                trading_day = soup.find('tradingday').string
                trade_list = list(set([i['text']
                                       for i in soup.find_all('data')]))
                datatype_id_list = sorted(
                    set([i['value'] for i in soup.find_all('data')]))

                for trade in trade_list:
                    csv_list = [trading_day, trade.strip()]
                    for datatype_id in datatype_id_list:
                        data_list = soup.find_all(
                            attrs={'text': trade, 'value': datatype_id})
                        volume_sum = 0
                        volume_change_sum = 0
                        for data in data_list:
                            volume = re.search(
                                r'<volume>(\d*)</volume>', str(data)).group(1)
                            volume_change = re.search(
                                r'<varvolume>(.*)</varvolume>', str(data)).group(1)

                            volume_sum += int(volume)
                            volume_change_sum += int(volume_change)
                        csv_list.extend([volume_sum, volume_change_sum])
                    self.writer_csv(csv_list)

    def __exit__(self, exc_type, exc_val, exc_tb):
        custom_log.log_info_cmd('spider finish')
        self.csv_file.close()


def spider_cff_info_job():
    last_trading_day = date_utils.get_last_trading_day("%Y%m/%d")
    url_list = []
    for type_name in trade_types:
        url_list.append(
            'http://www.cffex.com.cn/sj/ccpm/%s/%s.xml' %
            (last_trading_day, type_name))
    with SpiderCffInfo(url_list, last_trading_day) as tool:
        tool.spider_data()


if __name__ == '__main__':
    spider_cff_info_job()
