# -*- coding: utf-8 -*-
import json
import os
import urllib2
import requests
from bs4 import BeautifulSoup
from eod_aps.job import *
import pandas as pd

szse_url_templat = 'http://www.szse.cn/api/disc/announcement/annList?random=0.46377712218296496'
szse_pdf_title = 'http://disc.static.szse.cn'
sse_url = 'http://www.sse.com.cn/disclosure/listedinfo/announcement/s_docdatesort_desc_2016openpdf.htm'
check_words = [u'冻结', u'查封', u'违约', u'逾期', u'未按期', u'暂停上市', u'终止上市', u'问询函', u'立案调查']


class ExchangeNoticeMonitor(object):
    def __init__(self):
        self.__ticker_notice_dict = dict()
        self.__position_ticker_list = []
        self.__filter_message_list = []
        self.__email_message_list = []

    def start_work(self):
        self.__read_szse_content()
        self.__read_sse_content()
        self.__filter_notice_message()
        self.__send_email()

    def __read_sse_content(self):
        page = urllib2.urlopen(sse_url)
        soup = BeautifulSoup(page, 'lxml')
        a_set = soup.find_all('a')
        for a_item in a_set:
            if 'class' not in a_item.attrs:
                continue

            a_href = a_item.attrs['href']
            a_title = a_item.attrs['title']
            file_name = os.path.basename(a_href)
            ticker = file_name.split('_')[0]
            self.__ticker_notice_dict.setdefault(ticker, []).append([a_title, a_href])

    def __read_szse_content(self):
        # last_day_str = date_utils.get_last_day(-1, None, '%Y-%m-%d')
        # today_str = date_utils.get_last_day(-2, None, '%Y-%m-%d')
        today_str = date_utils.get_today_str('%Y-%m-%d')

        params = {'channelCode': ['listedNotice_disc'],
                  'pageNum': 1,
                  'pageSize': 30,
                  'seDate': [today_str, today_str]}
        r = requests.post(url=szse_url_templat, json=params)
        content_dict = json.loads(r.text.replace('：', ':'))
        content_size = content_dict['announceCount']

        page_size = int(int(content_size) / 30) + 1

        for page_num in range(1, page_size):
            params['pageNum'] = page_num
            r = requests.post(url=szse_url_templat, json=params)
            content_dict = json.loads(r.text.replace('：', ':'))
            if 'data' not in content_dict:
                continue
            for company_item in content_dict['data']:
                ticker = company_item['secCode'][0]
                attach_path = szse_pdf_title + company_item['attachPath']
                self.__ticker_notice_dict.setdefault(ticker, []).append([company_item['title'], attach_path])

    def __read_symbol_data(self):
        blacklist_path = "%s/Blacklist.csv" % BLACKLIST_FOLDER
        data_list = pd.read_csv(blacklist_path)
        last_day = data_list['date'].tail(1).values[0]
        symbol_list = [i.split('.')[0] for i in
                       data_list[data_list['date'] == last_day]['symbol'].values]
        return symbol_list, last_day

    def __filter_notice_message(self):
        self.__filter_message_list = []
        last_day = ''
        last_trading_day = date_utils.get_last_trading_day('%Y%m%d')
        custom_log.log_info_job('Notice Message Len:%s' % len(self.__ticker_notice_dict))
        for dict_key, dict_item_list in self.__ticker_notice_dict.items():
            exist_flag = False
            for (notice_message, attach_path) in dict_item_list:
                for check_word in check_words:
                    if check_word in notice_message:
                        a_attach_path = '<a href="%s" target="_blank">Open</a>' % attach_path
                        exist_flag_html = '<div style="color:#330033" target="_blank">%s</div>' % exist_flag
                        symbol_list, last_day = self.__read_symbol_data()
                        if dict_key in symbol_list:
                            exist_flag = True
                            exist_flag_html = '<div style="color:blue" target="_blank">%s</div>' % exist_flag
                        if last_trading_day != str(last_day):
                            if exist_flag:
                                font_color = 'blue'
                            else:
                                font_color = '#330033'
                            exist_flag_html = '<div style="background-color:yellow;color:%s" target="_blank">%s</div>' % (
                                font_color, exist_flag)
                        filter_message_item = (dict_key, check_word, exist_flag_html, notice_message, a_attach_path)
                        if filter_message_item in self.__email_message_list:
                            continue

                        self.__filter_message_list.append(filter_message_item)
        tagging_text = u'注：Black_List_Date:%s(黄色背景表示非上一交易日)' % last_day
        self.tagging_html = '<p style="color:#9900FF" target="_blank">%s</p>' % tagging_text
        custom_log.log_info_job('Filter Notice Message Len:%s' % len(self.__filter_message_list))

    def __send_email(self):
        if len(self.__filter_message_list) > 0:
            self.__filter_message_list.sort()
        self.__email_message_list.extend(self.__filter_message_list)

        html_list = email_utils9.list_to_html('Ticker,Key,Black_List,Title,Attach_Path', self.__filter_message_list)
        html_list.append(self.tagging_html)
        email_utils9.send_email_group_all('Company Notice Message!', ''.join(html_list), 'html')


if __name__ == '__main__':
    exchange_notice_monitor = ExchangeNoticeMonitor()
    exchange_notice_monitor.start_work()
