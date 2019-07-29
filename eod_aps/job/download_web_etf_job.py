# -*- coding: utf-8 -*-
import json
import sys
import os
import urllib
import urllib2
import traceback
from bs4 import BeautifulSoup
from eod_aps.job import *
import requests

download_etf_ticker_list = ['159922', '159923', '159924', '159925', '159927', '159928', '159929', '159930', '159931',
                            '159933', '159935', '159936', '159938', '159939', '159940', '159944', '159945', '159946',
                            '159901', '159902', '159903', '159905', '159906', '159907', '159908', '159909', '159910',
                            '159911', '159912', '159913', '159915', '159916', '159918', '159919', '159920', '159921',
                            '159932', '159934', '159937', '159942', '159943', '510450', '159926', '159941', '159001',
                            '159003', '159005']


def download_etf_web_job(filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    base_url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=sgshqd&TABKEY=tab1&txtStart=%s' % \
               filter_date_str
    base_soup = navigate(base_url)
    content_list = json.loads(base_soup.text)

    page_size = content_list[0]["metadata"]["pagecount"]

    for i in range(1, int(page_size) + 1):
        real_url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=sgshqd&TABKEY=tab1&\
txtStart=%s&PAGENO=%s' % (filter_date_str, i)
        page = urllib2.urlopen(real_url)
        soup = BeautifulSoup(page, 'lxml')

        span_set = soup.find_all('a')
        for span_str in span_set:
            onclick_message = span_str.get('href')
            if 'eft_download' in onclick_message and 'opencode' in onclick_message:
                file_name = onclick_message.split("filename=")[1].split('%')[0]

                found_flag = False
                for ticker in download_etf_ticker_list:
                    if ticker in file_name:
                        found_flag = True
                        break
                if not found_flag:
                    continue
                download_file_type_list = ['xml', 'txt']
                for file_type in download_file_type_list:
                    download_file_name = '%s.%s' % (file_name, file_type)
                    download_url = 'http://reportdocs.static.szse.cn/files/text/ETFDown/' + download_file_name
                    download_file_path = ETF_FILE_PATH + '/' + download_file_name
                    if not os.path.exists(ETF_FILE_PATH):
                        custom_log.log_error_job('file path:%s miss!' % download_file_name)
                        continue
                    # 文件下载
                    html = requests.get(download_url)
                    if html.status_code == 200:
                        with open(download_file_path, 'w') as f:
                            f.write(html.content)


def navigate(base_url):
    try_times = 0
    while True:
        if try_times > 4:
            raise Exception('Connection Failed!TryTimes:', try_times)
        try:
            req = BeautifulSoup(urllib2.urlopen(base_url).read(), 'lxml')
            return req
        except Exception:
            error_msg = traceback.format_exc()
            try_times += 1
            custom_log.log_error_job('Connection Failed!TryTimes:%s,error_msg:%s' % (str(try_times), error_msg))


if __name__ == '__main__':
    download_etf_web_job()
