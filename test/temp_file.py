# -*- coding: utf-8 -*-
import os

from eod_aps.model.schema_jobs import SpecialTickers
from eod_aps.model.server_constans import ServerConstant


server_constant = ServerConstant()
server_host = server_constant.get_server_model('host')
session_jobs = server_host.get_db_session('jobs')
special_ticker_list = []
for special_ticker in session_jobs.query(SpecialTickers).filter(SpecialTickers.date == '2019-02-27'):
    if special_ticker.describe in ('High_Stop', 'Low_Stop'):
        special_ticker_list.append(special_ticker.ticker)
print special_ticker_list

base_folder = 'E:/test'
last_ticker_list = []
last_folder = os.path.join(base_folder, '20190226_change')
for file_name in os.listdir(last_folder):
    with open(os.path.join(last_folder, file_name), 'rb') as fr:
        if not file_name.endswith('.txt'):
            continue
        for line in fr.readlines():
            last_ticker_list.append(line.split(',')[0])
print last_ticker_list

today_ticker_list = []
today_folder = os.path.join(base_folder, '20190227_change')
for file_name in os.listdir(today_folder):
    with open(os.path.join(today_folder, file_name), 'rb') as fr:
        if not file_name.endswith('.txt'):
            continue
        for line in fr.readlines():
            today_ticker_list.append(line.split(',')[0])
print today_ticker_list


tmp_list = [val for val in last_ticker_list if val in today_ticker_list]
print tmp_list
for ticker in tmp_list:
    if ticker in special_ticker_list:
        print ticker



