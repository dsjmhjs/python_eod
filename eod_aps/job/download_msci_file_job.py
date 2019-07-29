#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import traceback

import requests
import re
import os
import time
from eod_aps.job import *


def create_path(save_path, path):
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    real_save_path = save_path + path
    if not os.path.exists(real_save_path):
        os.mkdir(real_save_path)


def download_file(baseurl, path_list, base_url, base_path):
    login_data = {
        'password': 'S6rbiowsmhqit?',
        'username': 'hcqlztti',
    }
    headers_base = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'fileservice.msci.com',
        'Origin': 'https://fileservice.msci.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36',
    }
    session = requests.session()

    login_url = baseurl + "m/login"
    requests.urllib3.disable_warnings()
    response = requests.post(login_url, headers=headers_base, data=login_data)
    c = response.cookies.get_dict()

    file_list_dict = {}
    date_list = []
    for path in path_list:
        url = base_url + path
        s = session.get(url, cookies=c, verify=False)
        trs = re.findall(r'<tr.*?</tr>', s.content.replace('\n', '').replace(' ', ''))
        for tr in trs:
            if re.findall(r'[\w.*]+\.\w{3,4}', tr):
                file_date = re.findall(r'(\d{4}-\d{1,2}-\d{1,2})', tr)[-1]
                file_name = re.findall(r'[\w.*]+\.\w{3,4}', tr)[-1]
                date_str = file_date.replace('-', '')
                save_path = base_path % date_str
                create_path(save_path, path)
                file_url = url + file_name
                file_path = os.path.join(save_path + path, file_name)
                if file_name != 'folder.gif':
                    if date_str in file_list_dict:
                        file_list_dict[date_str].append((file_url, file_path))
                    else:
                        file_list_dict[date_str] = [(file_url, file_path)]
        date_list = file_list_dict.keys()
        date_list = sorted(date_list, reverse=True)
    ys_date = date_utils.get_last_trading_day("%Y%m%d")
    if ys_date not in file_list_dict or len(file_list_dict[ys_date]) == 0:
        time.sleep(300)
        return 0
    for date_item in date_list:
        file_list = file_list_dict[date_item]

        for file_item in file_list:
            file_url = file_item[0]
            save_path = file_item[1]
            s = session.get(file_url, cookies=c, verify=False)
            with open(save_path, 'wb') as f:
                f.write(s.content)
    return 1


def download_msci_file_job():
    download_flag = __download_msci_file()
    while 50000 <= int(date_utils.get_today_str('%H%M%S')) <= 80000 and not download_flag:
        download_flag = __download_msci_file()
    # check_barra_file()


def __download_msci_file():
    download_flag = False
    login_url = 'https://fileservice.msci.com/'
    base_url = 'https://fileservice.msci.com/m/home/hcqlztti/barra'
    path_list = ['/cne5/', '/cne5/model_receipt/', '/cne5/daily/', '/bime/']
    try:
        server_model = server_constant.get_server_model('host')
        msci_data_path = server_model.server_path_dict['msci_data_path']
        rst = download_file(login_url, path_list, base_url, msci_data_path)
        while not rst:
            rst = download_file(login_url, path_list, base_url, msci_data_path)
        download_flag = True
    except Exception as e:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]download_msci_file', error_msg)
        time.sleep(600)
        download_msci_file_job()
    return download_flag


# def check_barra_file():
#     file_list = ['SMD_CNE5_LOCALID_ID_%s.zip', 'SMD_CNE5S_100_%s.zip']
#     ys_date = date_utils.get_last_trading_day("%Y%m%d")
#     path = 'Z:/dailyjob/msci_data/%s/barra/bime' % ys_date
#     error_file_list = []
#     success_file_list = []
#     for filename in file_list:
#         filepath = os.path.join(path, filename % ys_date[2:])
#         if not os.path.exists(filepath):
#             error_file_list.append(filename % ys_date[2:])
#         else:
#             success_file_list.append(filename % ys_date[2:])
#     if len(error_file_list):
#         email_utils2.send_email_group_all('[Error]barra 文件下载失败', '</br>'.join(error_file_list), 'html')
    # else:
    #     email_utils2.send_email_group_all('[Success]barra 文件下载成功', '</br>'.join(success_file_list), 'html')


if __name__ == '__main__':
    download_msci_file_job()
    # print int(date_utils.get_today_str('%H%M%S'))
