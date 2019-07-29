#!/usr/bin/env python
# _*_ coding:utf-8 _*_


# import os
import time
# from eod_aps.model.server_constans import ServerConstant
# from eod_aps.tools.date_utils import DateUtils
from splinter.browser import Browser
import shutil
import os
import json


def download_by_website(login_url, path_list, base_url, browser_save_path, base_path):
    b = Browser(driver_name='chrome')
    b.visit(login_url)
    b.fill('username', 'hcqlztti')
    b.fill('password', 'S6rbiowsmhqit?')
    b.find_by_text('Sign In').click()
    for path in path_list:
        curr_url = base_url + path
        save_path = base_path + path
        b.visit(curr_url)
        file_list = b.find_by_id('row').first.find_by_tag('tbody').first.find_by_tag('tr')
        print len(file_list)
        for item in file_list:
            file_name = item.find_by_tag('td').first.text
            print file_name, '++++++++++++++++++'
            item.find_by_tag('td').first.click()
            file_path = os.path.join(browser_save_path, file_name)
            time.sleep(1)
            if os.path.exists(file_path):
                shutil.move(file_path, save_path)
            else:
                print file_name
    b.quit()


if __name__ == '__main__':
    # browser_save_path = 'C:/Users/wt/Downloads'
    # base_path = 'D:/work/barra/'
    # login_url = 'https://fileservice.msci.com/'
    # base_url = 'https://fileservice.msci.com/m/home/hcqlztti/barra/'
    # path_list = ['cne5/', '/cne5/model_receipt/', '/cne5/daily/', '/bime/']
    # download_by_website(login_url, path_list, base_url, browser_save_path, base_path)
    # shutil.rmtree('D:/work/download/')
    # shutil.move(browser_save_path, 'D:/work/barra/cne5/')

    b = Browser(driver_name='chrome')
    b.visit('https://vpn-guest.citicsinfo.com')
    b.fill('svpn_name', 'bj-dongsanhuan-1')
    b.fill('svpn_password', 'EQJtqXC2')
    b.find_by_text(u'登 录').click()
    time.sleep(2)
    b.find_by_id('sendSms').click()