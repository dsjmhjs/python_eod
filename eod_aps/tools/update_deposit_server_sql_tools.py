# -*- coding: utf-8 -*-
import os
import time
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from splinter.browser import Browser
from xmlrpclib import ServerProxy


date_utils = DateUtils()


def upload_sql_file(server_name, sql_file_path):
    server_model = server_constant.get_server_model(server_name)
    ftp_wsdl_address = server_model.ftp_wsdl_address
    deposit_ftp_server = ServerProxy(ftp_wsdl_address)

    source_file_path = sql_file_path
    target_file_path = '%s/%s' % \
        (server_model.ftp_upload_folder, date_utils.get_today_str())
    upload_flag = deposit_ftp_server.upload_file(source_file_path, target_file_path)
    if upload_flag:
        print 'upload:%s Success.' % source_file_path
    else:
        print'upload:%s Fail!' % source_file_path


def update_by_website(website_url, sql_file_name):
    b = Browser(driver_name='chrome')
    b.visit(website_url)

    # 登陆
    b.fill('username', 'admin')
    b.fill('password', 'admin')

    code = b.find_by_id('code')[0].text
    b.fill('code', code)

    b.find_by_text('Log in!').click()

    # 切换至'工具列表'--》'数据库更新'
    b.find_link_by_href('/tool_list/eod_tools').click()

    b.find_by_id('sql_file_name').fill(sql_file_name)

    b.find_by_id('update_database_btn').click()
    time.sleep(5)

    # # 退出
    # b.quit()


def update_sql_index(sql_file_path):
    server_name = 'citics'
    upload_sql_file(server_name, sql_file_path)

    website_url = 'http://172.16.10.128:8088/'
    sql_file_name = os.path.basename(sql_file_path)
    update_by_website(website_url, sql_file_name)


if __name__ == '__main__':
    update_sql_index('d:/update_sql_20180320.sql')
