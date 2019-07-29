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
    ftp_wsdl_address = server_model.server_path_dict['ftp_wsdl_address']
    deposit_ftp_server = ServerProxy(ftp_wsdl_address)

    source_file_path = sql_file_path
    target_file_path = '%s/%s' % \
        (server_model.server_path_dict['ftp_upload_folder'], date_utils.get_today_str())
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

    time.sleep(5)

    # 切换至'工具列表'--》'数据库更新'
    # b.find_link_by_href('/tool_list/eod_tools').click()
    #
    # b.find_by_id('sql_file_name').fill(sql_file_name)
    #
    # b.find_by_id('update_database_btn').click()
    # time.sleep(5)

    # # 退出
    # b.quit()


def update_sql_index(sql_file_path):
    server_name = 'citics'
    upload_sql_file(server_name, sql_file_path)

    website_url = 'http://172.16.10.128:8088/'
    sql_file_name = os.path.basename(sql_file_path)
    update_by_website(website_url, sql_file_name)


if __name__ == '__main__':
    # update_sql_index('d:/update_sql_20180320.sql')
    update_by_website('http://172.16.10.128:8088/', '')

# b.fill('name', 'admin')
# b.fill('password', '123456')
#
# b.find_by_xpath('//*[@id="app"]/form/div[4]/div/button').click()
#
# b.find_by_xpath('/html/body/div/section/aside/ul/li[2]').click()
# print(0)
# time.sleep(3)
# b.find_by_xpath('/html/body/div/section/aside/ul/li[2]/ul/li[3]').click()
# time.sleep(3)
# print(1)
#
# b.find_by_xpath('/html/body/div/section/section/main/div/button').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[1]/div/div/div[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[3]/div[1]/div[1]/ul/li[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[2]/div/div/div[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[4]/div[1]/div[1]/ul/li[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[3]/div/div/div[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[5]/div[1]/div[1]/ul/li[2]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[4]/div/div[1]/input').fill('xiaoming')
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[5]/div/div[1]/input').fill('123456789')
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[6]/div/div[1]/input').fill('123456789')
# time.sleep(1)
#
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[7]/div/label[1]/span[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[8]/div/label[1]/span[1]').click()
# time.sleep(1)
#
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[9]/div/div/div[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[6]/div[1]/div[1]/ul/li[5]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[10]/div/label[1]/span[1]').click()
# time.sleep(1)
# b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[11]/div/button[1]').click()
# time.sleep(1)
#
# b.quit()
