# # -*- coding: utf-8 -*-
#
#
# def __line_check(game_result):
#     for line_index in range(0, 3):
#         last_value = None
#         match_flag = True
#         for column_index in range(0, 3):
#             if last_value is None:
#                 if game_result[line_index][column_index] == '.':
#                     match_flag = False
#                 last_value = game_result[line_index][column_index]
#                 continue
#             elif game_result[line_index][column_index] != last_value:
#                 match_flag = False
#
#         if match_flag:
#             return last_value
#
#
# def __column_check(game_result):
#     for column_index in range(0, 3):
#         last_value = None
#         match_flag = True
#         for line_index in range(0, 3):
#             if last_value is None:
#                 if game_result[line_index][column_index] == '.':
#                     match_flag = False
#                 last_value = game_result[line_index][column_index]
#                 continue
#             elif game_result[line_index][column_index] != last_value:
#                 match_flag = False
#
#         if match_flag:
#             return last_value
#
#
# def __slash_check(game_result):
#     if game_result[0][0] == game_result[1][1] == game_result[2][2] and game_result[0][0] != '.':
#         return game_result[0][0]
#     elif game_result[0][2] == game_result[1][1] == game_result[2][0] and game_result[0][2] != '.':
#         return game_result[0][2]
#
#
# def checkio(game_result):
#     return_flag = __line_check(game_result)
#     if return_flag is not None:
#         return return_flag
#
#     return_flag = __column_check(game_result)
#     if return_flag is not None:
#         return return_flag
#
#     return_flag = __slash_check(game_result)
#     if return_flag is not None:
#         return return_flag
#     return "D"
#
#
# if __name__ == '__main__':
#     #These "asserts" using only for self-checking and not necessary for auto-testing
#     pass

#
# from splinter.browser import Browser
# from time import sleep
# import traceback
#
#
# class Buy_Tickets(object):
#     # 定义实例属性，初始化
#     def __init__(self, username, passwd, order, passengers, dtime, starts, ends):
#         self.username = username
#         self.passwd = passwd
#         # 车次，0代表所有车次，依次从上到下，1代表所有车次，依次类推
#         self.order = order
#         # 乘客名
#         self.passengers = passengers
#         # 起始地和终点
#         self.starts = starts
#         self.ends = ends
#         # 日期
#         self.dtime = dtime
#         # self.xb = xb
#         # self.pz = pz
#         self.login_url = 'https://kyfw.12306.cn/otn/login/init'
#         self.initMy_url = 'https://kyfw.12306.cn/otn/index/initMy12306'
#         self.ticket_url = 'https://kyfw.12306.cn/otn/leftTicket/init'
#         self.driver_name = 'chrome'
#         self.executable_path = 'C:\Python36\Scripts\chromedriver.exe'
#     # 登录功能实现
#     def login(self):
#         self.driver.visit(self.login_url)
#         self.driver.fill('loginUserDTO.user_name', self.username)
#         # sleep(1)
#         self.driver.fill('userDTO.password', self.passwd)
#         # sleep(1)
#         print('请输入验证码...')
#         while True:
#             if self.driver.url != self.initMy_url:
#                 sleep(1)
#             else:
#                 break
#     # 买票功能实现
#     def start_buy(self):
#         self.driver = Browser(driver_name=self.driver_name, executable_path=self.executable_path)
#         #窗口大小的操作
#         self.driver.driver.set_window_size(700, 500)
#         self.login()
#         self.driver.visit(self.ticket_url)
#         try:
#             print('开始购票...')
#             # 加载查询信息
#             self.driver.cookies.add({"_jc_save_fromStation": self.starts})
#             self.driver.cookies.add({"_jc_save_toStation": self.ends})
#             self.driver.cookies.add({"_jc_save_fromDate": self.dtime})
#             self.driver.reload()
#             count = 0
#             if self.order != 0:
#                 while self.driver.url == self.ticket_url:
#                     self.driver.find_by_text('查询').click()
#                     count += 1
#                     print('第%d次点击查询...' % count)
#                     try:
#                         self.driver.find_by_text('预订')[self.order-1].click()
#                         sleep(1.5)
#                     except Exception as e:
#                         print(e)
#                         print('预订失败...')
#                         continue
#             else:
#                 while self.driver.url == self.ticket_url:
#                     self.driver.find_by_text('查询').click()
#                     count += 1
#                     print('第%d次点击查询...' % count)
#                     try:
#                         for i in self.driver.find_by_text('预订'):
#                             i.click()
#                             sleep(1)
#                     except Exception as e:
#                         print(e)
#                         print('预订失败...')
#                         continue
#             print('开始预订...')
#             sleep(1)
#             print('开始选择用户...')
#             for p in self.passengers:
#
#                 self.driver.find_by_text(p).last.click()
#                 sleep(0.5)
#                 if p[-1] == ')':
#                     self.driver.find_by_id('dialog_xsertcj_ok').click()
#             print('提交订单...')
#             # sleep(1)
#             # self.driver.find_by_text(self.pz).click()
#             # sleep(1)
#             # self.driver.find_by_text(self.xb).click()
#             # sleep(1)
#             self.driver.find_by_id('submitOrder_id').click()
#             sleep(2)
#             print('确认选座...')
#             self.driver.find_by_id('qr_submit_id').click()
#             print('预订成功...')
#         except Exception as e:
#             print(e)
#
#
#
#
# if __name__ == '__main__':
#     # 用户名
#     username = 'xxxx'
#     # 密码
#     password = 'xxx'
#     # 车次选择，0代表所有车次
#     order = 2
#     # 乘客名，比如passengers = ['丁小红', '丁小明']
#     # 学生票需注明，注明方式为：passengers = ['丁小红(学生)', '丁小明']
#     passengers = ['丁彦军']
#     # 日期，格式为：'2018-01-20'
#     dtime = '2018-01-19'
#     # 出发地(需填写cookie值)
#     starts = '%u5434%u5821%2CWUY' #吴堡
#     # 目的地(需填写cookie值)
#     ends = '%u897F%u5B89%2CXAY' #西安
#
#     # xb =['硬座座']
#     # pz=['成人票']
#
#
#     Buy_Tickets(username, password, order, passengers, dtime, starts, ends).start_buy()

# import requests
# params = {'message': 'change to panic!', 'status': 200}
# r = requests.post(url='http://172.16.11.127:10000/server_notify', json=params)
# print r.text


from faker import Faker

fake = Faker(locale='zh_CN')

print fake.name()
print u'邓' + fake.last_name_male()
print u'邓' + fake.last_name_female()
