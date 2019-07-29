# -*- coding:utf8 -*-
from xmlrpclib import ServerProxy

# Z盘为NAS(//172.16.12.123/share/)


# test
def cta_test_task():
    s = ServerProxy('http://172.16.12.133:8000')
    print s.cta_test()


# 策略上模拟盘
# 将上线策略参数文件重命名并放置到以下路径：
# Z:/dailyjob/cta_update_info/strategy_parameter.csv
def backtest_init_task():
    s = ServerProxy('http://172.16.12.133:8000')
    s.backtest_init()


# 更新策略参数
# 将策略参数文件重命名并放置到以下路径：
# Z:/dailyjob/cta_update_info/para_insert_sql/，文件夹内包含nanhua,zhongxin,luzheng三个文件夹，
# （今后有新的交易服务器需要再添加），每个文件夹内的文件名为para_insert_sql_file.txt，一般直接把参数修改程序的结果放
# 过去即可
def load_strategy_parameter():
    s = ServerProxy('http://172.16.12.133:8000')
    s.load_strategy_parameter()


# 策略上、下线实盘
# 将上线策略参数文件重命名并放置到以下路径：
#  Z:/dailyjob/cta_update_info/strategy_online_offline_file.csv
def strategy_online_offline_task():
    s = ServerProxy('http://172.16.12.133:8000')
    s.strategy_online_offline_job()


if __name__ == "__main__":
    cta_test_task()

