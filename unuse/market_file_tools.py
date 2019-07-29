# WechatPush.py
# encoding: utf-8
import os
base_path = 'E:/dailyFiles/market'


def read_market_file(file_name):
    print 'file_name:', file_name
    input_file = open(os.path.join(base_path, file_name))
    rank_list = []
    for line in input_file.readlines():
        if 'InstrumentID=IH1612' in line:
            rank_list.append('0')
        elif 'InstrumentID=510050' in line:
            rank_list.append('1')

    rank_str = ''.join(rank_list)
    print "101:", rank_str.count("101")
    print "1001:" ,rank_str.count("1001")
    print "10001:" ,rank_str.count("10001")
    print "100001:" ,rank_str.count("100001")
    print "1000001:" ,rank_str.count("1000001")
    print "10000001:" ,rank_str.count("10000001")
    print "100000001:" ,rank_str.count("100000001")
    print "1000000001:" ,rank_str.count("1000000001")
    print "10000000001:" ,rank_str.count("10000000001")
    print "100000000001:" ,rank_str.count("100000000001")
    print "1000000000001:" ,rank_str.count("1000000000001")


if __name__ == '__main__':
    read_market_file('510050_IH1612_1205.txt')



