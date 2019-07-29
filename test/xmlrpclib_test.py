# -*- coding: utf-8 -*-
import re

import redis


# r = redis.Redis(host='ec2-13-115-230-126.ap-northeast-1.compute.amazonaws.com', port=42515, password='!made4crypt0$', db=0)
# pipeline_redis = r.pipeline()
#
# r.hset("Market", '1', '30.32')
#
# base_parameter_str = '[tq.All_Weather_1.max_short_position]0:0:0'
# reg = re.compile('^.*\[(?P<parameter_name>.*)\](?P<parameter_value>[^:]*):*')
# reg_match = reg.match(base_parameter_str)
# base_parameter_dict = reg_match.groupdict()
# print base_parameter_dict
import os,subprocess
# cwrsync_base_path = 'C:\\Program Files (x86)\\cwRsync\\bin'
# cmd = "rsync -e 'ssh -p22012' trader@180.166.154.117:/home/trader/all_market_nanhua_2018-03-23.tar.gz /cygdrive/D/data_backup/"
# os.chdir(cwrsync_base_path)
# rst = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# out_list = rst.stdout.readlines()
# err_list = rst.stderr.readlines()
# print out_list,err_list


import pandas as pd
import xmlrpclib
proxy = xmlrpclib.ServerProxy("http://172.16.12.166:8000/")


def dataframe_format(data_dict):
    df = pd.DataFrame(data_dict["data"], index=data_dict["index"],
                      columns=data_dict["fields"])
    return df


def get_basic_info_data():
    dict_ = proxy.get_basic_info_data()
    return dataframe_format(dict_)


def get_daily_data(date, fields=list()):
    dict_ = proxy.get_daily_data(date, fields)
    return dataframe_format(dict_)


if __name__ == '__main__':
    # print get_daily_data(20180620, ["est_pe_fy1", "market_value", "open", "high", "low", "close"])
    basic_data = get_basic_info_data()
    # tmp_basic_info = basic_data.loc[['000001', '000002', 'IH1809'], ['name', 'industry', 'conception']].to_dict()
