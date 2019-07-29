#!/usr/bin/env python
# _*_ coding:utf-8 _*_
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


def get_daily_data(date, fields=None):
    if fields is None:
        fields = []
    dict_ = proxy.get_daily_data(date, fields)
    return dataframe_format(dict_)


if __name__ == '__main__':
    print get_basic_info_data()
