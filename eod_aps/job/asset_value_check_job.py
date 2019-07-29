#!/usr/bin/env python
# _*_ coding:utf-8 _*_
from eod_aps.model.schema_jobs import RiskManagement, FundInfo, AssetValueInfo
from eod_aps.job import *


def asset_value_check_job():
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    expiry_fund_list = []
    for fund_info in session_jobs.query(FundInfo):
        if fund_info.expiry_time:
            expiry_fund_list.append(fund_info.name)
    fund_list = []
    for obj in session_jobs.query(RiskManagement):
        for item in obj.fund_risk_list.split(';'):
            fund_name = item.split('|')[0]
            if fund_name not in fund_list and fund_name not in expiry_fund_list:
                fund_list.append(fund_name)
    prev_2d_date = date_utils.get_interval_trading_day(-2)
    for fund_name in session_jobs.query(AssetValueInfo.product_name).filter(AssetValueInfo.date_str == prev_2d_date):
        if fund_name[0] in fund_list:
            fund_list.remove(fund_name[0])
    if fund_list:
        email_utils2.send_email_group_all('%s 产品净值缺失' % prev_2d_date, ','.join(fund_list))


if __name__ == '__main__':
    asset_value_check_job()
