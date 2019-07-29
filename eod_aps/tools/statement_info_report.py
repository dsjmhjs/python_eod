# -*- coding: utf-8 -*-
import csv

from eod_aps.model.schema_jobs import StatementInfo, AssetValueInfo, FundInfo
from eod_aps.model.server_constans import server_constant
import pandas as pd


def query_fund_info_df(query_date_str):
    host_server_model = server_constant.get_server_model('host')
    session_jobs = host_server_model.get_db_session('jobs')

    assetvalue_info_list = []
    for x in session_jobs.query(AssetValueInfo).filter(AssetValueInfo.date_str == query_date_str):
        assetvalue_info_list.append(x.to_dict())
    assetvalue_info_df = pd.DataFrame(assetvalue_info_list)
    if len(assetvalue_info_df) == 0:
        return []
    assetvalue_info_df["unit_net"] = assetvalue_info_df["unit_net"].astype(float)
    assetvalue_info_df.rename(columns={'product_name': 'fund_name'}, inplace=True)

    fund_info_list = []
    for x in session_jobs.query(FundInfo):
        fund_info_list.append(x.to_dict())
    fund_info_df = pd.DataFrame(fund_info_list)
    fund_info_df.rename(columns={'name': 'fund_name', 'name_chinese': 'fund'}, inplace=True)

    fund_merge_df = pd.merge(assetvalue_info_df[['fund_name', 'unit_net']], fund_info_df[['fund_name', 'fund']],
                             how='left', on=['fund_name']).fillna(0)
    return fund_merge_df


def statement_info_report_tools(query_fund, query_account, query_date_str, fund_info_df):
    host_server_model = server_constant.get_server_model('host')
    session_jobs = host_server_model.get_db_session('jobs')

    statement_info_list = []
    for x in session_jobs.query(StatementInfo).filter(StatementInfo.date <= query_date_str):
        statement_info_list.append(x.to_dict())
    statement_info_df = pd.DataFrame(statement_info_list)

    if query_fund:
        statement_info_df = statement_info_df[statement_info_df['fund'] == query_fund]
    if query_account:
        statement_info_df = statement_info_df[statement_info_df['account'] == query_account]

    statement_report_list = []
    for group_key, group in statement_info_df.groupby(['fund', 'account']):
        temp_list = []
        temp_list.extend(list(group_key))
        filter_redemption_df = group[group['type'].isin(['赎回'.decode('utf8'), '份额转让'.decode('utf8')])]
        temp_list.append(filter_redemption_df['confirm_money'].sum())

        filter_purchase_df = group[group['type'].isin(['申购'.decode('utf8'), '认购'.decode('utf8'), '份额受让'.decode('utf8')])]
        temp_list.append(filter_purchase_df['confirm_money'].sum())

        temp_list.append(group['confirm_units'].sum())
        statement_report_list.append(temp_list)
    statement_report_df = pd.DataFrame(statement_report_list, columns=['fund', 'account', 'redemption_money',
                                                                       'purchase_money', 'fund_units'])

    statement_report_df = pd.merge(statement_report_df, fund_info_df, how='left', on=['fund']).fillna(0)
    statement_report_df['account_balance'] = statement_report_df['fund_units'] * statement_report_df['unit_net']
    statement_report_df.loc['Total'] = statement_report_df[['redemption_money', 'purchase_money', 'fund_units',
                                                            'account_balance']].sum()
    statement_report_df = statement_report_df.fillna('')
    statement_report_df['fund'].iloc[-1] = u'合计'

    statement_report_df['return_rate'] = (statement_report_df['account_balance'] + statement_report_df['redemption_money'] - statement_report_df['purchase_money']) / statement_report_df['purchase_money']
    statement_report_df['return_rate'] = statement_report_df['return_rate'].apply(lambda x: '%.2f%%' % (x * 100))

    result_list = []
    for (index, dict_value) in statement_report_df.to_dict("index").items():
        result_list.append(dict_value)
    return result_list


def __format_number(input_value):
    if input_value == '':
        input_value = 0

    input_value = str(input_value).strip()
    if input_value == '-':
        result_value = 0
    else:
        result_value = input_value.replace(',', '')
    return float(result_value)


def insert_statement_info():
    csv_file = file(u"./衍盛中港精选.csv", 'rb')

    statement_info_list = []
    reader = csv.reader(csv_file)
    i = 0
    for line in reader:
        if i == 0:
            i += 1
            continue
        statement_info = StatementInfo()
        statement_info.fund_name = 'ch_selection'
        statement_info.fund = u'衍盛中港精选'
        statement_info.account = line[0].decode("gbk")
        statement_info.date = line[1]
        statement_info.type = line[3].decode("gbk")
        statement_info.confirm_date = line[2] if line[2] != '' else None
        statement_info.net_asset_value = __format_number(line[4])
        statement_info.request_money = __format_number(line[5])
        statement_info.confirm_money = __format_number(line[6])
        statement_info.confirm_units = __format_number(line[7])
        statement_info.fee = __format_number(line[8]) + __format_number(line[9])
        statement_info.performance_pay = __format_number(line[10])
        statement_info_list.append(statement_info)
        # if ('000300' == line[0]) and (u'次日权重' == line[5].decode("gbk")):

    host_server_model = server_constant.get_server_model('host')
    session_jobs = host_server_model.get_db_session('jobs')
    for statement_info_db in statement_info_list:
        session_jobs.merge(statement_info_db)
    session_jobs.commit()


if __name__ == '__main__':
    query_date_str = '2018-08-24'
    fund_info_df = query_fund_info_df(query_date_str)
    print fund_info_df
    print statement_info_report_tools(None, u'叶晓华', query_date_str, fund_info_df)
    # insert_statement_info()
