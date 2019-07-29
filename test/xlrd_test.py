# -*- coding: utf-8 -*-
from eod_aps.model.server_constans import server_constant
import xlrd
from eod_aps.model.schema_jobs import StatementInfo, FundInfo

workbook = xlrd.open_workbook(u'E:\\交易明细确认单\\20190301深圳市衍盛资产管理有限公司交易确认信息.xlsx')
booksheet = workbook.sheet_by_index(0)  # 用索引取第一个sheet

statement_info_db = StatementInfo()
colx_index = 0
title_row, value_row = 0, 1

server_model = server_constant.get_server_model('host')
session_job = server_model.get_db_session('jobs')

# xls_fund_name = booksheet.cell_value(3, 1).split(' ')[0]
# for x in session_job.query(FundInfo):
#     if xls_fund_name in x.name_alias:
#         statement_info_db.fund = x.name_chinese
#         break

for title_item in booksheet.row(title_row):
    if title_item.value.encode('utf-8') == '申请日期':
        statement_info_db.date = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '客户名称':
        statement_info_db.account = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '业务类型':
        statement_info_db.type = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '确认日期':
        statement_info_db.confirm_date = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '单位净值':
        statement_info_db.net_asset_value = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '申请金额':
        statement_info_db.request_money = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '确认金额':
        statement_info_db.confirm_money = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '确认份额':
        statement_info_db.confirm_units = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '交易费':
        statement_info_db.fee = booksheet.cell_value(value_row, colx_index)
    elif title_item.value.encode('utf-8') == '基金名称':
        xls_fund_name = booksheet.cell_value(value_row, colx_index)
    statement_info_db.performance_pay = 0
    colx_index += 1

for x in session_job.query(FundInfo):
    if xls_fund_name in x.name_alias:
        statement_info_db.fund = x.name_chinese
        break
print statement_info_db.to_dict()



