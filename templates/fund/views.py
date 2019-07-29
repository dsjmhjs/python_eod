# coding: utf-8
import os
import pickle

from eod_aps.model.eod_const import const
from eod_aps.model.schema_jobs import FundInfo, FundChangeInfo, RiskManagement, StatementInfo, AssetValueInfo, \
    FundAccountInfo
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.read_statement_file_tools import read_statement_file1, read_statement_file2
from eod_aps.tools.statement_info_report import statement_info_report_tools, query_fund_info_df
from flask import render_template, request, flash, redirect, url_for, jsonify, make_response
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from . import fund

date_utils = DateUtils()
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


@fund.route('/query_fund', methods=['GET', 'POST'])
def query_fund():
    query_params = request.json
    query_name = query_params.get('name')

    fund_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for fund_info_db in session_jobs.query(FundInfo):
        if query_name and query_name.lower() not in fund_info_db.name.lower():
            continue

        fund_item_dict = fund_info_db.to_dict()
        fund_item_dict['create_time'] = fund_info_db.create_time.strftime(
            '%Y-%m-%d') if fund_info_db.create_time is not None else None
        fund_item_dict['expiry_time'] = fund_info_db.expiry_time.strftime(
            '%Y-%m-%d') if fund_info_db.expiry_time is not None else None
        fund_item_dict['target_server_list'] = fund_info_db.target_servers.split(
            '|') if fund_info_db.target_servers is not None else []
        fund_list.append(fund_item_dict)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            fund_list = sorted(fund_list, key=lambda fund_item: fund_item[sort_prop],
                               reverse=True)
        else:
            fund_list = sorted(fund_list, key=lambda fund_item: fund_item[sort_prop])
    else:
        fund_list.sort(key=lambda obj: obj['name'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = fund_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(fund_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@fund.route('/query_account_fund', methods=['GET', 'POST'])
def query_account_fund():
    query_params = request.json
    query_server = query_params.get('server')
    query_type = query_params.get('type')
    query_product_name = query_params.get('product_name')

    fund_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for fund_account_info_db in session_jobs.query(FundAccountInfo):
        if query_server and query_server.lower() not in fund_account_info_db.server.lower():
            continue
        if query_type and query_type.lower() not in fund_account_info_db.type.lower():
            continue
        if query_product_name and query_product_name.lower() not in fund_account_info_db.product_name.lower():
            continue

        fund_item_dict = fund_account_info_db.to_dict()
        fund_item_dict[
            'investor'] = False if fund_account_info_db.investor == '0' else True if fund_account_info_db.investor is not None else None
        fund_item_dict[
            'margin_trading'] = False if fund_account_info_db.margin_trading == '0' else True if fund_account_info_db.margin_trading is not None else None
        fund_item_dict[
            'copper_options'] = False if fund_account_info_db.copper_options == '0' else True if fund_account_info_db.copper_options is not None else None
        fund_item_dict['inclusion_strategy_list'] = fund_account_info_db.inclusion_strategy.split(
            ',') if fund_account_info_db.inclusion_strategy is not None else []
        fund_item_dict[
            'kechuang_plate'] = False if fund_account_info_db.kechuang_plate == '0' else True if fund_account_info_db.kechuang_plate is not None else None
        fund_list.append(fund_item_dict)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            fund_list = sorted(fund_list, key=lambda fund_item: fund_item[sort_prop],
                               reverse=True)
        else:
            fund_list = sorted(fund_list, key=lambda fund_item: fund_item[sort_prop])
    else:
        fund_list.sort(key=lambda obj: obj['account_name'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = fund_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(fund_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@fund.route('/save_fund', methods=['GET', 'POST'])
def save_fund():
    params = request.json
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    id = params.get('id')
    if id:
        fund_info_db = session_jobs.query(FundInfo).filter(FundInfo.id == id).first()
    else:
        fund_info_db = FundInfo()

    fund_info_db.name = params.get('name')
    fund_info_db.name_chinese = params.get('name_chinese')
    fund_info_db.name_alias = params.get('name_alias')
    fund_info_db.create_time = params.get('create_time')
    fund_info_db.expiry_time = params.get('expiry_time')
    fund_info_db.describe = params.get('describe')
    fund_info_db.target_servers = '|'.join(params.get('target_server_list'))
    session_jobs.merge(fund_info_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, data=u"保存基金:%s成功" % params.get('name')), 200)


@fund.route('/save_account_fund', methods=['GET', 'POST'])
def save_account_fund():
    params = request.json
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    id = params.get('id')
    if id:
        fund_account_info_db = session_jobs.query(FundAccountInfo).filter(FundAccountInfo.id == id).first()
    else:
        fund_account_info_db = FundAccountInfo()
    fund_account_info_db.account_name = params.get('account_name')
    fund_account_info_db.product_name = params.get('product_name')
    fund_account_info_db.type = params.get('type')
    fund_account_info_db.broker = params.get('broker')
    fund_account_info_db.server = params.get('server')
    fund_account_info_db.service_charge = params.get('service_charge')
    fund_account_info_db.inclusion_strategy = ','.join(params.get('inclusion_strategy_list'))
    fund_account_info_db.hedging_limit = params.get('hedging_limit')
    fund_account_info_db.investor = params.get('investor')
    fund_account_info_db.matters_attention = params.get('matters_attention')
    fund_account_info_db.margin_trading = params.get('margin_trading')
    fund_account_info_db.copper_options = params.get('copper_options')
    fund_account_info_db.kechuang_plate = params.get('kechuang_plate')
    fund_account_info_db.describe = params.get('describe')
    session_jobs.merge(fund_account_info_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, data=u"保存基金账户:%s成功" % params.get('account_name')), 200)


@fund.route('/remove_fund', methods=['GET', 'POST'])
def remove_fund():
    params = request.json
    id = params.get('del_id')

    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    fund_info_db = session_jobs.query(FundInfo).filter(FundInfo.id == id).first()
    fund_name = fund_info_db.name
    session_jobs.delete(fund_info_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, message=u"删除基金:%s成功" % fund_name), 200)


@fund.route('/remove_account_fund', methods=['GET', 'POST'])
def remove_account_fund():
    params = request.json
    id = params.get('del_id')

    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    fund_account_info_db = session_jobs.query(FundAccountInfo).filter(FundAccountInfo.id == id).first()
    fund_account_name = fund_account_info_db.account_name
    session_jobs.delete(fund_account_info_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, message=u"删除基金账户:%s成功" % fund_account_name), 200)


@fund.route('/query_fund_list', methods=['GET', 'POST'])
def query_fund_list():
    fund_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for fund_info_db in session_jobs.query(FundInfo):
        item_dict = dict()
        item_dict['value'] = fund_info_db.id
        item_dict['label'] = fund_info_db.name
        fund_list.append(item_dict)
    return make_response(jsonify(code=200, data=fund_list), 200)


@fund.route('/query_fund_change', methods=['GET', 'POST'])
def query_fund_change():
    query_params = request.json
    query_fund_id = query_params.get('fund_id')

    fund_change_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    fund_info_dict = dict()
    for fund_info_db in session_jobs.query(FundInfo):
        fund_info_dict[fund_info_db.id] = fund_info_db

    for fund_change_info_db in session_jobs.query(FundChangeInfo):
        if query_fund_id and query_fund_id != fund_change_info_db.fund_id:
            continue
        fund_change_item_dict = fund_change_info_db.to_dict()
        fund_change_item_dict['date'] = fund_change_info_db.date.strftime('%Y-%m-%d')
        fund_change_item_dict['fund_name'] = fund_info_dict[fund_change_info_db.fund_id].name
        fund_change_list.append(fund_change_item_dict)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            fund_change_list = sorted(fund_change_list, key=lambda fund_item: fund_item[sort_prop],
                                      reverse=True)
        else:
            fund_change_list = sorted(fund_change_list, key=lambda fund_item: fund_item[sort_prop])
    else:
        fund_change_list.sort(key=lambda obj: obj['date'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = fund_change_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(fund_change_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@fund.route('/save_fund_change', methods=['GET', 'POST'])
def save_fund_change():
    params = request.json
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    id = params.get('id')
    if id:
        fund_change_info_db = session_jobs.query(FundChangeInfo).filter(FundChangeInfo.id == id).first()
    else:
        fund_change_info_db = FundChangeInfo()

    fund_change_info_db.date = params.get('date')
    fund_change_info_db.fund_id = params.get('fund_id')
    fund_change_info_db.type = params.get('type')
    fund_change_info_db.change_money = params.get('change_money')
    session_jobs.merge(fund_change_info_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, data=u"保存申赎信息成功"), 200)


@fund.route('/remove_fund_change', methods=['GET', 'POST'])
def remove_fund_change():
    params = request.json
    id = params.get('del_id')

    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    fund_change_info_db = session_jobs.query(FundChangeInfo).filter(FundChangeInfo.id == id).first()
    session_jobs.delete(fund_change_info_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, message=u"删除申赎信息成功"), 200)


@fund.route('/query_risk_management', methods=['GET', 'POST'])
def query_risk_management():
    query_params = request.json
    query_name = query_params.get('name')

    fund_name_set = set()
    for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
        for real_account in account_list:
            fund_name_set.add(real_account.fund_name)
    fund_name_list = list(fund_name_set)
    fund_name_list.sort()

    risk_management_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for risk_management_db in session_jobs.query(RiskManagement):
        if query_name and query_name not in risk_management_db.name:
            continue
        temp_item_dict = risk_management_db.to_dict()

        temp_item_dict['fund_risk_list'] = []
        temp_fund_risk_dict = dict()
        if risk_management_db.fund_risk_list != '':
            for fund_risk_item in risk_management_db.fund_risk_list.split(';'):
                [fund_name, warn_line, error_line, temp_warn_line, temp_error_line, expiry_time] = fund_risk_item.split(
                    '|')
                temp_fund_risk_dict[fund_name] = (warn_line, error_line, temp_warn_line, temp_error_line, expiry_time)
        for fund_name in fund_name_list:
            if fund_name in temp_fund_risk_dict:
                (warn_line, error_line, temp_warn_line, temp_error_line, expiry_time) = temp_fund_risk_dict[fund_name]
            else:
                warn_line = ''
                error_line = ''
                temp_warn_line = ''
                temp_error_line = ''
                expiry_time = ''
            temp_item_dict['fund_risk_list'].append(
                dict(fund_name=fund_name, warn_line=warn_line, error_line=error_line,
                     temp_warn_line=temp_warn_line, temp_error_line=temp_error_line,
                     expiry_time=expiry_time))
        risk_management_list.append(temp_item_dict)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = risk_management_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(risk_management_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@fund.route('/save_risk_management', methods=['GET', 'POST'])
def save_risk_management():
    params = request.json
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    id = params.get('id')
    if id:
        risk_management_db = session_jobs.query(RiskManagement).filter(RiskManagement.id == id).first()
    else:
        risk_management_db = RiskManagement()

    risk_management_db.name = params.get('name')
    risk_management_db.parameters = params.get('parameters')
    risk_management_db.monitor_index = params.get('monitor_index')
    risk_management_db.frequency = params.get('frequency')

    fund_risk_list = []
    for fund_risk_item in params.get('fund_risk_list'):
        if fund_risk_item['warn_line'].strip() == '':
            continue
        fund_risk_list.append('%s|%s|%s|%s|%s|%s' % (fund_risk_item['fund_name'], fund_risk_item['warn_line'],
                                                     fund_risk_item['error_line'], fund_risk_item['temp_warn_line'],
                                                     fund_risk_item['temp_error_line'], fund_risk_item['expiry_time']))

    if id:
        email_content_list = ['Title:%s' % risk_management_db.monitor_index,
                              'Describe:%s' % risk_management_db.describe]
        last_line_dict = dict()
        if risk_management_db.fund_risk_list != '':
            for fund_risk_item in risk_management_db.fund_risk_list.split(';'):
                fund_risk_item_list = fund_risk_item.split('|')
                last_line_dict[fund_risk_item_list[0]] = fund_risk_item_list[1:]

        line_dict = dict()
        for fund_risk_item in params.get('fund_risk_list'):
            if fund_risk_item['warn_line'].strip() == '':
                continue
            line_dict[fund_risk_item['fund_name']] = [fund_risk_item['warn_line'], fund_risk_item['error_line'],
                                                      fund_risk_item['temp_warn_line'],
                                                      fund_risk_item['temp_error_line'],
                                                      fund_risk_item['expiry_time']]

        temp_email_content = []
        for fund_name in set(last_line_dict.keys() + line_dict.keys()):
            temp_line_list = [fund_name]
            if fund_name in last_line_dict:
                last_line_value = last_line_dict[fund_name][:2]
            else:
                last_line_value = ['', '']
            temp_line_list.extend(last_line_value)

            if fund_name in line_dict:
                new_line_value = line_dict[fund_name][:2]
            else:
                new_line_value = ['', '']
            temp_line_list.extend(new_line_value)

            if last_line_value == new_line_value:
                continue
            temp_email_content.append(temp_line_list)
        html_table_list = email_utils2.list_to_html('FundName,Last_Warn_Line,Last_Error_Line,Warn_Line,Error_Line',
                                                    temp_email_content)
        email_content_list.append(''.join(html_table_list))
        email_utils2.send_email_group_all(u'风控参数修改提醒', '<br>'.join(email_content_list), 'html')

    risk_management_db.fund_risk_list = ';'.join(fund_risk_list)
    risk_management_db.describe = params.get('describe')
    session_jobs.merge(risk_management_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, data=u"保存风控信息成功"), 200)


@fund.route('/remove_risk_management', methods=['GET', 'POST'])
def remove_risk_management():
    params = request.json
    id = params.get('del_id')

    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    risk_management_db = session_jobs.query(RiskManagement).filter(RiskManagement.id == id).first()
    session_jobs.delete(risk_management_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, message=u"删除风控信息成功"), 200)


@fund.route('/query_fund_risk', methods=['GET', 'POST'])
def query_fund_risk():
    params = request.json
    query_fund_name = params.get('fund_name')

    risk_management_dict = const.EOD_POOL['risk_management_dict']
    fund_risk_df = risk_management_dict['fund_risk_df']
    underlying_risk_df = risk_management_dict['underlying_risk_df']
    stock_risk_df = risk_management_dict['stock_risk_df']
    update_time = risk_management_dict['update_time']
    # path = os.path.dirname(__file__)
    # fr = open(path + '/../../cfg/risk_pickle_data.txt', 'rb')
    # fund_risk_df = pickle.load(fr)
    # underlying_risk_df = pickle.load(fr)
    # stock_risk_df = pickle.load(fr)
    # fr.close()
    # update_time = ''

    fund_risk_list = []
    temp_fund_risk_df = fund_risk_df.fillna('')
    fund_risk_dict = temp_fund_risk_df.to_dict("index")
    for (dict_key, dict_value) in fund_risk_dict.items():
        if query_fund_name and dict_value['FundName'] != query_fund_name:
            continue

        dict_value['Net_Asset_Value'] = int(dict_value['Net_Asset_Value'])
        dict_value['TotalPnl'] = int(dict_value['TotalPnl'])
        if dict_value['NAV_Change_1D'] != '':
            dict_value['NAV_Change_1D'] = '%.2f%%' % (dict_value['NAV_Change_1D'] * 100)
        if dict_value['NAV_Change_3D'] != '':
            dict_value['NAV_Change_3D'] = '%.2f%%' % (dict_value['NAV_Change_3D'] * 100)
        if dict_value['NAV_Change_From_D1'] != '':
            dict_value['NAV_Change_From_D1'] = '%.2f%%' % (dict_value['NAV_Change_From_D1'] * 100)
        if dict_value['GEMPercent'] != '':
            dict_value['GEMPercent'] = '%.2f%%' % (dict_value['GEMPercent'] * 100)
        if dict_value['NetDelta_Percentage'] != '':
            dict_value['NetDelta_Percentage'] = '%.2f%%' % (dict_value['NetDelta_Percentage'] * 100)
        if dict_value['This_Year'] != '':
            dict_value['This_Year'] = '%.2f%%' % (dict_value['This_Year'] * 100)
        fund_risk_list.append(dict_value)

    underlying_risk_list = []
    temp_underlying_risk_df = underlying_risk_df.fillna('')
    underlying_risk_dict = temp_underlying_risk_df.to_dict("index")
    for (dict_key, dict_value) in underlying_risk_dict.items():
        if query_fund_name and dict_value['FundName'] != query_fund_name:
            continue
        if dict_value['NAV_Change_ByUnderlying'] != '':
            dict_value['NAV_Change_ByUnderlying'] = '%.4f%%' % (dict_value['NAV_Change_ByUnderlying'] * 100)
        underlying_risk_list.append(dict_value)

    stock_risk_list = []
    temp_stock_risk_df = stock_risk_df.fillna('')
    stock_risk_dict = temp_stock_risk_df.to_dict("index")
    for (dict_key, dict_value) in stock_risk_dict.items():
        if query_fund_name and dict_value['FundName'] != query_fund_name:
            continue
        dict_value['LastPrice'] = '%.2f' % dict_value['LastPrice']
        if dict_value['NAVPercentageByStock'] != '':
            dict_value['NAVPercentageByStock'] = '%.4f%%' % (dict_value['NAVPercentageByStock'] * 100)
        if dict_value['ADVPercentByStock'] != '':
            dict_value['ADVPercentByStock'] = '%.4f%%' % (dict_value['ADVPercentByStock'] * 100)
        stock_risk_list.append(dict_value)

    monitor_index_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for rm_db in session_jobs.query(RiskManagement):
        for fund_risk_item in rm_db.fund_risk_list.split(';'):
            [fund_name, warn_line, error_line, temp_warn_line, temp_error_line, expiry_time] = fund_risk_item.split('|')
            if temp_warn_line is None or temp_warn_line == '':
                warn_line_value = warn_line
            else:
                warn_line_value = temp_warn_line

            if temp_error_line is None or temp_error_line == '':
                error_line_value = error_line
            else:
                error_line_value = temp_error_line
            monitor_index_dict['%s|%s' % (rm_db.monitor_index, fund_name)] = (warn_line_value, error_line_value)
    sort_prop = params.get('sort_prop', '')
    sort_order = params.get('sort_order', '')
    if sort_prop == 'FundName':
        if sort_order == 'ascending':
            fund_risk_list = sorted(fund_risk_list, key=lambda data_item: data_item[sort_prop], reverse=True)
        else:
            fund_risk_list = sorted(fund_risk_list, key=lambda data_item: data_item[sort_prop])
    else:
        empty_value_list = filter(lambda item: item[sort_prop] == '', fund_risk_list)
        fund_risk_list = filter(lambda item: item[sort_prop] != '', fund_risk_list)
        if sort_order == 'ascending':
            fund_risk_list = sorted(fund_risk_list, key=lambda data_item: float(data_item[sort_prop].replace('%', '')),
                                    reverse=True)
        else:
            fund_risk_list = sorted(fund_risk_list, key=lambda data_item: float(data_item[sort_prop].replace('%', '')))
        fund_risk_list.extend(empty_value_list)
    return make_response(jsonify(code=200, update_time=update_time, fund_risk_list=fund_risk_list,
                                 underlying_risk_list=underlying_risk_list, stock_risk_list=stock_risk_list,
                                 monitor_index_dict=monitor_index_dict), 200)


@fund.route('/save_statement_info', methods=['GET', 'POST'])
def save_statement_info():
    params = request.json
    id = params.get('id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    if id:
        statement_info_db = session_job.query(StatementInfo).filter(StatementInfo.id == id).first()
    else:
        statement_info_db = StatementInfo()
    statement_info_db.date = params.get('date')
    statement_info_db.fund = params.get('fund')
    statement_info_db.account = params.get('account')
    statement_info_db.type = params.get('type')
    statement_info_db.confirm_date = params.get('confirm_date')
    statement_info_db.net_asset_value = params.get('net_asset_value')
    statement_info_db.request_money = params.get('request_money')
    statement_info_db.confirm_money = params.get('confirm_money')
    statement_info_db.confirm_units = params.get('confirm_units')
    statement_info_db.fee = params.get('fee')
    statement_info_db.performance_pay = params.get('performance_pay')
    session_job.merge(statement_info_db)
    session_job.commit()
    result_message = u"保存成功"
    return make_response(jsonify(code=200, data=result_message), 200)


@fund.route('/query_statement_funds', methods=['GET', 'POST'])
def query_statement_funds():
    fund_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for x in session_jobs.query(FundInfo):
        item_dict = dict()
        item_dict['value'] = x.name_chinese
        item_dict['label'] = x.name_chinese
        fund_list.append(item_dict)
    return make_response(jsonify(code=200, data=fund_list), 200)


@fund.route('/query_statement_accounts', methods=['GET', 'POST'])
def query_statement_accounts():
    account_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for statement_info_item in session_jobs.query(StatementInfo.account).group_by(StatementInfo.account):
        item_dict = dict()
        item_dict['value'] = statement_info_item[0]
        item_dict['label'] = statement_info_item[0]
        account_list.append(item_dict)
    return make_response(jsonify(code=200, data=account_list), 200)


@fund.route('/query_statement_types', methods=['GET', 'POST'])
def query_statement_types():
    type_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for statement_info_item in session_jobs.query(StatementInfo.type).group_by(StatementInfo.type):
        item_dict = dict()
        item_dict['value'] = statement_info_item[0]
        item_dict['label'] = statement_info_item[0]
        type_list.append(item_dict)
    return make_response(jsonify(code=200, data=type_list), 200)


@fund.route('/query_statement_info', methods=['GET', 'POST'])
def query_statement_info():
    query_params = request.json
    query_fund_name = query_params.get('fund_name')
    query_account = query_params.get('account')
    query_type = query_params.get('type')

    start_date, end_date = None, None
    if query_params.get('query_dates'):
        [start_date, end_date] = query_params.get('query_dates')

    statement_info_list = []
    total_request_money, total_confirm_money, total_confirm_units = 0., 0., 0.
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for statement_info_db in session_jobs.query(StatementInfo):
        if query_fund_name and query_fund_name not in statement_info_db.fund:
            continue
        if query_account and query_account not in statement_info_db.account:
            continue
        if query_type and query_type not in statement_info_db.type:
            continue
        if start_date and statement_info_db.date.strftime('%Y-%m-%d') < start_date:
            continue
        if end_date and statement_info_db.date.strftime('%Y-%m-%d') > end_date:
            continue

        temp_item_dict = statement_info_db.to_dict()
        temp_item_dict['date'] = temp_item_dict['date'].strftime('%Y-%m-%d')
        temp_item_dict['confirm_date'] = temp_item_dict['confirm_date'].strftime('%Y-%m-%d') if temp_item_dict[
                                                                                                    'confirm_date'] is not None else ''
        statement_info_list.append(temp_item_dict)

        total_request_money += temp_item_dict['request_money']
        total_confirm_money += temp_item_dict['confirm_money']
        total_confirm_units += temp_item_dict['confirm_units']

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            statement_info_list = sorted(statement_info_list, key=lambda statement_item: statement_item[sort_prop],
                                         reverse=True)
        else:
            statement_info_list = sorted(statement_info_list, key=lambda statement_item: statement_item[sort_prop])
    else:
        statement_info_list.sort(key=lambda obj: obj['date'], reverse=True)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = statement_info_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(statement_info_list), 'total_request_money': total_request_money,
                    'total_confirm_money': total_confirm_money, 'total_confirm_units': total_confirm_units}
    return make_response(jsonify(code=200, data=query_result), 200)


@fund.route('/del_statementinfo', methods=['GET', 'POST'])
def del_statementinfo():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    statement_info_db = session_job.query(StatementInfo).filter(StatementInfo.id == del_id).first()
    session_job.delete(statement_info_db)
    session_job.commit()
    result_message = u"删除成功"
    return make_response(jsonify(code=200, data=result_message), 200)


@fund.route('/statement_info_report', methods=['GET', 'POST'])
def statement_info_report():
    params = request.json
    query_fund_name = params.get('fund_name')
    query_account = params.get('account')
    query_date_str = params.get('date')

    fund_info_df = query_fund_info_df(query_date_str)
    # if len(fund_info_df) < 15:
    #     return make_response(jsonify(code=402, data={'message': '日期：%s 净值数据不全' % query_date_str}), 200)

    result_list = statement_info_report_tools(query_fund_name, query_account, query_date_str, fund_info_df)
    return make_response(jsonify(code=200, data={'data': result_list}), 200)


@fund.route('/import_statement_file', methods=['GET', 'POST'])
def import_statement_file():
    params = request.json
    statement_folder_path = params.get('folder_path')
    if not os.path.exists(statement_folder_path):
        return make_response(jsonify(code=402, data={'message': '文件路径错误'}), 200)

    statement_info_list = []
    for file_name in os.listdir(statement_folder_path):
        if not file_name.endswith('.xlsx'):
            continue
        statement_info = read_statement_file1(os.path.join(statement_folder_path, file_name))
        statement_info_dict = statement_info.to_dict()
        statement_info_list.append(statement_info_dict)
    return make_response(jsonify(code=200, data={'data': statement_info_list}), 200)


@fund.route('/save_import_statement_data', methods=['GET', 'POST'])
def save_import_statement_data():
    params = request.json
    statement_info_list = params.get('data')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    for item_dict in statement_info_list:
        statement_info_db = StatementInfo()
        statement_info_db.date = item_dict['date']
        statement_info_db.fund = item_dict['fund']
        statement_info_db.account = item_dict['account']
        statement_info_db.type = item_dict['type']
        statement_info_db.confirm_date = item_dict['confirm_date']
        statement_info_db.net_asset_value = item_dict['net_asset_value']
        statement_info_db.request_money = item_dict['request_money']
        statement_info_db.confirm_money = item_dict['confirm_money']
        statement_info_db.confirm_units = item_dict['confirm_units']
        statement_info_db.fee = item_dict['fee']
        statement_info_db.performance_pay = item_dict['performance_pay']
        session_job.merge(statement_info_db)
    session_job.commit()
    return make_response(jsonify(code=200, data={'message': '保存成功'}), 200)
