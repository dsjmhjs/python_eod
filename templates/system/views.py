# coding: utf-8
import json
import os
import re
from eod_aps.model.schema_common import AppInfo
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.schema_strategy import StrategyOnline
from eod_aps.tools.aggregator_message_utils import AggregatorMessageUtils
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.instrument_tools import query_instrument_list
from eod_aps.tools.phone_trade_tools import send_phone_trade, save_phone_trade_file
from eod_aps.tools.stock_utils import StockUtils
from flask import render_template, request, jsonify, make_response
from flask_login import login_required
from sqlalchemy import desc
from eod_aps.model.schema_jobs import DepositServerList, LocalParameters, ProjectDict, LocalServerList, \
    TradeServerList, FundInfo
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.tools.hillstone_utils import query_ip_flow_info
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.server_manage_tools import save_pf_position
from eod_aps.job.server_status_monitor_job import query_server_status
from eod_aps.job.account_position_check_job import pf_real_position_check
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.message_manage_tool import query_msg, read_msg
from . import system

date_utils = DateUtils()
stock_utils = StockUtils()
custom_enum_utils = CustomEnumUtils()
common_utils = CommonUtils()
instrument_type_dict = custom_enum_utils.enum_to_dict(const.INSTRUMENT_TYPE_ENUMS)
exchange_type_enums = custom_enum_utils.enum_to_dict(const.EXCHANGE_TYPE_ENUMS)
Instrument_Type_Enums = const.INSTRUMENT_TYPE_ENUMS
Direction_Enums = const.DIRECTION_ENUMS
Trade_Type_Enums = const.TRADE_TYPE_ENUMS
Hedge_Flag_Type_Enums = const.HEDGEFLAG_TYPE_ENUMS
IO_Type_Enums = const.IO_TYPE_ENUMS
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


@system.route('/query_all_server_names', methods=['GET', 'POST'])
def query_all_server_names():
    all_servers = server_constant.get_all_trade_servers()

    server_list = []
    for server_name in all_servers:
        server_item_dict = dict()
        server_item_dict['value'] = server_name
        server_item_dict['label'] = server_name
        server_list.append(server_item_dict)
    server_list.sort()
    return make_response(jsonify(code=200, data=server_list), 200)


@system.route('/query_trade_servers', methods=['GET', 'POST'])
def query_trade_servers():
    trade_servers = server_constant.get_trade_servers()

    server_list = []
    for server_name in trade_servers:
        server_item_dict = dict()
        server_item_dict['value'] = server_name
        server_item_dict['label'] = server_name
        server_list.append(server_item_dict)
    server_list.sort()
    return make_response(jsonify(code=200, data=server_list), 200)


@system.route('/query_stock_servers', methods=['GET', 'POST'])
def query_stock_servers():
    stock_servers = server_constant.get_stock_servers()

    stock_server_list = []
    for server_name in stock_servers:
        server_item_dict = dict()
        server_item_dict['value'] = server_name
        server_item_dict['label'] = server_name
        stock_server_list.append(server_item_dict)
    stock_server_list.sort()
    return make_response(jsonify(code=200, data=stock_server_list), 200)


@system.route('/query_cta_servers', methods=['GET', 'POST'])
def query_cta_servers():
    cta_servers = server_constant.get_cta_servers()

    cta_server_list = []
    for server_name in cta_servers:
        server_item_dict = dict()
        server_item_dict['value'] = server_name
        server_item_dict['label'] = server_name
        cta_server_list.append(server_item_dict)
    cta_server_list.sort()
    return make_response(jsonify(code=200, data=cta_server_list), 200)


@system.route('/query_deposit_servers', methods=['GET', 'POST'])
def query_deposit_servers():
    deposit_servers = server_constant.get_deposit_servers()

    server_list = []
    for server_name in deposit_servers:
        server_item_dict = dict()
        server_item_dict['value'] = server_name
        server_item_dict['label'] = server_name
        server_list.append(server_item_dict)
    server_list.sort()
    return make_response(jsonify(code=200, data=server_list), 200)


@system.route('/query_services', methods=['GET', 'POST'])
def query_services():
    services_list = const.EOD_CONFIG_DICT['service_list']
    return make_response(jsonify(code=200, data=services_list), 200)


@system.route('/status-info', methods=['GET', 'POST'])
@login_required
def status_info():
    trade_servers = ['guoxin', ]
    server_status_dict, email_index_list = query_server_status(trade_servers)
    model_dict = dict()
    model_dict['servers'] = trade_servers
    model_dict['status_items'] = email_index_list
    return render_template('system/system_status.html', server_status_dict=server_status_dict,
                           model_dict=model_dict, endpoint='.status_info')


@system.route('/query_all_funds', methods=['GET', 'POST'])
def query_all_funds():
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    result_list = []
    for fund_info_item in session_job.query(FundInfo):
        if fund_info_item.expiry_time is not None:
            continue
        item_dict = dict(
            value=fund_info_item.name,
            label=fund_info_item.name
        )
        result_list.append(item_dict)
    result_list.sort()
    return make_response(jsonify(code=200, data=result_list), 200)


@system.route('/query_funds', methods=['GET', 'POST'])
def query_funds():
    query_server_name = None
    params = request.json
    if params:
        query_server_name = params.get('server_name')

    fund_name_set = set()
    for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
        if query_server_name and query_server_name != server_name:
            continue
        for real_account in account_list:
            fund_name_set.add(real_account.fund_name)
    fund_name_list = list(fund_name_set)
    fund_name_list.sort()

    result_list = []
    for fund_name in fund_name_list:
        item_dict = dict()
        item_dict['value'] = fund_name
        item_dict['label'] = fund_name
        result_list.append(item_dict)
    return make_response(jsonify(code=200, data=result_list), 200)


@system.route('/query_fund_accout_info', methods=['GET', 'POST'])
def query_fund_accout_info():
    fund_info_list = []
    for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
        fund_name_set = set()
        for real_account in account_list:
            fund_name_set.add(real_account.fund_name)
        fund_name_list = list(fund_name_set)
        fund_name_list.sort()
        temp_fund_info_list = []
        for fund_name in fund_name_list:
            item_dict = dict()
            item_dict['value'] = fund_name
            item_dict['label'] = fund_name
            temp_fund_info_list.append(item_dict)
        fund_info_list.append((server_name, temp_fund_info_list))

    pf_account_info_list = []
    for (server_name, pf_account_list) in const.EOD_CONFIG_DICT['server_pf_account_dict'].items():
        pf_account_set = set()
        for pf_account in pf_account_list:
            pf_account_set.add(pf_account.fund_name)
        pf_account_name_list = list(pf_account_set)
        pf_account_name_list.sort()
        temp_pf_account_info_list = []
        for fund_name in pf_account_name_list:
            item_dict = dict()
            item_dict['value'] = fund_name
            item_dict['label'] = fund_name
            temp_pf_account_info_list.append(item_dict)
        pf_account_info_list.append((server_name, temp_pf_account_info_list))
    result = {'fund_info_list': fund_info_list, 'pf_account_info_list': pf_account_info_list}
    return make_response(jsonify(code=200, data=result), 200)


@system.route('/query_pf_accounts', methods=['GET', 'POST'])
def query_pf_accounts():
    group_name_set = set()
    for (server_name, pf_account_list) in const.EOD_CONFIG_DICT['server_pf_account_dict'].items():
        for pf_account in pf_account_list:
            group_name_set.add(pf_account.group_name)

    pf_account_list = []
    for group_name in group_name_set:
        item_dict = dict()
        item_dict['key'] = str(group_name)
        item_dict['label'] = group_name
        pf_account_list.append(item_dict)
    return make_response(jsonify(code=200, data=pf_account_list), 200)


@system.route('/query_instrument_types', methods=['GET', 'POST'])
def query_instrument_types():
    instrument_type_list = []
    for instrument_type in instrument_type_dict.keys():
        item_dict = dict()
        item_dict['value'] = instrument_type
        item_dict['label'] = instrument_type
        instrument_type_list.append(item_dict)
    return make_response(jsonify(code=200, data=instrument_type_list), 200)


@system.route('/query_exchanges', methods=['GET', 'POST'])
def query_exchanges():
    exchange_type_list = []
    for exchange_type in exchange_type_enums.keys():
        item_dict = dict()
        item_dict['value'] = exchange_type
        item_dict['label'] = exchange_type
        exchange_type_list.append(item_dict)
    return make_response(jsonify(code=200, data=exchange_type_list), 200)


@system.route('/query_ip_flow', methods=['GET', 'POST'])
def query_ip_flow():
    ip_flow_info = query_ip_flow_info()
    print ''.join(ip_flow_info)
    return make_response(jsonify(code=200, data=''.join(ip_flow_info)), 200)


@system.route('/save_server', methods=['GET', 'POST'])
def save_server():
    params = request.json

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    id = params.get('id')
    if id:
        trade_server_item = session_job.query(TradeServerList).filter(TradeServerList.id == id).first()
    else:
        trade_server_item = TradeServerList()
    trade_server_item.name = params.get('name')
    trade_server_item.ip = params.get('ip')
    trade_server_item.port = params.get('port')
    trade_server_item.backup_ip = params.get('backup_ip')
    trade_server_item.backup_port = params.get('backup_port')
    trade_server_item.user = params.get('user')
    trade_server_item.pwd = params.get('pwd')

    trade_server_item.db_ip = params.get('db_ip')
    trade_server_item.db_port = params.get('db_port')
    trade_server_item.db_ip_reserve = params.get('db_ip_reserve')
    trade_server_item.db_port_reserve = params.get('db_port_reserve')

    trade_server_item.db_user = params.get('db_user')
    trade_server_item.db_password = params.get('db_password')
    trade_server_item.connect_address = params.get('connect_address')
    trade_server_item.check_port_list = params.get('check_port_list')
    trade_server_item.etf_base_folder = params.get('etf_base_folder')
    trade_server_item.data_source_type = params.get('data_source_type')
    trade_server_item.market_source_type = ';'.join(params.get('market_source_type'))
    trade_server_item.market_file_template = params.get('market_file_template')
    trade_server_item.market_file_localpath = params.get('market_file_localpath')
    trade_server_item.strategy_group_list = params.get('strategy_group_list')

    trade_server_item.is_trade_stock = params.get('is_trade_stock')
    trade_server_item.is_trade_future = params.get('is_trade_future')
    trade_server_item.is_night_session = params.get('is_night_session')
    trade_server_item.is_cta_server = params.get('is_cta_server')
    trade_server_item.is_calendar_server = params.get('is_calendar_server')
    trade_server_item.is_oma_server = params.get('is_oma_server')
    trade_server_item.download_market_file_flag = params.get('download_market_file_flag')
    trade_server_item.server_parameter = params.get('server_parameter')
    trade_server_item.path_parameter = params.get('path_parameter')
    trade_server_item.enable = params.get('enable')
    session_job.merge(trade_server_item)
    session_job.commit()

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=u"保存服务器:%s成功" % trade_server_item.name), 200)


@system.route('/get_trade_servers', methods=['GET', 'POST'])
def get_trade_servers():
    server_list = []
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    for trade_server_item in session_job.query(TradeServerList):
        server_item_dict = dict()
        server_item_dict['id'] = trade_server_item.id
        server_item_dict['name'] = trade_server_item.name
        server_item_dict['ip'] = trade_server_item.ip
        server_item_dict['port'] = trade_server_item.port
        server_item_dict['ip_reserve'] = trade_server_item.ip_reserve
        server_item_dict['port_reserve'] = trade_server_item.port_reserve

        server_item_dict['user'] = trade_server_item.user
        server_item_dict['pwd'] = trade_server_item.pwd

        server_item_dict['db_ip'] = trade_server_item.db_ip
        server_item_dict['db_port'] = trade_server_item.db_port
        server_item_dict['db_ip_reserve'] = trade_server_item.db_ip_reserve
        server_item_dict['db_port_reserve'] = trade_server_item.db_port_reserve

        server_item_dict['db_user'] = trade_server_item.db_user
        server_item_dict['db_password'] = trade_server_item.db_password
        server_item_dict['connect_address'] = trade_server_item.connect_address
        server_item_dict['check_port_list'] = trade_server_item.check_port_list
        server_item_dict['etf_base_folder'] = trade_server_item.etf_base_folder
        server_item_dict['data_source_type'] = trade_server_item.data_source_type
        if trade_server_item.market_source_type:
            server_item_dict['market_source_type'] = trade_server_item.market_source_type.split(';')
        else:
            server_item_dict['market_source_type'] = []
        server_item_dict['market_file_template'] = trade_server_item.market_file_template
        server_item_dict['market_file_localpath'] = trade_server_item.market_file_localpath

        server_item_dict['download_market_file_flag'] = trade_server_item.download_market_file_flag
        server_item_dict['strategy_group_list'] = trade_server_item.strategy_group_list
        server_item_dict['is_trade_stock'] = trade_server_item.is_trade_stock
        server_item_dict['is_trade_future'] = trade_server_item.is_trade_future
        server_item_dict['is_night_session'] = trade_server_item.is_night_session
        server_item_dict['is_cta_server'] = trade_server_item.is_cta_server
        server_item_dict['is_calendar_server'] = trade_server_item.is_calendar_server
        server_item_dict['is_oma_server'] = trade_server_item.is_oma_server
        server_item_dict['server_parameter'] = trade_server_item.server_parameter
        server_item_dict['path_parameter'] = trade_server_item.path_parameter
        server_item_dict['enable'] = trade_server_item.enable
        server_list.append(server_item_dict)
    return make_response(jsonify(code=200, data=server_list), 200)


@system.route('/del_server', methods=['GET', 'POST'])
def del_server():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    server_list_item = session_job.query(TradeServerList).filter(TradeServerList.id == del_id).first()
    session_job.delete(server_list_item)
    session_job.commit()
    result_message = u"删除服务器成功"

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=result_message), 200)


@system.route('/save_deposit_server', methods=['GET', 'POST'])
def save_deposit_server():
    params = request.json

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    id = params.get('id')
    if id:
        deposit_server_list_item = session_job.query(DepositServerList).filter(DepositServerList.id == id).first()
    else:
        deposit_server_list_item = DepositServerList()
    deposit_server_list_item.name = params.get('name')
    deposit_server_list_item.ip = params.get('ip')
    deposit_server_list_item.db_ip = params.get('db_ip')
    deposit_server_list_item.db_user = params.get('db_user')
    deposit_server_list_item.db_password = params.get('db_password')
    deposit_server_list_item.db_port = params.get('db_port')
    deposit_server_list_item.connect_address = params.get('connect_address')
    deposit_server_list_item.ftp_type = params.get('ftp_type')
    deposit_server_list_item.ftp_user = params.get('ftp_user')
    deposit_server_list_item.ftp_password = params.get('ftp_password')
    deposit_server_list_item.ftp_wsdl_address = params.get('ftp_wsdl_address')
    deposit_server_list_item.ftp_upload_folder = params.get('ftp_upload_folder')
    deposit_server_list_item.ftp_download_folder = params.get('ftp_download_folder')
    deposit_server_list_item.is_trade_stock = params.get('is_trade_stock')
    deposit_server_list_item.is_ftp_monitor = params.get('is_ftp_monitor')
    deposit_server_list_item.is_ftp_monitor = params.get('is_ftp_monitor')
    deposit_server_list_item.enable = params.get('enable')
    session_job.merge(deposit_server_list_item)
    session_job.commit()

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=u"保存托管服务器:%s成功" % deposit_server_list_item.name), 200)


@system.route('/get_deposit_servers', methods=['GET', 'POST'])
def get_deposit_servers():
    deposit_server_list = []
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    for deposit_server_item in session_job.query(DepositServerList):
        deposit_server_dict = dict()
        deposit_server_dict['id'] = deposit_server_item.id
        deposit_server_dict['name'] = deposit_server_item.name
        deposit_server_dict['ip'] = deposit_server_item.ip
        deposit_server_dict['db_ip'] = deposit_server_item.db_ip
        deposit_server_dict['db_user'] = deposit_server_item.db_user
        deposit_server_dict['db_password'] = deposit_server_item.db_password
        deposit_server_dict['db_port'] = deposit_server_item.db_port
        deposit_server_dict['connect_address'] = deposit_server_item.connect_address

        deposit_server_dict['ftp_type'] = deposit_server_item.ftp_type
        deposit_server_dict['ftp_user'] = deposit_server_item.ftp_user
        deposit_server_dict['ftp_password'] = deposit_server_item.ftp_password
        deposit_server_dict['ftp_wsdl_address'] = deposit_server_item.ftp_wsdl_address
        deposit_server_dict['ftp_upload_folder'] = deposit_server_item.ftp_upload_folder
        deposit_server_dict['ftp_download_folder'] = deposit_server_item.ftp_download_folder
        deposit_server_dict['is_trade_stock'] = deposit_server_item.is_trade_stock
        deposit_server_dict['is_ftp_monitor'] = deposit_server_item.is_ftp_monitor
        deposit_server_dict['enable'] = deposit_server_item.enable
        deposit_server_list.append(deposit_server_dict)
    return make_response(jsonify(code=200, data=deposit_server_list), 200)


@system.route('/del_deposit_server', methods=['GET', 'POST'])
def del_deposit_server():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    deposit_server_item = session_job.query(DepositServerList).filter(DepositServerList.id == del_id).first()
    session_job.delete(deposit_server_item)
    session_job.commit()
    result_message = u"删除托管服务器成功"

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=result_message), 200)


@system.route('/query_local_parameters', methods=['GET', 'POST'])
def query_local_parameters():
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    local_parameters = session_job.query(LocalParameters).first()

    local_parameters_dict = dict()
    local_parameters_dict['ip'] = local_parameters.ip
    local_parameters_dict['db_ip'] = local_parameters.db_ip
    local_parameters_dict['db_user'] = local_parameters.db_user
    local_parameters_dict['db_password'] = local_parameters.db_password
    local_parameters_dict['db_port'] = local_parameters.db_port
    local_parameters_dict['smtp_server'] = local_parameters.smtp_server
    local_parameters_dict['smtp_port'] = local_parameters.smtp_port
    local_parameters_dict['smtp_username'] = local_parameters.smtp_username
    local_parameters_dict['smtp_password'] = local_parameters.smtp_password
    local_parameters_dict['smtp_from'] = local_parameters.smtp_from
    return make_response(jsonify(code=200, data=[local_parameters_dict, ]), 200)


@system.route('/save_local_parameters', methods=['GET', 'POST'])
def save_local_parameters():
    params = request.json

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    local_parameters = session_job.query(LocalParameters).first()
    local_parameters.ip = params.get('ip')
    local_parameters.db_ip = params.get('db_ip')
    local_parameters.db_user = params.get('db_user')
    local_parameters.db_password = params.get('db_password')
    local_parameters.db_port = params.get('db_port')
    local_parameters.smtp_server = params.get('smtp_server')
    local_parameters.smtp_port = params.get('smtp_port')
    local_parameters.smtp_username = params.get('smtp_username')
    local_parameters.smtp_password = params.get('smtp_password')
    local_parameters.smtp_from = params.get('smtp_from')

    session_job.merge(local_parameters)
    session_job.commit()

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=u"保存本地参数成功!"), 200)


@system.route('/save_project_dict', methods=['GET', 'POST'])
def save_project_dict():
    params = request.json

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    id = params.get('id')
    if id:
        project_dict_item = session_job.query(ProjectDict).filter(ProjectDict.id == id).first()
    else:
        project_dict_item = ProjectDict()
    project_dict_item.dict_type = params.get('dict_type')
    project_dict_item.dict_name = params.get('dict_name')
    project_dict_item.dict_value = params.get('dict_value')
    project_dict_item.dict_desc = params.get('dict_desc')

    session_job.merge(project_dict_item)
    session_job.commit()

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=u"保存字典:%s成功" % project_dict_item.dict_name), 200)


@system.route('/query_project_dict', methods=['GET', 'POST'])
def query_project_dict():
    query_params = request.json
    query_dict_type = query_params.get('dict_type')
    query_dict_name = query_params.get('dict_name')
    query_dict_value = query_params.get('dict_value')
    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))

    project_dict_list = []
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    for project_dict_item in session_job.query(ProjectDict).order_by(ProjectDict.dict_type, ProjectDict.dict_value):
        if query_dict_type and query_dict_type != project_dict_item.dict_type:
            continue
        if query_dict_name and query_dict_name not in project_dict_item.dict_name:
            continue
        if query_dict_value and query_dict_value not in project_dict_item.dict_value:
            continue

        item_dict = dict()
        item_dict['id'] = project_dict_item.id
        item_dict['dict_type'] = project_dict_item.dict_type
        item_dict['dict_name'] = project_dict_item.dict_name
        item_dict['dict_value'] = project_dict_item.dict_value
        item_dict['dict_desc'] = project_dict_item.dict_desc
        project_dict_list.append(item_dict)

    project_dict_list.sort(key=lambda obj: obj['dict_type'] + obj['dict_name'])
    result_list = project_dict_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(project_dict_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@system.route('/del_project_dict', methods=['GET', 'POST'])
def del_project_dict():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    project_dict_item = session_job.query(ProjectDict).filter(ProjectDict.id == del_id).first()
    session_job.delete(project_dict_item)
    session_job.commit()
    result_message = u"删除字典成功"

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=result_message), 200)


@system.route('/save_local_server', methods=['GET', 'POST'])
def save_local_server():
    params = request.json
    print params

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    id = params.get('id')
    if id:
        local_server_item = session_job.query(LocalServerList).filter(LocalServerList.id == id).first()
    else:
        local_server_item = LocalServerList()
    local_server_item.name = params.get('name')
    local_server_item.ip = params.get('ip')
    local_server_item.port = params.get('port')
    local_server_item.user = params.get('user')
    local_server_item.pwd = params.get('pwd')
    local_server_item.db_ip = params.get('db_ip')
    local_server_item.db_user = params.get('db_user')
    local_server_item.db_password = params.get('db_password')
    local_server_item.db_port = params.get('db_port')
    local_server_item.connect_address = params.get('connect_address')
    local_server_item.anaconda_home_path = params.get('anaconda_home_path')
    local_server_item.group_list = ';'.join(params.get('group_list'))
    local_server_item.enable = params.get('enable')

    session_job.merge(local_server_item)
    session_job.commit()

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=u"保存本地服务器:%s成功" % local_server_item.name), 200)


@system.route('/query_local_servers', methods=['GET', 'POST'])
def query_local_servers():
    local_server_list = []
    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    for local_server_item in session_job.query(LocalServerList):
        local_server_dict = local_server_item.to_dict()
        local_server_dict['group_list'] = local_server_item.group_list.split(
            ';') if local_server_item.group_list else []
        local_server_list.append(local_server_dict)
    return make_response(jsonify(code=200, data=local_server_list), 200)


@system.route('/del_local_server', methods=['GET', 'POST'])
def del_local_server():
    params = request.json
    del_id = params.get('del_id')

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')
    local_server_item = session_job.query(LocalServerList).filter(LocalServerList.id == del_id).first()
    session_job.delete(local_server_item)
    session_job.commit()
    result_message = u"删除本地服务器成功"

    # 字典更新后，重新加载内存缓存的数据
    server_constant.reload_by_mmap()
    return make_response(jsonify(code=200, data=result_message), 200)


@system.route('/query_strategy_list', methods=['GET', 'POST'])
def query_strategy_list():
    params = request.json
    query_server_name = params.get('server_name')
    query_fund_name = params.get('fund_name')

    strategy_list = []
    server_model = server_constant.get_server_model(query_server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    for pf_account_db in session_portfolio.query(PfAccount):
        if query_fund_name:
            fund_name = pf_account_db.fund_name.split('-')[2]
            if query_fund_name != fund_name:
                continue
        item_dict = dict(
            value=pf_account_db.fund_name,
            label=pf_account_db.fund_name
        )
        strategy_list.append(item_dict)

    filter_date_str = session_portfolio.query(PfPosition.date).order_by(desc(PfPosition.date)).first()
    query_result = {'server_name': query_server_name, 'fund_name': query_fund_name,
                    'filter_date_str': filter_date_str[0].strftime('%Y-%m-%d'), 'strategy_list': strategy_list}
    return make_response(jsonify(code=200, data=query_result), 200)


# @system.route('/update_tickers', methods=['GET', 'POST'])
# def update_tickers():
#     params = request.json
#     query_server_name = params.get('server_name')
#     filter_date_str = params.get('filter_date_str')
#     query_strategy1 = params.get('strategy1')
#
#     all_item_dict = dict()
#     all_item_dict['value'] = 'all'
#     all_item_dict['label'] = 'all'
#     ticker_list = [all_item_dict]
#
#     server_model = server_constant.get_server_model(query_server_name)
#     session_portfolio = server_model.get_db_session('portfolio')
#     for pf_position_db in session_portfolio.query(PfPosition).join(PfAccount, PfPosition.id == PfAccount.id)\
# .filter(PfAccount.fund_name == query_strategy1, PfPosition.date == filter_date_str):
#         item_dict = dict()
#         item_dict['value'] = pf_position_db.symbol
#         item_dict['label'] = pf_position_db.symbol
#         ticker_list.append(item_dict)
#     return make_response(jsonify(code=200, data=ticker_list), 200)

# @system.route('/update_long_short', methods=['GET', 'POST'])
# def update_long_short():
#     params = request.json
#     query_server_name = params.get('server_name')
#     filter_date_str = params.get('filter_date_str')
#     query_strategy1 = params.get('strategy1')
#     query_ticker1 = params.get('ticker1')
#
#     server_model = server_constant.get_server_model(query_server_name)
#     session_portfolio = server_model.get_db_session('portfolio')
#     pf_position_db = session_portfolio.query(PfPosition).join(PfAccount, PfPosition.id == PfAccount.id)\
# .filter(PfAccount.fund_name == query_strategy1, PfPosition.date == filter_date_str, \
# PfPosition.symbol == query_ticker1).first()
#     query_result = {'long1': int(pf_position_db.long), 'short1': int(pf_position_db.short)}
#     return make_response(jsonify(code=200, data=query_result), 200)


@system.route('/change_pf_position', methods=['GET', 'POST'])
def change_pf_position():
    params = request.json
    server_name = params.get('server_name')
    filter_date_str = params.get('filter_date_str')
    strategy1 = params.get('strategy1')
    strategy2 = params.get('strategy2')

    type_list = [Instrument_Type_Enums.CommonStock, ]
    instrument_db_dict = {x.ticker: x for x in query_instrument_list('host', type_list)}

    pf_position_dict1 = dict()
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    for pf_position_db in session_portfolio.query(PfPosition).join(PfAccount, PfPosition.id == PfAccount.id) \
            .filter(PfAccount.fund_name == strategy1, PfPosition.date == filter_date_str):
        pf_position_dict1[pf_position_db.symbol] = pf_position_db

    strategy1_items = strategy1.split('-')
    strategy2_items = strategy2.split('-')
    # strategy1所有的仓位都转移至strategy2
    phone_trade_list = []
    for (symbol, pf_position_db1) in pf_position_dict1.items():
        if pf_position_db1.long_avail < 100:
            continue
        if symbol not in instrument_db_dict:
            continue

        phone_trade_info = PhoneTradeInfo()
        phone_trade_info.fund = strategy1_items[2]
        phone_trade_info.strategy1 = '%s.%s' % (strategy1_items[1], strategy1_items[0])
        phone_trade_info.symbol = symbol
        phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
        phone_trade_info.tradetype = Trade_Type_Enums.Normal
        phone_trade_info.iotype = IO_Type_Enums.Inner2
        phone_trade_info.strategy2 = '%s.%s' % (strategy2_items[1], strategy2_items[0])
        phone_trade_info.server_name = server_name
        phone_trade_info.exprice = instrument_db_dict[symbol].prev_close
        phone_trade_info.direction = Direction_Enums.Sell
        phone_trade_info.exqty = __round_down(pf_position_db1.long_avail)
        phone_trade_list.append(phone_trade_info)

    if phone_trade_list:
        send_phone_trade(server_name, phone_trade_list)
    result_message = u"整体调仓成功"
    return make_response(jsonify(code=200, data=result_message), 200)


def __round_down(number_input):
    # 对股数向下取整
    return int(int(float(number_input) / float(100)) * 100)


def __build_pf_position(pf_position_db):
    pf_position_db.long_avail = pf_position_db.long
    pf_position_db.yd_position_long = pf_position_db.long
    pf_position_db.yd_long_remain = pf_position_db.long

    pf_position_db.short_avail = pf_position_db.short
    pf_position_db.yd_position_short = pf_position_db.short
    pf_position_db.yd_short_remain = pf_position_db.short
    pf_position_db.prev_net = pf_position_db.yd_position_long - pf_position_db.yd_position_short
    return pf_position_db


@system.route('/query_vpn_status', methods=['GET', 'POST'])
def query_vpn_status():
    trader_server_list = server_constant.get_trade_servers()
    deposit_server_list = server_constant.get_deposit_servers()
    status_list = []
    for server_name in trader_server_list + deposit_server_list:
        server_model = server_constant.get_server_model(server_name)
        vpn_status_flag = server_model.check_connect()

        item_dict = dict(
            Server=server_name,
            Vpn_Status=vpn_status_flag)
        status_list.append(item_dict)
    return make_response(jsonify(code=200, data=status_list), 200)


@system.route('/query_service_status', methods=['GET', 'POST'])
def query_service_status():
    server_list = server_constant.get_trade_servers()
    services_list = const.EOD_CONFIG_DICT['service_list']

    server_service_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    for app_info_db in session_common.query(AppInfo):
        if app_info_db.server_name in server_service_dict:
            server_service_dict[app_info_db.server_name].append(app_info_db.app_name)
        else:
            server_service_dict[app_info_db.server_name] = [app_info_db.app_name]

    status_dict = dict()
    for server_name in server_list:
        server_service_list = server_service_dict[server_name]
        try:
            server_model = server_constant.get_server_model(server_name)
            ssh_result = server_model.run_cmd_str('screen -ls')
        except Exception:
            ssh_result = ''

        for service_name in services_list:
            if service_name not in server_service_list:
                server_service_status = ''
            elif service_name in ssh_result:
                server_service_status = 'Active'
            else:
                server_service_status = 'InActive'
            status_dict['%s|%s' % (server_name, service_name)] = server_service_status

    status_list = []
    for service_name in services_list:
        service_status_list = [service_name]
        for server_name in server_list:
            service_status_list.append(status_dict['%s|%s' % (server_name, service_name)])
        status_list.append(service_status_list)

    date_utils = DateUtils()
    time_filter = date_utils.get_today_str('%H%M%S')
    if 85000 < int(time_filter) < 173000 or int(time_filter) > 200000:
        active_flag = True
    else:
        active_flag = False

    head_list = ['Service']
    head_list.extend(server_list)
    query_result = {'head_list': head_list, 'status_list': status_list, 'active_flag': active_flag}

    return make_response(jsonify(code=200, data=query_result), 200)


@system.route('/query_strategy_status', methods=['GET', 'POST'])
def query_strategy_status():
    cta_server_list = server_constant.get_cta_servers()

    strategy_status_dict = dict()
    aggregator_message_utils = AggregatorMessageUtils()
    aggregator_message_utils.login_aggregator()
    strategy_info_response_msg = aggregator_message_utils.query_strategy_info_msg()
    for strats_info in strategy_info_response_msg.Strats:
        server_name = common_utils.get_server_name(strats_info.Location)
        strategy_status_dict['%s|%s' % (server_name, strats_info.Name)] = strats_info.IsEnabled

    status_list = []
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.enable == 1):
        strategy_name = strategy_online_db.name
        strategy_status_list = [strategy_name]
        for server_name in cta_server_list:
            if server_name in strategy_online_db.target_server:
                find_key = '%s|%s' % (server_name, strategy_name)
                if find_key in strategy_status_dict:
                    strategy_status_list.append(strategy_status_dict[find_key])
                else:
                    strategy_status_list.append('')
            else:
                strategy_status_list.append('NONE')
        status_list.append(strategy_status_list)
    server_host.close()

    head_list = ['Strategy']
    head_list.extend(cta_server_list)
    query_result = {'head_list': head_list, 'status_list': status_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@system.route('/query_position_diff', methods=['GET', 'POST'])
def query_position_diff():
    all_trade_servers = server_constant.get_all_trade_servers()
    position_head_list = ['Fund', 'Ticker', 'Long', 'Short', 'PF_Long', 'PF_Short', 'Diff']

    position_diff_list = []
    for server_name in all_trade_servers:
        position_date, pf_position_date, compare_result_list = pf_real_position_check(server_name)
        position_diff_dict = dict()
        position_diff_dict['server'] = server_name
        position_diff_dict['position_date'] = position_date.strftime('%Y-%m-%d')
        position_diff_dict['pf_position_date'] = pf_position_date.strftime('%Y-%m-%d')
        position_diff_dict['compare_result_list'] = compare_result_list
        position_diff_dict['head_list'] = position_head_list
        position_diff_list.append(position_diff_dict)

    validate_time = date_utils.get_today_str('%H%M%S')
    position_date = date_utils.get_today_str('%Y-%m-%d')
    if validate_time > '170000':
        pf_position_date = date_utils.get_next_trading_day()
    else:
        pf_position_date = position_date
    query_result = {'position_date': position_date, 'pf_position_date': pf_position_date,
                    'position_diff_list': position_diff_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@system.route('/real_position_check', methods=['GET', 'POST'])
def real_position_check():
    validate_time = int(date_utils.get_today_str('%H%M%S'))
    if validate_time > 153000:
        validate_date_str = '%s 15:00:00' % date_utils.get_today_str('%Y-%m-%d')
    else:
        validate_date_str = '%s 08:30:00' % date_utils.get_today_str('%Y-%m-%d')

    all_trade_servers = server_constant.get_all_trade_servers()
    account_id_set = set()
    server_account_dict = dict()
    for server_name in all_trade_servers:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_sql = 'select a.AccountID, a.AccountType, b.update_date from portfolio.real_account a left join \
(select t.id, max(t.update_date) as update_date from portfolio.account_position t group by t.ID) b \
on a.AccountID = b.id where a.`ENABLE` = 1'
        for account_item in session_portfolio.execute(query_sql):
            id = account_item[0]
            account_type = account_item[1]
            update_date = account_item[2]
            if update_date is None:
                update_date = 'Null(Error)'
            else:
                update_date = update_date.strftime('%Y-%m-%d %H:%M:%S')
                if update_date < validate_date_str:
                    update_date = '%s(Error)' % update_date
            server_account_dict['%s|%s' % (server_name, id)] = '%s|%s' % (account_type, update_date)
            account_id_set.add(id)

    account_position_list = []
    for account_id in list(account_id_set):
        server_account_list = [account_id]
        for server_name in all_trade_servers:
            dict_key = '%s|%s' % (server_name, account_id)
            if dict_key in server_account_dict:
                server_account_list.append(server_account_dict[dict_key])
            else:
                server_account_list.append('')
        account_position_list.append(server_account_list)

    head_list = ['Server']
    head_list.extend(all_trade_servers)
    query_result = {'head_list': head_list, 'data_list': account_position_list, 'validate_date_str': validate_date_str}
    return make_response(jsonify(code=200, data=query_result), 200)


@system.route('/query_prev_close', methods=['GET', 'POST'])
def query_prev_close():
    params = request.json
    query_ticker = params.get('Ticker')
    prev_close = stock_utils.get_close(date_utils.get_last_trading_day('%Y%m%d'), query_ticker)
    if prev_close is not None:
        prev_close = '%.2f' % float(prev_close)
    return make_response(jsonify(code=200, data=prev_close), 200)


@system.route('/send_save_pf_position', methods=['GET', 'POST'])
def send_save_pf_position():
    trade_server_list = server_constant.get_trade_servers()
    for server_name in trade_server_list:
        save_pf_position(server_name)
    query_result = {'message': "Send Save Pf_Position Success!"}
    return make_response(jsonify(code=200, data=query_result), 200)


# 用于接收从服务器回传的消息
@system.route('/server_notify', methods=['GET', 'POST'])
def server_notify():
    params = request.json
    message_dict = json.loads(params.get('message'))
    email_utils2.send_email_group_all(message_dict['Title'], message_dict['Content'])
    return make_response(jsonify(code=200, message='server_notify'), 200)


@system.route('/query_strategy_grouping', methods=['GET', 'POST'])
def query_strategy_grouping():
    strategy_grouping_list = []
    for (group_name, sub_group_dict) in const.EOD_CONFIG_DICT['strategy_grouping_dict'].items():
        group_dict = dict()
        group_dict['key'] = group_name
        group_dict['value'] = group_name
        group_dict['label'] = group_name
        group_dict['children'] = []
        for (sub_group_name, strategy_list) in sub_group_dict.items():
            sub_group_dict = dict()
            sub_group_dict['value'] = sub_group_name
            sub_group_dict['label'] = sub_group_name
            sub_group_dict['children'] = []
            for strategy_name in strategy_list:
                strategy_dict = dict(
                    value=strategy_name,
                    label=strategy_name)
                sub_group_dict['children'].append(strategy_dict)
            group_dict['children'].append(sub_group_dict)
        strategy_grouping_list.append(group_dict)
    strategy_grouping_list.sort()

    strategy_grouping_list = sorted(strategy_grouping_list, cmp=lambda x, y: cmp(x['value'], y['value']))
    return make_response(jsonify(code=200, data=strategy_grouping_list), 200)


@system.route('/query_user_strategy_groups', methods=['GET', 'POST'])
def query_user_strategy_groups():
    params = request.json
    query_user_name = params.get('user_name')

    strategy_grouping_list = []
    server_model = server_constant.get_server_model('host')
    session_jobs = server_model.get_db_session('jobs')
    query_sql = "select strategy_group_list from jobs.user_list where user_id='%s'" % query_user_name
    strategy_group_list_str = session_jobs.execute(query_sql).first()[0]
    for strategy_group_item in strategy_group_list_str.split(','):
        temp_dict = dict()
        temp_dict['value'] = strategy_group_item
        temp_dict['label'] = strategy_group_item
        strategy_grouping_list.append(temp_dict)
    return make_response(jsonify(code=200, data=strategy_grouping_list), 200)


@system.route('/trader_version_check', methods=['GET', 'POST'])
def trader_version_check():
    result = []
    trade_servers_list = server_constant.get_trade_servers()
    for server_name in trade_servers_list:
        tr_dict = {}
        server_model = server_constant.get_server_model(server_name)
        cmd = 'ls -l %s/build64_release' % server_model.server_path_dict['tradeplat_project_folder']
        rst = server_model.run_cmd_str(cmd)
        # version = re.findall(r'\d+_\d+', rst)[-1]
        version = re.findall(r'bin.*/', rst)
        tr_dict['servername'] = server_name
        tr_dict['version'] = version
        result.append(tr_dict)
    return make_response(jsonify(code=200, data=result), 200)


@system.route('/messages', methods=['GET', 'POST'])
def messages():
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    params = request.json
    username = params.get('username')
    read_flag = params.get('read_flag')
    sort_prop = params.get('sort_prop')
    sort_order = params.get('sort_order')
    page = params.get('page')
    size = params.get('size')
    title = params.get('title')
    data = []
    for msg_obj in query_msg(username, session, read_flag):
        data.append(
            dict(create_time=str(msg_obj.create_time), title=msg_obj.title, content=msg_obj.content,
                 read_flag=msg_obj.read_flag, user=msg_obj.user.user_id, msg_id=msg_obj.id))
    if sort_prop:
        if sort_order == 'ascending':
            data = sorted(data, key=lambda data_item: data_item[sort_prop], reverse=True)
        else:
            data = sorted(data, key=lambda data_item: data_item[sort_prop])
    if title:
        data = filter(lambda data_item: title in data_item['title'], data)
    start = (int(page) - 1) * int(size)
    end = int(page) * int(size)
    total = len(data)
    if total < end:
        end = total
    data = data[start:end]

    pagination = {'total': total, 'size': int(size), 'currentPage': int(page)}
    result = {'data': data, 'pagination': pagination, }
    session.close()
    return make_response(jsonify(code=200, data=result), 200)


@system.route('/change_mesg_status', methods=['GET', 'POST'])
def change_mesg_status():
    params = request.json
    msg_id = params.get('msg_id')
    username = params.get('username')
    read_msg(username, msg_id)
    return make_response(jsonify(code=200), 200)


@system.route('/get_unread_msg_count', methods=['GET', 'POST'])
def get_unread_msg_count():
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    params = request.json
    username = params.get('username')
    unread_msg_count = len(query_msg(username, session))
    session.close()
    return make_response(jsonify(code=200, data=unread_msg_count), 200)


@system.route('/query_db_status', methods=['GET', 'POST'])
def query_db_status():
    check_rows_size = 10000
    check_data_size = 10485760
    temp_server_list = server_constant.get_all_trade_servers()
    temp_server_list.append('host')

    db_status_list = []
    connect_list = []
    for server_name in temp_server_list:
        server_model = server_constant.get_server_model(server_name)
        session = server_model.get_db_session('information_schema')
        query_sql = 'select schema_name from information_schema.SCHEMATA'
        for result_item in session.execute(query_sql):
            schema_name = result_item[0]
            if schema_name in ('information_schema', 'sys', 'performance_schema'):
                continue
            schema_query_sql = "select TABLE_NAME,TABLE_ROWS,DATA_LENGTH FROM TABLES WHERE TABLE_SCHEMA = '%s'" % schema_name
            for schema_item in session.execute(schema_query_sql):
                if schema_item[1] is None:
                    continue
                if int(schema_item[1]) < check_rows_size and int(schema_item[2]) < check_data_size:
                    continue
                item_dict = dict(
                    Server=server_name,
                    Table_Name='%s.%s' % (schema_name, schema_item[0]),
                    Table_Rows=schema_item[1],
                    Data_Length=schema_item[2],
                )
                db_status_list.append(item_dict)

        query_sql = "show status like 'Threads_connected'"
        for result_item in session.execute(query_sql):
            connected_num = result_item[1]

        query_sql = "show variables like '%max_connections%'"
        for result_item in session.execute(query_sql):
            max_num = result_item[1]
        item_dict = dict(
            Server=server_name,
            Connected_Num=connected_num,
            Max_Num=max_num,
        )
        connect_list.append(item_dict)
    return make_response(jsonify(code=200, db_status_list=db_status_list, connect_list=connect_list), 200)


@system.route('/get_today_date', methods=['GET', 'POST'])
def get_today_date():
    today_date = date_utils.get_today_str('%Y-%m-%d')
    return make_response(jsonify(code=200, data=today_date), 200)
