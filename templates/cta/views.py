# coding: utf-8
import os
import json
import re
from eod_aps.model.schema_common import User, UserDomain, FutureMainContract
from eod_aps.model.schema_jobs import FundInfo
from eod_aps.model.schema_strategy import StrategyGrouping, StrategyParameter
from flask import render_template, request, flash, redirect, url_for, jsonify, make_response
from eod_aps.model.server_constans import server_constant
from eod_aps.model.schema_strategy import StrategyOnline, StrategyServerParameter, StrategyServerParameterChange, \
    StrategyState
from eod_aps.model.eod_const import const
from eod_aps.tools.date_utils import DateUtils
from . import cta

date_utils = DateUtils()

trade_restrictions_list = [('hongyuan01', 'SM'), ('All_Weather', 'T'), ('All_Weather', 'TF')]

filter_fund_list = ['absolute_return', 'deriv_01']


@cta.route('/query_strategy', methods=['GET', 'POST'])
def query_strategy():
    query_params = request.json
    query_name = query_params.get('name')
    query_instance_name = query_params.get('instance_name')
    query_enable = query_params.get('enable')

    strategy_online_list = []

    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    strategy_grouping_dict = dict()
    for strategy_grouping_db in session_strategy.query(StrategyGrouping):
        strategy_grouping_dict[strategy_grouping_db.strategy_name] = strategy_grouping_db

    for strategy_online_db in session_strategy.query(StrategyOnline):
        if query_name and query_name.lower() not in strategy_online_db.name.lower():
            continue
        if query_instance_name and query_instance_name not in strategy_online_db.instance_name:
            continue
        if query_enable != '' and query_enable != strategy_online_db.enable:
            continue

        target_server_list = []
        cta_server_list = server_constant.get_cta_servers()
        if strategy_online_db.target_server is not None and strategy_online_db.target_server != '':
            target_server_list = strategy_online_db.target_server.split('|')
        target_server_list = filter(lambda x: x in cta_server_list, target_server_list)

        temp_item_dict = strategy_online_db.to_dict()
        temp_item_dict['target_server_list'] = target_server_list
        temp_item_dict['grouping_sub_name'] = strategy_grouping_dict[strategy_online_db.strategy_name].sub_name
        strategy_online_list.append(temp_item_dict)

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            strategy_online_list = sorted(strategy_online_list, key=lambda market_item: market_item[sort_prop],
                                          reverse=True)
        else:
            strategy_online_list = sorted(strategy_online_list, key=lambda market_item: market_item[sort_prop])
    else:
        strategy_online_list.sort(key=lambda obj: obj['name'])

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = strategy_online_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(strategy_online_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@cta.route('/save_strategy', methods=['GET', 'POST'])
def save_strategy():
    params = request.json
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    id = params.get('id')
    if id:
        strategy_online_db = session_strategy.query(StrategyOnline).filter(StrategyOnline.id == id).first()
    else:
        strategy_online_db = StrategyOnline()

    strategy_online_db.enable = params.get('enable')
    strategy_online_db.strategy_type = params.get('strategy_type')
    strategy_online_db.target_server = '|'.join(params.get('target_server_list'))
    strategy_online_db.name = params.get('name')

    strategy_name = params.get('name').split('.')[0]
    strategy_online_db.assembly_name = '%s_strategy' % strategy_name
    strategy_online_db.strategy_name = strategy_name

    strategy_online_db.instance_name = params.get('instance_name')
    strategy_online_db.data_type = params.get('data_type')
    strategy_online_db.date_num = params.get('date_num')
    strategy_online_db.parameter = params.get('parameter')
    session_strategy.merge(strategy_online_db)
    session_strategy.commit()

    # 新增策略，设置分组信息和账户访问权限
    if not id:
        grouping_sub_name = params.get('grouping_sub_name')
        strategy_grouping_db = session_strategy.query(StrategyGrouping).filter(
            StrategyGrouping.strategy_name == strategy_name).first()
        if not strategy_grouping_db:
            strategy_grouping_info = StrategyGrouping()
            strategy_grouping_info.group_name = 'CTA'
            strategy_grouping_info.sub_name = grouping_sub_name
            strategy_grouping_info.strategy_name = strategy_name
            session_strategy.add(strategy_grouping_info)
            session_strategy.commit()

            session_common = server_host.get_db_session('common')
            cta_user_list = []
            for user_db in session_common.query(User).filter(User.user_type == 'CTA'):
                cta_user_list.append(user_db.id)

            for user_domain_db in session_common.query(UserDomain).filter(UserDomain.user_id.in_(tuple(cta_user_list))):
                user_domain_db.strategy_info += '%s@*;' % strategy_name
                session_common.merge(strategy_online_db)
            session_common.commit()
    return make_response(jsonify(code=200, message=u"保存策略:%s成功" % params.get('name')), 200)


@cta.route('/remove_strategy', methods=['GET', 'POST'])
def remove_strategy():
    params = request.json
    id = params.get('del_id')

    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    strategy_online_db = session_strategy.query(StrategyOnline).filter(StrategyOnline.id == id).first()
    strategy_name = strategy_online_db.name
    session_strategy.delete(strategy_online_db)
    session_strategy.commit()
    return make_response(jsonify(code=200, message=u"删除策略:%s成功" % strategy_name), 200)


@cta.route('/ctp_market_file_check', methods=['GET', 'POST'])
def ctp_market_file_check():
    date_filter_str = date_utils.get_today_str('%Y-%m-%d')
    if int(date_utils.get_today_str('%H%M%S')) < 153000:
        date_filter_str = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
        ctp_market_file_name = 'CTP_Market_%s_2.txt' % (date_filter_str,)
    else:
        ctp_market_file_name = 'CTP_Market_%s_1.txt' % (date_filter_str,)
    server_list = server_constant.get_cta_servers()

    market_file_path = const.EOD_CONFIG_DICT['ctp_data_backup_path']
    th_list = []
    td_data = []
    td_item_dic = {'filename': ctp_market_file_name}
    for server_name in server_list:
        path = '%s/%s' % (market_file_path, server_name)
        file_path = '%s/%s' % (path, ctp_market_file_name)
        if os.path.exists(file_path):
            td_item_dic[server_name] = True
        else:
            td_item_dic[server_name] = False
        th_item_dic = {'label': server_name, 'prop': server_name, 'children': []}
        th_list.append(th_item_dic)
    td_data.append(td_item_dic)
    result = {'th_list': th_list, 'td_data': td_data}
    return make_response(jsonify(code=200, data=result), 200)


@cta.route('/update_strategy_online_check', methods=['GET', 'POST'])
def update_strategy_online_check():
    server_list = server_constant.get_cta_servers()
    root_path = const.EOD_CONFIG_DICT['source_backtest_info_path']
    folder_list = [('backtest_info_str', '.csv', []),
                   ('backtest_parameter_str', '.txt', []),
                   ('server_parameter', '.txt', server_list)]

    th_list = []
    path_list = []
    for (folder_name, file_type, children_path_list) in folder_list:
        if len(children_path_list) == 0:
            tmp_dic = {'label': folder_name, 'prop': '', 'children': []}
            path = '%s/%s' % (root_path, folder_name)
            path_list.append(path)
        else:
            tmp_dic = {'label': folder_name, 'prop': '', 'children': []}
            for children_path_name in children_path_list:
                prop_str = '%s_%s' % (folder_name, children_path_name)
                children_dict = {'label': children_path_name, 'prop': prop_str, 'children': []}
                children_path = '%s/%s/%s' % (root_path, folder_name, children_path_name)
                path_list.append(children_path)
                tmp_dic['children'].append(children_dict)
        th_list.append(tmp_dic)

    td_data = []
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    for strategy_online_db in session_strategy.query(StrategyOnline).filter(StrategyOnline.enable == 1,
                                                                            StrategyOnline.strategy_type == 'CTA'):
        strategy_name = strategy_online_db.name
        tmp_data_dic = {'strategy_name': strategy_name}
        for (folder_name, file_type, children_path_list) in folder_list:
            file_name = strategy_name + file_type
            if len(children_path_list) == 0:
                file_path = '%s%s/%s' % (root_path, folder_name, file_name)
                tmp_data_dic_key = folder_name

                if os.path.exists(file_path):
                    tmp_data_dic[tmp_data_dic_key] = True
                else:
                    tmp_data_dic[tmp_data_dic_key] = False
            else:
                for children_path_name in children_path_list:
                    file_path = '%s%s/%s/%s' % (root_path, folder_name, children_path_name, file_name)
                    tmp_data_dic_key = '%s_%s' % (folder_name, children_path_name)

                    if os.path.exists(file_path):
                        tmp_data_dic[tmp_data_dic_key] = True
                    else:
                        tmp_data_dic[tmp_data_dic_key] = False
        td_data.append(tmp_data_dic)
    result = {'th_list': th_list, 'td_data': td_data}
    return make_response(jsonify(code=200, data=result), 200)


@cta.route('/backtest_files_export_check', methods=['GET', 'POST'])
def backtest_files_export_check():
    root_path = const.EOD_CONFIG_DICT['backtest_state_insert_folder']
    server_list = server_constant.get_cta_servers()
    no_night_market_future_types = const.EOD_CONFIG_DICT['no_night_market_future_types']
    validate_time_str = int(date_utils.get_today_str('%H%M%S'))

    th_list = []
    db_strategy_dict = dict()
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        session_strategy = server_model.get_db_session('strategy')
        query_sql = 'select a.TIME,a.`NAME`,a.`VALUE` from strategy.strategy_state a, (select `NAME`, max(Time) Time \
from strategy.strategy_state group by `NAME`) b where a.`NAME` = b.`NAME` and a.TIME = b.TIME order by a.`NAME`'
        for query_item in session_strategy.execute(query_sql):
            dict_key = '%s|%s' % (server_name, query_item[1])
            db_strategy_dict[dict_key] = [query_item[2]]
            db_strategy_dict[dict_key].append(str(query_item[0]))
        tmp_dic = {'label': server_name, 'prop': '', 'children': []}
        th_list.append(tmp_dic)

    td_data = []
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    for strategy_online_db in session_strategy.query(StrategyOnline).filter(StrategyOnline.enable == 1,
                                                                            StrategyOnline.strategy_type == 'CTA'):
        ticker_type = filter(lambda x: not x.isdigit(), strategy_online_db.instance_name)
        if ticker_type in no_night_market_future_types.split(',') and validate_time_str < 155000:
            continue

        tmp_data_dic = {'filename': strategy_online_db.name}

        sql_file_name = '%s_state_insert_sql.txt' % strategy_online_db.name
        for server_name in server_list:
            sql_file_path = '%s%s/%s' % (root_path, server_name, sql_file_name)
            if not os.path.exists(sql_file_path):
                tmp_data_dic[server_name] = False
                continue
            find_key = '%s|%s' % (server_name, strategy_online_db.name)
            if find_key not in db_strategy_dict:
                tmp_data_dic[server_name] = False
                continue
            db_value = db_strategy_dict[find_key][0]
            state_db_dict = json.loads(db_value.replace('\n', ''))
            with open(sql_file_path) as f:
                line = f.readlines()[0]
                reg = re.compile(
                    "^Insert Into strategy.strategy_state\(TIME,NAME,VALUE\) VALUES\(sysdate\(\),'(?P<strategy_state_name>[^,]*)','(?P<strategy_state_value>.*)'\)")
                reg_name_dict = reg.match(line).groupdict()
                strategy_state_value = reg_name_dict['strategy_state_value']
                state_file_dict = json.loads(strategy_state_value)
            tmp_data_dic[server_name] = cmp(state_file_dict, state_db_dict) == 0
            tmp_data_dic['time'] = db_strategy_dict[find_key][1]
        td_data.append(tmp_data_dic)
    result = {'th_dict': th_list, 'td_data': td_data}
    return make_response(jsonify(code=200, data=result), 200)


@cta.route('/query_strategy_names', methods=['GET', 'POST'])
def query_strategy_names():
    strategy_name_list = []

    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    for strategy_name_item in session_strategy.query(StrategyOnline.strategy_name).filter(StrategyOnline.enable == 1) \
            .group_by(StrategyOnline.strategy_name):
        strategy_name_dict = dict()
        strategy_name_dict['value'] = strategy_name_item[0]
        strategy_name_dict['label'] = strategy_name_item[0]
        strategy_name_list.append(strategy_name_dict)
    return make_response(jsonify(code=200, data=strategy_name_list), 200)


@cta.route('/update_strategy_server_parameter', methods=['GET', 'POST'])
def update_strategy_server_parameter():
    server_host = server_constant.get_server_model('host')
    session_job = server_host.get_db_session('jobs')
    session_strategy = server_host.get_db_session('strategy')
    fund_name_list = []
    for item in session_job.query(FundInfo):
        if not item.expiry_time:
            fund_name_list.append(item.name)
    session_job.close()
    server_strategy_dict = dict()
    for strategy_online_db in session_strategy.query(StrategyOnline).filter(StrategyOnline.enable == 1,
                                                                            StrategyOnline.strategy_type == 'CTA'):
        for server_name in strategy_online_db.target_server.split('|'):
            if server_name in server_strategy_dict:
                server_strategy_dict[server_name].append(strategy_online_db.name)
            else:
                server_strategy_dict[server_name] = [strategy_online_db.name]

    ctp_account_dict = __query_ctp_account_dict()
    date_str = date_utils.get_today_str('%Y-%m-%d')
    # 删除后新增
    session_strategy.query(StrategyState).filter(StrategyState == date_str).delete()
    for (server_name, strategy_name_list) in server_strategy_dict.items():
        server_strategy_parameter_dict = dict()
        server_model = server_constant.get_server_model(server_name)
        session_server_strategy = server_model.get_db_session('strategy')
        query_sql = "select name, time, value from (select distinct name, time, value from strategy.strategy_parameter \
order by time desc) t group by name"
        query_result = session_server_strategy.execute(query_sql)
        for strategy_parameter_info in query_result:
            strategy_name = strategy_parameter_info[0]
            if 'PairTrading' in strategy_name:
                continue
            server_strategy_parameter_dict[strategy_name] = strategy_parameter_info[2].replace('\n', '')

        for strategy_name in strategy_name_list:
            account_parameter_dict = dict()
            if strategy_name in server_strategy_parameter_dict:
                parameter_dict = json.loads(server_strategy_parameter_dict[strategy_name])
                if 'Account' in parameter_dict and parameter_dict['Account'] != '':
                    for account_name in parameter_dict['Account'].split(';'):
                        if account_name not in fund_name_list:
                            continue
                        account_parameter_dict[account_name] = (
                            parameter_dict['tq.%s.max_long_position' % account_name],
                            parameter_dict['tq.%s.max_short_position' % account_name],
                            parameter_dict['tq.%s.qty_per_trade' % account_name]
                        )

            fund_list = ctp_account_dict[server_name]
            for fund_name in fund_list:
                strategy_server_parameter = StrategyServerParameter()
                strategy_server_parameter.date = date_str
                strategy_server_parameter.server_name = server_name
                strategy_server_parameter.strategy_name = strategy_name
                strategy_server_parameter.account_name = fund_name
                if fund_name in account_parameter_dict:
                    (max_long_position, max_short_position, qty_per_trade) = account_parameter_dict[fund_name]
                    strategy_server_parameter.max_long_position = max_long_position
                    strategy_server_parameter.max_short_position = max_short_position
                    strategy_server_parameter.qty_per_trade = qty_per_trade
                else:
                    strategy_server_parameter.max_long_position = 0
                    strategy_server_parameter.max_short_position = 0
                    strategy_server_parameter.qty_per_trade = 0
                session_strategy.add(strategy_server_parameter)
    session_strategy.commit()

    const.EOD_POOL['strategy_parameter_modify_dict'] = dict()
    return make_response(jsonify(code=200, message=u'更新服务器参数成功'), 200)


@cta.route('/query_commodity_list', methods=['GET', 'POST'])
def query_commodity_list():
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    commodity_set = set()
    for instance_name_item in session_strategy.query(StrategyOnline.instance_name).filter(StrategyOnline.enable == 1) \
            .group_by(StrategyOnline.instance_name):
        if instance_name_item[0] == '':
            continue
        commodity_type = filter(lambda x: not x.isdigit(), instance_name_item[0])
        commodity_set.add(commodity_type)

    commodity_list = []
    for commodity_item in list(commodity_set):
        commodity_item_dict = dict(
            value=commodity_item,
            label=commodity_item
        )
        commodity_list.append(commodity_item_dict)
    commodity_list.sort(key=lambda item: item['value'].lower())
    return make_response(jsonify(code=200, data=commodity_list), 200)


@cta.route('/query_parameter', methods=['GET', 'POST'])
def query_parameter():
    query_params = request.json
    query_name = query_params.get('name')
    query_commodity = query_params.get('commodity')

    th_list = []
    cta_server_list = server_constant.get_cta_servers()
    ctp_account_dict = __query_ctp_account_dict()
    for server_name in cta_server_list:
        th_item_dict = {'label': server_name, 'prop': '', 'children': []}
        for fund_name in ctp_account_dict[server_name]:
            prop_str = '%s|%s' % (server_name, fund_name)
            children_dict = {'label': fund_name, 'prop': prop_str, 'children': []}
            th_item_dict['children'].append(children_dict)
        th_list.append(th_item_dict)

    ssp_dict = dict()
    date_str = date_utils.get_today_str('%Y-%m-%d')
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    for ssp_item in session_strategy.query(StrategyServerParameter) \
            .filter(StrategyServerParameter.date == date_str):
        dict_key = '%s|%s|%s' % (ssp_item.server_name, ssp_item.account_name, ssp_item.strategy_name)
        ssp_dict[dict_key] = ssp_item

    if len(ssp_dict) == 0:
        query_result = {'th_list': th_list, 'tr_list': [], 'modify_list': []}
        return make_response(jsonify(code=200, data=query_result), 200)

    # spm_dict中缓存修改过的参数数据
    spm_modify_dict = dict()
    if 'strategy_parameter_modify_dict' in const.EOD_POOL:
        spm_modify_dict = const.EOD_POOL['strategy_parameter_modify_dict']

    tr_list = []
    for strategy_online_item in session_strategy.query(StrategyOnline).filter(StrategyOnline.enable == 1,
                                                                              StrategyOnline.strategy_type == 'CTA'):
        if 'PairTrading' in strategy_online_item.name:
            continue
        if query_name and query_name.lower() not in strategy_online_item.name.lower():
            continue
        if query_commodity and query_commodity.lower() != strategy_online_item.name.split('.')[1].split('_')[0].lower():
        # if query_commodity and query_commodity not in strategy_online_item.instance_name:
            continue

        tr_item_dict = {'Strategy_Name': strategy_online_item.name}
        for th_item_dict in th_list:
            server_name = th_item_dict['label']
            for children_dict in th_item_dict['children']:
                account_name = children_dict['label']
                find_key = '%s|%s|%s' % (server_name, account_name, strategy_online_item.name)
                if find_key in ssp_dict:
                    parameter_value = ssp_dict[find_key].max_long_position
                else:
                    parameter_value = None

                if find_key in spm_modify_dict:
                    parameter_value = '%s->%s' % (parameter_value, spm_modify_dict[find_key].max_long_position)
                tr_item_dict['%s|%s' % (server_name, account_name)] = parameter_value
        tr_list.append(tr_item_dict)

    modify_parameter_list = []
    for (dict_key, modify_parameter) in spm_modify_dict.items():
        server_parameter = ssp_dict[dict_key]
        item_dict = dict(
            date=modify_parameter.date,
            server_name=modify_parameter.server_name,
            strategy_name=modify_parameter.strategy_name,
            account_name=modify_parameter.account_name,
            max_long_position='%s->%s' % (server_parameter.max_long_position, modify_parameter.max_long_position),
            max_short_position='%s->%s' % (server_parameter.max_short_position, modify_parameter.max_short_position),
            qty_per_trade='%s->%s' % (server_parameter.qty_per_trade, modify_parameter.qty_per_trade),
        )
        modify_parameter_list.append(item_dict)
    modify_parameter_list.sort(key=lambda item: item['strategy_name'].lower())

    query_result = {'th_list': th_list, 'tr_list': tr_list, 'modify_list': modify_parameter_list}
    return make_response(jsonify(code=200, data=query_result), 200)


@cta.route('/clear_strategy_status', methods=['GET', 'POST'])
def clear_strategy_status():
    query_params = request.json
    server_name_list = query_params.get('server_name_list')
    sql_str = query_params.get('sql_str')

    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_server_strategy = server_model.get_db_session('strategy')
        session_server_strategy.execute(str(sql_str))
        result = session_server_strategy.commit()
        if result:
            return make_response(jsonify(code=200, message=u'成功清理策略状态'), 200)
        else:
            return make_response(jsonify(code=200, message=u'无需要清理的策略状态'), 200)


@cta.route('/modify_parameter_value', methods=['GET', 'POST'])
def modify_parameter_value():
    query_params = request.json
    server_name = query_params.get('server_name')
    account_name = query_params.get('account_name')
    strategy_name = query_params.get('strategy_name')
    change_value = query_params.get('change_value')
    change_type = query_params.get('change_type')

    date_str = date_utils.get_today_str('%Y-%m-%d')
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')

    strategy_online_db = session_strategy.query(StrategyOnline).filter(StrategyOnline.name == strategy_name).first()
    ticker_type = filter(lambda x: not x.isdigit(), strategy_online_db.instance_name)

    ssp_base = session_strategy.query(StrategyServerParameter) \
        .filter(StrategyServerParameter.date == date_str, StrategyServerParameter.server_name == server_name,
                StrategyServerParameter.account_name == account_name,
                StrategyServerParameter.strategy_name == strategy_name).first()

    ssp_list = []
    if change_type == 'spread':
        for ssp_db in session_strategy.query(StrategyServerParameter).filter(StrategyServerParameter.date == date_str,
                                                                             StrategyServerParameter.strategy_name == strategy_name):
            ssp_list.append(ssp_db)
    elif change_type == 'single':
        ssp_list.append(ssp_base)

    spm_modify_dict = dict()
    if 'strategy_parameter_modify_dict' in const.EOD_POOL:
        spm_modify_dict = const.EOD_POOL['strategy_parameter_modify_dict']

    assignment_ratio_dict = __init_assignment_ratio_dict()
    for ssp_item in ssp_list:
        modify_parameter = StrategyServerParameter()
        modify_parameter.date = date_str
        modify_parameter.server_name = ssp_item.server_name
        modify_parameter.account_name = ssp_item.account_name
        modify_parameter.strategy_name = ssp_item.strategy_name

        if (ssp_item.account_name, ticker_type) in trade_restrictions_list:
            continue

        base_assignment = assignment_ratio_dict['%s|%s' % (ssp_base.server_name, ssp_base.account_name)]
        account_assignment = assignment_ratio_dict['%s|%s' % (ssp_item.server_name, ssp_item.account_name)]
        account_change_value = int(
            round(int(change_value) / base_assignment * account_assignment)) if base_assignment > 0 else 0
        modify_parameter.max_long_position = account_change_value
        modify_parameter.max_short_position = account_change_value
        modify_parameter.qty_per_trade = max(ssp_item.qty_per_trade, 2 * account_change_value)

        dict_key = '%s|%s|%s' % (ssp_item.server_name, ssp_item.account_name, ssp_item.strategy_name)
        spm_modify_dict[dict_key] = modify_parameter
    const.EOD_POOL['strategy_parameter_modify_dict'] = spm_modify_dict
    return make_response(jsonify(code=200, message=u'参数修改成功'), 200)


def __init_assignment_ratio_dict():
    assignment_ratio_dict = dict()
    assignment_ratio_dict_str = const.EOD_CONFIG_DICT['assignment_ratio_dict']
    for assignment_ratio_item in assignment_ratio_dict_str.split(';'):
        server_name, account_name, assignment_ratio_value = assignment_ratio_item.split('|')
        assignment_ratio_dict['%s|%s' % (server_name, account_name)] = float(assignment_ratio_value)
    return assignment_ratio_dict


@cta.route('/submit_parameter_modify', methods=['GET', 'POST'])
def submit_parameter_modify():
    server_host = server_constant.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    date_str = date_utils.get_today_str('%Y-%m-%d')

    base_parameter_dict = dict()
    for parameter_item in session_strategy.query(StrategyServerParameter).filter(
            StrategyServerParameter.date == date_str):
        dict_key = '%s|%s|%s' % (parameter_item.server_name, parameter_item.account_name, parameter_item.strategy_name)
        base_parameter_dict[dict_key] = parameter_item

    spm_dict = const.EOD_POOL['strategy_parameter_modify_dict']
    modify_strategy_set = set()
    for (dict_key, parameter_item) in spm_dict.items():
        session_strategy.merge(parameter_item)

        base_parameter_item = base_parameter_dict[dict_key]
        parameter_change_item = StrategyServerParameterChange()
        parameter_change_item.date = date_utils.get_now()
        parameter_change_item.server_name = parameter_item.server_name
        parameter_change_item.strategy_name = parameter_item.strategy_name
        parameter_change_item.account_name = parameter_item.account_name
        parameter_change_item.max_long_position = '%s->%s' % \
                                                  (base_parameter_item.max_long_position,
                                                   parameter_item.max_long_position)
        parameter_change_item.max_short_position = '%s->%s' % \
                                                   (base_parameter_item.max_short_position,
                                                    parameter_item.max_short_position)
        parameter_change_item.qty_per_trade = '%s->%s' % \
                                              (base_parameter_item.qty_per_trade, parameter_item.qty_per_trade)
        session_strategy.add(parameter_change_item)
        modify_strategy_set.add(parameter_item.strategy_name)
    session_strategy.commit()

    const.EOD_POOL['strategy_parameter_modify_dict'] = dict()

    modify_strategy_list = list(modify_strategy_set)
    strategy_online_dict = dict()
    for strategy_online_db in session_strategy.query(StrategyOnline). \
            filter(StrategyOnline.name.in_(tuple(modify_strategy_list))):
        strategy_online_dict[strategy_online_db.name] = strategy_online_db

    funds_parameter_dict = dict()
    for parameter_item in session_strategy.query(StrategyServerParameter).filter(
            StrategyServerParameter.date == date_str,
            StrategyServerParameter.strategy_name.in_(tuple(modify_strategy_list))):
        dict_key = '%s|%s' % (parameter_item.server_name, parameter_item.strategy_name)
        if dict_key in funds_parameter_dict:
            funds_parameter_dict[dict_key].append(parameter_item)
        else:
            funds_parameter_dict[dict_key] = [parameter_item, ]

    ctp_account_dict = __query_ctp_account_dict()

    add_strategy_parameter_dict = dict()
    for strategy_name in modify_strategy_list:
        strategy_online_db = strategy_online_dict[strategy_name]

        for server_name in strategy_online_db.target_server.split('|'):
            strategy_parameter = StrategyParameter()
            strategy_parameter.time = date_utils.get_now()
            strategy_parameter.name = strategy_online_db.name

            fund_list = ctp_account_dict[server_name]
            find_key = '%s|%s' % (server_name, strategy_name)
            fund_parameter_list = funds_parameter_dict[find_key]
            strategy_parameter.value = __build_server_strategy_parameter(fund_list, strategy_online_db,
                                                                         fund_parameter_list)

            if server_name in add_strategy_parameter_dict:
                add_strategy_parameter_dict[server_name].append(strategy_parameter)
            else:
                add_strategy_parameter_dict[server_name] = [strategy_parameter, ]

    for (server_name, strategy_parameter_list) in add_strategy_parameter_dict.items():
        server_model = server_constant.get_server_model(server_name)
        server_session_strategy = server_model.get_db_session('strategy')
        for strategy_parameter in strategy_parameter_list:
            server_session_strategy.add(strategy_parameter)
        server_session_strategy.commit()
    return make_response(jsonify(code=200, message=u'参数修改更新成功'), 200)


def __query_ctp_account_dict():
    ctp_account_dict = dict()
    server_account_dict = const.EOD_CONFIG_DICT['server_account_dict']
    for (server_name, account_list) in server_account_dict.items():
        ctp_fund_list = []
        for account_info in account_list:
            if 'cff,any' not in account_info.allow_targets:
                continue
            if account_info.fund_name in filter_fund_list:
                continue
            ctp_fund_list.append(account_info.fund_name)
        ctp_account_dict[server_name] = ctp_fund_list
    return ctp_account_dict


def __build_server_strategy_parameter(fund_list, strategy_online_db, server_parameter_list):
    filter_parameter_list = ['Account', 'max_long_position', 'max_short_position', 'qty_per_trade']

    server_strategy_parameter_dict = dict()
    strategy_online_parameter = strategy_online_db.parameter
    for base_parameter_str in strategy_online_parameter.split(';'):
        reg = re.compile('^.*\[(?P<parameter_name>.*)\](?P<parameter_value>[^:]*):*')
        reg_match = reg.match(base_parameter_str)
        temp_dict = reg_match.groupdict()

        if temp_dict['parameter_name'] in filter_parameter_list:
            continue
        server_strategy_parameter_dict[temp_dict['parameter_name']] = temp_dict['parameter_value']

    server_strategy_parameter_dict['Account'] = ';'.join(fund_list)
    for account_name in fund_list:
        account_parameter_item = None
        for server_parameter_item in server_parameter_list:
            if server_parameter_item.account_name == account_name:
                account_parameter_item = server_parameter_item
                break

        if account_parameter_item is None:
            dict_key = 'tq.%s.max_long_position' % account_name
            server_strategy_parameter_dict[dict_key] = 0

            dict_key = 'tq.%s.max_short_position' % account_name
            server_strategy_parameter_dict[dict_key] = 0

            dict_key = 'tq.%s.qty_per_trade' % account_name
            server_strategy_parameter_dict[dict_key] = 0
        else:
            dict_key = 'tq.%s.max_long_position' % account_name
            server_strategy_parameter_dict[dict_key] = account_parameter_item.max_long_position

            dict_key = 'tq.%s.max_short_position' % account_name
            server_strategy_parameter_dict[dict_key] = account_parameter_item.max_short_position

            dict_key = 'tq.%s.qty_per_trade' % account_name
            server_strategy_parameter_dict[dict_key] = account_parameter_item.qty_per_trade
    server_strategy_parameter_dict['Target'] = strategy_online_db.instance_name
    return json.dumps(server_strategy_parameter_dict)


@cta.route('/query_future_main_contract', methods=['GET', 'POST'])
def query_future_main_contract():
    query_params = request.json
    ticker_type = query_params.get('ticker_type')
    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    data = []
    for obj in session_common.query(FutureMainContract):
        # print obj.update_flag, bool(obj.update_flag)
        data.append(dict(ticker_type=obj.ticker_type,
                         exchange_id=obj.exchange_id,
                         pre_main_symbol=obj.pre_main_symbol,
                         main_symbol=obj.main_symbol,
                         next_main_symbol=obj.next_main_symbol,
                         night_flag=bool(int(obj.night_flag)),
                         update_flag=bool(int(obj.update_flag)), ))
    session_common.close()
    data = sorted(data, key=lambda item: item['update_flag'], reverse=True)
    if sort_prop:
        if sort_order == 'ascending':
            data = sorted(data, key=lambda data_item: data_item[sort_prop], reverse=True)
        else:
            data = sorted(data, key=lambda data_item: data_item[sort_prop])
    if ticker_type:
        data = filter(lambda item: item['ticker_type'] == ticker_type, data)
    result = {'data': data}
    return make_response(jsonify(code=200, message=u'参数修改成功', data=result), 200)


@cta.route('/change_future_main_contract', methods=['GET', 'POST'])
def change_future_main_contract():
    query_params = request.json
    print query_params
    contract_change_parameter = query_params.get('contract_change_parameter')
    if not contract_change_parameter:
        return make_response(jsonify(code=200, message=u'没有换月合约', data={}), 200)
    contract_change_parameter_list = contract_change_parameter.split('\n')
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    for item in contract_change_parameter_list:
        if len(item) == 0:
            continue

        temp_data = item.split(',')
        if len(temp_data) != 6:
            session_common.close()
            err_msg = '%s:换月合约参数不正确' % item
            return make_response(jsonify(code=200, message=err_msg, data={}), 200)
        obj = FutureMainContract()
        obj.ticker_type = temp_data[1]
        obj.pre_main_symbol = temp_data[2]
        obj.main_symbol = temp_data[3]
        obj.next_main_symbol = temp_data[4]
        obj.exchange_id = temp_data[5]
        obj.update_flag = 1
        session_common.merge(obj)
    session_common.commit()
    session_common.close()
    return make_response(jsonify(code=200, message='换月合约参数插入成功', data={}), 200)
