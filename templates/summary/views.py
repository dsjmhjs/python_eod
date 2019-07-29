# coding: utf-8
from eod_aps.tools.risk_group_tools import RiskGroupTools
from flask import render_template, request, current_app, flash, redirect, url_for, jsonify, make_response
from . import summary
from eod_aps.model.eod_const import const
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils


@summary.route('/risk_summary', methods=['GET', 'POST'])
def risk_summary():
    if 'risk_dict' not in const.EOD_POOL:
        query_result = {'data': [],
                        'update_time': ''
                        }
        return make_response(jsonify(code=200, data=query_result), 200)

    query_params = request.json
    summary_type = query_params.get('summary_type')

    risk_group_tools = RiskGroupTools(summary_type)
    risk_group_list = risk_group_tools.risk_group_index()

    sort_prop = query_params.get('sort_prop')
    sort_order = query_params.get('sort_order')
    if sort_prop:
        if sort_order == 'ascending':
            query_result_list = sorted(risk_group_list[:-1], key=lambda data_item: data_item[sort_prop], reverse=True)
        else:
            query_result_list = sorted(risk_group_list[:-1], key=lambda data_item: data_item[sort_prop])
    else:
        query_result_list = risk_group_list[:-1]
    query_result_list.append(risk_group_list[-1])

    query_result = {'data': query_result_list,
                    'update_time': const.EOD_POOL['position_update_time']
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


def get_fund_info_dict():
    sql = 'SELECT `name`,`create_time`,`expiry_time` FROM `fund_info`;'
    server_host = server_constant.get_server_model('host')
    session = server_host.get_db_session('jobs')
    fund_info_dict = dict()
    for line in session.execute(sql):
        if line[2]:
            continue
        fund_info_dict[line[0]] = line[1]
    return fund_info_dict


@summary.route('/asset_value', methods=['GET', 'POST'])
def asset_value():
    # query_params = request.args
    query_params = request.json
    size = query_params.get('size', 10)
    page = query_params.get('page', 1)
    server_host = server_constant.get_server_model('host')
    session = server_host.get_db_session('jobs')
    fund_name = query_params.get('fund_name')
    search_date_item = query_params.get('search_date')
    export_flag = False
    fundNames = []
    for fund in session.execute('select product_name from asset_value_info group by product_name'):
        tmp_fund = dict(
            value=fund[0],
            label=fund[0]
        )
        fundNames.append(tmp_fund)
    data = []
    ret = session.execute(
        "select `unit_net`,`net_asset_value`,`product_name`,`date_str`,`sum_value`,`real_capital`,`nav_change` from \
        asset_value_info").fetchall()
    date_utils = DateUtils()
    for line in ret:
        tmp_data = dict(
            unit_net='%.4f' % float(line[0]),
            net_asset_value='%.2f' % float(line[1]),
            product_name=line[2],
            date=date_utils.datetime_toString(line[3]),
            sum_value='%.4f' % float(line[4]),
            real_capital='%.2f' % float(line[5]),
            nav_change='%.2f%%' % (float('%.4f' % float(line[6])) * 100)
        )
        data.append(tmp_data)
    data = sorted(data, key=lambda data_item: data_item['date'], reverse=True)
    sql = "select b.`product_name` AS `fund_name`,b.date_s AS `date`,b.`sum_value` AS `max_sum_value`,b.date_str AS \
`max_date`,d.`sum_value` AS `min_sum_value`,d.date_str AS `min_date`  from (select product_name,sum_value,date_str,date_format(`date_str`,'%Y')  AS `date_s` from (select * from asset_value_info order by date_str desc) as a group by product_name,date_format(a.`date_str`,'%Y')) as b  inner    join (select product_name,sum_value,date_str,date_format(`date_str`,'%Y')  AS `date_s` from (select * from asset_value_info order by date_str) as c group by product_name,date_format(c.`date_str`,'%Y')) as d on b.product_name=d.product_name and b.date_s=d.date_s"
    val_ret = session.execute(sql).fetchall()
    year_list = []
    val_change_data = []

    fund_info_dict = get_fund_info_dict()
    temp_net_rate_dict = {}
    for line in val_ret:
        if str(line[0]) not in temp_net_rate_dict:
            temp_net_rate_dict[line[0]] = [(line[1], line[4])]
            # temp_net_rate_dict[line[0]] = [(line[1], line[2])]
            temp_net_rate_dict[line[0]].append((line[1], line[2]))
        else:
            temp_net_rate_dict[line[0]].append((line[1], line[2]))
        if line[1] not in year_list:
            year_list.append(str(line[1]))
        # if temp_data:
        #     if str(line[0]) in temp_data.values():
        #         temp_data[str(line[1])] = '%.2f%%' % ((float(line[2]) - float(line[4])) / float(line[4]) * 100)
        #         if val_ret.index(line) + 1 == len(val_ret):
        #             val_change_data.append(temp_data)
        #     else:
        #         val_change_data.append(temp_data)
        #         temp_data = {'fund_name': str(line[0]),
        #                      str(line[1]): '%.2f%%' % ((float(line[2]) - float(line[4])) / float(line[4]) * 100)}
        # else:
        #     temp_data[str(line[1])] = '%.2f%%' % ((float(line[2]) - float(line[4])) / float(line[4]) * 100)
        #     temp_data['fund_name'] = str(line[0])
    for k, v in temp_net_rate_dict.items():
        if str(k) not in fund_info_dict:
            continue
        temp_data = dict()
        temp_data['fund_name'] = str(k)
        # v = sorted(v, key=lambda data_item: data_item[0], reverse=True)

        for index, item in enumerate(v):
            if index != 0:
                temp_data[item[0]] = '%.2f%%' % (
                        ((float(item[1]) - float(v[index - 1][1])) / float(v[index - 1][1])) * 100)
            # else:
            #     temp_data[item[0]] = '%.2f%%' % (((float(item[1]) - 1) / 1) * 100)
        tmp_dict = filter(lambda data_item: data_item['product_name'] == str(k), data)[0]
        temp_data['unit_net'] = tmp_dict['unit_net']
        temp_data['sum_value'] = tmp_dict['sum_value']
        temp_data['net_asset_value'] = tmp_dict['net_asset_value']
        temp_data['real_capital'] = tmp_dict['real_capital']
        temp_data['nav_change'] = tmp_dict['nav_change']
        temp_data['date'] = tmp_dict['date']
        temp_data['create_time'] = str(fund_info_dict[str(k)])
        val_change_data.append(temp_data)
    year_list = sorted(year_list, reverse=True)
    right_sort_prop = query_params.get('right_sort_prop', '')
    right_sort_order = query_params.get('right_sort_order', '')
    if right_sort_prop:
        if right_sort_order == 'ascending':
            val_change_data = sorted(val_change_data, key=lambda data_item: data_item[right_sort_prop], reverse=True)
        else:
            val_change_data = sorted(val_change_data, key=lambda data_item: data_item[right_sort_prop])
    session.close()
    sort_prop = query_params.get('sort_prop', '')
    sort_order = query_params.get('sort_order', '')
    if search_date_item:
        start_date, end_date = search_date_item
        data = filter(lambda data_item: start_date <= data_item['date'] <= end_date, data)
        export_flag = True
    if fund_name:
        data = filter(lambda data_item: data_item['product_name'] == fund_name, data)
        export_flag = True
    if sort_prop:
        if sort_order == 'ascending':
            data = sorted(data, key=lambda data_item: data_item[sort_prop], reverse=True)
        else:
            data = sorted(data, key=lambda data_item: data_item[sort_prop])
    start = (int(page) - 1) * int(size)
    end = int(page) * int(size)
    total = len(data)
    if total < end:
        end = total
    export_data = data
    if not export_flag:
        export_data = data[start:end]
    data = data[start:end]

    pagination = {'total': total, 'size': int(size), 'currentPage': int(page)}
    result = {'data': data, 'fundNames': fundNames, 'pagination': pagination, 'year_list': year_list,
              'val_change_data': val_change_data, 'export_data': export_data}
    return make_response(jsonify(code=200, data=result), 200)


@summary.route('/asset_value_history_detail', methods=['GET', 'POST'])
def asset_value_history_detail():
    query_params = request.json
    fundname = query_params.get('params')
    server_host = server_constant.get_server_model('host')
    session = server_host.get_db_session('jobs')
    sql = "select `unit_net`,`net_asset_value`,`product_name`,`date_str`,`sum_value`,`real_capital`,`nav_change` from \
    asset_value_info where `product_name`='%s'" % fundname
    date_utils = DateUtils()
    data = []
    for line in session.execute(sql):
        tmp_data = dict(
            unit_net=float(line[0]),
            net_asset_value=float('%.2f' % float(line[1])),
            product_name=line[2],
            date=date_utils.datetime_toString(line[3]),
            sum_value=float(line[4]),
            real_capital=float('%.2f' % float(line[5])),
            nav_change=float('%.4f' % float(line[6]))
        )
        data.append(tmp_data)
    session.close()
    data = sorted(data, key=lambda data_item: data_item['date'])
    date_list = map(lambda data_item: data_item['date'], data)
    asset_value_list = map(lambda data_item: data_item['net_asset_value'], data)
    unit_net_list = map(lambda data_item: data_item['unit_net'], data)
    sum_unit_net_list = map(lambda data_item: data_item['sum_value'], data)
    real_capital_list = map(lambda data_item: data_item['real_capital'], data)
    result = {'date_list': date_list, 'asset_value_list': asset_value_list, 'unit_net_list': unit_net_list,
              'sum_unit_net_list': sum_unit_net_list, 'real_capital_list': real_capital_list}
    return make_response(jsonify(code=200, data=result), 200)
