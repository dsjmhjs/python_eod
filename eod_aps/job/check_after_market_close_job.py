# coding=utf-8
import os
import ssh
import re
import traceback
from compiler.ast import flatten
from eod_aps.job.history_date_file_check_job import __history_date_check
from eod_aps.job import *

email_content_list = []


def order_empty_check(session_server):
    order_empty_flag = True
    query_sql = "select count(1) from om.order"
    order_number = session_server.execute(query_sql).first()[0]
    if order_number > 0:
        order_empty_flag = False
    return order_empty_flag


def trade_empty_check(session_server):
    trade_empty_flag = True
    query_sql = "select count(1) from om.trade2"
    trade_number = session_server.execute(query_sql).first()[0]
    if trade_number > 0:
        trade_empty_flag = False
    return trade_empty_flag


def order_history_max_time_check(session_server):
    query_sql = "select max(create_time) from om.order_history"
    max_order_time = session_server.execute(query_sql).first()[0]
    if max_order_time:
        max_order_time = date_utils.datetime_toString(max_order_time, '%Y-%m-%d')
    return max_order_time


def trade2_history_max_time_check(session_server):
    query_sql = "select max(time) from om.trade2_history"
    max_trade_time = session_server.execute(query_sql).first()[0]
    if max_trade_time:
        max_trade_time = date_utils.datetime_toString(max_trade_time, '%Y-%m-%d')
    return max_trade_time


def order_trade_backup_check(server_name_list):
    email_list = ['<br><br><li>Order_Trade_Check:</li>']
    html_title = ',%s' % ','.join(server_name_list)
    check_date_str = date_utils.get_today_str('%Y-%m-%d')
    table_info_list = [['Order Empty', 'Trade2 Empty', 'Order History Max Date', 'Trder2 History Max Date']]
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_om = server_model.get_db_session('om')
        order_empty_flag = order_empty_check(session_om)
        trade_empty_flag = trade_empty_check(session_om)
        max_order_time = order_history_max_time_check(session_om)
        max_trade_time = trade2_history_max_time_check(session_om)

        order_empty_str = order_empty_flag if order_empty_flag else '%s(Error)' % order_empty_flag
        trade_empty_str = trade_empty_flag if trade_empty_flag else '%s(Error)' % trade_empty_flag
        max_order_time = max_order_time if max_order_time == check_date_str else '%s(Warning)' % max_order_time
        max_trade_time = max_trade_time if max_trade_time == check_date_str else '%s(Warning)' % max_trade_time
        table_info_list.append([order_empty_str, trade_empty_str, max_order_time, max_trade_time])
        server_model.close()

    table_info_list = map(list, zip(*table_info_list))

    html_list = email_utils2.list_to_html(html_title, table_info_list)
    email_list.append(''.join(html_list))
    email_list.append('----------------------------------------------------------------------------------<br><br>')
    return email_list


def get_service_check_dict(server_name_list):
    service_check_dict = dict()
    server_model = server_constant.get_server_model('host')
    session_host = server_model.get_db_session('common')
    query_sql = 'select server_name, app_name from common.server_info'
    for query_item in session_host.execute(query_sql):
        service_check_dict.setdefault(query_item[0], []).append(query_item[1])
    return service_check_dict


def trade_version_check(server_name_list):
    email_list = "<br><br><li>trade version check:</li>"
    table_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        cmd = 'ls -l %s/build64_release' % server_model.server_path_dict['tradeplat_project_folder']
        rst = server_model.run_cmd_str(cmd)
        version = re.findall(r'\d+_\d+', rst)[-1]
        table_list.append((server_name, version))
    html_tilte = 'servername,version'
    html_list = email_utils2.list_to_html2(html_tilte, table_list)
    email_list += ''.join(html_list)
    email_list += '----------------------------------------------------------------------------------<br><br>'
    return email_list


def service_close_check(server_name_list):
    trade_server_list = server_constant.get_trade_servers()
    filter_server_list = [x for x in server_name_list if x in trade_server_list]

    email_list = ['<br><br><li>Service Status Check:</li>']
    html_title = ',%s' % ','.join(filter_server_list)
    service_check_dict = get_service_check_dict(filter_server_list)
    service_list = list(set(flatten(service_check_dict.values())))

    table_info_list = [service_list]
    for server_name in filter_server_list:
        server_model = server_constant.get_server_model(server_name)
        ssh_result = server_model.run_cmd_str('screen -ls')
        ssh_result = ssh_result.replace(unicode('年', 'utf-8'), '-').replace(unicode('月', 'utf-8'), '-') \
                               .replace(unicode('日', 'utf-8'), '')
        ssh_result = ssh_result.replace(unicode('时', 'utf-8'), ':').replace(unicode('分', 'utf-8'), ':') \
                               .replace(unicode('秒', 'utf-8'), '')

        tr_list = []
        for service_name in service_list:
            status_str = '/'
            if service_name in service_check_dict[server_name]:
                filter_time_str = date_utils.get_today_str('%H%M%S')
                if int(filter_time_str) < 160000 or int(filter_time_str) > 200000:
                    status_str = 'Active' if service_name in ssh_result else 'Inactive(Error)'
                else:
                    status_str = 'Active(Error)' if service_name in ssh_result else 'Inactive'
            tr_list.append(status_str)
        table_info_list.append(tr_list)
        server_model.close()

    table_info_list = map(list, zip(*table_info_list))

    html_list = email_utils2.list_to_html(html_title, table_info_list)
    email_list.append(''.join(html_list))
    email_list.append('----------------------------------------------------------------------------------<br><br>')
    return email_list


def aggregation_check(server_name_list):
    email_list = ['<br><br><li>Aggregation_Check:</li>']
    html_title = ',%s' % ','.join(server_name_list)

    table_info_list = [['order', 'trade2', 'pf_account', 'pf_position']]

    server_model_118 = server_constant.get_server_model('local118')
    session_aggregation = server_model_118.get_db_session('aggregation')
    start_date_str = date_utils.get_last_trading_day('%Y-%m-%d') + ' 16:00:00'
    today_date_str = date_utils.get_today_str("%Y-%m-%d")
    next_date_str = date_utils.get_next_trading_day("%Y-%m-%d")
    end_date_str = today_date_str + ' 16:00:00'

    query_sql = 'select server_name, count(1) from `order` where create_time > "%s" and create_time < "%s" \
group by server_name' % (start_date_str, end_date_str)
    order_dict_local = {x[0]: int(x[1]) for x in session_aggregation.execute(query_sql)}
    query_sql = 'select server_name, count(1) from trade2 where time > "%s" and time < "%s" \
group by server_name' % (start_date_str, end_date_str)
    trade_dict_local = {x[0]: int(x[1]) for x in session_aggregation.execute(query_sql)}
    query_sql = 'select server_name, count(1) from pf_account group by server_name'
    pf_account_dict_local = {x[0]: int(x[1]) for x in session_aggregation.execute(query_sql)}
    query_sql = 'select server_name, count(1) from pf_position where date = "%s" group by server_name' % next_date_str
    pf_position_dict_local = {x[0]: int(x[1]) for x in session_aggregation.execute(query_sql)}

    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_om = server_model.get_db_session('om')
        query_sql = 'select count(1) from order_history where create_time > "%s" and create_time < "%s"' % \
                     (start_date_str, end_date_str)
        o_l = session_om.execute(query_sql).first()[0]
        o_l_l = order_dict_local[server_name] if server_name in order_dict_local else 0

        query_sql = 'select count(1) from trade2_history where time > "%s" and time < "%s"' % \
                     (start_date_str, end_date_str)
        t_l = session_om.execute(query_sql).first()[0]
        t_l_l = trade_dict_local[server_name] if server_name in trade_dict_local else 0

        session_portfolio = server_model.get_db_session('portfolio')
        query_sql = 'select COUNT(1) from pf_account'
        p_a_l = session_portfolio.execute(query_sql).first()[0]
        p_a_l_l = pf_account_dict_local[server_name] if server_name in pf_account_dict_local else 0

        query_sql = 'select count(1) from portfolio.pf_position where date = "%s"' % next_date_str
        p_p_l = session_portfolio.execute(query_sql).first()[0]
        pf_p_l_l = pf_position_dict_local[server_name] if server_name in pf_position_dict_local else 0

        table_info_list.append(['%s/%s' % (o_l_l, o_l) if o_l_l == o_l else '%s/%s(Error)' % (o_l_l, o_l),
                                '%s/%s' % (t_l_l, t_l) if t_l_l == t_l else '%s/%s(Error)' % (t_l_l, t_l),
                                '%s/%s' % (p_a_l_l, p_a_l) if p_a_l_l == p_a_l else '%s/%s(Error)' % (p_a_l_l, p_a_l),
                                '%s/%s' % (pf_p_l_l, p_p_l) if pf_p_l_l == p_p_l else '%s/%s(Error)' % (pf_p_l_l, p_p_l)
                                ])
    table_info_list = map(list, zip(*table_info_list))

    html_list = email_utils2.list_to_html(html_title, table_info_list)
    email_list.append(''.join(html_list))
    email_list.append('----------------------------------------------------------------------------------<br><br>')
    return email_list


def history_file_check(server_name_list):
    email_list = ['<br><br><li>History_File_Check -- </li>']
    html_title = ',%s' % ','.join(server_name_list)

    table_info_list = [['Check_Result']]
    for server_name in server_name_list:
        validate_flag = __history_date_check(server_name)
        validate_str = '%s' % validate_flag if validate_flag else '%s(Error)' % validate_flag
        table_info_list.append([validate_str])

    table_info_list = map(list, zip(*table_info_list))

    html_list = email_utils2.list_to_html(html_title, table_info_list)
    email_list.append(''.join(html_list))
    email_list.append('----------------------------------------------------------------------------------<br><br>')
    return email_list


def data_download_check(server_name_list):
    data_download_server_list = []
    # ctp
    ctp_market_servers = server_constant.get_download_market_servers()
    ctp_server_check_list = [x for x in ctp_market_servers if x in server_name_list]

    # stock
    mktcenter_server_list = server_constant.get_mktcenter_servers()
    stock_data_download_server_list = [x for x in mktcenter_server_list if x in server_name_list]

    # get ctp info
    email_list = ['<br><br><li>Data_Download_Check:</li>']
    html_title = ',%s' % ','.join(server_name_list)

    ctp_file_1 = 'CTP_Market_%s_1.txt' % date_utils.get_today_str('%Y-%m-%d')
    ctp_file_2 = 'CTP_Market_%s_2.txt' % date_utils.get_last_trading_day('%Y-%m-%d')
    ctp_file_list = [ctp_file_1, ctp_file_2]

    market_file_path_dict = dict()
    market_file_list = []
    for server_name in stock_data_download_server_list:
        server_model = server_constant.get_server_model(server_name)
        download_file_folder = server_model.market_file_localpath
        for mktcenter_file_template in server_model.market_file_template.split(','):
            market_file_name = mktcenter_file_template % date_utils.get_today_str() + '.tar.gz'
            dict_key = '%s|%s' % (server_name, market_file_name)
            market_file_path_dict[dict_key] = download_file_folder + '/' + market_file_name
            market_file_list.append(market_file_name)

    table_info_list = [ctp_file_list + market_file_list]
    for server_name in server_name_list:
        tr_list = []
        for ctp_file_name in ctp_file_list:
            exists_flag = '/'
            if server_name in ctp_server_check_list:
                ctp_file_path = os.path.join(CTP_DATA_BACKUP_PATH, server_name, ctp_file_name)
                exists_flag = 'True' if os.path.exists(ctp_file_path) else 'False(Error)'
            tr_list.append(exists_flag)

        for market_file_name in market_file_list:
            exists_flag = '/'
            dict_key = '%s|%s' % (server_name, market_file_name)
            if dict_key in market_file_path_dict:
                market_file_path = os.path.join(market_file_path_dict[dict_key])
                exists_flag = 'True' if os.path.exists(market_file_path) else 'False(Error)'
            tr_list.append(exists_flag)
        table_info_list.append(tr_list)

    table_info_list = map(list, zip(*table_info_list))

    html_list = email_utils2.list_to_html(html_title, table_info_list)
    email_list.append(''.join(html_list))
    email_list.append('----------------------------------------------------------------------------------<br><br>')
    return email_list


def pf_position_check(server_name_list):
    email_list = ['<br><br><li>Pf_Position_Check:</li>']
    html_title = ',%s' % ','.join(server_name_list)
    check_date_str = date_utils.get_next_trading_day('%Y-%m-%d')

    table_info_list = [['Max_Date', 'Day_Long', 'Day_Short', 'StkIntraDay_Volume', 'Common_Stock_Short']]
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_sql = "select max(date) from portfolio.pf_position"
        max_date = date_utils.datetime_toString(session_portfolio.execute(query_sql).first()[0])

        sum_day_long, sum_day_short = 0, 0
        query_sql = "select sum(day_long), sum(day_short) from portfolio.pf_position where `DATE` = '%s'" % max_date
        for result_item in session_portfolio.execute(query_sql):
            sum_day_long = int(result_item[0])
            sum_day_short = int(result_item[1])
        sum_day_long = str(sum_day_long) + '(Error)' if sum_day_long > 0 else sum_day_long
        sum_day_short = str(sum_day_short) + '(Error)' if sum_day_short > 0 else sum_day_short

        error_volume_size = 0
        query_sql = "select a.`LONG`, a.SHORT from portfolio.pf_position a where a.DATE = '%s' and a.id in \
(select b.ID from portfolio.pf_account b where b.FUND_NAME like '%s')" % (max_date, '%StkIntraDayStrategy%')
        for result_item in session_portfolio.execute(query_sql):
            if int(result_item[0]) > 0 or int(result_item[1]) > 0:
                error_volume_size += 1
        error_volume_size = str(error_volume_size) + '(Error)' if error_volume_size > 0 else error_volume_size

        common_stock_error = 0
        query_sql = "select a.SYMBOL from portfolio.pf_position a where a.DATE = '%s' and a.SHORT > 0" % max_date
        for result_item in session_portfolio.execute(query_sql):
            temp_symbol = result_item[0]
            if temp_symbol.startswith('0') or temp_symbol.startswith('3') or temp_symbol.startswith('6'):
                common_stock_error += 1
        common_stock_error = str(common_stock_error) + '(Error)' if common_stock_error > 0 else common_stock_error

        max_date = max_date + '(Error)' if max_date != check_date_str else max_date
        table_info_list.append([max_date, sum_day_long, sum_day_short, error_volume_size, common_stock_error])
    table_info_list = map(list, zip(*table_info_list))

    html_list = email_utils2.list_to_html(html_title, table_info_list)
    email_list.append(''.join(html_list))
    email_list.append('----------------------------------------------------------------------------------<br><br>')
    return email_list


def check_after_market_close_job(server_name_list):
    email_info_list = []
    email_info_list.extend(order_trade_backup_check(server_name_list))
    email_info_list.extend(service_close_check(server_name_list))
    email_info_list.extend(pf_position_check(server_name_list))
    email_info_list.extend(aggregation_check(server_name_list))

    calendar_server_list = server_constant.get_calendar_servers()
    email_info_list.extend(history_file_check(calendar_server_list))

    email_info_list.extend(data_download_check(server_name_list))
    email_utils2.send_email_group_all(u'Check After Market Close', ''.join(email_info_list), 'html')


if __name__ == "__main__":
    all_server_list = server_constant.get_all_trade_servers()
    check_after_market_close_job(all_server_list)
