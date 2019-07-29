# -*- coding: utf-8 -*-
import json

from eod_aps.model.schema_common import FutureMainContract
from eod_aps.tools.date_utils import *
from eod_aps.job import *

trading_state_time1 = '02:50:00'
trading_state_time2 = '15:50:00'
interval_minutes = 5

month_eng_num_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                      'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

# 'ATRBarStr', 'ATRSDBarStr', 'ATRBrkBarStr', 'lastTradeSpreadSignal'
dot_filter_list = [',', '.', ';', '[', ']']
close_signal_list = ['NA', 'Neutral', 'CloseLong', 'CloseShort']


def get_strategy_name_list(server_model):
    session_server = server_model.get_db_session('strategy')
    query_sql = "select TARGET_SERVER, name from strategy.strategy_online where ENABLE = 1 and STRATEGY_TYPE = 'CTA'"
    query_result = session_server.execute(query_sql)
    strategy_name_list = []
    for result_line in query_result:
        strategy_name_list.append((result_line[0], result_line[1]))
    return strategy_name_list


def get_state_time_limit():
    time_now_str = date_utils.get_today_str('%H%M%S')
    if int(time_now_str) < 160000:
        start_date = date_utils.string_toDatetime(date_utils.get_last_trading_day("%Y-%m-%d"), "%Y-%m-%d")
        end_date_str = date_utils.get_last_day(1, start_date=start_date, format_str='%Y-%m-%d')
        end_date_str = end_date_str + ' ' + trading_state_time1
        end_date = date_utils.string_toDatetime(end_date_str, '%Y-%m-%d %H:%M:%S')
    else:
        start_date_str = date_utils.get_today_str("%Y-%m-%d")
        end_date_str = start_date_str + ' ' + trading_state_time2
        end_date = date_utils.string_toDatetime(end_date_str, '%Y-%m-%d %H:%M:%S')

    time_limit1 = date_utils.get_last_minutes(-interval_minutes, end_date)
    time_limit2 = date_utils.get_last_minutes(interval_minutes, end_date)
    time_limit1_str = date_utils.datetime_toString(time_limit1, '%Y-%m-%d %H:%M:%S')
    time_limit2_str = date_utils.datetime_toString(time_limit2, '%Y-%m-%d %H:%M:%S')
    return time_limit1_str, time_limit2_str


def get_trading_state_dict(server_model, strategy_name_list):
    [time_limit1_str, time_limit2_str] = get_state_time_limit()
    session_server = server_model.get_db_session('strategy')
    query_sql = "select * from strategy.strategy_state where time > '%s' and time < '%s'" % \
                (time_limit1_str, time_limit2_str)
    query_result = session_server.execute(query_sql)
    trading_state_dict = dict()
    for result_line in query_result:
        strategy_name = result_line[1]
        strategy_state_value = result_line[2]
        if strategy_name in strategy_name_list:
            strategy_state_value_dict = json.loads(strategy_state_value.replace('\n', ''))
            strategy_state_value_dict_new = dict()
            for [state_key, state_value] in strategy_state_value_dict.items():
                for [month_eng, month_num] in month_eng_num_dict.items():
                    state_value = state_value.replace(month_eng, month_num)
                re_list = re.findall(r'\d{4}-\d{2}-\d{2} ', state_value)
                for re_key in re_list:
                    state_value = state_value.replace(re_key, '')
                strategy_state_value_dict_new[state_key] = state_value
            trading_state_dict[strategy_name] = strategy_state_value_dict_new
    return trading_state_dict


def get_backtest_state_dict(server_model, strategy_name_list):
    time_limit1_str, time_limit2_str = get_state_time_limit()
    time_limit2 = date_utils.string_toDatetime(time_limit2_str, '%Y-%m-%d %H:%M:%S')
    session_server = server_model.get_db_session('strategy')
    query_sql = 'select * from strategy.strategy_state where time > "%s" ORDER BY time asc' % time_limit2_str
    query_result = session_server.execute(query_sql)
    backtest_state_dict = dict()
    backtest_state_time_dict = dict()
    for result_line in query_result:
        backtest_state_time, strategy_name, strategy_state_value = result_line[0], result_line[1], result_line[2]
        if backtest_state_time is None:
            continue
        if backtest_state_time > time_limit2 and strategy_name in strategy_name_list:
            strategy_state_value_dict = json.loads(strategy_state_value.replace('\n', ''))
            strategy_state_value_dict_new = dict()
            for [state_key, state_value] in strategy_state_value_dict.items():
                for [month_eng, month_num] in month_eng_num_dict.items():
                    state_value = state_value.replace(month_eng, month_num)
                re_list = re.findall(r'\d{4}-\d{2}-\d{2} ', state_value)
                for re_key in re_list:
                    state_value = state_value.replace(re_key, '')
                strategy_state_value_dict_new[state_key] = state_value
            if strategy_name in backtest_state_time_dict:
                if backtest_state_time > backtest_state_time_dict[strategy_name]:
                    backtest_state_dict[strategy_name] = strategy_state_value_dict_new
                    backtest_state_time_dict[strategy_name] = backtest_state_time
            else:
                backtest_state_dict[strategy_name] = strategy_state_value_dict_new
                backtest_state_time_dict[strategy_name] = backtest_state_time
    return backtest_state_dict


def __float_check(state_value):
    dot_flag = True
    first_check_flag = True
    for i in state_value:
        if first_check_flag:
            if not i.isdigit():
                if i != '-':
                    return False
                else:
                    continue
            first_check_flag = False
        if not i.isdigit():
            if i != '.':
                return False
            else:
                if dot_flag:
                    dot_flag = False
                else:
                    return False
    return True


def __float_list_check(state_value):
    if ';' not in state_value:
        return False
    state_list = state_value.split(';')
    for i in state_list:
        if not __float_check(i):
            return False
    return True


def __get_float_list(state_value):
    state_list = state_value.split(';')
    float_list = []
    for i in state_list:
        if i != '':
            float_list.append(float(i))
    return float_list


def __number_check(state_value):
    for i in state_value:
        if i not in dot_filter_list:
            if i.isalpha():
                return state_value
    state_value = state_value.strip()
    state_value = state_value.replace(',', ';')
    state_value = state_value.replace('[', '').replace(']', '')
    if __float_check(state_value):
        if state_value == '':
            return None
        else:
            return [float(state_value), 1]
    if __float_list_check(state_value):
        return __get_float_list(state_value)
    return state_value


def __state_compare(number_check1, number_check2):
    if type(number_check1) != list or type(number_check2) != list:
        if number_check1 == number_check2:
            return True
        else:
            if number_check1 in close_signal_list and number_check2 in number_check2:
                return True
            else:
                return False
    else:
        for i in range(len(number_check1)):
            if number_check1[i] != 0:
                if abs(number_check1[i] - number_check2[i]) / number_check1[i] > 0.01:
                    return False
                else:
                    return True
            else:
                if number_check2[i] == 0:
                    return True
                else:
                    return False
        return True


def get_state_key_filter_tuple():
    server_model = server_constant.get_server_model('host')
    session_strategy = server_model.get_db_session('strategy')
    query_sql = "select state_check_filter_key from strategy.state_check_filter_key"
    query_result = session_strategy.execute(query_sql)
    state_key_filter_tuple = []
    for query_line in query_result:
        state_key_filter_tuple.append(query_line[0])
    server_model.close()
    return state_key_filter_tuple


def compare_state_result_and_email(server_name_tuple, strategy_info_list, trading_state_server_dict,
                                   backtest_state_server_dict):
    no_night_market_list_lower = []
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    for future_main_contract in session_common.query(FutureMainContract).filter(FutureMainContract.night_flag == 0):
        no_night_market_list_lower.append(future_main_contract.ticker_type.lower())

    email_list = '<font>order_trade_backup_check:<br><br>'
    html_title = 'server name,%s' % ','.join(server_name_tuple)
    table_list = []
    error_list = ''

    state_key_filter_tuple = get_state_key_filter_tuple()
    for item in strategy_info_list:
        strategy_name = item[1]
        tr_list = [strategy_name]
        for server_name in server_name_tuple:
            if server_name not in item[0]:
                tr_list.append('/')
                continue
            if strategy_name not in trading_state_server_dict[server_name]:
                tr_list.append('No Real Trade State!(Error)')
                continue
            strategy_trading_state_server = trading_state_server_dict[server_name][strategy_name]
            if strategy_name not in backtest_state_server_dict[server_name]:
                trading_ticker = strategy_name.split('.')[1]
                no_night_market_flag = False
                if trading_ticker.lower() in no_night_market_list_lower:
                    tr_list.append('No Night Market!')
                    no_night_market_flag = True
                else:
                    for no_night_market_future in no_night_market_list_lower:
                        if no_night_market_future + '_' in trading_ticker.lower():
                            tr_list.append('No Night Market!')
                            no_night_market_flag = True
                if not no_night_market_flag:
                    tr_list.append('State Missing!(Error)')
                continue
            strategy_backtest_state_server = backtest_state_server_dict[server_name][strategy_name]
            error_flag = False
            state_key_missing_flag = False
            for [state_key, trading_state_value] in strategy_trading_state_server.items():
                if state_key in state_key_filter_tuple:
                    continue
                if state_key not in strategy_backtest_state_server:
                    error_flag = True
                    state_key_missing_flag = True
                    tr_list.append('State Key Missing!(Error)')
                    break
                backtest_state_value = strategy_backtest_state_server[state_key]
                trading_state_value_number_check = __number_check(trading_state_value)
                backtest_state_value_number_check = __number_check(backtest_state_value)
                if not __state_compare(trading_state_value_number_check, backtest_state_value_number_check):
                    error_list += 'Server_name: %s<br>' % server_name
                    error_list += 'Strategy_name: %s<br>' % strategy_name
                    error_list += '%s<br>' % state_key
                    error_list += 'trading state: <br>'
                    error_list += '%s<br>' % trading_state_value_number_check
                    error_list += 'backtest state: <br>'
                    error_list += '%s<br>' % backtest_state_value_number_check
                    error_list += '<br>'
                    error_flag = True
                    break

            if not error_flag and not state_key_missing_flag:
                tr_list.append('State Check')
            elif not state_key_missing_flag:
                tr_list.append('State Key Error!(Error)')
        table_list.append(tr_list)

    html_list = email_utils3.list_to_html(html_title, table_list)
    email_list += ''.join(html_list)
    email_list += '<br><br>-----------------------------------------------------------------------<br><br>'
    email_list += error_list
    email_utils3.send_email_group_all('Strategy_State_Check', email_list, 'html')


def strategy_state_check_job(server_name_tuple):
    server_host = server_constant.get_server_model('host')
    strategy_info_list = get_strategy_name_list(server_host)
    strategy_name_list = map(lambda item: item[1], strategy_info_list)
    trading_state_server_dict = dict()
    backtest_state_server_dict = dict()
    for server_name in server_name_tuple:
        server_model = server_constant.get_server_model(server_name)
        trading_state_dict = get_trading_state_dict(server_model, strategy_name_list)
        backtest_state_dict = get_backtest_state_dict(server_model, strategy_name_list)

        trading_state_server_dict[server_name] = trading_state_dict
        backtest_state_server_dict[server_name] = backtest_state_dict
        server_model.close()
    compare_state_result_and_email(server_name_tuple, strategy_info_list, trading_state_server_dict,
                                   backtest_state_server_dict)
    server_host.close()


if __name__ == '__main__':
    # strategy_state_check_job(('nanhua', 'zhongxin', 'luzheng'))
    cta_server_list = server_constant.get_cta_servers()
    strategy_state_check_job(cta_server_list)
