# -*- coding: utf-8 -*-
#
import json
import re
from eod_aps.job import *

message = '[2017-01-10 08:43:08.987025] [trivial] [info] IStrategy::ChangeParameter [strategy_name=ChannelBreak.p|\
paras_value={"Account":"All_Weather_1;All_Weather_2;All_Weather_3","BarDurationMin":"1","Length":"96","Target":"p1705"\
,"Threshold":"38","max_slippage":"3","scale":"10.000000","tq.All_Weather_1.max_long_position":"0","tq.All_Weather_1.\
max_short_position":"0","tq.All_Weather_1.qty_per_trade":"2","tq.All_Weather_2.max_long_position":"0",\
"tq.All_Weather_2.max_short_position":"0","tq.All_Weather_2.qty_per_trade":"2",\
"tq.All_Weather_3.max_long_position":"1","tq.All_Weather_3.max_short_position":"1",\
"tq.All_Weather_3.qty_per_trade":"2"}]'


def __analysis_placeorder(line_str):
    reg = re.compile(
        '^.*\[(?P<date>.*)\] \[trivial\] \[info\] IStrategy::ChangeParameter \[strategy_name=(?P<strategy_name>[^|]*)\|paras_value=(?P<paras_value>.*)\]')
    regMatch = reg.match(line_str)
    line_dict = regMatch.groupdict()
    return line_dict


def __get_server_strategy_value(server_name, strategy_name):
    date_str = date_utils.get_today_str('%Y%m%d')
    server_model = server_constant.get_server_model(server_name)
    grep_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                     'grep strategy_name=%s screenlog_StrategyLoader_%s*.log' % (strategy_name, date_str)
    ]
    cmd_messages = server_model.run_cmd_str(';'.join(grep_cmd_list))
    cmd_message = cmd_messages.split('\n')[-1]
    return __analysis_placeorder(cmd_message)


def __compare_json_str(strategy_value, log_strategy_value):
    compare_flag = True
    strategy_value_dict = json.loads(strategy_value)
    log_strategy_value_dict = json.loads(log_strategy_value)
    for (key, strategy_value) in strategy_value_dict.items():
        if key not in log_strategy_value_dict:
            compare_flag = False
            break
        log_strategy_value = log_strategy_value_dict[key]
        if strategy_value != log_strategy_value:
            compare_flag = False
            break
    return compare_flag


def query_strategy_state():
    server_host = server_constant.get_server_model('host')
    session_aggregation = server_host.get_db_session('aggregation')
    query_sql = "select server_name,time,name,value from aggregation.strategy_state where time like '%s'" \
                % (date_utils.get_today_str('%Y-%m-%d') + '%')
    id_query = session_aggregation.execute(query_sql)
    for strategy_state_info in id_query:
        server_name = strategy_state_info[0]
        insert_time = strategy_state_info[1]
        strategy_name = strategy_state_info[2]
        strategy_value = strategy_state_info[3]

        message_dict = __get_server_strategy_value(server_name, strategy_name)
        log_time = message_dict['date']
        log_strategy_name = message_dict['strategy_name']
        log_strategy_value = message_dict['paras_value']

        compare_flag = __compare_json_str(strategy_value, log_strategy_value)
        if not compare_flag:
            task_logger.error('%s,%s' % (server_name, strategy_name))
    server_host.close()


if __name__ == '__main__':
    query_strategy_state()
