# -*- coding: utf-8 -*-
import json
from eod_aps.model.server_constans import ServerConstant


# config
multiple_number = 1.5
server_name = 'nanhua'
account_name = 'All_Weather_1'

# sql file save path
position_change_insert_sql_path = "./position_change_insert_sql.txt"


def get_strategy_name_list():
    strategy_name_list = []
    server_model_host = ServerConstant().get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME` from strategy.strategy_online where strategy_type = 'CTA' and `ENABLE` = 1;"
    query_result = session_strategy.execute(query_sql)
    for query_line in query_result:
        strategy_name_list.append(query_line[0])
    return strategy_name_list


def position_change_job():
    strategy_name_list = get_strategy_name_list()

    server_model_server = ServerConstant().get_server_model(server_name)
    session_strategy = server_model_server.get_db_session('strategy')

    fr = open(position_change_insert_sql_path, 'w+')

    for strategy_name in strategy_name_list:
        print strategy_name
        parameter_value_str = ''
        query_sql = "select Value from strategy.strategy_parameter where `NAME` = '%s' order by time desc limit 1;" \
                    % strategy_name
        query_result = session_strategy.execute(query_sql)
        for query_line in query_result:
            parameter_value_str = query_line[0]
            break
        parameter_value_dict = json.loads(parameter_value_str)
        if 'tq.%s.max_long_position' % account_name in parameter_value_dict:
            max_long_value = float(parameter_value_dict['tq.%s.max_long_position' % account_name])
            max_short_value = float(parameter_value_dict['tq.%s.max_short_position' % account_name])
            qty_per_trade_value = float(parameter_value_dict['tq.%s.qty_per_trade' % account_name])
        else:
            continue

        new_max_long_value = int(max_long_value * multiple_number)
        new_max_short_value = int(max_short_value * multiple_number)
        new_qty_per_trade_value = int(qty_per_trade_value * multiple_number)

        if new_qty_per_trade_value < 2 * max(new_max_long_value, new_max_short_value):
            new_qty_per_trade_value = 2 * max(new_max_long_value, new_max_short_value)

        parameter_value_dict['tq.%s.max_long_position' % account_name] = new_max_long_value
        parameter_value_dict['tq.%s.max_short_position' % account_name] = new_max_short_value
        parameter_value_dict['tq.%s.qty_per_trade' % account_name] = new_qty_per_trade_value

        new_parameter_str = json.dumps(parameter_value_dict)
        insert_sql = '''Insert Into strategy.strategy_parameter(TIME,NAME,VALUE) VALUES(sysdate(),'%s','%s');''' \
                     % (strategy_name, new_parameter_str)
        fr.write(insert_sql + '\n')
    #     session_strategy.execute(insert_sql)
    # session_strategy.commit()
    fr.close()
    server_model_server.close()


if __name__ == '__main__':
    position_change_job()
