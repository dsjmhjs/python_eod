# -*- coding: utf-8 -*-
import json
from eod_aps.server_python import *

total_cancel_times = '400'


def strategy_parameter_update():
    print 'Enter strategy_parameter_update.'
    server_host = server_constant_local.get_server_model('host')
    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select `NAME` from  strategy.strategy_parameter t where " \
                "name like 'CalendarMA%' group by `NAME`"

    strategy_name_list = []
    for strategy_name_item in session_strategy.execute(query_sql):
        strategy_name_list.append(strategy_name_item[0])

    for strategy_name in strategy_name_list:
        query_sql = "select TIME, NAME, VALUE from strategy.strategy_parameter where NAME = '%s' \
            order by TIME desc limit 1" % strategy_name

        for strategy_parameter_item in session_strategy.execute(query_sql):
            time = strategy_parameter_item[0]
            name = strategy_parameter_item[1]
            strategy_parameter_dict = json.loads(strategy_parameter_item[2])
            for (item_key, item_value) in strategy_parameter_dict.items():
                if 'CancelTotalTimes' in item_key:
                    strategy_parameter_dict[item_key] = total_cancel_times
            update_sql = "update strategy.strategy_parameter set VALUE='%s' where time='%s' and name='%s'"\
                         % (json.dumps(strategy_parameter_dict), time, name)
            session_strategy.execute(update_sql)
    session_strategy.commit()
    server_host.close()
    print 'Exit strategy_parameter_update.'

if __name__ == '__main__':
    strategy_parameter_update()

