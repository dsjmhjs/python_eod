# -*- coding: utf-8 -*-
from eod_aps.model.server_constans import server_constant
from eod_aps.model.schema_strategy import StrategyOnline

server_host = server_constant.get_server_model('host')
session_strategy = server_host.get_db_session('strategy')
query = session_strategy.query(StrategyOnline)
for strategy_online_db in query.filter(StrategyOnline.strategy_type == 'CTA'):
    if 'All_Weather' not in strategy_online_db.parameter:
        continue

    temp_parameter_list = []
    for base_parameter_str in strategy_online_db.parameter.split(';'):
        if 'All_Weather' in base_parameter_str:
            continue
        if '[Target]' in base_parameter_str:
            continue
        temp_parameter_list.append(base_parameter_str)

    strategy_online_db.parameter = ';'.join(temp_parameter_list)
    session_strategy.merge(strategy_online_db)
session_strategy.commit()