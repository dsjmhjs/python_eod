# -*- coding: utf-8 -*-
# 本程序的目的是修改策略参数中的max_long_position和max_short_position，效果是除以二并四舍五入，将修改后的策略参数
# 写到数据库里，并生成修改方案发送邮件
import os
import json
import copy
import pandas as pd
from eod_aps.model.schema_strategy import StrategyOnline
from eod_aps.model.schema_portfolio import PfAccount
from eod_aps.job import *

date_utils = DateUtils()
strategy_filter_list = ['PairTrading.j_jm', 'PairTrading.j_jm_para2', 'PairTrading.j_jm_para3', 'PairTrading.j_jm_para4'
                        , 'PairTrading.m_rm_para1', 'PairTrading.m_rm_para2', 'PairTrading.m_rm_para3', 'PairTrading.m_rm_para4', 'CalendarMA.SU']

strategy_parameter_insert_sql_path = './strategy_parameter_insert_sql.txt'
trade_history_insert_sql_path = './trade_history_insert_sql.txt'
pf_position_update_sql_path = './pf_position_update_sql.txt'


def __get_strategy_online_list(server_model):
    session_strategy = server_model.get_db_session('strategy')

    strategy_online_list = []
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.enable == 1):
        if strategy_online_db.name in strategy_filter_list:
            continue

        strategy_name_key = strategy_online_db.name
        strategy_online_list.append(strategy_name_key)
    return strategy_online_list

def write_half_position(server_model, strategy_online_list):
    strategy_parameter_dict = dict()

    session = server_model.get_db_session('strategy')
    query_sql = 'select TIME,NAME,VALUE from (select distinct TIME,NAME,VALUE from strategy.strategy_parameter order by time desc) t group by name'
    strategy_paramter_query = session.execute(query_sql)
    fr = open(strategy_parameter_insert_sql_path, 'w+')
    for strategy_paramter in strategy_paramter_query:
        new_strategy_paramteter_str = copy.deepcopy(strategy_paramter[2])
        if strategy_paramter[1] not in strategy_online_list:
            continue
        strategy_paramtere_dict = json.loads(strategy_paramter[2].replace('\n', ''))
        for [parameter_item, parameter_value] in strategy_paramtere_dict.items():
            position_parameter_str = '"%s": "%s"' % (parameter_item, parameter_value)
            if 'max_long_position' in parameter_item:
                half_max_long_position = str(int(round(float(parameter_value)/ 2)))
                new_position_parameter_str = '"%s": "%s"' % (parameter_item, half_max_long_position)
                new_strategy_paramteter_str = new_strategy_paramteter_str.replace(position_parameter_str, new_position_parameter_str)
            if 'max_short_position' in parameter_item:
                half_max_short_position = str(int(round(float(parameter_value) / 2)))
                new_position_parameter_str = '"%s": "%s"' % (parameter_item, half_max_short_position)
                new_strategy_paramteter_str = new_strategy_paramteter_str.replace(position_parameter_str,
                                                                              new_position_parameter_str)
        insert_sql = '''Insert Into strategy.strategy_parameter(TIME,NAME,VALUE) VALUES(sysdate(),'%s','%s')'''
        insert_sql %= strategy_paramter[1], new_strategy_paramteter_str
        fr.write(insert_sql + '\n')
        strategy_parameter_dict[strategy_paramter[1]] = new_strategy_paramteter_str

    fr.close()
    return strategy_parameter_dict


def __get_pf_account_list(server_model, strategy_online_list):
    session_strategy = server_model.get_db_session('strategy')
    query_sql = "select name,time,value from (select distinct name,time,value from strategy.strategy_parameter order by time desc) t group by name"
    query_result = session_strategy.execute(query_sql)

    strategy_account_dict = dict()
    for strategy_parameter_info in query_result:
        strategy_name = strategy_parameter_info[0]
        if strategy_name not in strategy_online_list:
            continue

        strategy_value = strategy_parameter_info[2]
        parameter_dict = json.loads(strategy_value.replace('\n', ''))
        if 'Account' not in parameter_dict:
            continue

        account_used_str = parameter_dict['Account']
        strategy_account_dict[strategy_name] = account_used_str

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)

    pf_account_list = []
    for pf_account_db in query_pf_account:
        strategy_name_key = ('%s.%s' % (pf_account_db.group_name, pf_account_db.name))
        if strategy_name_key not in strategy_online_list:
            continue

        if strategy_name_key not in strategy_account_dict:
            continue

        account_used_str = strategy_account_dict[strategy_name_key]
        account_filter_str = pf_account_db.fund_name.split('-')[2]
        if account_filter_str not in account_used_str:
            continue

        pf_account_list.append(pf_account_db)
    return pf_account_list

def __get_pf_position_dict(server_model, pf_account_list):
    pf_position_dict = dict()

    strategy_id_list = []
    for strategy_online in pf_account_list:
        strategy_id_list.append(strategy_online.id)

    session_strategy = server_model.get_db_session('portfolio')

    max_date_str = None
    query_max_date_sql = "select MAX(date) from portfolio.pf_position;"
    max_date_query = session_strategy.execute(query_max_date_sql)
    for max_date_temp in max_date_query:
        max_date_str = str(max_date_temp[0])
        break

    query_sql = "select * from (select distinct * from portfolio.pf_position order by date desc) t \
group by id ORDER BY id asc"
    pf_position_query = session_strategy.execute(query_sql)
    for pf_position in pf_position_query:
        date_str = str(pf_position[0])
        account_id = pf_position[1]
        if account_id not in strategy_id_list:
            continue
        if date_str == max_date_str:
            pf_position_dict[account_id] = [pf_position[2], float(pf_position[6] - pf_position[11])]
        else:
            pf_position_dict[account_id] = [pf_position[2], 0]
    return pf_position_dict, max_date_str


def __get_max_long_short_dict(pf_account_list, strategy_parameter_dict):
    max_long_short_dict = dict()

    for pf_account in pf_account_list:
        strategy_name = (pf_account.group_name + '.' + pf_account.name)
        strategy_account = pf_account.fund_name.split('-')[2]
        if not strategy_parameter_dict.has_key(strategy_name):
            print 'error! %s' % strategy_name
            continue
        strategy_parameter = strategy_parameter_dict[strategy_name]
        parameter_dict = json.loads(strategy_parameter)
        max_long = parameter_dict['tq.%s.max_long_position' % strategy_account]
        max_short = parameter_dict['tq.%s.max_short_position' % strategy_account]
        max_long_short_dict[pf_account.id] = [strategy_name, strategy_account, max_long, max_short]
    return max_long_short_dict


def get_future_price(ticker_name):
    future_name = ''
    for i in ticker_name:
        if i.isalpha():
            future_name += i
        if i == ' ':
            break

    close_price = 0
    for file_name in os.listdir('Z:/data/future/backtest/all_type/bar/45s/'):
        file_future_name = ''
        for i in file_name:
            if i.isalpha():
                file_future_name += i
            if i == ' ':
                break
        if future_name.upper() == file_future_name:
            if file_name.split(future_name.upper())[0] == '':
                fr = open('Z:/data/future/backtest/all_type/bar/45s/' + file_name)
                for line in fr.readlines()[-10:]:
                    if line.strip() == '':
                        continue
                    close_price = line.split(',')[4]
    return float(close_price)


def get_update_pf_position_sql(server_model_server, hedge_trade_info, query_max_date, fr):
    trade_symbol = hedge_trade_info[0]
    trade_qty = hedge_trade_info[1]
    strategy_name = hedge_trade_info[2]
    account_name = hedge_trade_info[3]
    session_portfolio = server_model_server.get_db_session('portfolio')

    query_sql_base = "select id from portfolio.pf_account where group_name = '%s' and name = '%s' and fund_name like '%s'"
    query_sql = query_sql_base % (strategy_name.split('.')[0], strategy_name.split('.')[1], '%' + account_name + '%')
    query_result = session_portfolio.execute(query_sql)
    for query_line in query_result:
        strategy_id = query_line[0]

    query_sql_2_base = "select date,id,symbol,`long`,long_cost,long_avail,short,short_cost,short_avail,yd_position_long" \
                       ",yd_position_short,yd_long_remain,yd_short_remain,prev_net from portfolio.pf_position where " \
                       "date = '%s' and id = %s and symbol = '%s'"
    query_sql_2 = query_sql_2_base % (query_max_date, strategy_id, trade_symbol)
    query_result_2 = session_portfolio.execute(query_sql_2)
    for query_line in query_result_2:
        id_position = float(query_line[3] - query_line[6])
        id_cost = abs(float(query_line[4] - query_line[7]))

    target_position = id_position + trade_qty
    target_cost = id_cost / abs(id_position) * abs(target_position)

    if target_position >= 0:
        update_sql_base = "update portfolio.pf_position set `long` = %s, long_cost = %s, long_avail = %s, short = 0, " \
                          "short_cost = 0, short_avail = 0, yd_position_long = %s, yd_position_short = 0, " \
                          "yd_long_remain = %s, yd_short_remain = 0, prev_net = %s where date = '%s' and id = %s and " \
                          "symbol = '%s';"
        update_sql = update_sql_base % (target_position, target_cost, target_position, target_position, target_position,
                                        target_position, query_max_date, strategy_id, trade_symbol)
    else:
        update_sql_base = "update portfolio.pf_position set `long` = 0, long_cost = 0, long_avail = 0, short = %s, " \
                          "short_cost = %s, short_avail = %s, yd_position_long = 0, yd_position_short = %s, " \
                          "yd_long_remain = 0, yd_short_remain = %s, prev_net = %s where date = '%s' and id = %s and " \
                          "symbol = '%s';"
        update_sql = update_sql_base % (-target_position, target_cost, -target_position, -target_position, -target_position,
                                        target_position, query_max_date, strategy_id, trade_symbol)
    # print update_sql
    fr.write(update_sql + '\n')


def calculate_position_change_project(pf_position_dict, max_long_short_dict, server_model_server, query_max_date):
    id_position_change_dict = dict()
    account_future_position_change_dict = dict()

    fr1 = open(trade_history_insert_sql_path, 'w+')
    fr2 = open(pf_position_update_sql_path, 'w+')

    for [account_id, pf_position_info] in pf_position_dict.items():
        max_long_short_info = max_long_short_dict[account_id]
        symbol = pf_position_info[0]
        pf_position = pf_position_info[1]
        strategy = max_long_short_info[0]
        account = max_long_short_info[1]
        max_long = float(max_long_short_info[2])
        max_short = -1 * float(max_long_short_info[3])
        id_position_change = 0
        if pf_position > max_long:
            id_position_change = max_long - pf_position
            id_position_change_dict[account_id] = [symbol, id_position_change, strategy, account]
        elif pf_position < max_short:
            id_position_change = max_short - pf_position
            id_position_change_dict[account_id] = [symbol, id_position_change, strategy, account]
        else:
            id_position_change_dict[account_id] = [symbol, id_position_change, strategy, account]

        if not account_future_position_change_dict.has_key(account):
            account_future_position_change_dict[account] = dict()
            account_future_position_change_dict[account][symbol] = id_position_change
        else:
            if not account_future_position_change_dict[account].has_key(symbol):
                account_future_position_change_dict[account][symbol] = id_position_change
            else:
                account_future_position_change_dict[account][symbol] += id_position_change

    trade_time = date_utils.get_today_str('%Y-%m-%d') + ' 15:30:00'
    for [account, account_position_change_dict] in account_future_position_change_dict.items():
        for [future, future_position_change] in account_position_change_dict.items():
            id_position_chang_list = []
            for [account_id, id_position_change] in id_position_change_dict.items():
                if id_position_change[0] == future and id_position_change[3] == account and id_position_change[1] != 0:
                    id_position_chang_list.append(id_position_change)
            if len(id_position_chang_list) == 0:
                continue
            print account, future, future_position_change
            id_position_chang_list_pd = pd.DataFrame(id_position_chang_list)
            if future_position_change > 0:
                id_position_chang_list_pd_sorted = id_position_chang_list_pd.sort_values(by=1)
                hedge_position_sum = 0
                hedge_info_list = []
                position_change_info_list = []
                for position_chang_info in id_position_chang_list_pd_sorted.iterrows():
                    if position_chang_info[1].values[1] < 0:
                        hedge_position_sum += position_chang_info[1].values[1]
                        hedge_info_list.append(position_chang_info[1].values)
                    else:
                        if hedge_position_sum < 0:
                            if hedge_position_sum + position_chang_info[1].values[1] < 0:
                                hedge_position_sum += position_chang_info[1].values[1]
                                hedge_info_list.append(position_chang_info[1].values)
                            elif hedge_position_sum + position_chang_info[1].values[1] == 0:
                                hedge_position_sum += position_chang_info[1].values[1]
                                hedge_info_list.append(position_chang_info[1].values)
                            else:
                                position_change_info_temp = copy.deepcopy(position_chang_info[1].values)
                                position_change_info_temp[1] = position_chang_info[1].values[1] + hedge_position_sum
                                position_change_info_list.append(position_change_info_temp)

                                hedge_info_temp = copy.deepcopy(position_chang_info[1].values)
                                hedge_info_temp[1] = -hedge_position_sum
                                hedge_info_list.append(hedge_info_temp)

                                hedge_position_sum = 0
                        elif hedge_position_sum == 0:
                            position_change_info_temp = position_chang_info[1].values
                            position_change_info_list.append(position_change_info_temp)

                if len(hedge_info_list) != 0:
                    print "hedge!"
                for hedge_trade_info in hedge_info_list:
                    print hedge_trade_info
                    trade_price = get_future_price(hedge_trade_info[0].split(' ')[0])
                    insert_sql = '''Insert Into om.trade2_history (ID,TIME,SYMBOL,QTY,PRICE,TRADE_TYPE,STRATEGY_ID,HEDGEFLAG) VALUES(default,'%s','%s',%s,%s,2,'%s',0);'''\
                                 % (trade_time, hedge_trade_info[0], hedge_trade_info[1], trade_price,
                                    hedge_trade_info[2])
                    fr1.write(insert_sql + '\n')

                    get_update_pf_position_sql(server_model_server, hedge_trade_info, query_max_date, fr2)

                if len(position_change_info_list) != 0:
                    print "change position!"
                for position_change_info in position_change_info_list:
                    print position_change_info
                print ''
            elif future_position_change < 0:
                id_position_chang_list_pd_sorted = id_position_chang_list_pd.sort_values(by=1, ascending=False)
                hedge_position_sum = 0
                hedge_info_list = []
                position_change_info_list = []
                for position_chang_info in id_position_chang_list_pd_sorted.iterrows():
                    if position_chang_info[1].values[1] > 0:
                        hedge_position_sum += position_chang_info[1].values[1]
                        hedge_info_list.append(position_chang_info[1].values)
                    else:
                        if hedge_position_sum > 0:
                            if hedge_position_sum + position_chang_info[1].values[1] > 0:
                                hedge_position_sum += position_chang_info[1].values[1]
                                hedge_info_list.append(position_chang_info[1].values)
                            elif hedge_position_sum + position_chang_info[1].values[1] == 0:
                                hedge_position_sum += position_chang_info[1].values[1]
                                hedge_info_list.append(position_chang_info[1].values)
                            else:
                                position_change_info_temp = copy.deepcopy(position_chang_info[1].values)
                                position_change_info_temp[1] = position_chang_info[1].values[1] + hedge_position_sum
                                position_change_info_list.append(position_change_info_temp)

                                hedge_info_temp = copy.deepcopy(position_chang_info[1].values)
                                hedge_info_temp[1] = -hedge_position_sum
                                hedge_info_list.append(hedge_info_temp)

                                hedge_position_sum = 0
                        elif hedge_position_sum == 0:
                            position_change_info_temp = position_chang_info[1].values
                            position_change_info_list.append(position_change_info_temp)
                if len(hedge_info_list) != 0:
                    print "hedge!"
                for hedge_trade_info in hedge_info_list:
                    print hedge_trade_info
                    trade_price = get_future_price(hedge_trade_info[0].split(' ')[0])
                    insert_sql = '''Insert Into om.trade2_history (ID,TIME,SYMBOL,QTY,PRICE,TRADE_TYPE,STRATEGY_ID,HEDGEFLAG) VALUES(default,'%s','%s',%s,%s,2,'%s',0);'''\
                                 % (trade_time, hedge_trade_info[0], hedge_trade_info[1], trade_price,
                                    hedge_trade_info[2])
                    fr1.write(insert_sql + '\n')

                    get_update_pf_position_sql(server_model_server, hedge_trade_info, query_max_date, fr2)

                if len(position_change_info_list) != 0:
                    print "change position!"
                for position_change_info in position_change_info_list:
                    print position_change_info
                print ''
            else:
                id_position_chang_list_pd_sorted = id_position_chang_list_pd.sort_values(by=1)
                hedge_position_sum = 0
                hedge_info_list = []
                position_change_info_list = []
                for position_chang_info in id_position_chang_list_pd_sorted.iterrows():
                    if position_chang_info[1].values[1] < 0:
                        hedge_position_sum += position_chang_info[1].values[1]
                        hedge_info_list.append(position_chang_info[1].values)
                    else:
                        if hedge_position_sum < 0:
                            if hedge_position_sum + position_chang_info[1].values[1] < 0:
                                hedge_position_sum += position_chang_info[1].values[1]
                                hedge_info_list.append(position_chang_info[1].values)
                            elif hedge_position_sum + position_chang_info[1].values[1] == 0:
                                hedge_position_sum += position_chang_info[1].values[1]
                                hedge_info_list.append(position_chang_info[1].values)
                            else:
                                position_change_info_temp = copy.deepcopy(position_chang_info[1].values)
                                position_change_info_temp[1] = position_chang_info[1].values[1] + hedge_position_sum
                                position_change_info_list.append(position_change_info_temp)

                                hedge_info_temp = copy.deepcopy(position_chang_info[1].values)
                                hedge_info_temp[1] = -hedge_position_sum
                                hedge_info_list.append(hedge_info_temp)

                                hedge_position_sum = 0
                        elif hedge_position_sum == 0:
                            position_change_info_temp = position_chang_info[1].values
                            position_change_info_list.append(position_change_info_temp)
                if len(hedge_info_list) != 0:
                    print "hedge!"
                for hedge_trade_info in hedge_info_list:
                    print hedge_trade_info
                    trade_price = get_future_price(hedge_trade_info[0].split(' ')[0])
                    insert_sql = '''Insert Into om.trade2_history (ID,TIME,SYMBOL,QTY,PRICE,TRADE_TYPE,STRATEGY_ID,HEDGEFLAG) VALUES(default,'%s','%s',%s,%s,2,'%s',0);'''\
                                 % (trade_time, hedge_trade_info[0], hedge_trade_info[1], trade_price,
                                    hedge_trade_info[2])
                    fr1.write(insert_sql + '\n')

                    get_update_pf_position_sql(server_model_server, hedge_trade_info, query_max_date, fr2)

                if len(position_change_info_list) != 0:
                    print "change position!"
                for position_change_info in position_change_info_list:
                    print position_change_info
                print ''
    fr1.close()
    fr2.close()

def position_half_job():
    server_model_host = server_constant.get_server_model('host')
    server_model_server = server_constant.get_server_model('local125')

    # 将改动后的参数写入数据库
    strategy_online_list = __get_strategy_online_list(server_model_host)
    strategy_parameter_dict = write_half_position(server_model_server, strategy_online_list)

    # 计算需要改动的仓位
    pf_account_list = __get_pf_account_list(server_model_server, strategy_online_list)
    [pf_position_dict, query_max_date] = __get_pf_position_dict(server_model_server, pf_account_list)
    max_long_short_dict = __get_max_long_short_dict(pf_account_list, strategy_parameter_dict)

    calculate_position_change_project(pf_position_dict, max_long_short_dict, server_model_server, query_max_date)


if __name__ == "__main__":
    position_half_job()