# -*- coding: utf-8 -*-
# 主力合约换月
import os

from eod_aps.model.schema_portfolio import RealAccount
from eod_aps.model.schema_common import FutureMainContract
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.schema_strategy import StrategyParameter, StrategyOnline
from eod_aps.tools.aggregator_message_utils import AggregatorMessageUtils, common_utils
from eod_aps.tools.instrument_tools import query_instrument_list
from eod_aps.tools.phone_trade_tools import send_phone_trade, save_phone_trade_file
from eod_aps.job import *
from sqlalchemy import desc
from itertools import islice
import json

exchange_dict = custom_enum_utils.enum_to_dict(Exchange_Type_Enums, inversion_flag=True)


def __rebuild_calendar_parameter(server_name, future_maincontract_db):
    calendar_strategy_name = 'CalendarMA.SU'

    server_model = server_constant.get_server_model(server_name)
    session_strategy = server_model.get_db_session('strategy')
    query_strategy_parameter = session_strategy.query(StrategyParameter)
    strategy_parameter_db = query_strategy_parameter.filter(StrategyParameter.name == calendar_strategy_name) \
        .order_by(desc(StrategyParameter.time)).first()

    if strategy_parameter_db is None:
        return

    calendar_parameter_dict = json.loads(strategy_parameter_db.value)
    front_future_key = '%s.FrontFuture' % future_maincontract_db.ticker_type
    if front_future_key in calendar_parameter_dict:
        calendar_parameter_dict[front_future_key] = future_maincontract_db.main_symbol

    back_future_key = '%s.BackFuture' % future_maincontract_db.ticker_type
    if back_future_key in calendar_parameter_dict:
        next_symbol = future_maincontract_db.next_main_symbol
        if next_symbol in instrument_db_dict:
            calendar_parameter_dict[back_future_key] = future_maincontract_db.next_main_symbol
        else:
            email_content_list.append('%s:%s not in common.instrument' % (back_future_key, next_symbol))
            calendar_parameter_dict[back_future_key] = ''
    calendar_parameter_dict['%s.enable' % future_maincontract_db.ticker_type] = 0

    strategy_parameter = StrategyParameter()
    strategy_parameter.time = date_utils.get_now()
    strategy_parameter.name = calendar_strategy_name
    strategy_parameter.value = json.dumps(calendar_parameter_dict)
    session_strategy.add(strategy_parameter)
    session_strategy.commit()


def __rebuild_strategy_parameter(server_name, strategy_name_list, future_maincontract_db):
    pre_main_symbol = future_maincontract_db.pre_main_symbol
    main_symbol = future_maincontract_db.main_symbol

    server_model = server_constant.get_server_model(server_name)
    session_strategy = server_model.get_db_session('strategy')

    strategy_parameter_db_list = []
    query_strategy_parameter = session_strategy.query(StrategyParameter)
    for strategy_name in strategy_name_list:
        strategy_parameter_db = query_strategy_parameter.filter(StrategyParameter.name == strategy_name) \
            .order_by(desc(StrategyParameter.time)).first()
        if strategy_parameter_db is None:
            # custom_log.log_error_job('UnFind %s' % strategy_name)
            continue
        strategy_parameter_dict = json.loads(strategy_parameter_db.value)
        if 'Target' in strategy_parameter_dict:
            target_ticker_str = strategy_parameter_dict['Target']
            new_target_ticker_list = []
            change_flag = False
            for target_ticker in target_ticker_str.split(';'):
                if target_ticker == pre_main_symbol:
                    change_flag = True
                    new_target_ticker_list.append(main_symbol)
                else:
                    new_target_ticker_list.append(target_ticker)

            if change_flag:
                custom_log.log_info_job('MainContract Change:%s|%s ---> %s' % (strategy_name, target_ticker_str,
                                                                               ';'.join(new_target_ticker_list)))
            else:
                continue
            strategy_parameter_dict['Target'] = ';'.join(new_target_ticker_list)
            strategy_parameter_db.value = json.dumps(strategy_parameter_dict)
            strategy_parameter_db.time = date_utils.get_now()
            strategy_parameter_db_list.append(strategy_parameter_db)

    for strategy_parameter_db in strategy_parameter_db_list:
        session_strategy.add(strategy_parameter_db)
    session_strategy.commit()
    server_model.close()


def __main_contract_position_change(server_name, future_main_contract_db, pf_position_list):
    pre_main_symbol = future_main_contract_db.pre_main_symbol
    main_symbol = future_main_contract_db.main_symbol

    if future_main_contract_db.ticker_type in const.Index_Future_List:
        target_strategy_filter = 'SU-CalendarSpread'
    else:
        target_strategy_filter = 'Transfer-CalendarMA'

    pf_account_dict = dict()
    target_strategy_account_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        if target_strategy_filter in pf_account_db.fund_name:
            fund_name_items = pf_account_db.fund_name.split('-')
            if len(fund_name_items) >= 3:
                target_strategy_account_dict[fund_name_items[2]] = pf_account_db
            continue
        elif 'PutCallParity' in pf_account_db.fund_name:
            continue
        elif 'MarketMaking1' in pf_account_db.fund_name:
            continue
        pf_account_dict[pf_account_db.fund_name] = pf_account_db

    phone_trade_list = []
    for pf_position_item in pf_position_list:
        base_strategy_name, qty = pf_position_item
        if base_strategy_name not in pf_account_dict:
            continue

        pf_account_db = pf_account_dict[base_strategy_name]
        fund_name_items = pf_account_db.fund_name.split('-')
        if len(fund_name_items) < 3:
            custom_log.log_error_job('Error Fund_Name:%s' % pf_account_db.fund_name)
            continue
        fund = pf_account_db.fund_name.split('-')[2]
        transfer_account_db = target_strategy_account_dict[fund]

        close_phone_trade = PhoneTradeInfo()
        close_phone_trade.fund = fund
        close_phone_trade.strategy1 = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        close_phone_trade.symbol = pre_main_symbol
        if qty == 0:
            continue
        elif qty > 0:
            close_phone_trade.direction = Direction_Enums.Sell
            close_phone_trade.exqty = qty
        else:
            close_phone_trade.direction = Direction_Enums.Buy
            close_phone_trade.exqty = abs(qty)
        close_phone_trade.tradetype = Trade_Type_Enums.Close

        close_phone_trade.hedgeflag = Hedge_Flag_Type_Enums.Speculation
        close_phone_trade.iotype = IO_Type_Enums.Inner2
        close_phone_trade.server_name = server_name

        close_phone_trade.exprice = instrument_db_dict[pre_main_symbol].close
        close_phone_trade.strategy2 = '%s.%s' % (transfer_account_db.group_name, transfer_account_db.name)
        phone_trade_list.append(close_phone_trade)

        open_phone_trade = PhoneTradeInfo()
        open_phone_trade.fund = fund
        open_phone_trade.strategy1 = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        open_phone_trade.symbol = main_symbol
        if qty > 0:
            open_phone_trade.direction = Direction_Enums.Buy
            open_phone_trade.exqty = qty
        else:
            open_phone_trade.direction = Direction_Enums.Sell
            open_phone_trade.exqty = abs(qty)
        open_phone_trade.tradetype = Trade_Type_Enums.Open

        open_phone_trade.hedgeflag = Hedge_Flag_Type_Enums.Speculation
        open_phone_trade.iotype = IO_Type_Enums.Inner2
        open_phone_trade.server_name = server_name
        open_phone_trade.exprice = instrument_db_dict[main_symbol].close
        open_phone_trade.strategy2 = '%s.%s' % (transfer_account_db.group_name, transfer_account_db.name)
        phone_trade_list.append(open_phone_trade)

    if len(phone_trade_list) > 0:
        send_phone_trade(server_name, phone_trade_list)
        # server_save_path = os.path.join(PHONE_TRADE_FOLDER, server_name)
        # if not os.path.exists(server_save_path):
        #     os.mkdir(server_save_path)
        # phone_trade_file_path = '%s/main_contract_change_%s.csv' % (server_save_path, date_utils.get_today_str('%Y%m%d'))
        # save_phone_trade_file(phone_trade_file_path, phone_trade_list)


def __query_future_main_contract(server_model):
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    change_dict = dict()
    cfg_file_path = '%s/future_main_contract_change_info.csv' % MAIN_CONTRACT_CHANGE_FILE_FOLDER
    with open(cfg_file_path, 'rb') as fr:
        for line in islice(fr, 1, None):
            line_item = line.split(',')
            if len(line_item) != 6 or line_item[0] != filter_date_str:
                continue
            change_dict[line_item[1]] = [line_item[2], line_item[3], line_item[4]]

    session_common = server_model.get_db_session('common')
    query = session_common.query(FutureMainContract)

    maincontract_change_list = []
    change_dict_db = {}

    for future_maincontract_db in query.filter(FutureMainContract.update_flag == 1):
        maincontract_change_list.append(future_maincontract_db)
        change_dict_db[future_maincontract_db.ticker_type] = [future_maincontract_db.pre_main_symbol,
                                                              future_maincontract_db.main_symbol,
                                                              future_maincontract_db.next_main_symbol]

    if change_dict == change_dict_db:
        return maincontract_change_list
    else:
        email_utils2.send_email_group_all('[Error]换月参数错误', '数据库和文件里换月参数不一致')
    return []


def __export_cfg_file(maincontract_change_list):
    change_info_list = []
    for db_info in maincontract_change_list:
        change_info_list.append('%s,%s,%s,%s,%s' % (db_info.ticker_type, db_info.pre_main_symbol, db_info.main_symbol,
                                                    db_info.next_main_symbol, db_info.exchange_id))

    change_info_file_path = '%s/future_main_contract_change_info_%s.csv' % \
                            (MAIN_CONTRACT_CHANGE_FILE_FOLDER, date_utils.get_today_str('%Y-%m-%d'))
    with open(change_info_file_path, 'w+') as fr:
        fr.write('\n'.join(change_info_list))


def __change_maincontract(instance_str, future_maincontract_db):
    pre_main_symbol = future_maincontract_db.pre_main_symbol
    main_symbol = future_maincontract_db.main_symbol

    return_instance_str = instance_str
    if instance_str == pre_main_symbol:
        return_instance_str = main_symbol
    elif ';' in instance_str:
        temp_instance_list = []
        for instance_item in instance_str.split(';'):
            if instance_item == pre_main_symbol:
                instance_item = main_symbol
            temp_instance_list.append(instance_item)
        return_instance_str = ';'.join(temp_instance_list)
    return return_instance_str


def __update_strategy_online(future_main_contract_db):
    pre_main_symbol = future_main_contract_db.pre_main_symbol

    pre_change_strategy_list = []
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.strategy_type == 'CTA'):
        change_flag = False
        if pre_main_symbol in strategy_online_db.instance_name:
            new_instance_name = __change_maincontract(strategy_online_db.instance_name, future_main_contract_db)
            strategy_online_db.instance_name = new_instance_name
            change_flag = True

        if pre_main_symbol in strategy_online_db.parameter_server:
            parameter_server_list = strategy_online_db.parameter_server.split('|')
            new_parameter_server_list = []
            for parameter_server_item in parameter_server_list:
                parameter_server_dict = json.loads(parameter_server_item)
                target_ticker = parameter_server_dict['Target']
                new_target_ticker = __change_maincontract(target_ticker, future_main_contract_db)
                parameter_server_dict['Target'] = new_target_ticker
                new_parameter_server_list.append(json.dumps(parameter_server_dict))
            strategy_online_db.parameter_server = '|'.join(new_parameter_server_list)
            change_flag = True

        if change_flag:
            session_strategy.merge(strategy_online_db)
        if strategy_online_db.enable == 1:
            pre_change_strategy_list.append(strategy_online_db.name)
    session_strategy.commit()
    return pre_change_strategy_list


def __build_instrument_db_dict():
    global instrument_db_dict
    instrument_db_dict = dict()
    type_list = [Instrument_Type_Enums.Future, ]
    instrument_list = query_instrument_list('host', type_list)
    for instrument_db in instrument_list:
        instrument_db_dict[instrument_db.ticker] = instrument_db


def __update_track_undl_tickers(future_maincontract_db):
    update_sql = "update common.instrument set track_undl_tickers='%s' where type_id = 10 and \
del_flag = 0 and ticker like '%s'" % (future_maincontract_db.main_symbol, future_maincontract_db.ticker_type + '%')
    for server_name in server_constant.get_all_local_servers():
        server_model = server_constant.get_server_model(server_name)
        session_common = server_model.get_db_session('common')
        session_common.execute(update_sql)
        session_common.commit()


def __rebuild_strategy_loader_file(server_name):
    strategy_online_list = []
    session_strategy = server_host.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)
    for strategy_online_db in query.filter(StrategyOnline.enable == 1, StrategyOnline.strategy_type == 'CTA',
                                           StrategyOnline.target_server.like('%' + server_name + '%')):
        strategy_online_list.append(strategy_online_db)

    line_list = []
    with open('%s/config.strategyloader_%s.txt' % (STRATEGYLOADER_FILE_PATH, server_name), 'rb') as fr:
        for line in fr.readlines():
            line_list.append(line.replace('\n', ''))
    for strategy_online_db in strategy_online_list:
        line_list.append('[Strategy.lib%s.%s]' % (strategy_online_db.assembly_name, strategy_online_db.name))
        line_list.append('WatchList = %s' % strategy_online_db.instance_name)
        line_list.append('')

    file_path = '%s/%s/config.strategyloader.txt' % (STRATEGYLOADER_FILE_PATH, server_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(line_list))

    server_model = server_constant.get_server_model(server_name)
    server_model.upload_file(file_path,
                             '%s/config.strategyloader.txt' % server_model.server_path_dict['tradeplat_project_folder'])


def __update_strategy_parameter(future_main_contract_db):
    pre_main_symbol = future_main_contract_db.pre_main_symbol
    main_symbol = future_main_contract_db.main_symbol

    for server_name in server_constant.get_trade_servers():
        server_model = server_constant.get_server_model(server_name)
        server_session = server_model.get_db_session('strategy')
        query = server_session.query(StrategyParameter)
        key = 'Global.MarkVolCurve.%s' % pre_main_symbol
        for strategy_parameter_db in query.filter(StrategyParameter.name.like('%' + key + '%')):
            strategy_parameter_db.name = strategy_parameter_db.name.replace(pre_main_symbol, main_symbol)
            server_session.merge(strategy_parameter_db)
        server_session.commit()


def main_contract_change_job(server_list):
    global email_content_list
    email_content_list = []

    global server_host
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')

    __build_instrument_db_dict()
    main_contract_change_list = __query_future_main_contract(server_host)
    pf_position_dict = query_pf_position_from_aggregator()

    for future_main_contract_db in main_contract_change_list:
        # 1.更新strategy_online表
        pre_change_strategy_list = __update_strategy_online(future_main_contract_db)

        for server_name in server_list:
            server_model = server_constant.get_server_model(server_name)
            # 2.修改服务器上持仓数据
            find_key = '%s|%s' % (server_name, future_main_contract_db.pre_main_symbol)
            if find_key in pf_position_dict:
                pf_position_list = pf_position_dict[find_key]
                __main_contract_position_change(server_name, future_main_contract_db, pf_position_list)
            if not server_model.is_cta_server:
                continue

            # 3.修改服务器上策略参数
            __rebuild_strategy_parameter(server_name, pre_change_strategy_list, future_main_contract_db)
            # 4.修改calendar策略参数
            __rebuild_calendar_parameter(server_name, future_main_contract_db)
        if future_main_contract_db.ticker_type in 'm|SR|cu':
            # 5.更新期货期权的主力合约信息
            __update_track_undl_tickers(future_main_contract_db)
            # 6.更新期货期权的Global.MarkVolCurve参数
            __update_strategy_parameter(future_main_contract_db)

        future_main_contract_db.update_flag = 0
        session_common.merge(future_main_contract_db)
    session_common.commit()

    # 7.修改config.strategyloader.txt文件
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        if not server_model.is_cta_server:
            continue
        __rebuild_strategy_loader_file(server_name)
    server_host.close()

    table_list = []
    for db_info in main_contract_change_list:
        table_list.append((db_info.pre_main_symbol, db_info.main_symbol, db_info.next_main_symbol))

    if len(table_list) > 0:
        table_title = 'pre_main_symbol, main_symbol, next_main_symbol'
        table_html_list = email_utils2.list_to_html(table_title, table_list)

        email_content_list.append(''.join(table_html_list))
        email_utils2.send_email_group_all(u'期货换月报告', '\n'.join(email_content_list), 'html')


def query_pf_position_from_aggregator():
    """
        从aggregator获取当前策略仓位信息
    """
    aggregator_message_utils = AggregatorMessageUtils()
    aggregator_message_utils.login_aggregator()

    pf_position_dict = dict()
    instrument_msg_dict = aggregator_message_utils.query_instrument_dict()
    position_risk_msg = aggregator_message_utils.query_position_risk_msg()
    for holding_item in position_risk_msg.Holdings:
        strategy_name = holding_item.Key
        (base_strategy_name, server_ip_str) = strategy_name.split('@')
        server_name = common_utils.get_server_name(server_ip_str)
        for risk_msg_info in holding_item.Value:
            if risk_msg_info.Value.Long == 0 and risk_msg_info.Value.Short == 0:
                continue
            instrument_msg = instrument_msg_dict[int(risk_msg_info.Key)]
            ticker = instrument_msg.ticker
            qty = risk_msg_info.Value.Long - risk_msg_info.Value.Short
            dict_key = '%s|%s' % (server_name, ticker)
            pf_position_dict.setdefault(dict_key, []).append([base_strategy_name, qty])
    return pf_position_dict


if __name__ == '__main__':
    cta_servers = server_constant.get_cta_servers()
    main_contract_change_job(cta_servers)
