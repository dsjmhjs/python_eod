import os
import json
import shutil
import threading
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.job import *


account_list_dict = dict()
account_list_dict['nanhua_web'] = ['All_Weather_1', 'All_Weather_2', 'All_Weather_3']
account_list_dict['zhongxin'] = ['steady_return', 'huize01', 'hongyuan01']
account_list_dict['luzheng'] = ['All_Weather', ]
account_list_dict['guangfa'] = ['steady_return', ]

filter_parameter = ['Account', 'Target', 'tq.All_Weather_1.max_long_position',
                    'tq.All_Weather_1.max_short_position', 'tq.All_Weather_1.qty_per_trade',
                    'tq.All_Weather_2.max_long_position', 'tq.All_Weather_2.max_short_position',
                    'tq.All_Weather_2.qty_per_trade',  'tq.All_Weather_3.max_long_position',
                    'tq.All_Weather_3.max_short_position', 'tq.All_Weather_3.qty_per_trade',
                    'tq.absolute_return.max_long_position', 'tq.absolute_return.max_short_position',
                    'tq.absolute_return.qty_per_trade', 'tq.steady_return.max_long_position',
                    'tq.steady_return.max_short_position', 'tq.steady_return.qty_per_trade']

strategy_fileter_list = ['PairTrading.j_jm', 'PairTrading.j_jm_para2', 'PairTrading.j_jm_para3',
                         'PairTrading.j_jm_para4', 'PairTrading.m_rm_para1', 'PairTrading.m_rm_para2',
                         'PairTrading.m_rm_para3', 'PairTrading.m_rm_para4']

trade_start_date_dict = dict()
trade_start_date_dict['nanhua_web'] = '2016-10-01'
trade_start_date_dict['guoxin'] = '2017-02-14'
trade_start_date_dict['zhongxin'] = '2017-02-15'
trade_start_date_dict['luzheng'] = '2017-09-05'
trade_start_date_dict['guangfa'] = '2018-02-01'

email_utils = EmailUtils(EmailUtils.group14)


def clear_folder(folder):
    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder):
            for file_name in files:
                os.remove(folder + file_name)
            for file_name in dirs:
                shutil.rmtree(folder + file_name)
    else:
        os.makedirs(folder)


def get_holiday_list():
    task_logger.info('getting holiday list...')
    server_model_host = server_constant.get_server_model('host')
    session_history = server_model_host.get_db_session('history')
    query_sql = "select HOLIDAY from history.holiday_list order by HOLIDAY asc;"
    query_result = session_history.execute(query_sql)
    holiday_list_list = []
    for query_line in query_result:
        holiday_list_list.append(str(query_line[0]))

    with open(PL_CAL_INFO_FOLDER + 'holiday_list.csv', 'w+') as fr:
        fr.write('\n'.join(holiday_list_list))

    server_model_host.close()


def get_ticker_name_eng(ticker_name):
    new_ticker_name = ''
    for n in ticker_name:
        if not n.isalpha():
            break
        new_ticker_name += n
    return new_ticker_name


def get_strategy_name_list():
    task_logger.info('getting strategy name list...')
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME` from strategy.strategy_online where strategy_type = 'CTA' order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    strategy_name_list = []
    for query_line in query_result:
        strategy_name_list.append(query_line[0])

    with open(PL_CAL_INFO_FOLDER + 'strategy_name_list.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_name_list))

    server_model_host.close()
    return strategy_name_list


def get_strategy_future_info():
    task_logger.info('getting strategy future dict...')
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME`,instance_name from strategy.strategy_online where strategy_type = 'CTA' " \
                "order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    future_info_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        instance_name_str = query_line[1]
        future_list = []
        if ';' in instance_name_str:
            instance_name_list = instance_name_str.split(';')
            for instance_name in instance_name_list:
                future_list.append(get_ticker_name_eng(instance_name).upper())
        else:
            future_list.append(get_ticker_name_eng(instance_name_str).upper())

        future_info_list.append('%s,%s' % (strategy_name, '_'.join(future_list)))

    with open(PL_CAL_INFO_FOLDER + 'strategy_future_info.csv', 'w+') as fr:
        fr.write('\n'.join(future_info_list))

    server_model_host.close()


def get_strategy_parameter_info():
    task_logger.info('getting strategy parameter dict...')
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME`,parameter_server from strategy.strategy_online where strategy_type = 'CTA' " \
                "order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    strategy_parameter_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        strategy_parameter_str = query_line[1].split('|')[0]
        strategy_parameter_dict = json.loads(strategy_parameter_str)
        new_parameter_list = ['[Account]1:0:0']
        for (key_parameter, value_parameter) in strategy_parameter_dict.items():
            if key_parameter in filter_parameter:
                continue
            new_parameter_list.append('[%s]%s:0:0' % (key_parameter, value_parameter))
        strategy_parameter_info = ';'.join(new_parameter_list)
        strategy_parameter_list.append('%s,%s' % (strategy_name, strategy_parameter_info))

    with open(PL_CAL_INFO_FOLDER + 'strategy_parameter_info.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_parameter_list))
    server_model_host.close()


def get_strategy_backtest_info():
    task_logger.info('getting strategy backtest info list...')
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME`,strategy_name,assembly_name,instance_name,data_type,date_num,parameter_server" \
                " from strategy.strategy_online where strategy_type = 'CTA' order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    strategy_backtest_info_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        strategy_assembly_name_short = query_line[1]
        strategy_assembly_name = query_line[2]
        instance_name = query_line[3]
        data_type = query_line[4]
        date_num = query_line[5]

        strategy_parameter_dict = json.loads(query_line[6].split('|')[0])
        bardurationmin_value = 0
        if 'BarDurationMin' in strategy_parameter_dict:
            bardurationmin_value = strategy_parameter_dict['BarDurationMin']

        strategy_backtest_info_list.append('%s,%s,%s,%s,%s,%s,%s' %
                                           (strategy_name, strategy_assembly_name_short, strategy_assembly_name,
                                            instance_name, data_type, date_num, bardurationmin_value))

    with open(PL_CAL_INFO_FOLDER + 'strategy_backtest_info_list.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_backtest_info_list))
    server_model_host.close()


def get_position_control_strategy():
    task_logger.info('getting position control strategy...')
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME`,parameter_server from strategy.strategy_online where strategy_type = 'CTA' " \
                "order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    position_control_strategy_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        strategy_parameter_dict = json.loads(query_line[1].split('|')[0])
        position_control_flag = False
        if 'positionCtrlFlag' in strategy_parameter_dict:
            position_control_flag = bool(int(strategy_parameter_dict['positionCtrlFlag']))
        if position_control_flag:
            position_control_strategy_list.append(strategy_name)

    with open(PL_CAL_INFO_FOLDER + 'position_control_strategy_list.csv', 'w+') as fr:
        fr.write('\n'.join(position_control_strategy_list))
    server_model_host.close()


def get_strategy_enable_list():
    task_logger.info("getting strategy enable list...")
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME` from strategy.strategy_online where strategy_type = 'CTA' and " \
                "`enable` = 1 order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    strategy_enable_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        strategy_enable_list.append(strategy_name)

    with open(PL_CAL_INFO_FOLDER + 'strategy_enable_list.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_enable_list))
    server_model_host.close()


def get_future_close_price():
    task_logger.info('getting future close price...')
    server_model_host = server_constant.get_server_model('host')
    session_common = server_model_host.get_db_session('common')
    query_sql_1 = "select main_symbol from common.future_main_contract"
    query_result_1 = session_common.execute(query_sql_1)
    main_contract_list = []
    for query_line in query_result_1:
        main_contract_list.append(query_line[0])
    future_list = []
    future_close_price_list = []
    for main_contract_ticker in main_contract_list:
        future_name = get_ticker_name_eng(main_contract_ticker).upper()
        future_list.append(future_name)
        query_sql_2 = "select prev_close from common.instrument where ticker = '%s';" % main_contract_ticker
        query_result_2 = session_common.execute(query_sql_2)
        for query_line in query_result_2:
            future_close_price_list.append('%s,%s' % (future_name, str(query_line[0])))
    server_model_host.close()

    with open(PL_CAL_INFO_FOLDER + 'future_close_price.csv', 'w+') as fr:
        fr.write('\n'.join(future_close_price_list))
    return future_list


def get_future_value_per_point_info(future_list):
    task_logger.info('geting future value per point info...')
    server_model_118 = server_constant.get_server_model('host')
    session_common = server_model_118.get_db_session('common')
    future_value_per_point_list = []
    for future in future_list:
        query_sql = "select ticker,FUT_VAL_PT from common.instrument where ticker like '%s';" % ('%' + future + '%')
        query_result = session_common.execute(query_sql)
        for query_line in query_result:
            value_per_point = query_line[1]
            ticker_name = query_line[0]
            future_name_sql = get_ticker_name_eng(ticker_name)
            if future_name_sql.upper() == future.upper():
                future_value_per_point_list.append('%s,%s' % (future.upper(), str(value_per_point)))
                break
    server_model_118.close()

    with open(PL_CAL_INFO_FOLDER + 'future_value_per_point.csv', 'w+') as fr:
        fr.write('\n'.join(future_value_per_point_list))


def get_future_commission_rate_info(future_list):
    task_logger.info('getting commission rate...')
    server_model_nanhua = server_constant.get_server_model('nanhua_web')
    session_common = server_model_nanhua.get_db_session('common')
    query_sql = "select ticker_type,open_ratio_by_money,open_ratio_by_volume,close_ratio_by_money," \
                "close_ratio_by_volume,close_today_ratio_by_money,close_today_ratio_by_volume " \
                "from common.instrument_commission_rate;"
    query_result = session_common.execute(query_sql)
    commission_rate_dict = dict()
    for query_line in query_result:
        future_name_temp = query_line[0]
        if future_name_temp == 'SSE50':
            future_name = 'IH'
        elif future_name_temp == 'SHSZ300':
            future_name = 'IF'
        elif future_name_temp == 'SH000905':
            future_name = 'IC'
        else:
            future_name = future_name_temp.upper()
        commission_ratio_by_money = query_line[1]
        commission_ratio_by_volume = query_line[2]
        commission_rate_dict[future_name] = '%.8f,%.8f' % (commission_ratio_by_money, commission_ratio_by_volume)

    commission_rate_info_list = []
    for future in future_list:
        commission_rate_info_list.append('%s,%s' % (future, commission_rate_dict[future]))
    server_model_nanhua.close()

    with open(PL_CAL_INFO_FOLDER + 'future_commission_rate_info.csv', 'w+') as fr:
        fr.write('\n'.join(commission_rate_info_list))


def get_offline_strategy_name_list():
    task_logger.info('getting offline strategy name list...')
    server_model_118 = server_constant.get_server_model('local118')
    session_strategy = server_model_118.get_db_session('strategy')
    query_sql = "select `offline_strategy_name` from strategy.strategy_offline;"
    query_result = session_strategy.execute(query_sql)
    offline_strategy_name_list = []
    for query_line in query_result:
        offline_strategy_name = query_line[0]
        offline_strategy_name_list.append(offline_strategy_name)
    server_model_118.close()

    with open(PL_CAL_INFO_FOLDER + 'offline_strategy_name_list.csv', 'w+') as fr:
        fr.write('\n'.join(offline_strategy_name_list))


def get_strategy_start_time():
    task_logger.info('getting strategy start time...')
    server_model_118 = server_constant.get_server_model('local118')
    session_strategy = server_model_118.get_db_session('strategy')
    query_sql = "select `NAME`,strategy_start_time from strategy.strategy_start_time;"
    query_result = session_strategy.execute(query_sql)
    strategy_start_time_list = []
    for query_line in query_result:
        strategy_name = query_line[0]
        strategy_start_time = query_line[1]
        strategy_start_time_list.append('%s,%s' % (strategy_name, strategy_start_time))
    server_model_118.close()

    with open(PL_CAL_INFO_FOLDER + 'strategy_start_time.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_start_time_list))


def get_strategy_number_list(server_name, strategy_name_list):
    shared_disc_path_server = PL_CAL_INFO_FOLDER + '%s/' % server_name
    if not os.path.exists(shared_disc_path_server):
        os.mkdir(shared_disc_path_server)

    task_logger.info('getting strategy number list...')
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    strategy_number_list = []
    strategy_number_dict = dict()
    for strategy_name in strategy_name_list:
        query_sql = "select ID from portfolio.pf_account where group_name = '%s' and `NAME` = '%s';" \
                    % (strategy_name.split('.')[0], strategy_name.split('.')[1])
        query_result = session_portfolio.execute(query_sql)
        id_list = []
        for query_line in query_result:
            id_list.append(str(query_line[0]))
        if len(id_list) > 0:
            strategy_number_list.append('%s,%s' % (strategy_name, ','.join(id_list)))
            strategy_number_dict[strategy_name] = id_list
    server_model.close()

    with open(PL_CAL_INFO_FOLDER + 'strategy_number_list.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_number_list))
    return strategy_number_dict


def get_trade_list(server_name, strategy_name_list):
    shared_disc_path_server_tradelist = PL_CAL_INFO_FOLDER + '%s/%s/' % (server_name, 'trade_list')
    if not os.path.exists(shared_disc_path_server_tradelist):
        os.makedirs(shared_disc_path_server_tradelist)

    task_logger.info('getting trading list...')
    server_model = server_constant.get_server_model(server_name)
    session_om = server_model.get_db_session('om')
    strategy_ticker_name_dict = dict()
    for strategy_name in strategy_name_list:
        query_sql = "select `TIME`,SYMBOL,QTY,PRICE from om.trade2_history where strategy_id = '%s' and " \
                    "`TIME` > '2016-10-01 16:00:00' order by `TIME` asc;" % strategy_name
        query_result = session_om.execute(query_sql)
        trade_list = []
        ticker_name_list = []
        for query_line in query_result:
            ticker_name = query_line[1].split(' ')[0]
            if ticker_name not in ticker_name_list:
                ticker_name_list.append(ticker_name)
            trade_str = '%s,%s,%s,%s' % (query_line[0], ticker_name, query_line[2], query_line[3])
            trade_list.append(trade_str)

        strategy_ticker_name_dict[strategy_name] = ticker_name_list

        if len(trade_list) > 0:
            with open(shared_disc_path_server_tradelist + 'trade_list_%s.csv' % strategy_name, 'w+') as fr:
                fr.write('\n'.join(trade_list))
    server_model.close()

    with open(PL_CAL_INFO_FOLDER + '%s/strategy_ticker_info.csv' % server_name, 'w+') as fr:
        for [strategy_name, ticker_name_list] in sorted(strategy_ticker_name_dict.items()):
            fr.write(strategy_name + ',' + ','.join(ticker_name_list) + '\n')


def get_pf_position_list(server_name, strategy_name_list, strategy_number_dict):
    shared_disc_path_server_pf_position_list = PL_CAL_INFO_FOLDER + '%s/%s/' % (server_name, 'pf_position_list')
    if not os.path.exists(shared_disc_path_server_pf_position_list):
        os.makedirs(shared_disc_path_server_pf_position_list)

    task_logger.info('getting pf_position list...')
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    for strategy_name in strategy_name_list:
        if strategy_name not in strategy_number_dict:
            continue
        id_list = strategy_number_dict[strategy_name]
        query_sql = "select date,id,symbol,`Long`,short from portfolio.pf_position where id in (%s) " \
                    "and date > '2016-10-01' order by date asc;" % (','.join(id_list))
        query_result = session_portfolio.execute(query_sql)
        pf_position_list = []
        for query_line in query_result:
            pf_position_str = '%s,%s,%s,%s,%s' % (query_line[0], query_line[1], query_line[2], query_line[3],
                                                  query_line[4])
            pf_position_list.append(pf_position_str)
        if len(pf_position_list) > 0:
            with open(shared_disc_path_server_pf_position_list + 'pf_position_list_%s.csv' % strategy_name, 'w+') as fr:
                fr.write('\n'.join(pf_position_list))
    server_model.close()


def get_init_position_list(server_name, strategy_name_list, strategy_number_dict):
    shared_disc_path_server = PL_CAL_INFO_FOLDER + '%s/' % server_name
    if not os.path.exists(shared_disc_path_server):
        os.mkdir(shared_disc_path_server)

    task_logger.info('getting init_position list...')
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    init_position_list = []
    for strategy_name in strategy_name_list:
        if strategy_name not in strategy_number_dict:
            continue
        id_list = strategy_number_dict[strategy_name]
        query_sql = "select date,id,symbol,`Long`,short from portfolio.pf_position where id in (%s) and " \
                    "date <= '%s' order by date desc;" % (','.join(id_list), trade_start_date_dict[server_name])
        query_result = session_portfolio.execute(query_sql)
        init_position_flag = False
        init_date = trade_start_date_dict[server_name]
        for query_line in query_result:
            if not init_position_flag:
                init_date = query_line[0]
                init_position_flag = True
                init_position_str = '%s,%s,%s,%s,%s,%s' % (strategy_name, query_line[0], query_line[1],
                                                           query_line[2], query_line[3], query_line[4])
                init_position_list.append(init_position_str)
            else:
                if query_line[0] == init_date:
                    init_position_str = '%s,%s,%s,%s,%s,%s' % (strategy_name, query_line[0], query_line[1],
                                                               query_line[2], query_line[3], query_line[4])
                    init_position_list.append(init_position_str)
                else:
                    break

    if len(init_position_list) > 0:
        with open(shared_disc_path_server + 'init_position_list.csv', 'w+') as fr:
            fr.write('\n'.join(init_position_list))
    server_model.close()


def get_position_parameter_list(server_name):
    shared_disc_path_server = PL_CAL_INFO_FOLDER + '%s/' % server_name
    if not os.path.exists(shared_disc_path_server):
        os.mkdir(shared_disc_path_server)

    account_name_list = account_list_dict[server_name]
    if server_name == 'nanhua_web':
        server_name = 'nanhua'

    task_logger.info('getting strategy position parameter list...')
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `ENABLE`,`NAME`,target_server,parameter_server from strategy.strategy_online " \
                "where strategy_type = 'CTA' order by `NAME` asc;"
    query_result = session_strategy.execute(query_sql)
    strategy_position_parameter_list = []
    for query_line in query_result:
        strategy_enable = query_line[0]
        strategy_name = query_line[1]
        target_server_list = query_line[2].split('|')
        if strategy_name in strategy_fileter_list:
            continue
        if server_name not in target_server_list:
            continue
        if strategy_enable == 0:
            strategy_position_parameter = strategy_name
            for account_name in account_name_list:
                account_name += ''
                strategy_position_parameter += ',0'
                strategy_position_parameter += ',0'
            strategy_position_parameter_list.append(strategy_position_parameter)
            continue

        server_index = target_server_list.index(server_name)
        strategy_parameter_str = query_line[3].split('|')[server_index]
        strategy_parameter_dict = json.loads(strategy_parameter_str)

        strategy_position_parameter = strategy_name
        for account_name in account_name_list:
            if 'tq.%s.max_long_position' % account_name in strategy_parameter_dict \
                    and 'tq.%s.max_short_position' % account_name in strategy_parameter_dict:
                max_long_position = strategy_parameter_dict['tq.%s.max_long_position' % account_name]
                max_short_position = strategy_parameter_dict['tq.%s.max_short_position' % account_name]
                strategy_position_parameter += ',%s' % max_long_position
                strategy_position_parameter += ',%s' % max_short_position
            else:
                strategy_position_parameter += ',0'
                strategy_position_parameter += ',0'
        strategy_position_parameter_list.append(strategy_position_parameter)

    with open(shared_disc_path_server + 'strategy_position_parameter_list.csv', 'w+') as fr:
        fr.write('\n'.join(strategy_position_parameter_list))

    server_model_host.close()


def get_server_pl_info(server_name, strategy_name_list):
    strategy_number_dict = get_strategy_number_list(server_name, strategy_name_list)
    get_trade_list(server_name, strategy_name_list)
    get_pf_position_list(server_name, strategy_name_list, strategy_number_dict)
    get_init_position_list(server_name, strategy_name_list, strategy_number_dict)
    get_position_parameter_list(server_name)


def build_backtest_strategy_group_dict():
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select strategy_name, group_number from strategy.strategy_backtest_group;"
    query_result = session_strategy.execute(query_sql)
    backtest_strategy_group_dict = dict()

    with open(PL_CAL_INFO_FOLDER + 'strategy_group_str.csv', 'w+') as fr:
        for query_line in query_result:
            strategy_name = query_line[0]
            group_number = query_line[1]
            backtest_strategy_group_str = '%s,%s' % (strategy_name, group_number)
            fr.write(backtest_strategy_group_str + '\n')
    server_model_host.close()
    return backtest_strategy_group_dict


def get_pl_cal_sql_info_job():
    clear_folder(PL_CAL_INFO_FOLDER)
    get_holiday_list()
    strategy_name_list = get_strategy_name_list()
    get_strategy_future_info()
    get_strategy_parameter_info()
    get_strategy_backtest_info()
    get_position_control_strategy()
    get_strategy_enable_list()
    future_list = get_future_close_price()
    get_future_value_per_point_info(future_list)
    get_future_commission_rate_info(future_list)
    get_strategy_start_time()
    get_offline_strategy_name_list()

    threads = []
    server_name_list = server_constant.get_cta_servers(False)
    for server_name in server_name_list:
        t = threading.Thread(target=get_server_pl_info, args=(server_name, strategy_name_list))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # get guoxin mc.ys001 trade info
    strategy_number_dict = get_strategy_number_list('guoxin', ['MC.ys001', ])
    get_trade_list('guoxin', ['MC.ys001', ])
    get_pf_position_list('guoxin', ['MC.ys001', ], strategy_number_dict)
    get_init_position_list('guoxin', ['MC.ys001', ], strategy_number_dict)

    email_utils.send_email_group_all('Get P&L Sql Info Success!', '', 'html')


if __name__ == '__main__':
    get_pl_cal_sql_info_job()