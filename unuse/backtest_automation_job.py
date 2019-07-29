# -*- coding: utf-8 -*-
import json
import os
import six
import zmq
import zlib
import subprocess
import shutil
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer
from eod_aps.tools.date_utils import *
from eod_aps.model.instrument import Instrument
from eod_aps.model.strategy_online import StrategyOnline
from eod_aps.model.strategy_parameter import StrategyParameter
from sqlalchemy import desc
from eod_aps.model.AllProtoMsg_pb2 import ServerParameterChangeRequestMsg, TradeInfoRequestMsg, TradeInfoResponseMsg
from eod_aps.job import *

instrument_db_dict = dict()

# backtest ip and port information
local_ip = '172.16.10.188'
server_port_num_dict = dict()
server_port_num_dict['nanhua_web'] = '1'
server_port_num_dict['zhongxin'] = '2'
server_port_num_dict['luzheng'] = '3'

filter_parameter = ['Account', 'Target', 'tq.All_Weather_1.max_long_position', 'tq.All_Weather_1.max_short_position', 'tq.All_Weather_1.qty_per_trade',
                    'tq.All_Weather_2.max_long_position', 'tq.All_Weather_2.max_short_position', 'tq.All_Weather_2.qty_per_trade',
                    'tq.All_Weather_3.max_long_position', 'tq.All_Weather_3.max_short_position', 'tq.All_Weather_3.qty_per_trade',
                    'tq.steady_return.max_long_position', 'tq.steady_return.max_short_position', 'tq.steady_return.qty_per_trade',
                    'tq.absolute_return.max_long_position', 'tq.absolute_return.max_short_position', 'tq.absolute_return.qty_per_trade',
                    'tq.All_Weather.max_long_position', 'tq.All_Weather.max_short_position', 'tq.All_Weather.qty_per_trade',]
server_name_list = ['nanhua_web', 'zhongxin', 'luzheng', 'huabao']

month_eng_num_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', \
                      'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

backtest_dict_nanhua = {'nanhua_web/StratsPlatform1': ['DMIdiverge', 'Narrow_SD_SCL', 'AMASkew', 'AMACD', 'PriceVolRatio', ],
                        'nanhua_web/StratsPlatform2': ['ChannelBreak', 'CCIRvs', 'CCIMktRecg', 'LinearRegSlope', 'PairTrading', ],
                        'nanhua_web/StratsPlatform3': ['BreakBand', 'BreakBandInBar', 'BreakBandSL', 'EMABollinger', 'BollingerBandReversion'],
                        'nanhua_web/StratsPlatform4': ['Narrow_SD', 'TrendFollowLR', 'TrdFlwLRNewEx', 'head_shoulder', 'FollowTrend', 'FollowTrendIntraDay'],
                        'nanhua_web/StratsPlatform5': ['NarrowBandBreak', 'CCI', 'HighLowBandIntraDay', 'HighLowBand']}
backtest_dict_zhongxin = {'zhongxin/StratsPlatform1': ['DMIdiverge', 'Narrow_SD_SCL', 'AMASkew', 'AMACD', 'PriceVolRatio'],
                          'zhongxin/StratsPlatform2': ['ChannelBreak', 'CCIRvs', 'CCIMktRecg', 'LinearRegSlope', 'PairTrading', ],
                          'zhongxin/StratsPlatform3': ['BreakBand', 'BreakBandInBar', 'BreakBandSL', 'EMABollinger', 'BollingerBandReversion'],
                          'zhongxin/StratsPlatform4': ['Narrow_SD', 'TrendFollowLR', 'TrdFlwLRNewEx', 'head_shoulder', 'FollowTrend', 'FollowTrendIntraDay'],
                          'zhongxin/StratsPlatform5': ['NarrowBandBreak', 'CCI', 'HighLowBandIntraDay', 'HighLowBand']}
backtest_dict_luzheng = {'luzheng/StratsPlatform1': ['DMIdiverge', 'Narrow_SD_SCL', 'AMASkew', 'AMACD', 'PriceVolRatio'],
                          'luzheng/StratsPlatform2': ['ChannelBreak', 'CCIRvs', 'CCIMktRecg', 'LinearRegSlope', 'PairTrading', ],
                          'luzheng/StratsPlatform3': ['BreakBand', 'BreakBandInBar', 'BreakBandSL', 'EMABollinger', 'BollingerBandReversion'],
                          'luzheng/StratsPlatform4': ['Narrow_SD', 'TrendFollowLR', 'TrdFlwLRNewEx', 'head_shoulder', 'FollowTrend', 'FollowTrendIntraDay'],
                          'luzheng/StratsPlatform5': ['NarrowBandBreak', 'CCI', 'HighLowBandIntraDay', 'HighLowBand']}

# backtest_dict_nanhua = {}
# backtest_dict_zhongxin = {'zhongxin/StratsPlatform1': ['DMIdiverge', 'Narrow_SD_SCL', 'AMASkew', 'AMACD',], }

backtest_dict = dict(backtest_dict_nanhua.items() + backtest_dict_zhongxin.items() + backtest_dict_luzheng.items())

shift_time_dict = dict()
shift_time_dict['nanhua_web'] = '45'
shift_time_dict['zhongxin'] = '49'
shift_time_dict['luzheng'] = '55'

no_night_market_list = ['sm', 't', 'tf', 'IH', 'pp', 'cs', 'l', 'v']


def find_backtest_result_file(strategy_info, backtest_model):
    backtest_result_file_path = '%s/%s' % (
        backtest_model.backtest_result_file_folder, '[%s]%s' % (strategy_info.assembly_name, \
                                                                strategy_info.strategy_name))
    found_flag = False
    if not os.path.exists(backtest_result_file_path):
        return found_flag

    date_str = None
    for date_folder_name in os.listdir(backtest_result_file_path):
        date_str = date_folder_name

    if date_str is None:
        return found_flag
    else:
        found_flag = True
    return found_flag


def __build_minbar_file(strategy_info, backtest_model, start_time, end_time, server_name):
    instance_name = strategy_info.instance_name
    if ';' in instance_name:
        ticker_list = instance_name.split(';')
    else:
        ticker_list = [instance_name]

    for ticker in ticker_list:
        instrument_db = instrument_db_dict[ticker]
        if instrument_db.exchange_id == 20:
            exchange_name = 'SHF'
            source_ticker = ticker
            target_ticker = ticker
        elif instrument_db.exchange_id == 21:
            exchange_name = 'DCE'
            source_ticker = ticker
            target_ticker = ticker
        elif instrument_db.exchange_id == 22:
            exchange_name = 'ZCE'
            source_ticker = filter(lambda x: not x.isdigit(), ticker) + '1' + filter(lambda x: x.isdigit(), ticker)
            target_ticker = ticker
        elif instrument_db.exchange_id == 25:
            exchange_name = 'CFF'
            source_ticker = ticker
            target_ticker = ticker
        else:
            raise Exception("Error exchange_id:%s" % instrument_db.exchange_id)

        if server_name == 'nanhua_web':
            date_file_base_path = BACKTEST_DATA_FOLDER + '/data_history/'
        else:
            date_file_base_path = BACKTEST_DATA_FOLDER + '/data_history_%s/' % server_name

        market_file_folder = os.path.join(date_file_base_path, 'BAR', '%ss' % (shift_time_dict[server_name]))
        source_file_name = '%s %s %s' % (source_ticker, exchange_name, 'm')
        target_file_name = '%s %s %s' % (target_ticker, exchange_name, 'm')
        shutil.copy(market_file_folder + '/' + source_file_name.upper(),
                    backtest_model.minbar_target_folder_base + '/' + target_file_name)


def __build_quotes_file(strategy_info, backtest_model, start_time, end_time, server_name):
    instance_name = strategy_info.instance_name
    if ';' in instance_name:
        ticker_list = instance_name.split(';')
    else:
        ticker_list = [instance_name]

    for ticker in ticker_list:
        # if ticker not in instrument_db_dict:
        #     temp_ticker = ticker[:2] + '1' + ticker[2:]
        #     if temp_ticker not in instrument_db_dict:
        #         print 'error ticker:', ticker
        #         return
        #     else:
        #         instrument_db = instrument_db_dict[temp_ticker]
        # else:
        instrument_db = instrument_db_dict[ticker]

        if instrument_db.exchange_id == 20:
            exchange_name = 'SHF'
            market_file_ticker = ticker
        elif instrument_db.exchange_id == 21:
            exchange_name = 'DCE'
            market_file_ticker = ticker
        elif instrument_db.exchange_id == 22:
            exchange_name = 'ZCE'
            market_file_ticker = filter(lambda x: not x.isdigit(), ticker) + '1' + filter(lambda x: x.isdigit(), ticker)
        elif instrument_db.exchange_id == 25:
            exchange_name = 'CFF'
            market_file_ticker = ticker
        else:
            raise Exception("Error exchange_id:%s" % instrument_db.exchange_id)

        if server_name == 'nanhua_web':
            date_file_base_path = BACKTEST_DATA_FOLDER + '/data_history/'
        else:
            date_file_base_path = BACKTEST_DATA_FOLDER + '/data_history_%s/' % server_name

        market_file_folder = os.path.join(date_file_base_path, 'QUOTE', exchange_name + market_file_ticker.upper())
        market_file_list = []
        for file_name in os.listdir(market_file_folder):
            if start_time.replace('-', '') <= file_name.split('.')[0] <= end_time.replace('-', ''):
                market_file_list.append(file_name)

        market_file_list.sort()

        target_file_folder = '%s/%s%s' % (backtest_model.quotes_target_folder_base, exchange_name, ticker)
        if not os.path.exists(target_file_folder):
            os.mkdir(target_file_folder)
        for file_name in market_file_list:
            source_file_path = market_file_folder + '/' + file_name.upper()
            target_file_path = target_file_folder + '/' + file_name
            shutil.copy(source_file_path, target_file_path)


def __save_batch_file(content_str, backtest_model):
    file_object = open(backtest_model.batch_file_path, 'w+')
    file_object.write(content_str)
    file_object.close()


def check_receive_command(backtest_model):
    log_folder = backtest_model.backtest_log_folder
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    test_file_list = []
    for log_file_name in os.listdir(log_folder):
        if 'test' not in log_file_name:
            continue
        test_file_list.append(log_file_name)
    test_file_list = sorted(test_file_list)
    target_test_file = test_file_list[-1]
    target_test_file_path = log_folder + '/' + target_test_file
    fr = open(target_test_file_path)
    for line in fr.readlines():
        if 'Receive command BackTest' in line:
            return True
    return False


def send_batch_file_info(backtest_model, backtest_cmd_arg):
    context = zmq.Context().instance()
    task_logger.info("Connecting to aggregator server:%s" % backtest_model.ip)
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b'%s_test' % backtest_model.ip)
    port_number = str(17000 + int(backtest_model.port_num) * 100 + int(backtest_model.stratsplatform_num))
    socket.connect('tcp://%s:%s' % (backtest_model.ip, port_number))

    msg = ServerParameterChangeRequestMsg()
    msg.Command = backtest_cmd_arg
    msg_str = msg.SerializeToString()
    msg_list = [six.int2byte(13), msg_str]
    socket.send_multipart(msg_list)

    while True:
        msg2 = TradeInfoRequestMsg()
        msg2_str = msg2.SerializeToString()
        msg2_list = [six.int2byte(17), msg2_str]
        socket.send_multipart(msg2_list)

        recv_message = socket.recv_multipart()
        instrument_info_msg = TradeInfoResponseMsg()
        instrument_info_msg.ParseFromString(zlib.decompress(recv_message[1]))
        if instrument_info_msg.Trades:
            trade_msg = instrument_info_msg.Trades[-1]
            if trade_msg.Trade.ticker == 'TASK END' and trade_msg.Trade.StrategyID == 'TASK END':
                state_result = trade_msg.Trade.Note
                break
        time.sleep(1)

    return state_result

def __run_back_test_monitor(strategy_info, backtest_model):
    backtestmonitor_server = start_BacktestMonitor_server(strategy_info, backtest_model.backtestmonitor_exe_file)
    # 如果生成结果文件则认定为程序执行结束
    while not find_backtest_result_file(strategy_info, backtest_model):
        time.sleep(0.5)
    stop_exe_server(strategy_info, backtestmonitor_server)


def get_latest_state_file(backtest_model, strategy_name):
    state_file_folder = backtest_model.state_file_folder
    if not os.path.exists(state_file_folder):
        os.mkdir(state_file_folder)
    state_file_list = []
    for state_file_name in os.listdir(state_file_folder):
        if state_file_name[15:].replace('_state_file.txt', '') == strategy_name:
            state_file_list.append(state_file_name)
    if len(state_file_list) == 0:
        return None
    else:
        latest_state_file = sorted(state_file_list)[-1]
        latest_state_file_path = state_file_folder + latest_state_file
    return latest_state_file_path


def save_state_into_file(backtest_model, strategy_info, strategy_state_value):
    if not os.path.exists(backtest_model.state_file_folder):
        os.mkdir(backtest_model.state_file_folder)

    datetime_str = date_utils.get_today_str('%Y%m%d%H%M%S')
    file_name = '%s_%s_state_file.txt' % (datetime_str, strategy_info.name)
    state_file_save_path = backtest_model.state_file_folder + file_name

    fr = open(state_file_save_path, 'w+')
    fr.write(strategy_state_value)
    fr.close()


def save_state_into_share_disk(backtest_model, strategy_info, strategy_state_value, server_name):
    if not os.path.exists(BACKTEST_RESULT_FILE_FOLDER):
        os.mkdir(BACKTEST_RESULT_FILE_FOLDER)

    datetime_str = date_utils.get_today_str('%Y%m%d%H%M%S')
    file_name = '%s_%s_%s_state_file.txt' % (strategy_info.name, datetime_str, server_name)
    state_file_save_path = BACKTEST_RESULT_FILE_FOLDER + '/' + file_name
    strategy_state_value = strategy_state_value.replace(', ', ', \n').replace('{', '').replace('}', '')
    fr = open(state_file_save_path, 'w+')
    fr.write(strategy_state_value)
    fr.close()


def run_back_test(backtest_model, strategy_info_list, server_name, host_server_model):
    strategy_backtest_list = []
    for strategy_info in strategy_info_list:
        # 通过parameter_server获取parameter参数
        strategy_parameter_dict = json.loads(strategy_info.parameter_server.split('|')[0])
        new_parameter_list = ['[Account]1:0:0']
        for (key_parameter, value_parameter) in strategy_parameter_dict.items():
            if key_parameter in filter_parameter:
                continue
            new_parameter_list.append('[%s]%s:0:0' % (key_parameter, value_parameter))
        strategy_parameter_info = ';'.join(new_parameter_list)

        # 得到策略最新的state文件路径，如果不存在，返回
        start_time_flag = False
        latest_state_file_path = get_latest_state_file(backtest_model, strategy_info.name)
        if latest_state_file_path is None:
            start_time_flag = False
        else:
            state_content = ''
            fr = open(latest_state_file_path)
            for line in fr.readlines():
                state_content += line.replace('\n', '')
            state_dict = json.loads(state_content)
            if not state_dict.has_key('state_time'):
                start_time_flag = False
            else:
                start_time_flag = True

        if start_time_flag:
            backtest_cmd_base = 'BackTest --StratsName=%s --WatchList=%s --Parameter=%s --StartDate=%s --EndDate=%s --StartTime=%s --StateFile=%s --AssemblyName=%s --Parallel=0'
            end_date = date_utils.get_next_trading_day('%Y-%m-%d')
            start_date = state_dict['state_time'].split(' ')[0]
            start_time = state_dict['state_time'].split(' ')[1]
            if strategy_info.data_type == 'minbar':
                __build_minbar_file(strategy_info, backtest_model, start_date, end_date, server_name)
                pass
            elif strategy_info.data_type == 'quotes':
                __build_quotes_file(strategy_info, backtest_model, start_date, end_date, server_name)
                pass

            backtest_cmd_arg = backtest_cmd_base % (
                strategy_info.strategy_name, strategy_info.instance_name, strategy_parameter_info, start_date,
                end_date, start_time, latest_state_file_path, strategy_info.assembly_name)
            # __save_batch_file(backtest_cmd_arg, backtest_model)

        else:
            backtest_cmd_base = 'BackTest --StratsName=%s --WatchList=%s --Parameter=%s --StartDate=%s --EndDate=%s --AssemblyName=%s --Parallel=0'
            end_time = date_utils.get_next_trading_day('%Y-%m-%d')
            if strategy_info.data_type == 'minbar':
                start_time = date_utils.get_last_day(-strategy_info.date_num, '%Y-%m-%d')
                __build_minbar_file(strategy_info, backtest_model, start_time, end_time, server_name)
            elif strategy_info.data_type == 'quotes':
                start_time = date_utils.get_last_day(-strategy_info.date_num, '%Y-%m-%d')
                __build_quotes_file(strategy_info, backtest_model, start_time, end_time, server_name)
            else:
                raise Exception("Error data_type:%s" % strategy_info.data_type)

            backtest_cmd_arg = backtest_cmd_base % (
                strategy_info.strategy_name, strategy_info.instance_name, strategy_parameter_info, start_time,
                end_time,
                strategy_info.assembly_name)
            # __save_batch_file(backtest_cmd_arg, backtest_model)

        del_expired_files(backtest_model)

        stratsplatform_server = start_Backtestcpp_server(strategy_info, backtest_model.cpp_exe_path)
        # check_start_success(backtest_model, server_name, strategy_info)
        time.sleep(3)

        state_result = send_batch_file_info(backtest_model, backtest_cmd_arg)

        # print state_result

        strategy_state_dict = dict()

        for temp_message in state_result.split('|'):
            if temp_message == '':
                continue
            (state_key, state_value) = temp_message.split(':', 1)
            if state_key == 'Time':
                continue
            elif state_key == 'StateTime':
                for [month_eng, month_num] in month_eng_num_dict.items():
                    state_value = state_value.replace(month_eng, month_num)
                strategy_state_dict['state_time'] = state_value
                continue
            strategy_state_dict[state_key] = state_value

        strategy_state_value = json.dumps(strategy_state_dict)

        stop_exe_server(strategy_info, stratsplatform_server)

        if strategy_state_dict.has_key('state_time'):
            if strategy_state_dict['state_time'] == 'not-a-date-time':
                task_logger.error('no market data, continue...')
                continue

        strategy_backtest_list.append(strategy_info.name)

        # 将state结果保存回本地
        save_state_into_file(backtest_model, strategy_info, strategy_state_value, )

        # 将backtest结果保存至共享盘供查看
        save_state_into_share_disk(backtest_model, strategy_info, strategy_state_value, server_name)

        update_db_strategy_state(strategy_info, strategy_state_value, server_name, host_server_model)

    return strategy_backtest_list


def check_start_success(backtest_model, server_name, strategy_info):
    log_folder = backtest_model.backtest_log_folder
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    while True:
        log_path_flag = False
        for log_file_name in os.listdir(log_folder):
            if 'test' not in log_file_name:
                continue
            datetime_1min_ago = date_utils.get_last_seconds(-5)
            date_file_str = log_file_name.split('.')[0].replace('test_', '')
            time_file_str = log_file_name.split('.')[1]
            datetime_file_str = date_file_str + ' ' + time_file_str
            datetime_file = date_utils.string_toDatetime(datetime_file_str, "%Y%m%d %H%M%S")
            if datetime_file > datetime_1min_ago:
                log_file_name = log_file_name
                log_path_flag = True
                break
        if log_path_flag:
            break
        time.sleep(0.2)

    if log_file_name is None:
        raise Exception("Log File is Missing!")
    log_file_path = log_folder + '/' + log_file_name

    while True:
        start_success_flag = False
        fr = open(log_file_path)
        for line in fr.readlines():
            if 'TradingFramework start command loop now' in line:
                start_success_flag = True
                break
        fr.close()
        if start_success_flag:
            break
        time.sleep(0.2)

    return True

def start_Backtestcpp_server(strategy_info, exe_file_path):
    (file_path, exe_file_name) = os.path.split(exe_file_path)
    os.chdir(file_path)
    child = subprocess.Popen(exe_file_path)
    task_logger.info('start server:%s,pid:%s' % (strategy_info.name, child.pid))
    return child

def start_BacktestMonitor_server(strategy_info, exe_file_path):
    (file_path, exe_file_name) = os.path.split(exe_file_path)
    os.chdir(file_path)
    child = subprocess.Popen(exe_file_path)

    task_logger.info('start server:%s,pid:%s' % (strategy_info.name, child.pid))
    return child


def stop_exe_server(strategy_info, exe_server):
    exe_server.kill()
    task_logger.info('stop server:%s' % strategy_info.name)


def __build_instrument_dict(server_model):
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        instrument_db_dict[instrument_db.ticker] = instrument_db


def __get_strategy_list(server_model, backtest_model):
    strategy_info_list = []
    session_strategy = server_model.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)

    for strategy_info in query.filter(StrategyOnline.enable == 1,
                                      StrategyOnline.strategy_name == backtest_model.strategy_name):
        # if "AMACD.cs_5min_para1_1" not in strategy_info.name:
        #     continue
        strategy_info_list.append(strategy_info)
    return strategy_info_list


def __get_strategy_list_update_parameter(server_model, backtest_model):
    strategy_info_list = []
    session_strategy = server_model.get_db_session('strategy')
    query = session_strategy.query(StrategyOnline)

    for strategy_info in query.filter(StrategyOnline.strategy_name == backtest_model.strategy_name):
        # if "AMACD.cs_5min_para1_1" not in strategy_info.name:
        #     continue
        strategy_info_list.append(strategy_info)
    return strategy_info_list


def del_expired_files(backtest_model):
    # 删除之前生成的result文件夹
    for folder_name in os.listdir(backtest_model.backtest_result_file_folder):
        if os.path.exists(backtest_model.backtest_result_file_folder + '/' + folder_name):
            shutil.rmtree(backtest_model.backtest_result_file_folder + '/' + folder_name)

    if os.path.exists(backtest_model.cpp_log_path):
        try:
            shutil.rmtree(backtest_model.cpp_log_path)
            os.mkdir(backtest_model.cpp_log_path)
        except:
            pass


def get_last_file(strategy_log_file_list):
    strategy_log_file_list_dict = dict()
    strategy_log_file_num_list = []
    for strategy_log_file in strategy_log_file_list:
        file_num = int(strategy_log_file.split('.')[-2])
        strategy_log_file_num_list.append(file_num)
        strategy_log_file_list_dict[file_num] = strategy_log_file
    last_file = strategy_log_file_list_dict[sorted(strategy_log_file_num_list)[-1]]
    return last_file


# 解析运行结果的日志信息
def analysis_backtest_log(strategy_info, backtest_model):
    strategy_log_file_list = []
    backtest_log_folder = backtest_model.backtest_log_folder

    for file_name in os.listdir(backtest_log_folder):
        if strategy_info.strategy_name in file_name:
            strategy_log_file_list.append(file_name)
    # strategy_log_file_list.sort()
    last_file = get_last_file(strategy_log_file_list)

    key_word = 'state_calculation'
    keyword_message_list = []
    fr = open(backtest_log_folder + '/' + strategy_log_file_list[-1])
    for line in fr.readlines():
        if key_word in line:
            keyword_message_list.append(line.strip('\n'))
    fr.close()
    strategy_state_dict = dict()
    state_time = None
    for keyword_message in keyword_message_list[-2:]:
        keyword_message = keyword_message.split('=')[1]
        for temp_message in keyword_message.split('|'):
            if temp_message == '':
                continue
            (state_key, state_value) = temp_message.split(':', 1)
            if state_key == 'Time':
                state_time = state_value
                continue
            elif state_key == 'state_time':
                for [month_eng, month_num] in month_eng_num_dict.items():
                    state_value = state_value.replace(month_eng, month_num)
            strategy_state_dict[state_key] = state_value

    strategy_state_value = json.dumps(strategy_state_dict)
    email_content = 'name:%s  time:%s\n' % \
                    (strategy_info.name, state_time)
    # print email_content
    return strategy_state_value, email_content


def clear_share_disc_backtest_result():
    if not os.path.exists(BACKTEST_RESULT_FILE_FOLDER):
        os.mkdir(BACKTEST_RESULT_FILE_FOLDER)
    if not os.path.exists(BACKTEST_RESULT_HISTORY_FOLDER):
        os.mkdir(BACKTEST_RESULT_HISTORY_FOLDER)
    for state_file_name in os.listdir(BACKTEST_RESULT_FILE_FOLDER):
        if 'state_file' in state_file_name:
            shutil.move(BACKTEST_RESULT_FILE_FOLDER + '/' + state_file_name,
                        BACKTEST_RESULT_HISTORY_FOLDER + '/' + state_file_name)
    return


# 将回测日志中的结果更新至数据库
def update_db_strategy_state(strategy_info, strategy_state_value, server_name, host_server_model):
    global state_save_lock

    if state_save_lock.acquire():
        server_model = server_constant.get_server_model(server_name)
        session = server_model.get_db_session('strategy')
        insert_sql = '''Insert Into strategy.strategy_state(TIME,NAME,VALUE) VALUES(sysdate(),'%s','%s')'''
        insert_sql = insert_sql % (strategy_info.name, strategy_state_value)
        session.execute(insert_sql)
        session.commit()
        server_model.close()

        session_local = host_server_model.get_db_session('aggregation')
        insert_sql = '''Insert Into aggregation.strategy_state(SERVER_NAME,TIME,NAME,VALUE) VALUES('%s',sysdate(),'%s','%s')'''
        insert_sql = insert_sql % (server_name, strategy_info.name, strategy_state_value)
        session_local.execute(insert_sql)
        session_local.commit()
        state_save_lock.release()


def __update_strategy_online(host_server_model, backtest_model):
    global strategy_online_lock
    if strategy_online_lock.acquire():
        host_session = host_server_model.get_db_session('strategy')
        query = host_session.query(StrategyOnline)
        for strategy_online_db in query.filter(StrategyOnline.enable == 1, StrategyOnline.strategy_name == backtest_model.strategy_name):
            target_server_list = strategy_online_db.target_server.split('|')
            strategy_parameter_dict_server_list = []
            for target_server in target_server_list:
                if target_server == 'nanhua':
                    target_server = 'nanhua_web'
                server_model = server_constant.get_server_model(target_server)
                server_session = server_model.get_db_session('strategy')
                query_strategy_parameter = server_session.query(StrategyParameter)
                strategy_parameter_db = query_strategy_parameter.filter(
                    StrategyParameter.name == strategy_online_db.name).order_by(
                    desc(StrategyParameter.time)).first()
                if strategy_parameter_db is None:
                    continue

                strategy_parameter_dict_server_list.append(strategy_parameter_db.value)
                server_model.close()

            if len(strategy_parameter_dict_server_list) > 0:
                strategy_online_db.parameter_server = '|'.join(strategy_parameter_dict_server_list)

            strategy_parameter_dict = json.loads(strategy_online_db.parameter_server.split('|')[0])
            new_parameter_list = ['[Account]1:0:0']
            for (key_parameter, value_parameter) in strategy_parameter_dict.items():
                if key_parameter in filter_parameter:
                    continue
                new_parameter_list.append('[%s]%s:0:0' % (key_parameter, value_parameter))
            # print ';'.join(new_parameter_list)
            # strategy_online_db.parameter = ';'.join(new_parameter_list)
            host_session.merge(strategy_online_db)

        host_session.commit()

        strategy_online_list = []
        for strategy_online_db in query.filter(StrategyOnline.enable == 1,
                    StrategyOnline.strategy_name == backtest_model.strategy_name):
            strategy_online_list.append(strategy_online_db)
        strategy_online_lock.release()
        return strategy_online_list


def __update_strategy_online_by_strategy(host_server_model, backtest_model, target_strategy_name):
    global strategy_online_lock
    if strategy_online_lock.acquire():
        host_session = host_server_model.get_db_session('strategy')
        query = host_session.query(StrategyOnline)
        for strategy_online_db in query.filter(StrategyOnline.enable == 1, StrategyOnline.strategy_name == backtest_model.strategy_name):
            if strategy_online_db.name != target_strategy_name:
                continue
            target_server_list = strategy_online_db.target_server.split('|')
            strategy_parameter_dict_server_list = []
            for target_server in target_server_list:
                if target_server == 'nanhua':
                    target_server = 'nanhua_web'
                server_model = server_constant.get_server_model(target_server)
                server_session = server_model.get_db_session('strategy')
                query_strategy_parameter = server_session.query(StrategyParameter)
                strategy_parameter_db = query_strategy_parameter.filter(
                    StrategyParameter.name == strategy_online_db.name).order_by(
                    desc(StrategyParameter.time)).first()
                if strategy_parameter_db is None:
                    continue

                strategy_parameter_dict_server_list.append(strategy_parameter_db.value)
                server_model.close()

            if len(strategy_parameter_dict_server_list) > 0:
                strategy_online_db.parameter_server = '|'.join(strategy_parameter_dict_server_list)

            strategy_parameter_dict = json.loads(strategy_online_db.parameter_server.split('|')[0])
            new_parameter_list = ['[Account]1:0:0']
            for (key_parameter, value_parameter) in strategy_parameter_dict.items():
                if key_parameter in filter_parameter:
                    continue
                new_parameter_list.append('[%s]%s:0:0' % (key_parameter, value_parameter))
            # print ';'.join(new_parameter_list)
            # strategy_online_db.parameter = ';'.join(new_parameter_list)
            host_session.merge(strategy_online_db)

        host_session.commit()

        strategy_online_list = []
        for strategy_online_db in query.filter(StrategyOnline.enable == 1,
                    StrategyOnline.strategy_name == backtest_model.strategy_name):
            if strategy_online_db.name != target_strategy_name:
                continue
            strategy_online_list.append(strategy_online_db)
        strategy_online_lock.release()
        return strategy_online_list


def __backtest_automation_job(backtest_folder_name, strategy_name_list):
    global strategy_backtest_list_dict
    global server_name_list
    server_name = backtest_folder_name.split('/')[0]
    global server_host
    server_host = server_constant.get_server_model('host')

    if not strategy_backtest_list_dict.has_key(server_name):
        strategy_backtest_list_dict[server_name] = []
    if server_name not in server_name_list:
        server_name_list.append(server_name)

    for strategy_name in strategy_name_list:
        backtest_model = BackTest_Model(backtest_folder_name, strategy_name)
        __build_instrument_dict(server_host)

        strategy_online_list = __update_strategy_online(server_host, backtest_model)
        strategy_backtest_list = run_back_test(backtest_model, strategy_online_list, server_name, server_host)
        strategy_backtest_list_dict[server_name] += strategy_backtest_list
    server_host.close()


def __backtest_automation_job_by_strategy(backtest_folder_name, strategy_name_list, target_strategy_name):
    global strategy_backtest_list_dict
    global server_name_list
    server_name = backtest_folder_name.split('/')[0]
    global server_host
    server_host = server_constant.get_server_model('host')

    if not strategy_backtest_list_dict.has_key(server_name):
        strategy_backtest_list_dict[server_name] = []
    if server_name not in server_name_list:
        server_name_list.append(server_name)

    for strategy_name in strategy_name_list:
        backtest_model = BackTest_Model(backtest_folder_name, strategy_name)
        __build_instrument_dict(server_host)

        strategy_online_list = __update_strategy_online_by_strategy(server_host, backtest_model, target_strategy_name)
        strategy_backtest_list = run_back_test(backtest_model, strategy_online_list, server_name, server_host)
        strategy_backtest_list_dict[server_name] += strategy_backtest_list
    server_host.close()


def get_ticker_name_eng(ticker_name):
    new_ticker_name = ''
    for n in ticker_name:
        if not n.isalpha():
            break
        new_ticker_name += n
    return new_ticker_name


def get_strategy_online_list(host_server_model):
    strategy_online_list = []
    session_strategy = host_server_model.get_db_session('strategy')
    query_sql = "select `NAME`,INSTANCE_NAME from strategy.strategy_online where strategy_type = 'CTA' and `enable` = 1"
    query_result = session_strategy.execute(query_sql)
    for query_line in query_result:
        strategy_name = query_line[0]
        instance_name = query_line[1]
        strategy_online_list.append(strategy_name)
    return strategy_online_list


def build_email_html(host_server_model):
    global server_name_list
    global strategy_backtest_list_dict
    email_content_list = [BACKTEST_RESULT_FILE_FOLDER]

    html_title = 'strategy name,%s' % ','.join(server_name_list)
    table_list = []
    strategy_online_list = get_strategy_online_list(host_server_model)
    for strategy in strategy_online_list:
        for server_name in server_name_list:
            if strategy in strategy_backtest_list_dict[server_name]:
                table_list.append([strategy, 'Checked'])
            else:
                table_list.append([strategy, 'Not Calculated!(Error)'])

    html_list = email_utils3.list_to_html(html_title, table_list)
    email_content_list.append(''.join(html_list))
    return email_content_list


def get_future_strategy_list(server_model, change_month_future):
    future_strategy_list = []
    session_strategy = server_model.get_db_session('strategy')
    query_sql = "select INSTANCE_NAME, NAME from strategy.strategy_online where STRATEGY_TYPE = 'CTA';"
    query_result = session_strategy.execute(query_sql)
    for query_line in query_result:
        strategy_ticker_list = query_line[0].split(';')
        for strategy_ticker in strategy_ticker_list:
            strategy_future_name = get_ticker_name_eng(strategy_ticker).upper()
            if strategy_future_name == change_month_future:
                future_strategy_list.append(query_line[1])
    return future_strategy_list


def delete_change_month_file(server_model):
    today_str = date_utils.get_today_str('%Y-%m-%d')
    change_month_info_file_path = '%s/future_main_contract_change_info_%s.csv' % \
                                             (MAIN_CONTRACT_CHANGE_FILE_FOLDER, today_str)
    if not os.path.exists(change_month_info_file_path):
        return
    fr = open(change_month_info_file_path)
    for line in fr.readlines():
        if line.strip() == '':
            continue
        change_month_future = line.split(',')[0].upper()
        future_strategy_list = get_future_strategy_list(server_model, change_month_future)
        for future_strategy in future_strategy_list:
            delete_state_file_by_strategy(future_strategy)


def backtest_automation_job():
    # 周末清空所有state文件重新计算
    if date_utils.get_now().weekday() == 5:
        delete_state_file(backtest_dict, '20170101', '>=')

    global strategy_backtest_list_dict
    strategy_backtest_list_dict = dict()
    global server_name_list
    server_name_list = []

    delete_state_file(backtest_dict, date_utils.get_last_day(-10), '<=')

    global state_save_lock
    global strategy_online_lock
    state_save_lock = threading.Lock()
    strategy_online_lock = threading.Lock()

    global server_host
    server_host = server_constant.get_server_model('host')

    delete_change_month_file(server_host)

    clear_share_disc_backtest_result()

    threads = []
    for (backtest_folder_name, strategy_name_list) in backtest_dict.items():
        t = threading.Thread(target=__backtest_automation_job, args=(backtest_folder_name, strategy_name_list))
        threads.append(t)

    # 启动所有线程
    for t in threads:
        t.start()
        time.sleep(1)
    # 主线程中等待所有子线程退出
    for t in threads:
        t.join()

    # print strategy_backtest_list_dict
    email_content_list = build_email_html(server_host)
    email_utils3.send_email_group_all('BackTest Result', '\n\n\n'.join(email_content_list), 'html')
    server_host.close()
    return 0


def backtest_automation_job_by_strategy(target_strategy_name):
    strategy_backtest_list_dict = dict()
    server_name_list = []
    global strategy_backtest_list_dict
    global server_name_list

    delete_state_file(backtest_dict, date_utils.get_last_day(-10), '<=')

    global state_save_lock
    global strategy_online_lock
    state_save_lock = threading.Lock()
    strategy_online_lock = threading.Lock()

    global server_host
    server_host = server_constant.get_server_model('host')

    clear_share_disc_backtest_result()

    threads = []
    for (backtest_folder_name, strategy_name_list) in backtest_dict.items():
        t = threading.Thread(target=__backtest_automation_job_by_strategy, args=(backtest_folder_name, strategy_name_list, target_strategy_name))
        threads.append(t)

    # 启动所有线程
    for t in threads:
        t.start()
        time.sleep(1)
    # 主线程中等待所有子线程退出
    for t in threads:
        t.join()

    # print strategy_backtest_list_dict
    email_content_list = build_email_html(server_host)
    email_utils3.send_email_group_all('BackTest Result', '\n\n\n'.join(email_content_list), 'html')
    server_host.close()


def __update_strategy_online_parameter_backtest(backtest_folder_name, strategy_name_list):
    server_name = backtest_folder_name.split('/')[0]
    for strategy_name in strategy_name_list:
        backtest_model = BackTest_Model(backtest_folder_name, strategy_name)
        __build_instrument_dict(server_host)
        # strategy_info_list = __get_strategy_list_update_parameter(server_host, backtest_model)
        strategy_online_list = __update_strategy_online(server_host, backtest_model)


def update_strategy_online_parameter_backtest_job():
    delete_state_file(backtest_dict, date_utils.get_last_day(-10), '<=')

    global state_save_lock
    global strategy_online_lock
    state_save_lock = threading.Lock()
    strategy_online_lock = threading.Lock()


    threads = []
    for (backtest_folder_name, strategy_name_list) in backtest_dict.items():
        t = threading.Thread(target=__update_strategy_online_parameter_backtest, args=(backtest_folder_name, strategy_name_list))
        threads.append(t)

    # 启动所有线程
    for t in threads:
        t.start()
        time.sleep(1)
    # 主线程中等待所有子线程退出
    for t in threads:
        t.join()


def delete_state_file(backtest_dict, date_str, delete_mode='<='):
    for (backtest_folder_name, strategy_name_list) in backtest_dict.items():
        for strategy_name in strategy_name_list:
            backtest_model = BackTest_Model(backtest_folder_name, strategy_name)
            if not os.path.exists(backtest_model.state_file_trash_folder):
                os.mkdir(backtest_model.state_file_trash_folder)
            for state_file_name in os.listdir(backtest_model.state_file_folder):
                if not backtest_model.strategy_name in state_file_name:
                    continue
                file_date_str = state_file_name[0:8]
                if delete_mode == '<=':
                    if int(file_date_str) <= int(date_str):
                        task_logger.info("delete state file: %s success!" % (backtest_model.state_file_folder + state_file_name))
                        shutil.move(backtest_model.state_file_folder + state_file_name,
                                    backtest_model.state_file_trash_folder + state_file_name)
                elif delete_mode == '>=':
                    if int(file_date_str) >= int(date_str):
                        task_logger.info("delete state file: %s success!" % (backtest_model.state_file_folder + state_file_name))
                        shutil.move(backtest_model.state_file_folder + state_file_name,
                                    backtest_model.state_file_trash_folder + state_file_name)
                else:
                    pass


def delete_state_file_by_strategy(strategy_name):
    if not '.' in strategy_name:
        for (backtest_folder_name, strategy_name_list) in backtest_dict.items():
            if strategy_name in strategy_name_list:
                backtest_model = BackTest_Model(backtest_folder_name, strategy_name)
                strategy_state_file_folder = backtest_model.state_file_folder
                strategy_state_trash_folder = backtest_model.state_file_trash_folder
                for state_file_name in os.listdir(strategy_state_file_folder):
                    file_strategy_name = state_file_name.split('.')[0][15:]
                    if file_strategy_name == strategy_name:
                        task_logger.info("delete state file: %s success!" % (backtest_model.state_file_folder + state_file_name))
                        shutil.move(strategy_state_file_folder + state_file_name, strategy_state_trash_folder + state_file_name)
    else:
        strategy_group_name = strategy_name.split('.')[0]
        for (backtest_folder_name, strategy_name_list) in backtest_dict.items():
            if strategy_group_name in strategy_name_list:
                backtest_model = BackTest_Model(backtest_folder_name, strategy_group_name)
                strategy_state_file_folder = backtest_model.state_file_folder
                strategy_state_trash_folder = backtest_model.state_file_trash_folder
                for state_file_name in os.listdir(strategy_state_file_folder):
                    file_strategy_name = state_file_name[15:].replace('_state_file.txt', '')
                    if file_strategy_name == strategy_name:
                        task_logger.info("delete state file: %s success!" % (backtest_model.state_file_folder + state_file_name))
                        shutil.move(strategy_state_file_folder + state_file_name, strategy_state_trash_folder + state_file_name)


class BackTest_Model:
    strategy_name = None
    folder_name = None
    cpp_exe_path = None
    cpp_log_path = None
    backtest_log_folder = None
    backtestmonitor_exe_file = None
    minbar_target_folder_base = None
    quotes_target_folder_base = None
    batch_file_path = None
    backtest_result_file_folder = None

    backtest_path_dict = {
        'nanhua_web/StratsPlatform1': ['D:/dailyjob/nanhua_web/StratsPlatform1', 'D:/nanhua_web/backtest_result1'],
        'nanhua_web/StratsPlatform2': ['D:/dailyjob/nanhua_web/StratsPlatform2', 'D:/nanhua_web/backtest_result2'],
        'nanhua_web/StratsPlatform3': ['D:/dailyjob/nanhua_web/StratsPlatform3', 'D:/nanhua_web/backtest_result3'],
        'nanhua_web/StratsPlatform4': ['D:/dailyjob/nanhua_web/StratsPlatform4', 'D:/nanhua_web/backtest_result4'],
        'nanhua_web/StratsPlatform5': ['D:/dailyjob/nanhua_web/StratsPlatform5', 'D:/nanhua_web/backtest_result5'],
        'nanhua_web/StratsPlatform6': ['D:/dailyjob/nanhua_web/StratsPlatform6', 'D:/nanhua_web/backtest_result6'],
        'nanhua_web/StratsPlatform7': ['D:/dailyjob/nanhua_web/StratsPlatform7', 'D:/nanhua_web/backtest_result7'],
        'nanhua_web/StratsPlatform8': ['D:/dailyjob/nanhua_web/StratsPlatform8', 'D:/nanhua_web/backtest_result8'],
        'nanhua_web/StratsPlatform9': ['D:/dailyjob/nanhua_web/StratsPlatform9', 'D:/nanhua_web/backtest_result9'],
        'nanhua_web/StratsPlatform10': ['D:/dailyjob/nanhua_web/StratsPlatform10', 'D:/nanhua_web/backtest_result10'],
        'nanhua_web/StratsPlatform11': ['D:/dailyjob/nanhua_web/StratsPlatform11', 'D:/nanhua_web/backtest_result11'],
        'nanhua_web/StratsPlatform12': ['D:/dailyjob/nanhua_web/StratsPlatform12', 'D:/nanhua_web/backtest_result12'],
        'zhongxin/StratsPlatform1': ['D:/dailyjob/zhongxin/StratsPlatform1', 'D:/zhongxin/backtest_result1'],
        'zhongxin/StratsPlatform2': ['D:/dailyjob/zhongxin/StratsPlatform2', 'D:/zhongxin/backtest_result2'],
        'zhongxin/StratsPlatform3': ['D:/dailyjob/zhongxin/StratsPlatform3', 'D:/zhongxin/backtest_result3'],
        'zhongxin/StratsPlatform4': ['D:/dailyjob/zhongxin/StratsPlatform4', 'D:/zhongxin/backtest_result4'],
        'zhongxin/StratsPlatform5': ['D:/dailyjob/zhongxin/StratsPlatform5', 'D:/zhongxin/backtest_result5'],
        'zhongxin/StratsPlatform6': ['D:/dailyjob/zhongxin/StratsPlatform6', 'D:/zhongxin/backtest_result6'],
        'zhongxin/StratsPlatform7': ['D:/dailyjob/zhongxin/StratsPlatform7', 'D:/zhongxin/backtest_result7'],
        'zhongxin/StratsPlatform8': ['D:/dailyjob/zhongxin/StratsPlatform8', 'D:/zhongxin/backtest_result8'],
        'zhongxin/StratsPlatform9': ['D:/dailyjob/zhongxin/StratsPlatform9', 'D:/zhongxin/backtest_result9'],
        'zhongxin/StratsPlatform10': ['D:/dailyjob/zhongxin/StratsPlatform10', 'D:/zhongxin/backtest_result10'],
        'zhongxin/StratsPlatform11': ['D:/dailyjob/zhongxin/StratsPlatform11', 'D:/zhongxin/backtest_result11'],
        'zhongxin/StratsPlatform12': ['D:/dailyjob/zhongxin/StratsPlatform12', 'D:/zhongxin/backtest_result12'],
        'luzheng/StratsPlatform1': ['D:/dailyjob/luzheng/StratsPlatform1', 'D:/luzheng/backtest_result1'],
        'luzheng/StratsPlatform2': ['D:/dailyjob/luzheng/StratsPlatform2', 'D:/luzheng/backtest_result2'],
        'luzheng/StratsPlatform3': ['D:/dailyjob/luzheng/StratsPlatform3', 'D:/luzheng/backtest_result3'],
        'luzheng/StratsPlatform4': ['D:/dailyjob/luzheng/StratsPlatform4', 'D:/luzheng/backtest_result4'],
        'luzheng/StratsPlatform5': ['D:/dailyjob/luzheng/StratsPlatform5', 'D:/luzheng/backtest_result5'],
        'luzheng/StratsPlatform6': ['D:/dailyjob/luzheng/StratsPlatform6', 'D:/luzheng/backtest_result6'],
        'luzheng/StratsPlatform7': ['D:/dailyjob/luzheng/StratsPlatform7', 'D:/luzheng/backtest_result7'],
        'luzheng/StratsPlatform8': ['D:/dailyjob/luzheng/StratsPlatform8', 'D:/luzheng/backtest_result8'],
        'luzheng/StratsPlatform9': ['D:/dailyjob/luzheng/StratsPlatform9', 'D:/luzheng/backtest_result9'],
        'luzheng/StratsPlatform10': ['D:/dailyjob/luzheng/StratsPlatform10', 'D:/luzheng/backtest_result10'],
        'luzheng/StratsPlatform11': ['D:/dailyjob/luzheng/StratsPlatform11', 'D:/luzheng/backtest_result11'],
        'luzheng/StratsPlatform12': ['D:/dailyjob/luzheng/StratsPlatform12', 'D:/luzheng/backtest_result12']}

    def __init__(self, folder_name, strategy_name):
        self.folder_name = folder_name
        self.strategy_name = strategy_name

        backtest_base_path = self.backtest_path_dict[folder_name][0]
        self.backtest_result_file_folder = self.backtest_path_dict[folder_name][1]

        self.cpp_exe_path = '%s/Release_%s/BackTestCpp.exe' % (backtest_base_path, strategy_name)
        self.cpp_log_path = '%s/Release_%s/log' % (backtest_base_path, strategy_name)
        self.backtest_log_folder = '%s/Release_%s/log' % (backtest_base_path, strategy_name)

        self.backtestmonitor_exe_file = '%s/BackTestMonitor/BackTestMonitor.exe' % backtest_base_path
        self.minbar_target_folder_base = '%s/bars' % backtest_base_path
        self.quotes_target_folder_base = '%s/quotes' % backtest_base_path
        self.batch_file_path = backtest_base_path + '/batch_file/backtest_batch_file.txt'

        server_root_path = '/'.join(backtest_base_path.split('/')[:-1])
        self.state_file_folder = server_root_path + '/state_file/'
        self.state_file_trash_folder = server_root_path + '/state_file/Trash/'

        self.ip = local_ip
        server_name = folder_name.split('/')[0]
        self.stratsplatform_num = folder_name.split('/')[1].replace('StratsPlatform', '')
        self.port_num = server_port_num_dict[server_name]


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.10.188', 8888))
    s.register_function(backtest_automation_job)
    s.serve_forever()