import os
import zmq
import six
import zlib
import time
import json
import shutil
import datetime
import subprocess
import multiprocessing
from eod_aps.job import *
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from SimpleXMLRPCServer import SimpleXMLRPCServer
from eod_aps.model.AllProtoMsg_pb2 import ServerParameterChangeRequestMsg, TradeInfoRequestMsg, TradeInfoResponseMsg


date_utils = DateUtils()
email_utils = EmailUtils(EmailUtils.group3)

# rename list in ZCE
rename_list = ['CF', 'FG', 'JR', 'LR', 'MA', 'OI', 'PM', 'RI', 'RM', 'RS', 'SF', 'SM', 'SR', 'TA', 'WH', 'ZC']

# month eng num dict
month_eng_num_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                      'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

# seconds shift dict
seconds_shift_dict = dict()
seconds_shift_dict['nanhua_web'] = '45'
seconds_shift_dict['zhongxin'] = '49'
seconds_shift_dict['luzheng'] = '55'

# server_name_list
server_name_list = ['nanhua_web', 'zhongxin', 'luzheng']
# server_name_list = ['nanhua_web',]

# net constant
ip_str = '172.16.11.113'
port_base = 17000
server_port_group_dict = dict()
server_port_group_dict['nanhua_web'] = 1
server_port_group_dict['zhongxin'] = 2
server_port_group_dict['luzheng'] = 3


def clear_folder(folder):
    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder):
            for name in files:
                os.remove(folder + name)
            for name in dirs:
                shutil.rmtree(folder + name)
    else:
        os.mkdir(folder)


def rename_filter(file_dir):
    if 'ZCE' in file_dir:
        ticker_name_temp = file_dir.replace('ZCE', '')
        ticker_name = ''
        ticker_month = ''
        for i in ticker_name_temp:
            if i.isalpha():
                ticker_name += i
            if i.isdigit():
                ticker_month += i
        if ticker_name in rename_list:
            return 'ZCE' + ticker_name + ticker_month[1:]
        else:
            return file_dir
    else:
        return file_dir


def rename_file_folder(folder_path):
    for file_name in os.listdir(folder_path):
        for rename_ticker in rename_list:
            if rename_ticker in file_name:
                if file_name.split(rename_ticker)[0] == '':
                    new_file_name = rename_ticker + rename_ticker.join(file_name.split(rename_ticker)[1:])[1:]
                    os.rename(folder_path + file_name, folder_path + new_file_name)


def download_backtest_data():
    if not os.path.exists(LOCAL_BACKTEST_DATA_PATH_BASE):
        os.makedirs(LOCAL_BACKTEST_DATA_PATH_BASE)
    for server_name in server_name_list:
        # build source data folder path
        if server_name == 'nanhua_web':
            source_backtest_data_bar_path = SOURCE_BACKTEST_DATA_PATH_BASE + \
                                            'data_history/BAR/%ss/' % seconds_shift_dict[server_name]
            source_backtest_data_quote_path = SOURCE_BACKTEST_DATA_PATH_BASE + 'data_history/QUOTE/'
        else:
            source_backtest_data_bar_path = SOURCE_BACKTEST_DATA_PATH_BASE + \
                                            'data_history_%s/BAR/%ss/' % (server_name, seconds_shift_dict[server_name])
            source_backtest_data_quote_path = SOURCE_BACKTEST_DATA_PATH_BASE + 'data_history_%s/QUOTE/' % server_name

        # build local data folder path
        local_backtest_data_path = LOCAL_BACKTEST_DATA_PATH_BASE + '%s/' % server_name
        local_backtest_data_bar_path = LOCAL_BACKTEST_DATA_PATH_BASE + '%s/' % server_name + 'bars/'
        local_backtest_data_quote_path = LOCAL_BACKTEST_DATA_PATH_BASE + '%s/' % server_name + 'quotes/'
        if not os.path.exists(local_backtest_data_path):
            os.makedirs(local_backtest_data_path)
        if not os.path.exists(local_backtest_data_quote_path):
            os.makedirs(local_backtest_data_quote_path)
        if os.path.exists(local_backtest_data_bar_path):
            shutil.rmtree(local_backtest_data_bar_path)

        # download data
        print 'downloading bar data for %s' % server_name
        shutil.copytree(source_backtest_data_bar_path, local_backtest_data_bar_path)
        rename_file_folder(local_backtest_data_bar_path)

        print 'downloading quote data for %s' % server_name
        for file_dir_temp in os.listdir(source_backtest_data_quote_path):
            source_ticker_backtest_data_quote_path = source_backtest_data_quote_path + file_dir_temp + '/'
            if os.path.isdir(source_ticker_backtest_data_quote_path):
                file_dir = rename_filter(file_dir_temp)
                local_ticker_backtest_data_quote_path = local_backtest_data_quote_path + file_dir + '/'
                print source_ticker_backtest_data_quote_path, local_ticker_backtest_data_quote_path
                if not os.path.exists(local_ticker_backtest_data_quote_path):
                    os.makedirs(local_ticker_backtest_data_quote_path)

                for file_name in os.listdir(source_ticker_backtest_data_quote_path)[-10:]:
                    if file_name.split('.')[0] > '20150301':
                        print 'downloading %s...' % (source_ticker_backtest_data_quote_path + file_name)
                        shutil.copyfile(source_ticker_backtest_data_quote_path + file_name,
                                        local_ticker_backtest_data_quote_path + file_name)

                for file_name in os.listdir(source_ticker_backtest_data_quote_path):
                    if file_name.split('.')[0] > '20150301':
                        if not os.path.exists(local_ticker_backtest_data_quote_path + file_name):
                            print 'downloading %s...' % (source_ticker_backtest_data_quote_path + file_name)
                            shutil.copyfile(source_ticker_backtest_data_quote_path + file_name,
                                            local_ticker_backtest_data_quote_path + file_name)


def download_backtest_info():
    if os.path.exists(LOCAL_BACKTEST_INFO_PATH):
        shutil.rmtree(LOCAL_BACKTEST_INFO_PATH)
    shutil.copytree(SOURCE_BACKTEST_INFO_PATH, LOCAL_BACKTEST_INFO_PATH)


def get_ticker_future_name(ticker_name):
    ticker_future_name = ''
    for i in ticker_name.split(' ')[0]:
        if i.isalpha():
            ticker_future_name += i
    return ticker_future_name


def get_future_strategy_list(change_month_future):
    future_strategy_list = []
    for file_name in os.listdir(LOCAL_BACKTEST_INFO_PATH + 'backtest_info_str/'):
        instance_name = ''
        fr = open(LOCAL_BACKTEST_INFO_PATH + 'backtest_info_str/%s' % file_name)
        for line in fr.readlines():
            if line.strip() == '':
                continue
            strategy_backtest_info_str = line.strip()
            instance_name = strategy_backtest_info_str.split(',')[2]
            break
        future_name = get_ticker_future_name(instance_name)
        if future_name.upper() == change_month_future.upper():
            future_strategy_list.append(file_name.replace('.csv', ''))
        fr.close()
    return future_strategy_list


def delete_state_file_by_strategy(strategy_name_list):
    for server_name in server_name_list:
        for file_name in os.listdir(STATE_FILE_FOLDER_BASE + '%s/' % server_name):
            file_strategy_name = file_name.split('_20')[0]
            if file_strategy_name in strategy_name_list:
                print "delete state file: %s success!" % (STATE_FILE_FOLDER_BASE + '%s/' % server_name + file_name)
                os.remove(STATE_FILE_FOLDER_BASE + '%s/' % server_name + file_name)


def delete_state_file_by_time(delete_start_date_str):
    for server_name in server_name_list:
        for file_name in os.listdir(STATE_FILE_FOLDER_BASE + '%s/' % server_name):
            file_date_str = file_name.split('_state_file.txt')[0][-14:-6]
            if int(file_date_str) >= int(delete_start_date_str):
                print "delete state file: %s success!" % (STATE_FILE_FOLDER_BASE + '%s/' % server_name + file_name)
                os.remove(STATE_FILE_FOLDER_BASE + '%s/' % server_name + file_name)


def delete_state_file_change_month():
    today_str = date_utils.get_today_str('%Y-%m-%d')
    change_month_info_file_path = CHANGE_MONTH_INFO_PATH + 'future_main_contract_change_info_%s.csv' % today_str
    if not os.path.exists(change_month_info_file_path):
        return
    fr = open(change_month_info_file_path)
    for line in fr.readlines():
        if line.strip() == '':
            continue
        change_month_future = line.split(',')[0].upper()
        future_strategy_list = get_future_strategy_list(change_month_future)
        for future_strategy in future_strategy_list:
            delete_state_file_by_strategy(future_strategy)
    fr.close()


def delete_state_file_weekend():
    if datetime.datetime.now().weekday() == 5:
        delete_state_file_by_time('20160101')


def clear_insert_sql_folder():
    if os.path.exists(STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE):
        clear_folder(STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE)
    # os.makedirs(state_insert_sql_save_folder_path_base)


def get_strategy_name_list():
    strategy_name_list = []
    fr = open(LOCAL_BACKTEST_INFO_PATH + 'strategy_name_list/strategy_name_list.csv')
    for line in fr.readlines():
        if line.strip() == '':
            continue
        strategy_name = line.strip()
        strategy_name_list.append(strategy_name)
    fr.close()
    return strategy_name_list


def get_strategy_group_dict():
    strategy_group_dict = dict()
    fr = open(LOCAL_BACKTEST_INFO_PATH + 'strategy_group_str/strategy_group_str.csv')
    for line in fr.readlines():
        if line.strip() == '':
            continue
        strategy_name = line.strip().split(',')[0]
        group_number = int(line.strip().split(',')[1])
        if datetime.datetime.now().weekday() == 5:
            if 'PairTrading' in strategy_name:
                continue
        if group_number in strategy_group_dict:
            strategy_group_dict[group_number].append(strategy_name)
        else:
            strategy_group_dict[group_number] = [strategy_name, ]
    fr.close()
    return strategy_group_dict


def get_strategy_parameter_str(strategy_name):
    strategy_parameter_str = ''
    fr = open(LOCAL_BACKTEST_INFO_PATH + 'backtest_parameter_str/%s.txt' % strategy_name)
    for line in fr.readlines():
        if line.strip() == '':
            continue
        strategy_parameter_str = line.strip()
    fr.close()
    return strategy_parameter_str


class BacktestInfo(object):
    assembly_name = None
    strategy_name = None
    instance_name = None
    data_type = None
    date_num = None


def get_strategy_backtest_info(strategy_name):
    backtest_info = BacktestInfo()
    fr = open(LOCAL_BACKTEST_INFO_PATH + 'backtest_info_str/%s.csv' % strategy_name)
    for line in fr.readlines():
        if line.strip() == '':
            continue
        strategy_backtest_info_str = line.strip()
        backtest_info.assembly_name = strategy_backtest_info_str.split(',')[0]
        backtest_info.strategy_name = strategy_backtest_info_str.split(',')[1]
        backtest_info.instance_name = strategy_backtest_info_str.split(',')[2]
        backtest_info.data_type = strategy_backtest_info_str.split(',')[3]
        backtest_info.date_num = strategy_backtest_info_str.split(',')[4]
    fr.close()
    return backtest_info


def get_latest_state_file(strategy_name, server_name):
    state_file_folder = STATE_FILE_FOLDER_BASE + server_name + '/'
    # if not os.path.exists(state_file_folder):
    #     os.makedirs(state_file_folder)
    state_file_list = []
    for file_name in os.listdir(state_file_folder):
        if file_name.split('_20')[0] == strategy_name:
            state_file_list.append(file_name)
    if len(state_file_list) == 0:
        return None
    else:
        latest_state_file = sorted(state_file_list)[-1]
        latest_state_file_path = state_file_folder + latest_state_file
    return latest_state_file_path


def try_delete_history_log(log_folder):
    if os.path.exists(log_folder):
        for root, dirs, files in os.walk(log_folder):
            for file_name in files:
                try:
                    os.remove(log_folder + file_name)
                except:
                    pass
            for file_name in dirs:
                try:
                    shutil.rmtree(log_folder + file_name)
                except:
                    pass
    else:
        os.makedirs(log_folder)


def start_backtestcpp_server(strategy_name, exe_file_path):
    (file_path, exe_file_name) = os.path.split(exe_file_path)
    os.chdir(file_path)
    child = subprocess.Popen(exe_file_path)
    print 'start server:%s,pid:%s' % (strategy_name, child.pid)
    return child


def send_batch_file_info(port_number, backtest_cmd_arg):
    context = zmq.Context().instance()
    print "Connecting to aggregator server:%s" % ip_str
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b'%s_test' % ip_str)
    socket.connect('tcp://%s:%s' % (ip_str, port_number))

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


def stop_exe_server(strategy_name, exe_server):
    exe_server.kill()
    print 'stop server:', strategy_name


def save_state_into_file(strategy_name, strategy_state_value, server_name):
    state_file_folder = STATE_FILE_FOLDER_BASE + server_name + '/'
    # if not os.path.exists(state_file_folder):
    #     os.makedirs(state_file_folder)

    datetime_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = '%s_%s_state_file.txt' % (strategy_name, datetime_str)
    state_file_save_path = state_file_folder + file_name

    fr = open(state_file_save_path, 'w+')
    fr.write(strategy_state_value)
    fr.close()


def save_state_into_share_disk(strategy_name, strategy_state_value, server_name):
    # if not os.path.exists(backtest_result_folder_path_base + '%s/' % server_name):
    #     os.makedirs(backtest_result_folder_path_base + '%s/' % server_name)

    file_name = '%s_state_file.txt' % strategy_name
    state_file_save_path = BACKTEST_RESULT_FOLDER_PATH_BASE + '%s/' % server_name + file_name
    strategy_state_value_str = strategy_state_value.replace(', ', ', \n').replace('{', '').replace('}', '')
    fr = open(state_file_save_path, 'w+')
    fr.write(strategy_state_value_str)
    fr.close()


def save_state_insert_sql(strategy_name, strategy_state_value, server_name):
    # if not os.path.exists(state_insert_sql_save_folder_path_base + '%s/' % server_name):
    #     os.makedirs(state_insert_sql_save_folder_path_base + '%s/' % server_name)

    state_insert_sql_base = '''Insert Into strategy.strategy_state(TIME,NAME,VALUE) VALUES(sysdate(),'%s','%s')'''
    state_insert_sql = state_insert_sql_base % (strategy_name, strategy_state_value)
    file_name = '%s_state_insert_sql.txt' % strategy_name
    state_insert_sql_file_path = STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE + '%s/' % server_name + file_name
    fr = open(state_insert_sql_file_path, 'w+')
    fr.write(state_insert_sql)
    fr.close()


def run_backtest(strategy_name, group_number, server_name):
    strategy_parameter_str = get_strategy_parameter_str(strategy_name)
    strategy_backtest_info = get_strategy_backtest_info(strategy_name)

    latest_state_file_path = get_latest_state_file(strategy_name, server_name)
    state_dict = dict()
    if latest_state_file_path is None:
        start_time_flag = False
    else:
        state_content = ''
        fr = open(latest_state_file_path)
        for line in fr.readlines():
            if line.strip() == '':
                continue
            state_content += line.replace('\n', '')
        fr.close()
        state_dict = json.loads(state_content)
        if 'state_time' not in state_dict:
            start_time_flag = False
        else:
            start_time_flag = True

    if start_time_flag:
        backtest_cmd_base = 'BackTest --StratsName=%s --WatchList=%s --Parameter=%s --StartDate=%s --EndDate=%s ' \
                            '--StartTime=%s --StateFile=%s --AssemblyName=%s --Parallel=0'
        end_date = date_utils.get_next_trading_day('%Y-%m-%d')
        start_date = state_dict['state_time'].split(' ')[0]
        start_time = state_dict['state_time'].split(' ')[1]

        backtest_cmd_arg = backtest_cmd_base % (
            strategy_backtest_info.strategy_name, strategy_backtest_info.instance_name, strategy_parameter_str,
            start_date, end_date, start_time, latest_state_file_path, strategy_backtest_info.assembly_name)

    else:
        backtest_cmd_base = 'BackTest --StratsName=%s --WatchList=%s --Parameter=%s --StartDate=%s --EndDate=%s ' \
                            '--AssemblyName=%s --Parallel=0'
        end_time = date_utils.get_next_trading_day('%Y-%m-%d')

        start_time = (datetime.datetime.now() + datetime.timedelta(days=-int(strategy_backtest_info.date_num)))\
            .strftime('%Y-%m-%d')

        backtest_cmd_arg = backtest_cmd_base % (
            strategy_backtest_info.strategy_name, strategy_backtest_info.instance_name, strategy_parameter_str,
            start_time, end_time, strategy_backtest_info.assembly_name)
    print backtest_cmd_arg

    backtestcpp_path = BACKTESTCPP_PATH_BASE + '%s/Release_%s_%s/BackTestCpp.exe' % \
                                               (server_name, strategy_backtest_info.strategy_name, str(group_number))
    backtestcpp_log_folder = BACKTESTCPP_PATH_BASE + '%s/Release_%s_%s/log/' % \
                                               (server_name, strategy_backtest_info.strategy_name, str(group_number))
    try_delete_history_log(backtestcpp_log_folder)
    stratsplatform_server = start_backtestcpp_server(strategy_name, backtestcpp_path)
    time.sleep(3)
    port_number = str(port_base + server_port_group_dict[server_name] * 100 + group_number)
    state_result = send_batch_file_info(port_number, backtest_cmd_arg)
    print state_result

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
    print strategy_state_value

    stop_exe_server(strategy_name, stratsplatform_server)

    if 'state_time' in strategy_state_dict:
        if strategy_state_dict['state_time'] == 'not-a-date-time':
            print 'no market data, continue...'
            return None

    save_state_into_file(strategy_name, strategy_state_value, server_name)
    save_state_into_share_disk(strategy_name, strategy_state_value, server_name)

    # generate state insert sql
    save_state_insert_sql(strategy_name, strategy_state_value, server_name)


def __backtest_automation_job(group_number, group_name_list, strategy_name_list, server_name):
    for group_name in group_name_list:
        group_strategy_name_list = []
        for strategy_name in strategy_name_list:
            if strategy_name.split('.')[0] == group_name:
                group_strategy_name_list.append(strategy_name)
        for group_strategy_name in group_strategy_name_list:
            run_backtest(group_strategy_name, group_number, server_name)


def get_pair_trading_strategy_list(strategy_name_list):
    pair_trading_strategy_list = []
    for strategy_name in strategy_name_list:
        if 'PairTrading' in strategy_name:
            pair_trading_strategy_list.append(strategy_name)
    return pair_trading_strategy_list


def get_backtest_strategy_list(server_name):
    backtest_strategy_list = []
    if not os.path.exists(STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE + '%s/' % server_name):
        return backtest_strategy_list
    for file_name in os.listdir(STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE + '%s/' % server_name):
        strategy_name = file_name.replace('_state_insert_sql.txt', '')
        backtest_strategy_list.append(strategy_name)
    return backtest_strategy_list


def build_email_html(strategy_name_list):
    email_content_list = []
    email_content_head = r'Z:/dailyjob/backtest_result/'
    email_content_list.append(email_content_head)
    table_list = '<table border="1">'
    table_header = '<tr><th align="center" font-size:12px; bgcolor="#70bbd9"><b>strategy name</b></th>'
    for server_name in server_name_list:
        table_header += '<th align="center" font-size:12px; bgcolor="#70bbd9"><b>%s</b></th>' % server_name
    table_header += '</tr>'

    table_list += table_header
    for strategy in strategy_name_list:
        table_line = '<tr><td align="center" font-size:12px; bgcolor="#ee4c50"><b>%s</b></td>' % strategy
        for server_name in server_name_list:
            backtest_strategy_list = get_backtest_strategy_list(server_name)
            if strategy in backtest_strategy_list:
                table_line += '<td align="center" font-size:8px;>Checked</td>'
            else:
                table_line += '<td align="center" font-size:8px style="color:red;";>Not Calculated!</td>'
        table_line += '</tr>'
        table_list += table_line
    table_list += '</table>'
    email_content_list.append(table_list)
    return email_content_list


def backtest_automation_job():
    today_str = date_utils.get_today_str('%Y%m%d')
    delete_state_file_by_time(today_str)
    __backtest_automation()
    return 0


def pre_make_directions(server_name):
    # state file folder
    state_file_folder = STATE_FILE_FOLDER_BASE + server_name + '/'
    if not os.path.exists(state_file_folder):
        os.makedirs(state_file_folder)

    # backtest result folder
    if not os.path.exists(BACKTEST_RESULT_FOLDER_PATH_BASE + '%s/' % server_name):
        os.makedirs(BACKTEST_RESULT_FOLDER_PATH_BASE + '%s/' % server_name)

    # insert sql folder
    if not os.path.exists(STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE + '%s/' % server_name):
        os.makedirs(STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE + '%s/' % server_name)


def __backtest_automation(strategy_name_list=None):
    if strategy_name_list is None:
        strategy_name_list = []
    # download data
    download_backtest_data()

    # download backtest info
    download_backtest_info()

    # delete state files
    delete_state_file_change_month()
    delete_state_file_weekend()

    # clear insert sql folder
    clear_insert_sql_folder()

    # get backtest info
    if not strategy_name_list:
        strategy_name_list = get_strategy_name_list()
    backtest_strategy_dict = get_strategy_group_dict()

    # backtest calculation
    for server_name in server_name_list:
        pre_make_directions(server_name)
        processes = []
        for (group_number, group_name_list) in backtest_strategy_dict.items():
            p = multiprocessing.Process(target=__backtest_automation_job, args=(group_number, group_name_list,
                                                                                strategy_name_list, server_name))
            processes.append(p)
        for p in processes:
            p.start()
        for p in processes:
            p.join()

    # backtest PairTrading weekend
    if datetime.datetime.now().weekday() == 5:
        for server_name in server_name_list:
            pair_trading_strategy_list = get_pair_trading_strategy_list(strategy_name_list)
            processes = []
            group_number = 90
            for pairtrading_strategy in pair_trading_strategy_list:
                group_number += 1
                p = multiprocessing.Process(target=run_backtest, args=(pairtrading_strategy, group_number, server_name))
                processes.append(p)
            for p in processes:
                p.start()
            for p in processes:
                p.join()

    # send email
    email_content_list = build_email_html(strategy_name_list)
    email_utils.send_email_group_all('BackTest Result', '\n\n\n'.join(email_content_list), 'html')


if __name__ == "__main__":
    s = SimpleXMLRPCServer(('172.16.11.113', 8888))
    s.register_function(backtest_automation_job)
    s.serve_forever()

