# -*- coding: utf-8 -*-
# 每日下载ctp的行情数据，并存储至磁盘整列，进行分割后发布至VirtualExchange

import os
from eod_aps.model.instrument_history import InstrumentHistory
from eod_aps.model.quote_bar_info import QuoteBarInfo
from eod_aps.job import *

instrument_history_dict = dict()
trading_time_dict = dict()

LOCAL_QUOTES_FOLDER = 'H:/data_history/VEX/quotes_base'


def __build_ticker_exchange():
    server_model = server_constant.get_server_model('host')
    session_history = server_model.get_db_session('history')
    query = session_history.query(InstrumentHistory)
    for instrument_history_db in query.filter(InstrumentHistory.type_id == 1):
        if instrument_history_db.exchange_id == 22:
            ticker_key = instrument_history_db.ticker[:2] + instrument_history_db.ticker[3:]
        else:
            ticker_key = instrument_history_db.ticker
        instrument_history_dict[ticker_key] = instrument_history_db

        trading_time_list = []
        if instrument_history_db.thours is None:
            continue
        for trading_time in instrument_history_db.thours.replace('(', '').replace(')', '').split(';'):
            (start_time, end_time) = trading_time.split(',')
            if __count_time_number(start_time) > __count_time_number(end_time):
                trading_time_list.append((start_time, '24:00'))
                trading_time_list.append(('00:00', end_time))
            else:
                trading_time_list.append((start_time, end_time))
        trading_time_dict[ticker_key] = trading_time_list
    server_model.close()


def __analytical_ctp_market_file(file_path, date_filter_str):
    if not os.path.exists(file_path):
        task_logger.error('miss file:%s' % file_path)
        return

    task_logger.info('start read file:%s' % file_path)
    fr = open(file_path, 'r')
    try:
        for line in fr.xreadlines():
            message_item = line.replace('\n', '').replace('	', '').split(',')
            if len(message_item) < 10:
                continue

            quote_bar_info = QuoteBarInfo()
            ticker = message_item[1]
            quote_bar_info.ticker = ticker

            date_str = message_item[43]
            date_time = '%s-%s-%s %s.%s0000' % (
                date_str[:4], date_str[4:6], date_str[6:8], message_item[20], message_item[21].zfill(3))

            # 日盘数据头几条可能包含前日夜盘的数据,进行拼接后导致2016-05-26的quote文件中出现2016-05-26 11:30:00,需要过滤掉,
            # 避免最终quote文件的时间戳不是顺序增长
            if date_time > date_filter_str + ' 15:30:00.0000000':
                continue

            quote_bar_info.date_time = date_time
            quote_bar_info.price = message_item[4]
            quote_bar_info.volume = message_item[11]
            quote_bar_info.bid1 = __rebuild_value(message_item[22])
            quote_bar_info.bid_size1 = __rebuild_value(message_item[23])
            quote_bar_info.ask1 = __rebuild_value(message_item[24])
            quote_bar_info.ask_size1 = __rebuild_value(message_item[25])

            prev_close = __rebuild_value(message_item[6])
            nominal_price = prev_close
            if quote_bar_info.price > 0:
                if quote_bar_info.price <= quote_bar_info.bid1:
                    nominal_price = quote_bar_info.bid1
                elif quote_bar_info.ask1 > quote_bar_info.bid1:
                    if quote_bar_info.price > quote_bar_info.ask1:
                        nominal_price = quote_bar_info.ask1
                    else:
                        nominal_price = quote_bar_info.price
                else:
                    nominal_price = quote_bar_info.price
            else:
                if prev_close <= quote_bar_info.bid1:
                    nominal_price = quote_bar_info.bid1
                elif quote_bar_info.ask1 > quote_bar_info.bid1:
                    if prev_close > quote_bar_info.ask1:
                        nominal_price = quote_bar_info.ask1
                    else:
                        nominal_price = prev_close
                else:
                    nominal_price = prev_close
            quote_bar_info.nominal_price = nominal_price

            if ticker in quote_dict:
                quote_dict[ticker].append(quote_bar_info)
            else:
                quote_dict[ticker] = [quote_bar_info]
    finally:
        fr.close()


def __rebuild_value(data_value):
    if data_value == '0.000000':
        data_value = '0'
    return data_value


# save_type=1:覆盖写入，2：叠加写入
def __save_quote_file(ticker, date_str, line_array, save_type):
    if len(line_array) == 0:
        return

    if ticker in instrument_history_dict:
        instrument_history_db = instrument_history_dict[ticker]
        if instrument_history_db.exchange_id == 20:
            exchange_name = 'SHF'
        if instrument_history_db.exchange_id == 21:
            exchange_name = 'DCE'
        if instrument_history_db.exchange_id == 22:
            exchange_name = 'ZCE'
            if len(ticker) == 5:
                ticker = ticker[:2] + '1' + ticker[2:]
        if instrument_history_db.exchange_id == 25:
            exchange_name = 'CFF'
    else:
        task_logger.error('%s not found' % ticker)
        return

    file_save_path = '%s/%s%s' % (LOCAL_QUOTES_FOLDER, exchange_name, ticker)
    if not os.path.exists(file_save_path):
        os.mkdir(file_save_path)

    file_path = '%s/%s.csv' % (file_save_path, date_str.replace('-', ''))
    if save_type == 1:
        file_object = open(file_path, 'w')
        file_object.write('\n'.join(line_array))
    elif save_type == 2:
        file_object = open(file_path, 'a')
        file_object.write('\n'.join(line_array) + '\n')
    file_object.close()
    file_object.close()


def __quote_filter(ticker, data_list):
    filter_bar_list = []
    for quote_bar_info in data_list:
        # if __is_trading_time(quote_bar_info.date_time, ticker):
        #     temp_quote_info = quote_bar_info.copy()
        filter_bar_list.append(quote_bar_info.to_quote_str())
    return filter_bar_list


def __is_trading_time(date_time, ticker):
    trading_time_flag = False
    if not date_utils.is_trading_day(str(date_time)[:10]):
        return trading_time_flag

    if ticker in trading_time_dict:
        trading_time_list = trading_time_dict[ticker]
    else:
        task_logger.info('unfind ticker:%s trading time' % ticker)
        trading_time_list = [('9:00', '10:15'), ('10:30', '11:30'), ('13:30', '15:00'), ('21:00', '23:30')]

    for (start_time, end_time) in trading_time_list:
        if __count_time_number(start_time) <= __count_time_number(str(date_time)[11:16]) < __count_time_number(
                end_time):
            trading_time_flag = True
            break
    return trading_time_flag


def __count_time_number(time_str):
    return int(time_str.split(':')[0]) * 60 + int(time_str.split(':')[1])


def comp(x, y):
    x_items = x[x.rfind('_') + 1:].replace('.txt', '')
    y_items = y[y.rfind('_') + 1:].replace('.txt', '')

    if int(x_items) < int(y_items):
        return -1
    elif int(x_items) > int(y_items):
        return 1
    else:
        return 0


def deal_ctp_message_file():
    global quote_dict
    quote_dict = dict()

    global today_filter_str
    today_filter_str = date_utils.get_today_str('%Y-%m-%d')

    task_logger.info('Enter deal_ctp_message_file_job.')
    __build_ticker_exchange()

    validate_time = long(date_utils.get_today_str('%H%M%S'))
    if validate_time > 153000:
        ctp_market_file_name = 'CTP_Market_%s_1.txt' % today_filter_str
    else:
        last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d')
        ctp_market_file_name = 'CTP_Market_%s_2.txt' % last_trading_day

    market_file_path = '%s/%s' % (CTP_DATA_BACKUP_PATH, ctp_market_file_name)
    __analytical_ctp_market_file(market_file_path, today_filter_str)

    for (ticker_str, quote_data_list) in quote_dict.items():
        quote_data_list = __quote_filter(ticker_str, quote_data_list)
        __save_quote_file(ticker_str, today_filter_str, quote_data_list, 2)
    task_logger.info('Exit deal_ctp_message_file_job.')


def deal_ctp_message_file_history(date_filter_str):
    global quote_dict
    quote_dict = dict()
    task_logger.info('Enter deal_ctp_message_file_job.')
    __build_ticker_exchange()

    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
    ctp_market_file_1 = 'CTP_Market_%s_2.txt' % (last_trading_day,)
    ctp_market_file_2 = 'CTP_Market_%s_1.txt' % (date_filter_str,)

    for ctp_market_file_name in (ctp_market_file_1, ctp_market_file_2):
        market_file_path = '%s/%s' % (CTP_DATA_BACKUP_PATH, ctp_market_file_name)
        __analytical_ctp_market_file(market_file_path, date_filter_str)

    for (ticker_str, quote_data_list) in quote_dict.items():
        quote_data_list = __quote_filter(ticker_str, quote_data_list)
        __save_quote_file(ticker_str, date_filter_str, quote_data_list, 1)
    task_logger.info('Exit deal_ctp_message_file_job.')


if __name__ == '__main__':
    deal_ctp_message_file_history('2016-09-14')

