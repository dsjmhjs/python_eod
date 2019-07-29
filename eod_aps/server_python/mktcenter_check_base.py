# -*- coding: utf-8 -*-
# 行情中心重建校验
import os
import datetime
import time
import subprocess
import sys
from eod_aps.model.BaseModel import *

filterFiled = 'ExchangeID,PreClosePrice,OpenPrice,ClosePrice,IOPV,YieldToMaturity,HighPrice,LowPrice,LastPrice,\
TradeCount,TotalTradeVolume,TotalTradeValue,TotalBidVolume,WeightedAvgBidPrice,AltWeightedAvgBidPrice,TotalOfferVolume,\
WeightedAvgOfferPrice,AltWeightedAvgOfferPrice,BidPriceLevel,OfferPriceLevel,Rebuilt,OfferCount1,OfferCount2,\
OfferCount3,OfferCount4,OfferCount5,OfferCount6,OfferCount7,OfferCount8,OfferCount9,OfferCountA,BidCount1,BidCount2,\
BidCount3,BidCount4,BidCount5,BidCount6,BidCount7,BidCount8,BidCount9,BidCountA'

cfg_file_path = '/home/trader/apps/MktdtCtr/cfg'
cfg_file_name_list = ('rba_instruments.csv', 'rbb_instruments.csv', 'rbc_instruments.csv')
data_base_path = '/home/trader/apps/MktdtCtr/data'
mktdt_center_path = '/home/trader/dailyjob/MktdtCtr'

now = datetime.datetime.now()


def save_file(file_path, content, add_flag):
    a = '\n'
    if add_flag:
        file_object = open(file_path, 'a')
    else:
        file_object = open(file_path, 'w+')
    file_object.write(a.join(content))
    file_object.close()


def compare_date_time(start_t, end_t):
    s_time = time.mktime(start_t.timetuple())
    e_time = time.mktime(end_t.timetuple())

    if float(s_time) >= float(e_time):
        return False
    return True


def build_recapture_config_file(ticker):
    del_commond = 'cd %s;rm -rf rebuild_mktdt_etf_tcp_%s.dat' % (data_base_path, todayStr)
    print 'commond:', del_commond
    return_code = subprocess.call(del_commond, shell=True)
    print return_code

    message = []
    config_path = '%s/cfg/config_base_recapture.ini' % (mktdt_center_path,)
    fr = open(config_path)
    for line in fr.readlines():
        if 'export_filename' in line:
            rebuild_file_name = 'rebuild_mktdt_etf_tcp_%s' % (todayStr,)
            line += '%s/%s' % (data_base_path, rebuild_file_name)
        elif 'interested_instruments' in line:
            line += ticker + ','
        elif 'import_file' in line:
            line += '%s/%s' % (data_base_path, mktcenter_file_name)
        message.append(line.replace('\n', ''))
    file_path = '%s/cfg/config_mkt_recapture.ini' % (mktdt_center_path,)
    save_file(file_path, message, False)
    return rebuild_file_name


def build_config_file(ticker):
    message = []
    config_path = '%s/cfg/config_base_rb_2.ini' % (mktdt_center_path,)
    fr = open(config_path)
    for line in fr.readlines():
        if 'interested_instruments' in line:
            line += ticker + ','
        elif 'import_file' in line:
            line += '%s/rebuild_mktdt_etf_tcp_%s.dat' % (data_base_path, todayStr)
        message.append(line.replace('\n', ''))
    file_path = '%s/cfg/config_mkt_temp_rb_2.ini' % (mktdt_center_path,)
    save_file(file_path, message, False)


def run_mkt_center(ticker):
    os.chdir(mktdt_center_path)
    mkt_file_name = 'mkt_%s_%s.txt' % (ticker, todayStr)
    base_commond = './build64_release/libatp.mktdt/MktdtSvr --cfg=./cfg/config_mkt_temp_rb_2.ini --uc=false > ./mkt_files/%s'
    commond_str = base_commond % (mkt_file_name,)
    return_code = subprocess.call(commond_str, shell=True)
    print 'commond:', commond_str, ' return:', return_code


def file_reset(file_base_path, filename):
    file_path = '%s/%s' % (file_base_path, filename)
    fr = open(file_path)
    bid_price_mkt_1 = 0
    bid_volume_mkt_1 = 0
    offer_price_mkt_1 = 0
    offer_volume_mkt_1 = 0

    bid_price_rb_1 = 0
    bid_volume_rb_1 = 0
    offer_price_rb_1 = 0
    offer_volume_rb_1 = 0

    reset_message_list = []

    for line in fr:
        line = line.replace('\n', '')
        if 'CSecurityFtdcL2MarketDataField' in line:
            line = rebuild_message(line)
            reset_message_list.append(line)
        elif 'Security_Mktdt_L10T' in line:
            line = rebuild_message(line)
            (line, bid_price, bid_volume, offer_price, offer_volume) = rebuild_message_simple(line)
            if (bid_price_rb_1 != bid_price) or (bid_volume_rb_1 != bid_volume) or (offer_price_rb_1 != offer_price) \
                    or (offer_volume_rb_1 != offer_volume):
                bid_price_rb_1 = bid_price
                bid_volume_rb_1 = bid_volume
                offer_price_rb_1 = offer_price
                offer_volume_rb_1 = offer_volume

                reset_message_list.append(line)
            else:
                continue

        elif 'CSecurityFtdcL2OrderField' in line:
            reset_message_list.append(line)
        elif 'CSecurityFtdcL2TradeField' in line:
            reset_message_list.append(line)
        elif 'Trade Delayed' in line:
            reset_message_list.append(line)
        else:
            pass

    (save_name, save_type) = filename.split('.')

    save_filename_reset = '%s_%s.%s' % (save_name, 'reset', save_type)

    save_file_path = '%s//%s' % (file_base_path, save_filename_reset)
    save_file(save_file_path, reset_message_list, False)


def rebuild_message(message):
    item_array = message.split(',')
    for item in item_array:
        if '=' not in item:
            continue
        (key, value) = item.split('=')
        if key.strip() in filterFiled:
            message = message.replace(item + ',', '')
    return message


def rebuild_message_simple(message):
    item_array = message.split(',')
    for item in item_array:
        if '=' not in item:
            continue
        (key, value) = item.split('=')
        if key.strip() in filterFiled:
            message = message.replace(item + ',', '')

        if key.strip() == 'BidPrice1':
            bid_price1 = value
        elif key.strip() == 'BidVolume1':
            bid_volume1 = value
        elif key.strip() == 'OfferPrice1':
            offer_price1 = value
        elif key.strip() == 'OfferVolume1':
            offer_volume1 = value
    return message, bid_price1, bid_volume1, offer_price1, offer_volume1


def file_check(file_path):
    mkt_num = 0
    found_mkt_num = 0
    rebuild_mkt_num = 0
    soft_found_mkt_num = 0
    bid_price1_last = 0
    bid_volume1_last = 0
    offer_price1_last = 0
    offer_volume1_last = 0
    check_date = ''
    order_list = []
    trade_list = []
    unfound_mkt_list = []
    trade_delayed_message_list = []
    rebuild_mkt_dict = dict()

    fr = open(file_path)

    last_found = False
    interval_list = []
    unfound_time_list = []

    for line in fr.readlines():
        if ('CSecurityFtdcL2MarketDataField' in line) or ('Security_Mktdt_L10T' in line):
            item_array = line.split(',')
            mkt_model_content = BaseModel()
            for item in item_array:
                if '=' not in item:
                    if ':' in item:
                        setattr(mkt_model_content, 'Mkt_Time', item)
                    continue
                (key, value) = item.split('=')
                setattr(mkt_model_content, key.strip(), value.strip())

            if 'CSecurityFtdcL2MarketDataField' in line:
                ticker_snapshot = getattr(mkt_model_content, 'InstrumentID')
                mkt_time_snapshot = getattr(mkt_model_content, 'Mkt_Time', '')
                #                 timeStamp_snapshot = getattr(mkt_model_content, 'TimeStamp', '')
                bid_price1_snapshot = getattr(mkt_model_content, 'BidPrice1', '')
                bid_volume1_snapshot = '%.0f' % (float(getattr(mkt_model_content, 'BidVolume1', '')),)
                offer_price1_snapshot = getattr(mkt_model_content, 'OfferPrice1', '')
                offer_volume1_snapshot = '%.0f' % (float(getattr(mkt_model_content, 'OfferVolume1', '')),)

                snapshot_datetime = datetime.datetime.strptime(mkt_time_snapshot, "%Y-%m-%d %H:%M:%S.%f")
                if check_date == '':
                    check_date = snapshot_datetime.strftime('%Y-%m-%d')
                # d2 = datetime.datetime.strptime(check_date + ' 09:30:00.000001', "%Y-%m-%d %H:%M:%S.%f")
                d3 = datetime.datetime.strptime(check_date + ' 14:57:00.000001', "%Y-%m-%d %H:%M:%S.%f")
                # 晚于14:57的不处理
                if compare_date_time(d3, snapshot_datetime):
                    break

                if ticker_snapshot not in rebuild_mkt_dict:
                    continue
                else:
                    dict_value_list = rebuild_mkt_dict.get(ticker_snapshot)
                mkt_num += 1

                fund_flag = False
                bid_found = False
                ask_found = False
                if (last_found and bid_price1_last == bid_price1_snapshot and bid_volume1_last == bid_volume1_snapshot
                        and offer_price1_last == offer_price1_snapshot and offer_volume1_last == offer_volume1_snapshot):
                    fund_flag = True
                else:
                    times = len(dict_value_list)
                    while times > 0:
                        rebuild_mkt_model = dict_value_list[times - 1]
                        mkt_time_rebuild = getattr(rebuild_mkt_model, 'Mkt_Time', '')
                        rebuild_datetime = datetime.datetime.strptime(mkt_time_rebuild, "%Y-%m-%d %H:%M:%S.%f")

                        # ticker = getattr(rebuild_mkt_model, 'InstrumentID', '')
                        # timeStamp = getattr(rebuild_mkt_model, 'TimeStamp', '')
                        bid_price1 = getattr(rebuild_mkt_model, 'BidPrice1', '')
                        bid_volume1 = '%.0f' % (float(getattr(rebuild_mkt_model, 'BidVolume1', '')),)
                        offer_price1 = getattr(rebuild_mkt_model, 'OfferPrice1', '')
                        offer_volume1 = '%.0f' % (float(getattr(rebuild_mkt_model, 'OfferVolume1', '')),)

                        temp_bid_dict = dict()
                        temp_offer_dict = dict()
                        for a in ('1', '2', '3', '4', '5', '6', '7', '8', '9', 'A'):
                            temp_bid_price = getattr(rebuild_mkt_model, 'BidPrice' + a, '')
                            temp_bid_volume = '%.0f' % (float(getattr(rebuild_mkt_model, 'BidVolume' + a, '')),)
                            temp_bid_dict[temp_bid_price] = temp_bid_volume
                            temp_offer_price = getattr(rebuild_mkt_model, 'OfferPrice' + a, '')
                            temp_offer_volume = '%.0f' % (float(getattr(rebuild_mkt_model, 'OfferVolume' + a, '')),)
                            temp_offer_dict[temp_offer_price] = temp_offer_volume

                        if (bid_price1 == bid_price1_snapshot) and (bid_volume1 == bid_volume1_snapshot) \
                                and (offer_price1 == offer_price1_snapshot) and (
                                offer_volume1 == offer_volume1_snapshot):
                            [bid_price1_last, bid_volume1_last, offer_price1_last, offer_volume1_last] = [
                                bid_price1_snapshot, bid_volume1_snapshot, offer_price1_snapshot,
                                offer_volume1_snapshot]
                            last_found = True
                            fund_flag = True
                            interval_list.append((mkt_time_snapshot, mkt_time_rebuild))
                            break
                        elif (bid_price1_snapshot in temp_bid_dict) and (
                                bid_volume1_snapshot == temp_bid_dict[bid_price1_snapshot]):
                            bid_found = True
                        elif (offer_price1_snapshot in temp_offer_dict) and (
                                offer_volume1_snapshot == temp_offer_dict[offer_price1_snapshot]):
                            ask_found = True
                        times -= 1

                        if (snapshot_datetime - rebuild_datetime).seconds > 20:
                            break

                if fund_flag:
                    found_mkt_num += 1
                elif ask_found and bid_found:
                    soft_found_mkt_num += 1
                    unfound_time_list.append('soft found:\n' + line)
                else:
                    last_found = False
                    unfound_time_list.append(line)
                    unfound_mkt_list.append(mkt_model_content)

            elif 'Security_Mktdt_L10T' in line:
                rebuild_mkt_num += 1
                ticker = getattr(mkt_model_content, 'InstrumentID', '')
                if ticker in rebuild_mkt_dict:
                    rebuild_mkt_dict[ticker].append(mkt_model_content)
                else:
                    mkt_model_list = [mkt_model_content]
                    rebuild_mkt_dict[ticker] = mkt_model_list
        elif 'CSecurityFtdcL2OrderField' in line:
            item_array = line.split(',')
            mkt_model_content = BaseModel()
            for item in item_array:
                if '=' not in item:
                    if ':' in item:
                        setattr(mkt_model_content, 'Mkt_Time', item)
                    continue
                (key, value) = item.split('=')
                setattr(mkt_model_content, key.strip(), value.strip())
            order_list.append(mkt_model_content)
        elif 'CSecurityFtdcL2TradeField' in line:
            item_array = line.split(',')
            mkt_model_content = BaseModel()
            for item in item_array:
                if '=' not in item:
                    if ':' in item:
                        setattr(mkt_model_content, 'Mkt_Time', item)
                    continue
                (key, value) = item.split('=')
                setattr(mkt_model_content, key.strip(), value.strip())
            trade_list.append(mkt_model_content)
        elif 'Trade Delayed' in line:
            trade_delayed_message_list.append(line)

    # 未匹配的快照，查询是否有TimeStamp在其之前，但接收时间在其之后的order或trade
    receive_time_error_num = 0
    # 快照往后查找3秒内重建，可匹配的数目
    rebuild_later_num = 0
    delay_matching_list = []

    (rebuild_start_time1, rebuild_start_time2) = get_rebuild_start_time(trade_delayed_message_list)

    # for unfound_mkt_model in unfound_mkt_list:
    #     receive_time_error_flag = False
    #
    #     mkt_time_unfound = datetime.datetime.strptime(getattr(unfound_mkt_model, 'Mkt_Time', ''),
    #                                                   "%Y-%m-%d %H:%M:%S.%f")
    #     check_time_unfound = mkt_time_unfound + datetime.timedelta(seconds=20)
    #     timestamp_unfound = getattr(unfound_mkt_model, 'TimeStamp', '')
    #     # unfound_snapshot_datetime = datetime.datetime.strptime('%s %s' % (check_date, timestamp_unfound),
    #     #                                                        '%Y-%m-%d %H:%M:%S')
    #     ticker = getattr(unfound_mkt_model, 'InstrumentID', '')
    #     bid_price1_snapshot = getattr(unfound_mkt_model, 'BidPrice1', '')
    #     bid_volume1_snapshot = '%.0f' % (float(getattr(unfound_mkt_model, 'BidVolume1', '')),)
    #     offer_price1_snapshot = getattr(unfound_mkt_model, 'OfferPrice1', '')
    #     offer_volume1_snapshot = '%.0f' % (float(getattr(unfound_mkt_model, 'OfferVolume1', '')),)
    #
    #     rebuild_mkt_list = rebuild_mkt_dict.get(ticker)
    #     for rebuild_mkt in rebuild_mkt_list:
    #         rebuild_datetime = datetime.datetime.strptime(getattr(rebuild_mkt, 'Mkt_Time', ''),
    #                                                       "%Y-%m-%d %H:%M:%S.%f")
    #         if (compare_date_time(mkt_time_unfound, rebuild_datetime) == True) and (
    #                     (rebuild_datetime - mkt_time_unfound).seconds > 4):
    #             break
    #         elif compare_date_time(mkt_time_unfound, rebuild_datetime):
    #             # ticker = getattr(rebuild_mkt, 'InstrumentID', '')
    #             # timeStamp = getattr(rebuild_mkt, 'TimeStamp', '')
    #             bid_price1 = getattr(rebuild_mkt, 'BidPrice1', '')
    #             bid_volume1 = '%.0f' % (float(getattr(rebuild_mkt, 'BidVolume1', '')),)
    #             offer_price1 = getattr(rebuild_mkt, 'OfferPrice1', '')
    #             offer_volume1 = '%.0f' % (float(getattr(rebuild_mkt, 'OfferVolume1', '')),)
    #             if (bid_price1 == bid_price1_snapshot) and (bid_volume1 == bid_volume1_snapshot) and (
    #                         offer_price1 == offer_price1_snapshot) and (offer_volume1 == offer_volume1_snapshot):
    #                 delay_matching_message = 'found delay matching---snapshot_datetime:%s rebuild_datetime:%s' % (
    #                     mkt_time_unfound, rebuild_datetime)
    #                 print delay_matching_message
    #                 delay_matching_list.append(delay_matching_message)
    #                 rebuild_later_num += 1
    #                 break
    #
    #     for order_content in order_list:
    #         mkt_time = datetime.datetime.strptime(getattr(order_content, 'Mkt_Time', ''), "%Y-%m-%d %H:%M:%S.%f")
    #         if compare_date_time(mkt_time, mkt_time_unfound):
    #             continue
    #         elif compare_date_time(check_time_unfound, mkt_time):
    #             break
    #
    #         timestamp = getattr(order_content, 'OrderTime', '')
    #         if (timestamp < timestamp_unfound) and (compare_date_time(mkt_time_unfound, mkt_time) == True):
    #             receive_time_error_num += 1
    #             receive_time_error_flag = True
    #             break
    #
    #     if receive_time_error_flag:
    #         continue
    #
    #     for trade_content in trade_list:
    #         mkt_time = datetime.datetime.strptime(getattr(trade_content, 'Mkt_Time', ''), "%Y-%m-%d %H:%M:%S.%f")
    #         if compare_date_time(mkt_time, mkt_time_unfound):
    #             continue
    #         elif compare_date_time(check_time_unfound, mkt_time):
    #             break
    #
    #         timestamp = getattr(trade_content, 'TradeTime', '')
    #         if (timestamp < timestamp_unfound) and compare_date_time(mkt_time_unfound, mkt_time):
    #             receive_time_error_num += 1
    #             break
    return (mkt_num - rebuild_later_num, found_mkt_num, soft_found_mkt_num, receive_time_error_num, rebuild_mkt_num,
            interval_list, unfound_time_list, delay_matching_list, rebuild_start_time1, rebuild_start_time2)


def get_rebuild_start_time(trade_delayed_message_list):
    trade_rebuild_times = 0
    rebuild_start_time1 = None
    time1_found_flag = False
    rebuild_start_time2 = None
    time2_found_flag = False

    for trade_delayed_message in trade_delayed_message_list:
        for message_item in trade_delayed_message.split(','):
            if 'T=' in message_item:
                message_time = message_item.strip().replace('T=', '')

        if '09:30:00' <= message_time <= '09:50:00':
            if time1_found_flag:
                continue

            if 'Trade Delayed: Yes' in trade_delayed_message:
                rebuild_start_time1 = None
                trade_rebuild_times = 0
            elif 'Trade Delayed: No' in trade_delayed_message:
                if rebuild_start_time1 is None:
                    rebuild_start_time1 = message_time
                trade_rebuild_times += 1
                if trade_rebuild_times == 20:
                    trade_rebuild_times = 0
                    time1_found_flag = True
                    continue
            else:
                print 'error trade_delayed_message'
        elif '13:00:00' < message_time < '13:20:00':
            if time2_found_flag:
                continue

            if 'Trade Delayed: Yes' in trade_delayed_message:
                rebuild_start_time2 = None
                trade_rebuild_times = 0
            elif 'Trade Delayed: No' in trade_delayed_message:
                if rebuild_start_time2 is None:
                    rebuild_start_time2 = message_time
                trade_rebuild_times += 1
                if trade_rebuild_times == 20:
                    trade_rebuild_times = 0
                    time2_found_flag = True
                    continue
            else:
                print 'error trade_delayed_message'
    return rebuild_start_time1, rebuild_start_time2


def files_tar():
    mkt_file_path = '%s/mkt_files' % (mktdt_center_path,)
    os.chdir(mkt_file_path)
    output = os.popen('tar -zcf mkt_files_%s.tar.gz *.txt' % (todayStr,))
    print output.read()


def __read_instrument_files():
    ticker_array = []
    for instrument_file_name in cfg_file_name_list:
        file_real_path = '%s/%s' % (cfg_file_path, instrument_file_name)
        fr = open(file_real_path)
        lines = fr.readlines()
        fr.close()
        for line in lines[1:]:
            line_item = line.split(',')
            ticker_array.append(line_item[0])
    ticker_array.append('002746')
    return ticker_array


def mktcenter_check(ticker_array):
    os.chdir(mktdt_center_path)
    rebuild_file_name = build_recapture_config_file(','.join(ticker_array))
    base_commond = './build64_release/libatp.mktdt/MktdtSvr --cfg=./cfg/config_mkt_recapture.ini --uc=false'
    commond_str = base_commond
    print 'commond:', commond_str
    return_code = subprocess.call(commond_str, shell=True)
    print return_code

    for ticker in ticker_array:
        build_config_file(ticker)  # 生成配置文件
        run_mkt_center(ticker)  # 运行mktcenter,生成重建文件

    # 删除/home/trader/apps/MktdtCtr/data目录下的临时文件
    del_rebuild_file_commond = 'cd %s;rm -rf %s.dat' % (data_base_path, rebuild_file_name)
    print 'commond:', del_rebuild_file_commond
    return_code = subprocess.call(del_rebuild_file_commond, shell=True)
    print return_code


def mktcenter_check_start():
    # 删除/home/trader/dailyjob/MktdtCtr/mkt_files目录下的临时文件
    del_mkt_files_commond = 'cd %s/mkt_files;rm -rf *.txt' % mktdt_center_path
    print 'commond:', del_mkt_files_commond
    return_code = subprocess.call(del_mkt_files_commond, shell=True)
    print return_code

    ticker_array = __read_instrument_files()
    i = 0
    temp_ticker_array = []
    for ticker in ticker_array:
        temp_ticker_array.append(ticker)
        if i % 30 == 0:
            mktcenter_check(temp_ticker_array)
            temp_ticker_array = []
        i += 1

    if len(temp_ticker_array) > 0:
        mktcenter_check(temp_ticker_array)

    print 'start make rest files'
    mkt_file_path = '%s/mkt_files' % (mktdt_center_path,)
    for rt, dirs, files in os.walk(mkt_file_path):
        for f in files:
            if todayStr not in f:
                continue
            if ('mkt' not in f) or ('txt' not in f) or ('reset' in f) or ('simple' in f):
                continue
            file_reset(rt, f)

    print 'start check result:'
    content = []
    reset_mkt_file_list = []
    for rt, dirs, files in os.walk(mkt_file_path):
        for f in files:
            if (todayStr not in f) or ('_reset' not in f):
                continue
            else:
                file_path = '%s/%s' % (rt, f)
                reset_mkt_file_list.append(file_path)

    reset_mkt_file_list.sort()
    for file_path in reset_mkt_file_list:
        ticker_exist_flag = True

        if ticker_exist_flag:
            content.append(file_path)
            if os.path.getsize(file_path) == 0:
                print 'file:%s is empty' % (file_path,)
                continue
            print 'check file:', file_path
            (mkt_num, found_mkt_num, soft_found_mkt_num, receive_time_error_num, rebuild_mkt_num, interval_list,
             unfound_time_list, delay_matching_list, rebuild_start_time1, rebuild_start_time2) = file_check(file_path)
            content.append('first rebuild time1:%s, time2:%s' % (rebuild_start_time1, rebuild_start_time2))
            content.append('\n'.join(delay_matching_list))

            interval_total = 0
            for (mkt_time_base, mkt_time) in interval_list:
                d1 = datetime.datetime.strptime(mkt_time_base, "%Y-%m-%d %H:%M:%S.%f")
                d2 = datetime.datetime.strptime(mkt_time, "%Y-%m-%d %H:%M:%S.%f")
                interval_total += (d1 - d2).seconds

            if mkt_num == 0 or len(interval_list) == 0:
                continue

            result_message = 'mkt_num:%s,found_mkt_num:%s,soft_found_mkt_num:%s,\
rebuild_num:%s,matching_rate:%s,time_interval_avg:%s,unfound_time:\n' % (
                mkt_num, found_mkt_num, soft_found_mkt_num, rebuild_mkt_num,
                '%.2f%%' % (found_mkt_num * 100 / float(mkt_num)), interval_total / float(len(interval_list)))
            print result_message
            print '--------------------------------------------'

            content.append(result_message)
            content.append('\n'.join(unfound_time_list))
            content.append('--------------------------------------------')
            check_file_path = '%s/check_result_%s.txt' % (rt, todayStr)
            save_file(check_file_path, content, False)
    files_tar()


if __name__ == '__main__':
    global todayStr
    global mktcenter_file_name
    todayStr = str(sys.argv[1])
    mktcenter_file_name = str(sys.argv[2])
    mktcenter_check_start()
