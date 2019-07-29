# -*- coding: utf-8 -*-
import os
import shutil
import pandas as pd
import numpy as np
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.tools.phone_trade_tools import send_phone_trade, save_phone_trade_file
from eod_aps.job import *


direction_dict = custom_enum_utils.enum_to_dict(Direction_Enums)
trade_type_dict = custom_enum_utils.enum_to_dict(Trade_Type_Enums)
hedge_flag_dict = custom_enum_utils.enum_to_dict(Hedge_Flag_Type_Enums)
io_type_dict = custom_enum_utils.enum_to_dict(IO_Type_Enums)


def send_by_file(file_path):
    phone_trade_list = []
    with open(file_path, 'rb') as fr:
        for line in fr.readlines():
            line_item = line.replace('\r\n', '').split(',')
            if len(line_item) != 11:
                continue
            phone_trade_info = PhoneTradeInfo()
            phone_trade_info.fund = line_item[0]
            if line_item[1] == 'manual.default':
                phone_trade_info.strategy1 = ''
            else:
                phone_trade_info.strategy1 = line_item[1]

            phone_trade_info.symbol = line_item[2]

            phone_trade_info.direction = direction_dict[line_item[3]]
            phone_trade_info.tradetype = trade_type_dict[line_item[4]]
            phone_trade_info.hedgeflag = hedge_flag_dict[line_item[5]]

            phone_trade_info.exprice = float(line_item[6])
            phone_trade_info.exqty = int(line_item[7])

            phone_trade_info.iotype = io_type_dict[line_item[8]]

            if line_item[9] == 'manual.default':
                phone_trade_info.strategy2 = ''
            else:
                phone_trade_info.strategy2 = line_item[9]

            phone_trade_info.server_name = line_item[10]
            phone_trade_list.append(phone_trade_info)
    send_phone_trade(server_name, phone_trade_list)


def __build_phone_trade(buy_file_name, sell_file_name, ticker, phone_number):
    phone_trade_info = PhoneTradeInfo()
    phone_trade_info.fund = __rebuild_fund_name(buy_file_name)
    phone_trade_info.strategy1 = __rebuild_strategy_name(buy_file_name)
    phone_trade_info.symbol = ticker
    phone_trade_info.direction = Direction_Enums.Buy
    phone_trade_info.tradetype = Trade_Type_Enums.Normal
    phone_trade_info.hedgeflag = Hedge_Flag_Type_Enums.Speculation
    phone_trade_info.exqty = phone_number

    phone_trade_info.iotype = IO_Type_Enums.Inner2
    phone_trade_info.strategy2 = __rebuild_strategy_name(sell_file_name)
    return phone_trade_info


def __rebuild_strategy_name(change_file_name):
    name_item = change_file_name.split('-')
    rename_str = '%s.%s' % (name_item[1], name_item[0])
    return rename_str


def __rebuild_fund_name(change_file_name):
    name_item = change_file_name.split('-')
    return name_item[2]


def __get_prev_close_dict():
    type_list = [Instrument_Type_Enums.CommonStock, ]
    instrument_dict = query_instrument_dict('host', type_list)
    return instrument_dict


def build_phone_trade(pf_trade_list):
    fund = pf_trade_list[0][0]

    buy_list = []
    sell_list = []
    for pf_trade_info in pf_trade_list:
        if pf_trade_info[3] >= 0:
            buy_list.append(pf_trade_info)
        else:
            sell_list.append(pf_trade_info)

    change_trade_list = []
    phone_trade_list = []
    for i in range(len(buy_list) - 1, -1, -1):
        buy_position_info = buy_list[i]
        buy_qty = buy_position_info[3]

        for j in range(len(sell_list) - 1, -1, -1):
            sell_position_info = sell_list[j]
            sell_qty = sell_position_info[3]
            trade_qty = min(buy_qty, abs(sell_qty))

            phone_trade_info = __build_phone_trade(buy_position_info[1], sell_position_info[1],
                                                   buy_position_info[2], trade_qty)
            phone_trade_list.append(phone_trade_info)

            change_trade_list.append([fund, buy_position_info[1], buy_position_info[2], -trade_qty])
            change_trade_list.append([fund, sell_position_info[1], sell_position_info[2], trade_qty])

            buy_qty = max(buy_qty - trade_qty, 0)
            temp_sell_qty = min(sell_qty + trade_qty, 0)

            if temp_sell_qty == 0:
                sell_list.pop(j)
            else:
                sell_list[j][3] = temp_sell_qty

            if buy_qty == 0:
                break

        if buy_qty > 0:
            buy_list[i][3] = buy_qty
            break
        else:
            buy_list.pop(i)
    return change_trade_list, phone_trade_list


# 根据调仓文件生成phonetrade. result_type---1:生成文件，2:直接发送消息
def phone_trade_by_change_file(server_name, base_folder, result_type=1):
    original_dateframe = []
    for file_name in os.listdir(base_folder):
        if not file_name.endswith('.txt'):
            continue

        file_name_items = file_name.split('-')
        fund_name = file_name_items[2]
        with open(os.path.join(base_folder, file_name), 'rb') as fr:
            for line in fr.readlines():
                line_item = line.replace('\r\n', '').split(',')
                if len(line_item) != 2:
                    continue

                trade_ticker = line_item[0]
                item_volume = int(line_item[1])
                original_dateframe.append([fund_name, file_name, trade_ticker, item_volume])

    change_trade_list = []
    phone_trade_list = []
    original_df = pd.DataFrame(original_dateframe, columns=['Fund', 'FileName', 'Ticker', 'Volume'])
    for group_key, group in original_df.groupby(['Fund', 'Ticker']):
        if len(group) >= 2:
            pf_trade_list = np.array(group[['Fund', 'FileName', 'Ticker', 'Volume']]).tolist()
            temp_change_trade_list, temp_phone_trade_list = build_phone_trade(pf_trade_list)
            change_trade_list.extend(temp_change_trade_list)
            phone_trade_list.extend(temp_phone_trade_list)

    # 无可自撮合的条目
    if len(phone_trade_list) == 0:
        return

    instrument_dict = __get_prev_close_dict()
    for phone_trade_info in phone_trade_list:
        phone_trade_info.server_name = server_name
        instrument_db = instrument_dict[phone_trade_info.symbol]
        phone_trade_info.exprice = instrument_db.prev_close

    if result_type == 1:
        phone_trade_file_name = 'phone_trade.csv'
        phone_trade_file_path = '%s/%s' % (base_folder, phone_trade_file_name)
        save_phone_trade_file(phone_trade_file_path, phone_trade_list)
    elif result_type == 2:
        send_phone_trade(server_name, phone_trade_list)

    change_df = pd.DataFrame(change_trade_list, columns=['Fund', 'FileName', 'Ticker', 'Volume'])
    sum_change_list = []
    for group_key, group in change_df.groupby(['Fund', 'FileName', 'Ticker']):
        vol_sum = group['Volume'].sum()
        temp = group.head(1)
        temp.loc[:, 'Volume'] = vol_sum
        sum_change_list.append(temp)
    change_df = pd.concat(sum_change_list)

    merge_df = pd.merge(original_df, change_df, on=['Fund', 'FileName', 'Ticker'], how='left').fillna(0)
    merge_df['Volume'] = merge_df['Volume_x'] + merge_df['Volume_y']
    merge_result = merge_df[merge_df['Volume'] != 0]
    merge_list = np.array(merge_result[['FileName', 'Ticker', 'Volume']]).tolist()

    merge_dict = dict()
    for merge_info in merge_list:
        temp_message = '%s,%s' % (merge_info[1], int(merge_info[2]))
        if merge_info[0] in merge_dict:
            merge_dict[merge_info[0]].append(temp_message)
        else:
            merge_dict[merge_info[0]] = [temp_message]

    for (change_file_name, message_info_list) in merge_dict.items():
        file_save_path = '%s/%s' % (base_folder, change_file_name)
        with open(file_save_path, 'w') as fr:
            fr.write('\n'.join(message_info_list))


if __name__ == '__main__':
    server_name = 'guoxin'
    filter_date_str = '20171212'

    base_save_folder = '%s/%s/%s_base' % (STOCK_SELECTION_FOLDER, server_name, filter_date_str.replace('-', ''))
    change_save_folder = '%s/%s/%s_change' % (STOCK_SELECTION_FOLDER, server_name, filter_date_str.replace('-', ''))
    if os.path.exists(change_save_folder):
        shutil.rmtree(change_save_folder)
        os.mkdir(change_save_folder)
    for file_name in os.listdir(base_save_folder):
        shutil.copyfile('%s/%s' % (base_save_folder, file_name),
                        '%s/%s' % (change_save_folder, file_name))
    phone_trade_by_change_file(server_name, change_save_folder)