# -*- coding: utf-8 -*-
# 重新生成行情中心配置文件
import os
from itertools import islice
from eod_aps.model.schema_common import Instrument
from eod_aps.job.upload_to_server_job import upload_mkt_cfg_file_job
from eod_aps.job import *

group_number = 3
cfg_file_title = 'TICKER,EXCHANGE_ID,TYPE_ID,'
mg1_file_title = 'TICKER,EXCHANGE_ID,RECEIVE_FROM,'
ticker_list_part1 = [('159901', 19, 7), ('159902', 19, 7), ('159915', 19, 7), ('159919', 19, 7), ('159905', 19, 7),
                     ('159903', 19, 7), ('159910', 19, 7), ('159935', 19, 7), ('159939', 19, 7), ('159921', 19, 7),
                     ('159922', 19, 7), ('159943', 19, 7), ('159929', 19, 7), ('159931', 19, 7), ('159928', 19, 7),
                     ('159912', 19, 7), ('159907', 19, 7), ('159942', 19, 7), ('159944', 19, 7), ('159945', 19, 7),
                     ('159933', 19, 7), ('159916', 19, 7), ('159946', 19, 7), ('159940', 19, 7), ('159918', 19, 7),
                     ('159930', 19, 7), ('159924', 19, 7), ('159913', 19, 7), ('159936', 19, 7), ('159911', 19, 7),
                     ('159917', 19, 7), ('159001', 19, 15), ('159003', 19, 15), ('159005', 19, 15), ('150169', 19, 16),
                     ('150170', 19, 16), ('150175', 19, 16), ('150176', 19, 16), ('161831', 19, 16), ('164705', 19, 16),
                     ('002133', 18, 4)]
cfg_file_dict = {"huabao": "rb7,rb8,rb9", "guoxin": "rb4,rb5,rb6"}


def __get_ticker_list():
    stock_dict = dict()
    server_host = server_constant.get_server_model('host')
    session_history = server_host.get_db_session('common')
    query = session_history.query(Instrument)

    for instrument_db in query.filter(Instrument.type_id == Instrument_Type_Enums.CommonStock):
        stock_dict[instrument_db.ticker] = instrument_db
    server_host.close()

    ticker_list_part2 = []
    indx_members = query.filter(Instrument.ticker == 'SHSZ300').first().indx_members
    for ticker in indx_members.split(';'):
        stock_db = stock_dict[ticker]
        if stock_db.exchange_id == 19:
            ticker_list_part2.append((ticker, stock_db.exchange_id, stock_db.type_id))
    return ticker_list_part1 + ticker_list_part2


def __get_transactions_volume(ticker_list, transactions_file_path):
    result = []

    ticker_volume_dict = dict()
    with open(transactions_file_path, 'rb') as fr:
        for line in islice(fr, 1, None):
            (ticker, volume) = line.split(',')
            ticker_volume_dict[ticker] = int(volume)

    for (ticker, exchange_id, type_id) in ticker_list:
        if ticker not in ticker_volume_dict:
            custom_log.log_error_job('unfind ticker:%s volume' % ticker)
            volume = 0
        else:
            volume = ticker_volume_dict[ticker]
        result.append((ticker, exchange_id, type_id, volume))
    return sorted(result, cmp=lambda x, y: cmp(int(x[3]), int(y[3])), reverse=True)


def __comp(x, y):
    x_sum = 0
    for x_item in x:
        x_sum += x_item[3]

    y_sum = 0
    for y_item in y:
        y_sum += y_item[3]

    if int(x_sum) < int(y_sum):
        return -1
    elif int(x_sum) > int(y_sum):
        return 1
    else:
        return 0


def __group_ticker(ticker_volume_list):
    load_balance_groups = [[] for grp in range(group_number)]
    for ticker_volume in ticker_volume_list:
        load_balance_groups.sort(__comp)
        load_balance_groups[0].append(ticker_volume)
    return load_balance_groups


def __save_config_file(server_name, load_balance_groups):
    remove_file_list = []
    for file_name in os.listdir(MKTDTCTR_CFG_FOLDER):
        remove_file_list.append('%s/%s' % (MKTDTCTR_CFG_FOLDER, file_name))
    for file_path in remove_file_list:
        os.remove(file_path)

    file_name_array = cfg_file_dict[server_name].split(',')
    email_content_list = []

    mg1_pre_bind_map_list = []
    fh_instruments_list = []
    for i in range(len(load_balance_groups)):
        load_balance_group = load_balance_groups[i]

        save_group_list = []
        for group_item in load_balance_group:
            save_group_list.append('%s,%s,%s,' % (group_item[0], group_item[1], group_item[2]))
            mg1_pre_bind_map_list.append('%s,%s,%s,' % (group_item[0], group_item[1], file_name_array[i]))
            fh_instruments_list.append('%s,%s,%s,' % (group_item[0], group_item[1], group_item[2]))

        if i == 0:
            save_group_list.append('000789,19,4,')
            mg1_pre_bind_map_list.append('000789,19,%s,' % file_name_array[i])
            fh_instruments_list.append('000789,19,4,')

        file_path = '%s/%s_instruments.csv' % (MKTDTCTR_CFG_FOLDER, file_name_array[i])
        with open(file_path, 'w+') as fr:
            fr.write('\n'.join(cfg_file_title + '\n' + '\n'.join(save_group_list)))
        email_content_list.append('\n'.join(save_group_list))

    file_path = '%s/rb3_instruments.csv' % MKTDTCTR_CFG_FOLDER
    with open(file_path, 'w+') as fr:
        fr.write(cfg_file_title + '\n' + '\n'.join(fh_instruments_list))

    file_path = '%s/mg1_pre_bind_map_file.csv' % MKTDTCTR_CFG_FOLDER
    with open(file_path, 'w+') as fr:
        fr.write(mg1_file_title + '\n' + '\n'.join(mg1_pre_bind_map_list))

    file_path = '%s/fh7_instruments.csv' % MKTDTCTR_CFG_FOLDER
    with open(file_path, 'w+') as fr:
        fr.write(mg1_file_title + '\n' + '\n'.join(fh_instruments_list))

    email_utils5.send_email_group_all('MktCenter Group Info_' + server_name, '\n\n'.join(email_content_list))
    upload_mkt_cfg_file_job((server_name,))


def reset_mktdtctr_cfg_file_job(today_str=None):
    if today_str is None:
        today_str = date_utils.get_today_str()

    ticker_list = __get_ticker_list()
    transactions_file_path = TRANSACTIONS_FILE_PATH_TEMPLATE % today_str
    # 如果volume信息文件未生成
    if not os.path.exists(transactions_file_path):
        email_utils5.send_email_group_all('[Error]MktCenter Group Info', 'file:%s is missing!' % transactions_file_path)
        return

    ticker_volume_list = __get_transactions_volume(ticker_list, transactions_file_path)
    load_balance_groups = __group_ticker(ticker_volume_list)

    for server_name in ('guoxin',):
        __save_config_file(server_name, load_balance_groups)


if __name__ == '__main__':
    reset_mktdtctr_cfg_file_job('20180510')
