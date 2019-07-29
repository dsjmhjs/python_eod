# -*- coding: utf-8 -*-
from eod_aps.model.schema_om import OrderHistory
from eod_aps.job import *
import numpy as np
import matplotlib.pyplot as plt

out_put_list = []


def order_statistics_job(server_name, account, filter_date_str):
    server_model = server_constant.get_server_model(server_name)
    session_om = server_model.get_db_session('om')
    query_om_order = session_om.query(OrderHistory)

    order_history_list = []
    for om_order_db in query_om_order.filter(OrderHistory.account == account,
                                             OrderHistory.create_time.like('%' + filter_date_str + '%'),
                                             OrderHistory.algo_type == 0).order_by(OrderHistory.create_time):
        if not om_order_db.symbol.startswith('0') and not om_order_db.symbol.startswith('6'):
            continue
        order_history_list.append([om_order_db.create_time.strftime('%Y-%m-%d %H:%M:%S'),
                                   om_order_db.transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
                                   om_order_db.status])

    create_time_dict = dict()
    for order_history_item in order_history_list:
        if order_history_item[0] in create_time_dict:
            create_time_dict[order_history_item[0]].append(order_history_item)
        else:
            create_time_dict[order_history_item[0]] = [order_history_item]

    transaction_time_dict = dict()
    for order_history_item in order_history_list:
        if order_history_item[1] in transaction_time_dict:
            transaction_time_dict[order_history_item[1]].append(order_history_item)
        else:
            transaction_time_dict[order_history_item[1]] = [order_history_item]

    start_time_str = date_utils.string_toDatetime('%s 09:30:00' % filter_date_str, "%Y-%m-%d %H:%M:%S")
    end_time_str = date_utils.string_toDatetime('%s 15:00:00' % filter_date_str, "%Y-%m-%d %H:%M:%S")
    temp_list = date_utils.get_between_second_list(start_time_str, end_time_str)
    second_list = [date_utils.datetime_toString(item, "%Y-%m-%d %H:%M:%S") for item in temp_list]

    filled_ratio_list = []
    canceled_ratio_list = []
    last_order_num = 0
    last_filled_num = 0
    last_canceled_num = 0
    for second_str in second_list:
        if second_str in create_time_dict:
            last_order_num += len(create_time_dict[second_str])
        if second_str in transaction_time_dict:
            for transaction_time_item in transaction_time_dict[second_str]:
                if int(transaction_time_item[2]) == 2:
                    last_filled_num += 1
                elif int(transaction_time_item[2]) == 4:
                    last_canceled_num += 1
                # else:
                #     print 'Status:', transaction_time_item[2]

        if last_order_num > 0:
            filled_ratio = last_filled_num / float(last_order_num)
        else:
            filled_ratio = 0

        if last_order_num > 0:
            canceled_ratio = last_canceled_num / float(last_order_num)
        else:
            canceled_ratio = 0
        filled_ratio_list.append('%.3f' % filled_ratio)
        canceled_ratio_list.append('%.3f' % canceled_ratio)

    out_put_list.append('--------%s,%s-------------' % (filter_date_str, account))
    out_put_list.append(u'委托成交比峰值:%s' % min(filled_ratio_list[1000:]))
    out_put_list.append(u'委托撤单比:%s' % max(canceled_ratio_list[1000:]))
    server_model.close()


def order_statistics_job2(server_name, account, filter_date_str):
    server_model = server_constant.get_server_model(server_name)
    session_om = server_model.get_db_session('om')
    query_om_order = session_om.query(OrderHistory)

    order_history_list = []
    for om_order_db in query_om_order.filter(OrderHistory.account == account,
                                             OrderHistory.create_time.like('%' + filter_date_str + '%'),
                                             OrderHistory.algo_type == 0).order_by(OrderHistory.create_time):
        if not om_order_db.symbol.startswith('0') and not om_order_db.symbol.startswith('6'):
            continue
        order_history_list.append([om_order_db.create_time.strftime('%Y-%m-%d %H:%M:%S'),
                                   om_order_db.transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
                                   om_order_db.status])

    create_time_dict = dict()
    for order_history_item in order_history_list:
        if order_history_item[0] in create_time_dict:
            create_time_dict[order_history_item[0]].append(order_history_item)
        else:
            create_time_dict[order_history_item[0]] = [order_history_item]

    transaction_time_dict = dict()
    for order_history_item in order_history_list:
        if order_history_item[1] in transaction_time_dict:
            transaction_time_dict[order_history_item[1]].append(order_history_item)
        else:
            transaction_time_dict[order_history_item[1]] = [order_history_item]

    start_time_str = date_utils.string_toDatetime('%s 09:30:00' % filter_date_str, "%Y-%m-%d %H:%M:%S")
    end_time_str = date_utils.string_toDatetime('%s 15:00:00' % filter_date_str, "%Y-%m-%d %H:%M:%S")
    temp_list = date_utils.get_trading_second_list(start_time_str, end_time_str)
    second_list = [date_utils.datetime_toString(item, "%Y-%m-%d %H:%M:%S") for item in temp_list]

    time_list = []
    filled_ratio_list = []
    canceled_ratio_list = []
    rejected_ratio_list = []
    other_ratio_list = []

    last_order_num = 0
    # PartialFilled, Filled
    last_filled_num = 0
    last_canceled_num = 0
    last_rejected_num = 0
    # pendingnew, accepted, none
    last_other_num = 0
    for second_str in second_list:
        if second_str in create_time_dict:
            last_order_num += len(create_time_dict[second_str])
        if second_str in transaction_time_dict:
            for transaction_time_item in transaction_time_dict[second_str]:
                if int(transaction_time_item[2]) == 1 or int(transaction_time_item[2]) == 2:
                    last_filled_num += 1
                elif int(transaction_time_item[2]) == 4:
                    last_canceled_num += 1
                elif int(transaction_time_item[2]) == 8:
                    last_rejected_num += 1
                elif int(transaction_time_item[2]) == 10 or int(transaction_time_item[2]) == 16 or\
                      int(transaction_time_item[2]) == -1 or int(transaction_time_item[2]) == 0:
                    last_other_num += 1
                else:
                    custom_log.log_info_job('Status:%s' % transaction_time_item[2])
                    return

        if last_order_num > 0:
            filled_ratio = last_filled_num / float(last_order_num)
        else:
            filled_ratio = 0

        if last_order_num > 0:
            canceled_ratio = last_canceled_num / float(last_order_num)
        else:
            canceled_ratio = 0

        if last_rejected_num > 0:
            rejected_ratio = last_rejected_num / float(last_order_num)
        else:
            rejected_ratio = 0

        if last_other_num > 0:
            other_ratio = last_other_num / float(last_order_num)
        else:
            other_ratio = 0

        time_list.append(second_str[11:])
        filled_ratio_list.append(float('%.2f' % filled_ratio))
        canceled_ratio_list.append(float('%.2f' % canceled_ratio))
        rejected_ratio_list.append(float('%.2f' % rejected_ratio))
        other_ratio_list.append(float('%.2f' % other_ratio))

    x_axis = range(0, len(time_list))
    x_min = min(x_axis)
    x_max = max(x_axis)
    plot_lable_num = 9
    step = int((x_max - x_min) / plot_lable_num)
    x_axis_nparray = np.arange(x_min, x_max, step)

    x_axis_str_list = []
    for x_axis_num in x_axis_nparray:
        x_axis_str_list.append(time_list[x_axis_num])

    y_axis_nparray = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

    fig1 = plt.figure(figsize=(10, 8))
    ax1 = fig1.add_subplot(1, 1, 1)
    ax1.plot(x_axis, filled_ratio_list, '', label="$FilledRatio$",color="red", linewidth=2)
    ax1.plot(x_axis, canceled_ratio_list, '', label="$CanceledRatio$",color="blue", linewidth=2)
    ax1.plot(x_axis, rejected_ratio_list, '', label="$RejectedRatio$",color="green", linewidth=2)
    ax1.plot(x_axis, other_ratio_list, '', label="$OtherRatio$",color="yellow", linewidth=2)

    plt.xticks(x_axis_nparray)
    ax1.set_xticklabels(x_axis_str_list)
    plt.yticks(y_axis_nparray)

    plt.title("%s_%s" % (account, filter_date_str))
    plt.xlabel("Time(s)")
    plt.ylabel("Value")
    plt.grid(x_axis_str_list)
    plt.legend()
    # plt.show()
    order_statistics_report_folder = ORDER_STATISTICS_REPORT_FOLDER_TEMPLATE % server_name
    save_file_name = 'order_%s_%s.png' % (account, filter_date_str)
    pic_save_path = '%s/%s' % (order_statistics_report_folder, save_file_name)
    plt.savefig(pic_save_path)
    plt.close()
    server_model.close()


if __name__ == '__main__':
    server_name = 'guoxin'
    account_list = ['198800888042-TS-balance01-', '198800888076-TS-xhms01-', '198800888077-TS-xhhm02-']
    date_list = ['2017-09-11','2017-09-12','2017-09-13','2017-09-14','2017-09-15',
                     '2017-09-18','2017-09-19','2017-09-20','2017-09-21','2017-09-22',
                     '2017-09-25','2017-09-26','2017-09-27','2017-09-28','2017-09-29',
                     '2017-10-09','2017-10-10','2017-10-11','2017-10-12','2017-10-13',
                     '2017-10-16', '2017-10-17']
    for date_str in date_list:
        for account in account_list:
            order_statistics_job2(server_name, account, date_str)
