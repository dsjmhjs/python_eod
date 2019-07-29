# -*- coding: utf-8 -*-
import os
from name_classify import ban_start_title, hedge_long_list
import MySQLdb
# get_value_list = []


def get_folder_strat_type_list(path):
    files = [x[:-4] for x in os.listdir(path) if x.endswith('.csv')]
    strat_types = [x.split('_')[-4] for x in files]
    strat_types = list(set(strat_types))
    strat_types.sort()
    strat_types.insert(0, 'ALL')
    return strat_types


def get_folder_factor_list(path, if_all=True):
    files = [x for x in os.listdir(path)
             if x.split('_')[0] not in ban_start_title and x.endswith('.csv')]
    factor_list = [x.split('_')[0] for x in files]
    factor_list = list(set(factor_list))
    factor_list.sort()
    if if_all:
        factor_list.insert(0, 'ALL')
    return factor_list


def get_folder_index_list(path, if_all=True):
    files = [x for x in os.listdir(path)
             if x.split('_')[0] not in ban_start_title and x.endswith('.csv')]
    index_list = [x.split('_')[1] for x in files]
    index_list = list(set(index_list))
    index_list.sort()
    if if_all:
        index_list.insert(0, 'ALL')
    return index_list


def sort_and_add_all(change_list):
    change_list = list(set(change_list))
    change_list.sort()
    change_list.insert(0, 'ALL')
    return change_list


def get_126_conn(db='jobs'):
    conn = MySQLdb.connect(
        host="172.16.10.126",
        user="llh",
        passwd="llh@yansheng",
        db="%s" % db
    )
    return conn

filename_template_orders = [
    'FactorName', 'SelectPool', 'FreqOfOptim', 'OptimType', 'NormType',
    'FactorCombineType', 'FreqOfTrade', 'StockNum', 'StratVersion'
]
template_order_map = {
    ind: filename_template_orders[ind] for ind in range(len(filename_template_orders))
}


def get_bar_multi_config(report_path):
    multi_options = {}
    display_order = ['FactorName']
    all_files = [x[:-4] for x in os.listdir(report_path) if x.endswith('csv')]
    attr_list_groups = [x.split('_') for x in all_files]
    # print attr_list_groups
    factor_names = [x[0] for x in attr_list_groups if x[0] not in hedge_long_list]
    # print factor_names
    multi_options['FactorName'] = sort_and_add_all(factor_names)
    # insert one column for LongOnly file attrs
    for item in attr_list_groups:
        if len(item) < len(template_order_map):
            item.insert(0, 'LongOnly')

    for ind in range(1, len(template_order_map)):
        attr_list = [x[ind] for x in attr_list_groups]
        multi_options[template_order_map[ind]] = sort_and_add_all(attr_list)
        display_order.append(template_order_map[ind])
    # print multi_options, display_order
    return multi_options, display_order


if __name__ == '__main__':
    path_ = '/nas/longling/StockSelection/latest_production_position'
