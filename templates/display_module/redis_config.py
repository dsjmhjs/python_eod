# -*- coding: utf-8 -*-
# __auther__ = 'luolinhua'
from eod_aps.model.eod_const import const
from web_util import get_bar_multi_config

# report_root_path_map = {
#     'Linux': '/nas/longling',
#     'Windows': r'\172.16.12.123\share\temp\longling'
# }
#
# report_root_path = report_root_path_map[platform.system()]
# report_path = os.path.join(report_root_path,
#                            'StockSelection/result_production_1/ScatteredTrading/report/20150504')

wind_report_path = '%s/latest_production_pnl' % const.EOD_CONFIG_DICT['Multi_Factor_Folder']
live_report_path = '%s/latest_production_position' % const.EOD_CONFIG_DICT['Multi_Factor_Folder']
# ----------------------------- time config for bar picture ---------------------------------------
show_period_length = [1, 2, 7, 14, 30, 60, 90, 180, 365, 730]
period_day_map = {
    1: 1, 2: 2, 7: 5, 14: 10, 30: 22, 60: 44, 90: 66, 180: 132, 365: 252,
    730: 504
}
time_list = [
    '1_day', '2_day', '1_week', '2_week', '1_month',
    '2_month', '3_month', 'half_year', 'a_year', '2_year', 'input_time',
    'letter_order'
]

time_sort_num = {
    time_list[ind]: ind for ind in range(len(time_list))
}

# ------------------------------ bar option for sub index -----------------------------------------
# index_selection = ['SH50', 'CSI300', 'ZZ500']
index_selection = ['SSE50', 'SHSZ300', 'SH000905']
sub_index_option = map(lambda x: 'sub %s' % x, index_selection)
sub_index_option.insert(0, 'normal')
sub_index_option.append('DIY_index')
single_option = {
    'report_type': ['Factor', 'Hedge', 'LongOnly', 'RealPosition', 'Event_Real', "Multi_Factor"],
    'index_option': sub_index_option,
    'sort_by_rule': time_list,
    'sort_direction': ['up', 'down'],
    'live_sort': [
        'up_to_now', '1_min', '3_min',
        '5_min', '10_min', '15_min',
        '30_min', 'one_hour', 'two_hour',
        'letter_order',
    ]
}
live_sort_map = {
    'up_to_now': 0,
    '1_min': 1,
    '3_min': 3,
    '5_min': 5,
    '10_min': 10,
    '15_min': 15,
    '30_min': 30,
    'one_hour': 60,
    'two_hour': 120,
}
live_sort_rule = {
    single_option['live_sort'][ind]: ind
    for ind in range(len(single_option['live_sort']) - 1)
}

# for backtest_module
single_display_order = [
    'report_type', 'index_option', 'sort_by_rule', 'sort_direction'
]
# for live_module
live_single_display_order = [x for x in single_display_order]
# live_single_display_order.insert(0, 'data_type')
live_single_display_order[-2] = 'live_sort'


# multi_option, multi_display_order = get_bar_multi_config(wind_report_path)
# report_bar_attrs = {
#     'single_option': single_option, 'single_display_order': single_display_order,
#     'multi_option': multi_option, 'multi_display_order': multi_display_order,
#     'live_single_display_order': live_single_display_order
# }

# live_multi_option, live_multi_display_order = get_bar_multi_config(live_report_path)
# live_report_bar_attrs = {
#     'single_option': single_option, 'single_display_order': single_display_order,
#     'multi_option': live_multi_option, 'multi_display_order': live_multi_display_order,
#     'live_single_display_order': live_single_display_order
# }

index_name_map = {
    'SSE50': 'SH50', 'SHSZ300': 'CSI300', 'SH000905': 'ZZ500'
}


