# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np


class BacktestParamConfig:
    def __init__(self):
        pass

    param_config_ls = []

    time_adj_column = ['aggressive_wait_milli_secs',
                       'v_aggressive_wait_milli_secs',
                       'vv_aggressive_wait_milli_secs']
    time_adj_dat = np.array([[2000, 1500, 1000]])
    time_adj_df = pd.DataFrame(data=time_adj_dat, columns=time_adj_column)

    param_config_ls.append(time_adj_df)

    #==========================================
    vol_adj_column = ['child_round_secs', 'unwind_round_secs',
                      'child_max_shares_multiplier', 'child_unit_shares_multiplier',
                      'strat_max_vol_imbalance_ratio', 'order_vol_adj_exp']
    vol_adj_dat = np.array([[300, 900, 20, 1, .2, 1.5],
                            [600, 900, 40, 1, .25, 1.5],
                            [1200, 900, 80, 1, .3, 1.5],
                            [300, 900, 20, 1, .2, 2],
                            [600, 900, 40, 1, .25, 2],
                            [1200, 900, 80, 1, .3, 2],
                            [300, 900, 20, 1, .2, 2.5],
                            [600, 900, 40, 1, .25, 2.5],
                            [1200, 900, 80, 1, .3, 2.5]]
                           )
    vol_adj_df = pd.DataFrame(data=vol_adj_dat, columns=vol_adj_column)
    param_config_ls.append(vol_adj_df)
    #==============================================

    thold_column = ['passive_thold', 'aggressive_thold',
                    'v_aggressive_thold', 'vv_aggressive_thold']
    thold_dat = np.array([[.5, 1.5, 3, 4.5]])
    thold_df = pd.DataFrame(data=thold_dat, columns=thold_column)

    param_config_ls.append(thold_df)
    #================================================

    ret_weight_column = ['ret_weight_30s', 'ret_weight_60s',
                         'ret_weight_120s', 'ret_weight_300s']
    ret_weight_dat = np.array([[1, 0, 0, 0],
                               [0, 1, 0, 0],
                               [0, 0, 1, 0],
                               [0, 0, 0, 1],
                               [0, .75, .25, 0],
                               [.25, .75, 0, 0],
                               [0, .75, 0, .25],
                               [.5, .5, 0, 0],
                               [0, .5, .5, 0],
                               [0, .5, 0, .5],
                               [.75, .25, 0, 0],
                               [0, .25, .75, 0],
                               [0, 0, .75, .25]]
                              )

    ret_weight_df = pd.DataFrame(data=ret_weight_dat, columns=ret_weight_column)
    param_config_ls.append(ret_weight_df)
    #=======================================

    adj_factor_column = ['adj_factor_30s', 'adj_factor_60s',
                         'adj_factor_120s', 'adj_factor_300s']
    adj_factor_dat = np.array([[1, 1, 1, 1],
                               [2**.5, 1, .5**.5, .2**.5],
                               [2**1, 1, .5**1, .2**1]]
                              )
    adj_factor_df = pd.DataFrame(data=adj_factor_dat, columns=adj_factor_column)
    param_config_ls.append(adj_factor_df)
    # =========================================================

if __name__ == '__main__':
    param_config = BacktestParamConfig()
    crossJoin = param_config.param_config_ls[0]
    for i in range(1, len(param_config.param_config_ls)):
        dataframe = param_config.param_config_ls[i]
        crossJoin['key'] = 1
        dataframe['key'] = 1
        crossJoin = pd.merge(crossJoin, dataframe, on="key")
        del crossJoin['key']
    print crossJoin







    # for param_config_items in param_config.param_config_ls:
    #     title_list = param_config_items.columns
    #     values_list = param_config_items.values
    #     for line_values in values_list:
    #         output_str = []
    #         for i in range(0, len(line_values)):
    #             output_str.append('%s:%s' % (title_list[i], str(line_values[i])))
    #         print ';'.join(output_str)


