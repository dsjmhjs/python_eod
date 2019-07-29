# -*- coding: utf-8 -*-
import os
import pandas as pd

base_file_path = '/mnt/ssd1/Data'
out_put_path = '/mnt/ssd1'

def read_pnl_fils():
    pnl_list = []
    for rt, dirs, files in os.walk(base_file_path):
        for file_name in files:
            if 'pnl.csv' not in file_name:
                continue
            pnl_file_path = '%s/%s' % (rt, file_name)
            with open(pnl_file_path) as fr:
                for line in fr.readlines():
                    if line == '':
                        continue

                    temp_dict = dict()
                    for line_content in line.replace('\n', '').split('|'):
                        dict_key, dict_value = line_content.split('=')
                        temp_dict[dict_key] = dict_value
                    pnl_list.append(temp_dict)
    pnl_df = pd.DataFrame(pnl_list)
    pnl_df = pnl_df[['start_date', 'watchlist', 'pnl', 'bought_volume', 'bought_vwap', 'sold_volume', 'sold_vwap', 'commission']]
    pnl_df.rename(columns={'start_date': 'date', 'watchlist': 'symbol', 'bought_volume': 'buy vol', 'bought_vwap': 'buy vwap', 'sold_volume': 'sell vol', 'sold_vwap': 'sell vwap'}, inplace=True)
    pnl_df['symbol'] = pnl_df['symbol'].astype(str)
    pnl_df = pnl_df.sort_values(['date', ], ascending=[False])

    # date_str = pnl_df['date'].max()
    pnl_df.to_csv('%s/pnl_summary.csv' % out_put_path)


if __name__ == '__main__':
    read_pnl_fils()


