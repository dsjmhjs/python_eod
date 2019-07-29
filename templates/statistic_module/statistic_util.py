# -*-coding:utf-8-*-
import pandas as pd
from eod_aps.tools.data_api import DataReceiver

dr = DataReceiver()


def get_index_pie_data(base_info_dict):
    df_weight = dr.get_weight_of_basket('MultiFactor')
    ticker_list = df_weight.index.values
    index_info_dict = {'zz500': [], 'csi300': [], 'sh50': []}
    for ticker, item in base_info_dict.items():
        if 'market_value' in item and item['market_value']:
            if item['zz500'] != 0.0:
                index_info_dict['zz500'].append(ticker)
            if item['hs300'] != 0.0:
                index_info_dict['csi300'].append(ticker)
            if item['sz50'] != 0.0:
                index_info_dict['sh50'].append(ticker)
    zz500 = index_info_dict['zz500']
    csi300 = index_info_dict['csi300']
    sh50 = index_info_dict['sh50']
    index_ticker = set(zz500).union(set(csi300)).union(set(sh50))
    others = list(set(ticker_list).difference(index_ticker))
    all_three_index = set(zz500).intersection(set(csi300)).intersection(set(sh50))

    both_zz500_sh50 = list(set(zz500).intersection(set(sh50)).difference(all_three_index))
    both_csi300_sh50 = list(set(csi300).intersection(set(sh50)).difference(all_three_index))
    both_zz500_csi300 = list(set(zz500).intersection(set(csi300)).difference(all_three_index))
    all_three_index = list(all_three_index)
    csi300 = list(set(csi300).difference(all_three_index).difference(
        both_csi300_sh50).difference(both_zz500_csi300))
    sh50 = list(set(sh50).difference(all_three_index).difference(both_csi300_sh50).difference(
        both_zz500_sh50))
    zz500 = list(set(zz500).difference(all_three_index).difference(
        both_zz500_csi300).difference(both_zz500_sh50))

    index_dict = {
        'Only_ZZ500': zz500, 'Only_CSI300': csi300, 'Both_CSI300_SH50': both_csi300_sh50,
        'Only_SH50': sh50, 'Both_CSI_ZZ500': both_zz500_csi300, 'Both_ZZ500_SH50': both_zz500_sh50,
        'Others': others, 'all_three_index': all_three_index

    }

    index_pie_list = []
    for index_str in index_dict:
        list_ = index_dict[index_str]
        if len(list_) > 0:
            weight_sum = df_weight.reindex(list_)["weight"].sum()
            weight_sum = round(weight_sum, 2)
            index_pie_list.append([index_str, weight_sum])
    index_pie_list = sorted(index_pie_list, key=lambda x: x[1],
                            reverse=True)
    return index_pie_list, index_dict


def get_market_value_pie_data(basket_name, market_value):
    df_weight = dr.get_weight_of_basket(basket_name)
    df_mv = pd.DataFrame(market_value, columns=['market_value', 'ticker'])
    df_mv.sort_values('market_value', inplace=True)
    df_mv['rate'] = range(len(df_mv))
    df_mv['rate'] = df_mv['rate'].astype(int)
    size = int(round(len(df_mv) / 10.0))
    df_mv['rate'] = df_mv['rate'].apply(lambda x: int(int(x) / size) + 1)
    df = pd.merge(df_weight, df_mv, how='left',
                  left_index=True, right_on='ticker')

    result_list = []
    group = df.groupby('rate')
    for rate, data in group:
        result_list.append([rate, len(data)])

    result_list = sorted(result_list, key=lambda x: x[0], reverse=True)
    for ind in range(len(result_list)):
        result_list[ind][0] = 'Market_value_level-%d' % result_list[ind][0]
    return result_list


def get_industry_bar_data(basket_name, base_info_dict):
    df_weight = dr.get_weight_of_basket(basket_name)
    # industry_data = get_industry_dataframe()

    base_info_list = []
    for symbol, v in base_info_dict.items():
        temp_base_dict = {'symbol': symbol}
        temp_base_dict.update(v)
        base_info_list.append(temp_base_dict)
    industry_data = pd.DataFrame(base_info_list)

    industry_data['symbol'] = industry_data['symbol'].apply(lambda x: str(x)[:6])
    industry_data = industry_data[['symbol', 'industry']]
    industry_data.index = industry_data['symbol']
    industry_data.columns = ['S_CON_WINDCODE', 'IND_NAME']
    df_weight = df_weight.merge(industry_data, left_index=True,
                                right_index=True, how="left")
    ticker_ret = dr.get_tickers_ret()
    df_weight = df_weight.merge(ticker_ret, left_index=True,
                                right_index=True, how="left")
    bar_data_list = list()
    df_weight["weight_ret"] = df_weight["weight"] * df_weight["ret"]
    for industry, data in df_weight.groupby("IND_NAME"):
        ret_sum = round(data["weight_ret"].sum() * 100, 2)
        bar_data_list.append([industry, ret_sum])
    bar_data_list = sorted(bar_data_list, key=lambda x: x[1],
                           reverse=True)
    return bar_data_list


if __name__ == '__main__':
    # get_industry_compoent_pie_data("Earning")
    # get_industry_bar_data("Earning")
    # get_index_pie_data("Earning")
    get_index_pie_data('MultiFactor')
