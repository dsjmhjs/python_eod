import math
import datetime
import numpy as np
import pandas as pd
import copy
from redis_config import live_sort_rule, single_option
from eod_aps.tools.data_api import DataReceiver

time_pos_list = [0, 1, 3, 5, 10, 15, 30, 60, 120]
long_list = ['ZZ500', 'CSI300', 'ZZ800', 'WindA', 'YSPool2']
hedge_list = ['IF', 'IC']
var_list = [
    'SelectPool', 'FreqOfOptim', 'OptimType', 'NormType',
    'FactorCombineType', 'FreqOfTrade', 'StockNum', 'StratVersion',
]


class TargetSelector(object):

    def __init__(self, config=None):
        self.config = self.config_processor(config)
        self.dr = DataReceiver()
        self.minute_list = self.create_minute_demo()

    @staticmethod
    def create_minute_demo():
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        idx_am = pd.timedelta_range(start='09:30:00', end='11:30:00',
                                    freq='1min', closed='right')

        idx_pm = pd.timedelta_range(start='13:00:00', end='15:00:00',
                                    freq='1min', closed='right')
        idx = idx_am.append(idx_pm)
        idx = map(lambda x: '%s %s' % (today, str(x).split(' ')[-1]), idx)
        return idx

    @staticmethod
    def config_processor(config):
        if config is None:
            config = {
                u'index_option': u'DIY_index', u'CSI300': u'0',
                u'ZZ500': u'1',
                u'report_type': u'Event_Real',
                u'live_sort': u'up_to_now',
                u'show_limit_num': 10,
                u'sort_direction': u'down',
            }
        return config

    def get_report_type_files(self):
        report_type = self.config['report_type']
        report_files = self.dr.get_basket_of_strategy('report_file')
        attr_dict = {x: x.split('_') for x in report_files}

        header = copy.deepcopy(var_list)
        if report_type == 'Factor':
            header.insert(0, 'FactorName')
            non_factor = list(set(long_list + hedge_list))
            all_factor_list = [
                x for x in attr_dict if attr_dict[x][0] not in non_factor
            ]
            factor_filter = self.config['FactorName']
            if 'ALL' not in factor_filter:
                select_list = filter(lambda x:
                                     attr_dict[x][0] in factor_filter,
                                     all_factor_list)
            else:
                select_list = all_factor_list

        elif report_type == 'Hedge':
            header.insert(0, report_type)
            select_list = filter(lambda x: attr_dict[x][0] in hedge_list,
                                 attr_dict)
        elif report_type == 'LongOnly':
            select_list = filter(lambda x: attr_dict[x][0] in long_list,
                                 attr_dict)
        else:
            select_list = list()

        data_list = [attr_dict[x] for x in select_list]
        df_var = pd.DataFrame(data_list, columns=header, index=select_list)
        # print df_var.head()
        return df_var

    def var_filter(self, df_var):
        for var_ in df_var.columns:
            var_elements = self.config[var_]
            if 'ALL' not in var_elements:
                df_var = df_var[np.in1d(df_var[var_].values, var_elements)]

            elements = set(df_var[var_].values)
            if len(elements) <= 1:
                del df_var[var_]

        df_var['name'] = df_var.apply(lambda x: ' '.join(x), axis=1)

        return list(df_var.index.values), list(df_var['name'].values)

    def get_report_file_basket(self):
        df_var = self.get_report_type_files()
        basket_list, name_list = self.var_filter(df_var)
        return basket_list, name_list

    def get_product_strategy_basket(self, strategy):
        basket_list = self.dr.get_basket_of_strategy(strategy)
        name_list = list()
        # add nominal amount information on name
        for basket in basket_list:
            nominal_amount = self.dr.get_nominal_amount_of_basket(basket)
            if nominal_amount is not None and float(nominal_amount) == 0.0:
                name_list.append(basket_list)
                continue

            new_basket_name = '%s / %s' % (basket, nominal_amount)
            name_list.append(new_basket_name)
        return basket_list, name_list

    def basket_select(self):
        strategy = self.config['report_type']
        if strategy not in self.dr.get_stratey_list():
            basket_list, name_list = self.get_report_file_basket()
        else:
            basket_list, name_list = self.get_product_strategy_basket(strategy)
        # print basket_list, name_list
        return basket_list, name_list

    def index_select(self):
        """
        :return:
        according to index_vars, return dict of index final
        dict = {
            'SSE50': 1, 'SH000300': 0, 'SH000905': 0
        }
        """
        index_option = self.config['index_option']
        index_option = index_option.replace('sub ', '')
        index_list = self.dr.get_basket_of_strategy('Index')
        if index_option == 'normal':
            index_dict = {x: 1.0 for x in index_list}
            self.config['hedge'] = False
        elif index_option in index_list:
            index_dict = {index_option: 1}
            self.config['hedge'] = True
        elif index_option == 'DIY_index':
            index_dict = {
                'SHSZ300': float(self.config['SHSZ300']),
                'SH000905': float(self.config['SH000905'])
            }
            self.config['hedge'] = True
        else:
            index_dict = dict()
        return index_dict


class Bar(TargetSelector):

    def __init__(self, config=None):
        """
        self.data_dict = {
            'basket_name': basket_bar_value_list,
            ......
        }
        :param config: dict(), config from website interface
        """
        super(Bar, self).__init__(config)
        self.data_dict = dict()

    def get_time_pos_val(self, record_name, data_list):
        """
        save basket_bar_value_list in self.data_dict
        list = [latest_val, 1min_change, 3min_change, 5min_change,
        10min_change, 15min_change, 30min_change, hour_change, two_hour_change]
        :param record_name: basket output name
        :param data_list: data_list of valid value of basket now
        :return: None
        """
        val_list = [data_list[-1]]
        data_size = len(data_list)
        latest_val = data_list[-1]
        for ind in range(1, len(time_pos_list)):
            time_pos = time_pos_list[ind]
            data_pos = data_size - 1 - time_pos
            data = data_list[data_pos] if data_pos >= 0 else 0
            val_list.append(latest_val - data)
        self.data_dict[record_name] = val_list

    def sort_data_by_dimension(self, sort_list):
        """
        according to live sort dimension, to order your basket output
        :return: a basket list ordered by sort dimension
        """
        sort_dimension = self.config['live_sort']
        direction = self.config['sort_direction']
        reverse = True if direction == 'down' else False
        if sort_dimension == 'letter_order':
            sort_list.sort(reverse=reverse)
        else:
            sort_pos = live_sort_rule[sort_dimension]
            val_dict = {
                self.data_dict[x][sort_pos]: x for x in self.data_dict.keys()
                if x not in self.index_select()
            }
            # print val_dict
            # print '-------------------'
            sort_list = sorted(val_dict.keys(), reverse=reverse)
            sort_list = [val_dict[x] for x in sort_list]
        return sort_list

    def bar_option_filter(self):
        """
        step1. save all basket and index's bar_value_list in self.data_dict
        step2. according to sort rules(e.g order by up_to_now dimension),
        reorder output order
        step3. if basket's length > show_limit_num, just ignore the extra basket

        :return: list(), basket_list filtered by function
        """
        basket_list, name_list = self.basket_select()
        index_list = self.index_select().keys()
        all_list = basket_list + index_list
        all_name_list = name_list + index_list

        # step 1
        for ind, basket in enumerate(all_list):
            record_name = all_name_list[ind]
            data = self.dr.get_minute_return_of_basket(basket)
            data.dropna(inplace=True)
            self.get_time_pos_val(record_name, data['ret'].values)

        # step 2. sort by different rule
        sort_list = self.sort_data_by_dimension(basket_list)

        # step 3. show number limit
        if self.config['show_limit_num'] < len(sort_list):
            sort_list = sort_list[:self.config['show_limit_num']]

        if self.config['hedge'] is False:
            sort_list.extend(index_list)

        return sort_list

    def hedge_index_or_not(self, df):
        """
        if hedge is True, just sub bar_value_list of selected index target
        :param df: data_frame with index(time_str_list), and
        columns(basket_list)
        :return:
        """
        if self.config['hedge'] is False:
            return df

        index_dict = self.index_select()
        val_list = [0.0 for x in range(len(live_sort_rule))]
        for live_sort in live_sort_rule:
            pos = live_sort_rule[live_sort]
            for index in self.index_select():
                val_list[pos] += self.data_dict[index][pos] * index_dict[index]

        sub_index = pd.DataFrame(val_list, index=df.index, columns=['ret'])
        sub_index['ret'] *= 100
        for col in df.columns:
            df[col] -= sub_index['ret']
        return df

    def pack_data(self, data):
        """
        pack data into a json data format for showing in website interface
        :param data: data_frame with index(time_str_list), and
        columns(basket_list)
        :return: dict(), keys are shown below
        """
        data = data.T
        basket_order = list(data.index.values)
        cols = list(data.columns)
        # reorder output columns, to set "up_to_now" at the end
        show_order = cols[1:] + cols[:1]

        html_dict = {
            'title': self.config['report_type'],
            'report_date': datetime.datetime.now().strftime('%Y%m%d'),
            'period': show_order,
            'strategy': basket_order,
            'length': len(live_sort_rule),
            'report_data': data[show_order].to_dict(orient='list')
        }
        return html_dict

    def combine_with_index(self, sort_list):
        """
        according to sort_list order, to pack a data format for html interface
        :param sort_list: list()
        :return: dict data
        """
        time_str_list = single_option['live_sort'][:-1]
        data_dict = [self.data_dict[x] for x in sort_list]
        df = pd.DataFrame(data_dict, columns=time_str_list, index=sort_list).T
        df *= 100
        df = self.hedge_index_or_not(df)
        return df

    def get_bar_report(self):
        sort_list = self.bar_option_filter()
        data = self.combine_with_index(sort_list)

        html_dict = self.pack_data(data)
        return html_dict


class Line(TargetSelector):

    def __init__(self, config=None):
        super(Line, self).__init__(config)

    @staticmethod
    def frame_to_list(basket_data):
        data = np.array(basket_data).tolist()
        for ind in range(240):
            if math.isnan(data[ind][1]):
                data[ind][1] = None
        data = map(lambda item: [item[0], float('%.2f' % item[1]) if item[1] else None], data)
        return data

    def hedge_index_or_not(self, basket_list):
        index_dict = self.index_select()
        basket_data_dict = {}
        if self.config['hedge'] is True:
            if len(index_dict) == 1:  # normal hedge
                index_data = [[]]
                for index in index_dict:
                    index_data = self.dr.get_minute_return_of_basket(index)

            else:  # combine index hedge
                index_data = self.dr.get_minute_return_of_basket('SSE50')
                index_data['ret'] = 0.0
                for index in index_dict:
                    weight_index = self.dr.get_minute_return_of_basket(index)
                    weight = index_dict[index]
                    index_data['ret'] += weight_index['ret'] * weight

            index_data['ret'] += 1
            for basket in basket_list:
                basket_data = self.dr.get_minute_return_of_basket(basket)
                basket_data['ret'] += 1
                basket_data['ret'] /= index_data['ret']
                basket_data['ret'] *= 100
                basket_data_dict[basket] = self.frame_to_list(basket_data)

        else:
            index_basket = [x for x in index_dict]
            basket_list.extend(index_basket)
            for basket in basket_list:
                basket_data = self.dr.get_minute_return_of_basket(basket)
                basket_data['ret'] *= 100
                basket_data_dict[basket] = self.frame_to_list(basket_data)

        return basket_data_dict

    @staticmethod
    def pack_data(basket_data_dict):
        html_dict = {
            'name': [x for x in basket_data_dict],
            'report_data': basket_data_dict
        }
        return html_dict

    def get_line_report(self):
        basket_list, name_list = self.basket_select()
        """
            show_limit_num and order
            # num = self.config['show_limit_num']
            # if num < len(basket_list):
            #     basket_list = basket_list[:num]
        """
        basket_data_dict = self.hedge_index_or_not(basket_list)
        html_dict = self.pack_data(basket_data_dict)
        return html_dict


if __name__ == '__main__':
    config_ = {
        u'FactorName': [u'ALL'], u'index_option': u'DIY_index',
        u'FactorCombineType': [u'ALL'], u'SelectPool': [u'ALL'],
        u'live_sort': u'up_to_now', u'start_day': u'',
        u'FreqOfTrade': [u'ALL'], u'CSI300': u'0', u'show_limit_num': 10,
        u'NormType': [u'ALL'], u'end_day': u'', u'sort_direction': u'down',
        u'StratVersion': [u'ALL'], u'report_type': u'Multi_Factor',
        u'time_now': u'10:21:48', u'ZZ500': u'1', u'StockNum': [u'ALL'],
        u'OptimType': [u'ALL'], u'FreqOfOptim': [u'ALL'], u'select_day': u''
    }
    # LatestBar(config_).get_bar_report()
    # Line().get_line_report()

    print Bar().get_bar_report()
