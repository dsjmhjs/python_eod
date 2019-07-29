# -*- coding: utf-8 -*-
# 通过本地wind数据库来获取市场数据
import os
from itertools import islice
from eod_aps.model.schema_common import Instrument
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.eod_const import const

date_utils = DateUtils()
server_host = server_constant.get_server_model('host')
FUTURE_SUMMARY_FILE_TEMPLATE = '%s/future/gta/%%s/tick/summary.csv' % const.EOD_CONFIG_DICT['data_file_folder']
STOCK_SUMMARY_FILE_TEMPLATE = '%s/wind/stock/%%s/market_data/summary.csv' % const.EOD_CONFIG_DICT['data_file_folder']
wind_server_name = 'wind_db'


class StockUtils(object):
    """
        股票数据常用工具类
    """

    def __init__(self):
        pass

    def __enter__(self):
        return self

    # 获取当天st股票列表
    def get_st_stock(self):
        now_date_str = date_utils.get_today_str()

        server_model = server_constant.get_server_model(wind_server_name)
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = 'select s_info_windcode, entry_dt, remove_dt from dump_wind.ASHAREST a where not exists(select 1 from dump_wind.ASHAREST where s_info_windcode = a.s_info_windcode and entry_dt > a.entry_dt)'
        query = session_dump_wind.execute(query_sql)

        st_stock_list = []
        for asharest_db in query:
            s_info_windcode = asharest_db[0]
            remove_dt = asharest_db[2]
            if remove_dt is not None and remove_dt < now_date_str:
                continue
            ticker = s_info_windcode.split('.')[0]
            st_stock_list.append(ticker)
        st_stock_list.sort()
        return st_stock_list

    # 获取当天停牌股票列表
    def get_suspend_stock(self):
        now_date_str = date_utils.get_today_str()
        server_model = server_constant.get_server_model(wind_server_name)
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = "select S_INFO_WINDCODE from dump_wind.ASHARETRADINGSUSPENSION t where t.S_DQ_SUSPENDDATE = '%s'" % now_date_str
        query = session_dump_wind.execute(query_sql)

        suspend_stock_list = []
        for asharest_db in query:
            s_info_windcode = asharest_db[0]
            ticker = s_info_windcode.split('.')[0]
            suspend_stock_list.append(ticker)
        suspend_stock_list.sort()
        return suspend_stock_list

    # 获取当天复牌股票列表
    # 来源：本地wind数据库
    def get_resume_stocks(self):
        now_date_str = date_utils.get_today_str()
        server_model = server_constant.get_server_model(wind_server_name)
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = "select S_INFO_WINDCODE from dump_wind.ASHARETRADINGSUSPENSION t where t.S_DQ_RESUMPDATE = '%s' \
                     group by S_INFO_WINDCODE" % now_date_str
        query = session_dump_wind.execute(query_sql)

        resum_stock_list = []
        for asharest_db in query:
            s_info_windcode = asharest_db[0]
            ticker = s_info_windcode.split('.')[0]
            resum_stock_list.append(ticker)
        return resum_stock_list

    # 来源：本地wind数据库
    def get_ticker_dict(self):
        server_model = server_constant.get_server_model(wind_server_name)
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = "select S_INFO_WINDCODE,S_INFO_NAME from dump_wind.ASHAREDESCRIPTION"
        query = session_dump_wind.execute(query_sql)

        ticker_dict = dict()
        for asharest_db in query:
            s_info_windcode = asharest_db[0]
            ticker = s_info_windcode.split('.')[0]
            ticker_name = asharest_db[1]
            ticker_dict[ticker] = (ticker_name,)
        return ticker_dict

    def get_find_key(self, ticker):
        if ticker.isdigit():
            find_key = ticker.split(' ')[0]
        else:
            ticker_month = filter(lambda x: x.isdigit(), ticker)
            if len(ticker_month) == 3:
                find_key = filter(lambda x: not x.isdigit(), ticker) + '1' + ticker_month
            else:
                find_key = ticker
            find_key = find_key.upper()
        return find_key

    # 获取股票前收价
    def get_prev_close(self, date_str, ticker):
        if '-' in date_str:
            date_str = date_str.replace('-', '')

        prev_close = None
        if ticker.isdigit():
            summary_file_path = STOCK_SUMMARY_FILE_TEMPLATE % date_str
            if not os.path.exists(summary_file_path):
                # print 'not find summary file, date:%s' % date_str
                return prev_close
            find_ticker = ticker
            prev_close_index = 7
        else:
            summary_file_path = FUTURE_SUMMARY_FILE_TEMPLATE % date_str
            if not os.path.exists(summary_file_path):
                # print 'not find summary file, date:%s' % date_str
                return prev_close

            ticker_month = filter(lambda x: x.isdigit(), ticker)
            if len(ticker_month) == 3:
                find_ticker = filter(lambda x: not x.isdigit(), ticker) + '1' + ticker_month
            else:
                find_ticker = ticker
            find_ticker = find_ticker.upper()
            prev_close_index = 12

        with open(summary_file_path, 'rb') as fr:
            for line in islice(fr, 1, None):
                line_items = line.split(',')
                if find_ticker == line_items[0]:
                    prev_close = line_items[prev_close_index]
                    break
        return prev_close

    def get_prev_close_dict(self, date_str):
        """
            获取股票前收价字典  key：全大写，另ZCE的需要转换为4位数组，如:ZC706->ZC1706
        :param date_str:
        :return:
        """
        if '-' in date_str:
            date_str = date_str.replace('-', '')

        prev_close_dict = dict()
        stock_summary_file = STOCK_SUMMARY_FILE_TEMPLATE % date_str
        if not os.path.exists(stock_summary_file):
            print 'can not find summary file, date:%s' % date_str
        else:
            with open(stock_summary_file, 'rb') as fr:
                prev_close_index = 7
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    prev_close = line_items[prev_close_index]
                    prev_close_dict[ticker] = prev_close

        future_summary_file = FUTURE_SUMMARY_FILE_TEMPLATE % date_str
        if not os.path.exists(future_summary_file):
            print 'can not find summary file, date:%s' % date_str
        else:
            with open(future_summary_file, 'rb') as fr:
                prev_close_index = 12
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    prev_close = line_items[prev_close_index]
                    prev_close_dict[ticker] = prev_close
        return prev_close_dict

    def get_close(self, date_str, ticker):
        """
           获取股票收盘价
        :param date_str:
        :param ticker:
        :return:
        """
        if '-' in date_str:
            date_str = date_str.replace('-', '')

        prev_close = None
        if ticker.isdigit():
            summary_file_path = STOCK_SUMMARY_FILE_TEMPLATE % date_str
            if not os.path.exists(summary_file_path):
                print 'stock not find summary file, date:%s' % date_str
                return prev_close
            find_ticker = ticker
            close_index = 6
        else:
            summary_file_path = FUTURE_SUMMARY_FILE_TEMPLATE % date_str
            if not os.path.exists(summary_file_path):
                print 'future not find summary file, date:%s' % date_str
                return prev_close

            ticker_month = filter(lambda x: x.isdigit(), ticker)
            if len(ticker_month) == 3:
                find_ticker = filter(lambda x: not x.isdigit(), ticker) + '1' + ticker_month
            else:
                find_ticker = ticker
            find_ticker = find_ticker.upper()
            close_index = 10

        with open(summary_file_path, 'rb') as fr:
            for line in islice(fr, 1, None):
                line_items = line.split(',')
                if find_ticker == line_items[0]:
                    prev_close = line_items[close_index]
                    break
        return prev_close

    def get_close_dict(self, date_str):
        """
            获取某天股票收盘价字典  key：全大写，另ZCE的需要转换为4位数组，如:ZC706->ZC1706
        :param date_str:
        :return:
        """
        if '-' in date_str:
            date_str = date_str.replace('-', '')

        close_dict = dict()
        stock_summary_file = STOCK_SUMMARY_FILE_TEMPLATE % date_str
        if not os.path.exists(stock_summary_file):
            print 'can not find summary file, date:%s' % date_str
        else:
            with open(stock_summary_file, 'rb') as fr:
                close_index = 6
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    prev_close = line_items[close_index]
                    close_dict[ticker] = prev_close

        future_summary_file = FUTURE_SUMMARY_FILE_TEMPLATE % date_str
        if not os.path.exists(future_summary_file):
            print 'can not find summary file, date:%s' % date_str
        else:
            with open(future_summary_file, 'rb') as fr:
                close_index = 10
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    prev_close = line_items[close_index]
                    close_dict[ticker] = prev_close
        return close_dict

    def build_instrument_dict(self):
        instrument_dict = dict()
        session_common = server_host.get_db_session('common')
        query = session_common.query(Instrument)
        for instrument_db in query:
            instrument_dict[instrument_db.ticker] = instrument_db
        return instrument_dict

    def get_stock_suspend_date_list(self, now_date_str=None):
        if now_date_str is None:
            now_date_str = date_utils.get_today_str()
        server_model = server_constant.get_server_model(wind_server_name)
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = "select S_INFO_WINDCODE from dump_wind.ASHARETRADINGSUSPENSION t where t.S_DQ_SUSPENDDATE = '%s'" % now_date_str
        query = session_dump_wind.execute(query_sql)

        suspend_stock_list = []
        for asharest_db in query:
            s_info_windcode = asharest_db[0]
            suspend_stock_list.append(s_info_windcode)

        suspend_date_list = []
        for suspend_stock in suspend_stock_list:
            query_sql = "select t.S_DQ_SUSPENDDATE, t.S_DQ_RESUMPDATE from dump_wind.ASHARETRADINGSUSPENSION t where t.S_INFO_WINDCODE = '%s' order by t.S_DQ_SUSPENDDATE desc" % suspend_stock
            query = session_dump_wind.execute(query_sql)
            suspenddate = None
            for suspen_item in query:
                suspenddate_temp = suspen_item[0]
                resumpdate = suspen_item[1]
                if resumpdate is not None and resumpdate < now_date_str:
                    break
                suspenddate = suspenddate_temp
            suspend_date_str = '%s-%s-%s' % (suspenddate[0:4], suspenddate[4:6], suspenddate[6:8])
            suspend_date_list.append('%s,%s' % (suspend_stock.split('.')[0], suspend_date_str))
        return suspend_date_list

    # 获取前一交易日一字跌停股票
    def get_yzd_stocks(self):
        yzd_stock_list = []
        stock_summary_file = STOCK_SUMMARY_FILE_TEMPLATE % date_utils.get_last_trading_day('%Y%m%d')
        if not os.path.exists(stock_summary_file):
            print 'can not find summary file, date:%s' % date_utils.get_last_trading_day('%Y%m%d')
        else:
            with open(stock_summary_file, 'rb') as fr:
                high_index = 4
                low_limited_index = 12
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    if float(line_items[high_index]) > 0 and float(line_items[high_index]) == float(line_items[low_limited_index]):
                        yzd_stock_list.append(ticker)
        return yzd_stock_list

    # 获取前一交易日一字涨停股票
    def get_yzz_stocks(self):
        yzz_stock_list = []
        stock_summary_file = STOCK_SUMMARY_FILE_TEMPLATE % date_utils.get_last_trading_day('%Y%m%d')
        if not os.path.exists(stock_summary_file):
            print 'can not find summary file, date:%s' % date_utils.get_last_trading_day('%Y%m%d')
        else:
            with open(stock_summary_file, 'rb') as fr:
                low_index = 5
                high_limited_index = 11
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    if float(line_items[low_index]) > 0 and float(line_items[low_index]) == float(line_items[high_limited_index]):
                        yzz_stock_list.append(ticker)
        return yzz_stock_list

    def get_first_trading_stocks(self):
        """
            获取昨日涨幅超过40%的股票(新股第一天上市)
        """
        new_stock_list = []
        stock_summary_file = STOCK_SUMMARY_FILE_TEMPLATE % date_utils.get_last_trading_day('%Y%m%d')
        if not os.path.exists(stock_summary_file):
            print 'can not find summary file, date:%s' % date_utils.get_last_trading_day('%Y%m%d')
        else:
            with open(stock_summary_file, 'rb') as fr:
                prev_close_index = 7
                close_index = 6
                for line in islice(fr, 1, None):
                    line_items = line.split(',')
                    ticker = line_items[0]
                    prev_close_value = line_items[prev_close_index]
                    close_value = line_items[close_index]
                    if float(prev_close_value) == 0:
                        continue

                    if float(close_value) / float(prev_close_value) - 1 > 0.4:
                        new_stock_list.append(ticker)
        return new_stock_list

    def get_pre_listed_ticker(self):
        """
            即将上市股票
        """
        filter_date_str = date_utils.get_today_str()

        server_model = server_constant.get_server_model(wind_server_name)
        session_dump_wind = server_model.get_db_session('dump_wind')
        query_sql = "select S_INFO_CODE from dump_wind.ASHAREDESCRIPTION where S_INFO_LISTDATE > %s" % filter_date_str
        query = session_dump_wind.execute(query_sql)

        return [x[0] for x in query]

    def __exit__(self, type, value, traceback):
        pass


if __name__ == '__main__':
    stock_utils = StockUtils()
    # print u'一字跌停:', stock_utils.get_yzd_stocks()
    # print u'第一天上市:', stock_utils.get_new_stocks()
    print stock_utils.get_pre_listed_ticker()
