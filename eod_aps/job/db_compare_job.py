# -*- coding: utf-8 -*-
# 校验各数据库中instrument表之间的不同
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.instrument_tools import query_use_instrument_dict
from eod_aps.job import *

filter_columns = ('LAST_PRICE', 'MARKET_STATUS_ID', 'CREATE_DATE', 'EFFECTIVE_SINCE', 'MARKET_STATUS_ID',
                  'BID', 'ASK', 'UPDATE_DATE', 'PREV_CLOSE_UPDATE_TIME', 'FAIR_PRICE', 'CONVERSION_RATE', 'COPY',
                  'LONGMARGINRATIO', 'SHORTMARGINRATIO', 'BUY_COMMISSION', 'SELL_COMMISSION', 'PCF')
column_filter_list = ['update_date', 'close_update_time', 'prev_close_update_time', 'buy_commission', 'sell_commission',
                      'fair_price', 'max_limit_order_vol', 'max_market_order_vol', 'is_settle_instantly',
                      'inactive_date', 'close', 'volume', 'shortmarginratio', 'shortmarginratio_hedge',
                      'shortmarginratio_speculation', 'shortmarginratio_arbitrage', 'longmarginratio_hedge',
                      'longmarginratio', 'longmarginratio_speculation', 'longmarginratio_arbitrage',
                      'stamp_cost', 'copy']


def __build_db_dict(server_name_list, build_level=1):
    global server_instrument_id_dict
    server_instrument_id_dict = dict()
    server_host = server_constant.get_server_model('host')
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        if build_level <= 2:
            instrument_list = query_use_instrument_dict(server_name)
            for instrument_db in instrument_list:
                dick_key = '%s|%s' % (server_name, instrument_db.id)
                server_instrument_id_dict[dick_key] = instrument_db
        server_model.close()
    server_host.close()


def __instrument_check(server_name_list):
    instrument_id_list = []
    filter_server_name = server_name_list[0]
    for dict_key in server_instrument_id_dict.keys():
        if filter_server_name not in dict_key:
            continue
        instrument_id_list.append(server_instrument_id_dict[dict_key].id)

    check_column_list = []
    for column_name in dir(Instrument):
        if column_name not in ['id', 'ticker']:
            continue
        check_column_list.append(column_name)

    table_list = []
    for column_name in check_column_list:
        # if column_name != 'ticker':
        #     continue
        for instrument_id in instrument_id_list:
            tr_list = [column_name, instrument_id]
            error_flag = False
            check_value = '|'
            for server_name in server_name_list:
                dict_key = '%s|%s' % (server_name, instrument_id)
                if dict_key in server_instrument_id_dict:
                    instrument_db = server_instrument_id_dict[dict_key]
                    column_value = getattr(instrument_db, column_name)
                    if check_value == '|':
                        check_value = column_value

                    if column_value != check_value:
                        error_flag = True
                        tr_list.append('%s(Error)' % column_value)
                    else:
                        tr_list.append(column_value)
                else:
                    error_flag = True
                    tr_list.append('/(Error)')

            if error_flag:
                table_list.append(tr_list)

    html_title = 'Column,ID,%s' % ','.join(server_name_list)
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))


def db_compare_job(server_list):
    global email_list
    email_list = []

    table_list = []
    type_title_list = []
    for server_name in server_list:
        tr_list = [server_name]

        server_model = server_constant.get_server_model(server_name)
        session_common = server_model.get_db_session('common')
        group_sql = 'select TYPE, sum(1) from common.instrument_all group by TYPE'
        sql_query = session_common.execute(group_sql)

        type_dict = {x[0]: x[1] for x in sql_query}
        if len(type_title_list) == 0:
            type_title_list = list(type_dict.keys())

        for index, type_title in enumerate(type_title_list):
            if len(table_list) > 0 and table_list[0][index + 1] != type_dict[type_title]:
                tr_list.append(str(type_dict[type_title]) + '(Error)')
            else:
                tr_list.append(type_dict[type_title])
        table_list.append(tr_list)
    html_title = 'Type,%s' % ','.join(type_title_list)
    html_list = email_utils2.list_to_html(html_title, table_list)
    email_list.append(''.join(html_list))

    # append instrument check
    trade_servers_list = server_constant.get_all_trade_servers()
    __build_db_dict(trade_servers_list)
    email_list.append('<br><br><li>Check Instrument</li>')
    __instrument_check(trade_servers_list)

    email_utils2.send_email_group_all('DB Compare Result', '\n'.join(email_list), 'html')


if __name__ == '__main__':
    all_trade_servers = server_constant.get_all_servers()
    db_compare_job(all_trade_servers)
