# coding: utf-8
import traceback
from cfg import custom_log
from eod_aps.job.strategy_index_deeplearning_job import index_deeplearning_init_job
from eod_aps.job.strategy_multifactor_init_job import strategy_multifactor_init_job
from eod_aps.job.strategy_stock_deeplearning_job import stock_deeplearning_init_job
from eod_aps.job.upload_deposit_server_job import upload_deposit_server_job
from eod_aps.job.update_deposit_server_db_job import update_deposit_server_db_job
from eod_aps.job.ts_position_revise_job import ts_position_revise_job
from eod_aps.model.schema_common import Instrument
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.server_manage_tools import server_service_rum_cmd
from eod_aps.job.update_server_instrument_job import re_update_instrument, start_update_etf
from eod_aps.job.fair_price_calculation_job import fair_price_calculation_job
from eod_aps.job.update_server_db_job import update_position_job
from eod_aps.job.daily_db_check_job import account_check_job
from eod_aps.tools.stock_wind_utils import StockWindUtils
from flask import request, flash, redirect, url_for, jsonify, make_response
from . import critical_job

operation_enums = const.BASKET_FILE_OPERATION_ENUMS
date_utils = DateUtils()
email_utils = EmailUtils(const.EMAIL_DICT['group2'])


@critical_job.route('/re_update_price_job', methods=['GET', 'POST'])
def re_update_price_job():
    try:
        re_update_instrument()
        start_update_etf()

        trade_servers_list = server_constant.get_trade_servers()
        fair_price_calculation_job()

        for server_name in trade_servers_list:
            server_service_rum_cmd(server_name, 'MainFrame', 'update pf')
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_info_task(error_msg)
        email_utils.send_email_group_all('[Error]re_update_price_job Fail!', error_msg)
        return make_response(jsonify(code=100, data=u'执行失败'), 200)
    return make_response(jsonify(code=200, data=u'行情更新成功'), 200)


@critical_job.route('/update_real_position', methods=['GET', 'POST'])
def update_real_position():
    try:
        params = request.json
        server_name = params.get('server_name')

        update_position_job([server_name, ])
        account_check_job([server_name, ])
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_info_task(error_msg)
        email_utils.send_email_group_all('[Error]update_real_position Fail!', error_msg)
        return make_response(jsonify(code=100, data=u'执行失败'), 200)
    return make_response(jsonify(code=200, data=u'执行成功'), 200)


@critical_job.route('/update_index_price', methods=['GET', 'POST'])
def update_index_price():
    try:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
        with StockWindUtils() as stock_wind_utils:
            ticker_type_list = [const.INSTRUMENT_TYPE_ENUMS.Index, ]
            index_ticker_list = stock_wind_utils.get_ticker_list(ticker_type_list)
            prev_close_dict = stock_wind_utils.get_prev_close_dict(filter_date_str, index_ticker_list)
            all_local_server_list = server_constant.get_all_local_servers()
            for server_name in all_local_server_list:
                server_model = server_constant.get_server_model(server_name)
                session_common = server_model.get_db_session('common')
                for index_db in session_common.query(Instrument).filter(
                        Instrument.type_id == const.INSTRUMENT_TYPE_ENUMS.Index):
                    if index_db.ticker not in prev_close_dict:
                        print 'Error ticker:' % index_db.ticker
                        continue
                    if str(prev_close_dict[index_db.ticker]) == 'nan':
                        continue
                    index_db.prev_close = prev_close_dict[index_db.ticker]
                    session_common.merge(index_db)
                session_common.commit()
    except Exception:

        error_msg = traceback.format_exc()
        custom_log.log_info_task(error_msg)
        email_utils.send_email_group_all('[Error]update_index_price Fail!', error_msg)
        return make_response(jsonify(code=100, data=u'执行失败'), 200)
    return make_response(jsonify(code=200, data=u'指数行情更新成功'), 200)


# @critical_job.route('/ts_pf_account_list', methods=['GET', 'POST'])
# def ts_pf_account_list():
#     try:
#         ts_pf_account_list = []
#         server_name = 'guoxin'
#         group_name_list = ['Event_Real', 'manual']
#         fund_name = 'balance01'
#         server_model = server_constant.get_server_model(server_name)
#         session_portfolio = server_model.get_db_session('portfolio')
#         for pf_account_db in session_portfolio.query(PfAccount).filter(PfAccount.group_name.in_(group_name_list),
#                                                                        PfAccount.fund_name.like('%' + fund_name + '%')):
#             server_item_dict = dict()
#             server_item_dict['value'] = pf_account_db.fund_name
#             server_item_dict['label'] = pf_account_db.fund_name
#             ts_pf_account_list.append(server_item_dict)
#     except Exception:
#         error_msg = traceback.format_exc()
#         custom_log.log_info_task(error_msg)
#         email_utils.send_email_group_all('[Error]ts_pf_account_list Fail!', error_msg)
#         return make_response(jsonify(code=100, data=u'执行失败'), 200)
#     return make_response(jsonify(code=200, data=ts_pf_account_list), 200)


@critical_job.route('/ts_position_revise', methods=['GET', 'POST'])
def ts_position_revise():
    try:
        params = request.json
        pf_account_name = params.get('pf_account_name')

        account_name = '198800888042'
        ts_position_revise_job(account_name, pf_account_name)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_info_task(error_msg)
        email_utils.send_email_group_all('[Error]ts_position_revise Fail!', error_msg)
        return make_response(jsonify(code=100, data=u'执行失败'), 200)
    return make_response(jsonify(code=200, data=u'执行成功'), 200)


@critical_job.route('/rerun_stkintraday_jobs', methods=['GET', 'POST'])
def rerun_stkintraday_jobs():
    try:
        params = request.json
        server_name = params.get('server_name')
        download_sql_flag = params.get('download_sql_flag')
        deeplearning_run_flag = params.get('deeplearning_run_flag')

        server_model = server_constant.get_server_model(server_name)
        if download_sql_flag == 'Yes' and server_model.type == 'deposit_server':
            sql_library_list = ['common', 'portfolio']
            update_deposit_server_db_job((server_name, ), sql_library_list)
        if deeplearning_run_flag == 'Yes':
            stock_deeplearning_init_job(server_name)
            index_deeplearning_init_job(server_name)

        email_list1, email_list2 = [], []
        strategy_multifactor_init_job(server_name, email_list1, email_list2)

        if len(email_list1) > 0:
            email_title = '[Warning]Algo File Build Report'
            email_utils.send_email_group_all(email_title, ''.join(email_list1), 'html')

        if server_model.type == 'deposit_server':
            upload_deposit_server_job((server_name, ))
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_info_task(error_msg)
        email_utils.send_email_group_all('[Error]rerun_stkintraday_jobs Fail!', error_msg)
        return make_response(jsonify(code=100, data=u'执行失败'), 200)
    return make_response(jsonify(code=200, data=u'执行成功'), 200)
