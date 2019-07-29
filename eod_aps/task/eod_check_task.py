# -*- coding: utf-8 -*-
import threading
import time
import traceback
from xmlrpclib import ServerProxy
from eod_aps.job.history_date_file_check_job import history_date_file_check_job
from eod_aps.model.schema_common import Instrument
from eod_aps.model.server_constans import server_constant
from eod_aps.job.server_monitor_job import server_monitor_index_job, server_connection_monitor_job, \
    local_server_connection_monitor_job, strategy_start_check, order_route_log_check, check_ts_order_group, \
    query_ip_url_check
from eod_aps.job.account_position_check_job import pf_real_position_check_job, pf_account_check
from eod_aps.job.order_check_job import order_check_job
from eod_aps.job.server_status_monitor_job import server_status_monitor_job
from eod_aps.job.daily_db_check_job import db_check_job, db_check_future_job
from eod_aps.job.server_speed_test_job import server_speed_test_job
from eod_aps.job.check_after_market_close_job import check_after_market_close_job
from eod_aps.job.check_after_market_close_job import service_close_check, trade_version_check
from eod_aps.job.basket_value_check_job import BasketValueReport
from eod_aps.job.change_month_check_job import change_month_check
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.model.eod_const import const
from eod_logger import log_wrapper, log_trading_wrapper

date_utils = DateUtils()
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


@log_wrapper
def history_date_file_check():
    """
        检查history_date文件是否正常生成
    """
    trade_servers_list = server_constant.get_trade_servers()
    calendar_server_list = []
    for server_name in trade_servers_list:
        server_model = server_constant.get_server_model(server_name)
        if server_model.is_calendar_server:
            calendar_server_list.append(server_name)
    history_date_file_check_job(calendar_server_list)


@log_trading_wrapper
def server_connection_monitor_am():
    """
        检测服务器VPN是否正常
    """
    # 校验GUI获取外网ip的网址
    trade_servers_list = server_constant.get_trade_servers()
    query_ip_url_check()
    server_connection_monitor_job(trade_servers_list)
    local_server_connection_monitor_job()


@log_trading_wrapper
def order_check():
    """
        启动检查隔夜单任务
    """
    night_session_server_list = server_constant.get_night_session_servers()
    order_check_job(night_session_server_list)


@log_trading_wrapper
def db_check_am():
    """
        和网络数据进行比对，校验更新
    """
    trade_servers_list = server_constant.get_trade_servers()
    db_check_job(trade_servers_list)


@log_trading_wrapper
def after_start_check_am():
    """
        系统启动后检查
    """
    cta_server_list = server_constant.get_cta_servers()
    trade_servers_list = server_constant.get_trade_servers()
    # ts_servers = server_constant.get_ts_servers()

    email_message_list = []
    email_message_list.extend(strategy_start_check(cta_server_list))
    email_message_list.extend(order_route_log_check(trade_servers_list))
    email_message_list.extend(service_close_check(trade_servers_list))
    email_message_list.extend(trade_version_check(trade_servers_list))
    # email_message_list.extend(check_ts_order_group(ts_servers))
    email_utils2.send_email_group_all('系统启动后检查', ''.join(email_message_list), 'html')


@log_trading_wrapper
def pf_real_position_check_am():
    """
        比较真实仓位和策略仓位
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    pf_real_position_check_job(all_trade_servers_list)


# @log_trading_wrapper
# def mkt_center_log_check():
#     """
#         校验国信日志文件
#     """
#     from eod_aps.job.mkt_center_log_check_job import mkt_center_log_check_job
#     for server_name in tdf_servers:
#         mkt_center_log_check_job(server_name)


@log_trading_wrapper
def server_status_check():
    """
        检查服务器状态
    """
    trade_servers_list = server_constant.get_trade_servers()
    server_status_monitor_job(trade_servers_list)


@log_trading_wrapper
def server_speed_test():
    """
        测试国信的网速
    """
    server_speed_test_job(['guoxin', ])


@log_trading_wrapper
def main_contract_change_check():
    """
        检查主力合约修改流程
    """
    change_month_check()


@log_trading_wrapper
def db_compare():
    """
        比较数据库instrument表数据是否一致
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    from eod_aps.job.db_compare_job import db_compare_job
    compare_server_list = ['host']
    compare_server_list.extend(all_trade_servers_list)
    db_compare_job(compare_server_list)


@log_trading_wrapper
def basket_value_check():
    """
        各篮子股票和期货价值对比
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    basket_value_report = BasketValueReport(all_trade_servers_list)
    basket_value_report.check_index()


@log_trading_wrapper
def pf_real_position_check_pm1():
    """
        真实仓位和策略仓位比对[PM1]
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    pf_real_position_check_job(all_trade_servers_list)
    pf_account_check(all_trade_servers_list)


@log_trading_wrapper
def check_after_market_close():
    """
        收盘后任务检查
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    check_after_market_close_job(all_trade_servers_list)


@log_trading_wrapper
def server_connection_monitor_pm():
    """
        检测服务器VPN是否正常
    """
    night_session_server_list = server_constant.get_night_session_servers()
    query_ip_url_check()
    server_connection_monitor_job(night_session_server_list)
    local_server_connection_monitor_job()


@log_trading_wrapper
def db_check_pm():
    """
        夜盘前数据库检查
    """
    night_session_server_list = server_constant.get_night_session_servers()
    db_check_future_job(night_session_server_list)



@log_trading_wrapper
def pf_real_position_check_pm2():
    """
        仓位比对
    """
    night_session_server_list = server_constant.get_night_session_servers()
    pf_real_position_check_job(night_session_server_list)


@log_trading_wrapper
def after_start_check_pm():
    """
        启动后检查
    """
    night_cta_server_list = server_constant.get_night_cta_servers()
    night_session_server_list = server_constant.get_night_session_servers()

    email_message_list = []
    email_message_list.extend(strategy_start_check(night_cta_server_list))
    email_message_list.extend(order_route_log_check(night_session_server_list))
    email_message_list.extend(service_close_check(night_session_server_list))
    email_utils2.send_email_group_all('系统启动后检查', ''.join(email_message_list), 'html')


@log_trading_wrapper
def server_monitor():
    """
        定时检测服务器各服务是否正常
    """
    if int(date_utils.get_today_str("%H%M%S")) < 91000 or int(date_utils.get_today_str("%H%M%S")) > 235500:
        return

    night_session_server_list = server_constant.get_night_session_servers()
    trade_servers_list = server_constant.get_trade_servers()
    night_market_flag = date_utils.is_night_market()
    if night_market_flag:
        server_monitor_index_job(night_session_server_list)
    else:
        server_monitor_index_job(trade_servers_list)


@log_wrapper
def server_monitor_future():
    """
        定时检测服务器各服务是否正常(为兼顾周六，暂不做交易日过滤，节假日可能会有问题)
    """
    now_time = long(date_utils.get_today_str('%H%M%S'))
    if now_time >= 23000:
        return

    night_session_server_list = server_constant.get_night_session_servers()
    server_monitor_index_job(night_session_server_list)


# @log_trading_wrapper
# def server_order_monitor():
#     """
#         订单检查
#     """
#     trade_servers_list = server_constant.get_trade_servers()
#     trading_time_flag = date_utils.is_trading_time()
#     if not trading_time_flag:
#         return
#     from eod_aps.job.server_order_monitor_job import server_order_monitor_job
#     server_order_monitor_job(trade_servers_list)


@log_wrapper
def heart_beat_monitor():
    """
        心跳程序（鉴于部分VPN会通过一段时间是否有流量来判断是否主动断开连接）
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    threads = []
    for server_name in all_trade_servers_list:
        t = threading.Thread(target=__heart_beat_monitor, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def __heart_beat_monitor(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        source_file_path = '%s/heart_beat.txt' % const.EOD_CONFIG_DICT['daily_files_folder']

        if server_model.type == 'deposit_server':
            upload_folder_path = '%s/%s' % (server_model.ftp_upload_folder, date_utils.get_today_str())
            if not server_model.is_exist(upload_folder_path):
                server_model.mkdir(upload_folder_path)
        else:
            upload_folder_path = server_model.server_path_dict['eod_project_folder']

        target_file_path = '%s/heart_beat.txt' % upload_folder_path
        server_model.upload_file(source_file_path, target_file_path)
    except Exception:
        hour = int(date_utils.get_today_str('%H'))
        if 7 <= hour <= 22:
            error_msg = traceback.format_exc()
            email_utils2.send_email_group_all('[Error]__heart_beat_monitor:%s' % server_name, error_msg)


@log_trading_wrapper
def server_risk_validate():
    """
        检查服务器状态
    """
    trade_servers_list = server_constant.get_trade_servers()
    from eod_aps.job.server_risk_backup_job import server_risk_validate_job
    server_risk_validate_job(trade_servers_list)


if __name__ == '__main__':
    basket_value_check()
