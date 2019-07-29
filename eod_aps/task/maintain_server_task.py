# -*- coding: utf-8 -*-
import threading
import time
from eod_aps.job.strategy_stock_deeplearning_job import stock_deeplearning_init_job
from eod_aps.job.strategy_index_deeplearning_job import index_deeplearning_init_job
from eod_aps.job.strategy_multifactor_init_job import strategy_multifactor_init_job
from eod_aps.job.exchange_notice_monitor_job import ExchangeNoticeMonitor
from eod_aps.job.factordata_file_rebuild_job import factordata_file_rebuild_job
from eod_aps.job.special_tickers_init_job import SpecialTickersInitJob
from eod_aps.job.stkintraday_report_job import StkintradayReport
from eod_aps.model.server_constans import server_constant
from eod_aps.job.log_file_zip_job import zip_log_file_job
from eod_aps.job.download_server_file_job import download_mktcenter_file_job, tar_tradeplat_log_job, \
    download_trade_server_log_job, download_deposit_server_log_job, download_ctp_market_file_job
from eod_aps.job.mktcenter_check_job import mktcenter_rebuild_check_job
from eod_aps.job.algo_file_build_job import StrategyBasketInfo
from eod_aps.job.algo_file_compare_job import algo_file_compare_job
from eod_aps.job.aggregation_analysis_job import aggregation_analysis_job
from eod_aps.job.order_trade_backup_job import order_trade_backup_job
from eod_aps.job.trading_signal_check_job import trading_position_check_job
from eod_aps.job.mc_order_report_job import mc_order_report_job
from eod_aps.job.close_position_automation_job import close_position_automation_job
from eod_aps.job.download_target_file_job import download_target_file_job
from eod_aps.job.strategy_state_check_job import strategy_state_check_job
from eod_aps.job.main_contract_change_job import main_contract_change_job
from eod_aps.job.daily_return_calculation_job import index_return_calculation_job
from eod_aps.job.special_ticker_report_job import special_ticker_report_job
from eod_aps.job.index_constitute_report_job import index_constitute_report_job
from eod_aps.job.scattered_stock_repair_job import ScatteredStockTools
from eod_aps.job.margin_ratio_check_job import margin_ratio_check_job
from eod_aps.job.server_disk_clear_job import server_disk_clear_job
from eod_aps.job.get_backtest_info_job import get_backtest_info_job
from eod_aps.job.tradeplat_init_index_job import tradeplat_init_index_job
from eod_aps.job.insert_strategy_state_sql_job import insert_strategy_state_sql_job
from eod_aps.job.oma_quota_build_job import oma_quota_build_job
from eod_aps.job.build_calendarma_transfer_parameter_job import build_calendarma_transfer_parameter_job
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.server_manage_tools import restart_server_service, save_pf_position
from eod_aps.tools.vwap_stream_cal_tools import VwapCalTools
from eod_logger import log_trading_wrapper, log_wrapper
from eod_aps.model.eod_const import const
from eod_aps.job.stock_index_future_position_check import stock_index_future_position_check_job
from eod_aps.job.spider_cff_info_job import spider_cff_info_job

date_utils = DateUtils()
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])
email_utils9 = EmailUtils(const.EMAIL_DICT['group9'])
email_utils16 = EmailUtils(const.EMAIL_DICT['group16'])

BACKTEST_WSDL_ADDRESS = const.EOD_CONFIG_DICT['backtest_wsdl_address']
operation_enums = const.BASKET_FILE_OPERATION_ENUMS


@log_trading_wrapper
def zip_log_file():
    trade_servers_list = server_constant.get_trade_servers()
    zip_log_file_job(trade_servers_list)


@log_trading_wrapper
def db_backup():
    from eod_aps.job.db_backup_job import db_backup_job
    trade_servers_list = server_constant.get_trade_servers()
    db_backup_job(trade_servers_list)


@log_wrapper
def clear_deposit_ftp_job():
    deposit_servers = server_constant.get_deposit_servers()
    from eod_aps.job.clear_deposit_ftp_job import clear_deposit_ftp_job
    clear_deposit_ftp_job(deposit_servers)


@log_trading_wrapper
def download_mktcenter_file():
    """
        下载当日的mktcenter文件
    """
    mkt_center_servers = server_constant.get_mktcenter_servers()
    download_servers = list(reversed(mkt_center_servers))
    download_mktcenter_file_job(download_servers)


@log_trading_wrapper
def mktcenter_rebuild_check():
    """
        当校验日行情重建正确率
    """
    mkt_center_servers = server_constant.get_mktcenter_servers()
    mktcenter_rebuild_check_job(mkt_center_servers)


@log_trading_wrapper
def build_calendarma_transfer_parameter():
    """
        生成calendarma.transfer的参数
    """
    cta_server_list = server_constant.get_cta_servers()
    build_calendarma_transfer_parameter_job(cta_server_list)


@log_trading_wrapper
def db_pre_update_am():
    """
        数据库预更新操作a.更新南华账号的enable值
    """
    trade_servers_list = server_constant.get_trade_servers()
    from eod_aps.job.db_pre_update_job import db_pre_update_job
    db_pre_update_job(trade_servers_list)


@log_trading_wrapper
def db_pre_update_pm():
    trade_servers_list = server_constant.get_trade_servers()
    from eod_aps.job.db_pre_update_job import db_pre_update_job
    db_pre_update_job(trade_servers_list)


def __ln_mktdt_center_config(server_name, config_file_name):
    server_model = server_constant.get_server_model(server_name)
    ln_cmd_str = ['cd %s/cfg' % server_model.server_path_dict['tradeplat_project_folder'],
                  'rm config_mktdt_center.ini',
                  'ln -s %s config_mktdt_center.ini' % config_file_name
                  ]
    server_model.run_cmd_str(';'.join(ln_cmd_str))


@log_trading_wrapper
def server_disk_clear():
    """
        每周五对各托管服务器上的文件进行清理
    """
    trade_servers_list = server_constant.get_trade_servers()
    server_disk_clear_job(trade_servers_list)


@log_wrapper
def download_ctp_market_file_am():
    download_market_file_servers = server_constant.get_download_market_servers()
    download_ctp_market_file_job(download_market_file_servers)


@log_wrapper
def download_ctp_market_file_pm():
    download_market_file_servers = server_constant.get_download_market_servers()
    download_ctp_market_file_job(download_market_file_servers)


@log_wrapper
def update_strategy_online_am():
    """
        将策略参数等信息更新至共享盘并缓存到NAS供回测使用
    """
    cta_server_list = server_constant.get_cta_servers()
    get_backtest_info_job(cta_server_list)


@log_wrapper
def update_strategy_online_pm():
    cta_server_list = server_constant.get_cta_servers()
    get_backtest_info_job(cta_server_list)


@log_wrapper
def update_strategy_state_check_am():
    """
        启动回测程序获取DMI的结果
    """
    import datetime
    if datetime.datetime.now().weekday() == 5:
        time.sleep(3600)
    cta_server_list = server_constant.get_cta_servers()
    insert_strategy_state_sql_job(cta_server_list)
    # strategy_state_check_job(cta_server_list)
    trading_position_check_job(cta_server_list)


@log_wrapper
def update_strategy_state_check_pm():
    cta_server_list = server_constant.get_cta_servers()
    insert_strategy_state_sql_job(cta_server_list)
    # strategy_state_check_job(cta_server_list)
    trading_position_check_job(cta_server_list)


@log_wrapper
def reset_mktdtctr_cfg_file():
    """
        行情重建ticker按照前日交易量进行分组
    """
    from eod_aps.job.reset_mktdtctr_cfg_file_job import reset_mktdtctr_cfg_file_job
    reset_mktdtctr_cfg_file_job()


@log_wrapper
def volume_profile_upload():
    """
        上传volume_profile文件
    """
    mkt_center_servers = server_constant.get_mktcenter_servers()
    from eod_aps.job.volume_profile_upload_job import volume_profile_upload_job
    volume_profile_upload_job(mkt_center_servers)


@log_wrapper
def algo_file_compare():
    """
        比较多因子策略购买清单和实际持仓清单比较
    """
    algo_file_compare_job()


@log_trading_wrapper
def aggregation_analysis():
    """
        从各服务器同步持仓数据
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    for server_name in all_trade_servers_list:
        aggregation_analysis_job(server_name)


@log_trading_wrapper
def order_trade_backup():
    """
        将order和trade保存至对应的history表中
    """
    trade_servers_list = server_constant.get_trade_servers()
    order_trade_backup_job(trade_servers_list)


# @log_trading_wrapper
# def trading_position_check_am():
#     """
#         回测state仓位与数据库pf_position仓位对比
#     """
#     trading_position_check_job(cta_server_list)
#
#
# @log_trading_wrapper
# def trading_position_check_pm():
#     trading_position_check_job(cta_server_list)


@log_trading_wrapper
def strategy_state_check_am():
    """
        对比检查实盘保存的state和回测计算得到的state
    """
    cta_server_list = server_constant.get_cta_servers()
    strategy_state_check_job(cta_server_list)


@log_trading_wrapper
def strategy_state_check_pm():
    cta_server_list = server_constant.get_cta_servers()
    strategy_state_check_job(cta_server_list)


@log_trading_wrapper
def download_target_file():
    """
        下载一些时效性要求较高的文件
    """
    download_target_file_job()


@log_trading_wrapper
def close_position_automation():
    close_position_automation_job()


@log_trading_wrapper
def tar_tradeplat_log():
    """
        备份tradeplat日志文件
    """
    trade_servers_list = server_constant.get_trade_servers()
    tar_tradeplat_log_job(trade_servers_list)


@log_wrapper
def download_trade_server_log():
    """
        下载tradeplat日志文件
    """
    trade_servers_list = server_constant.get_trade_servers()
    download_trade_server_log_job(trade_servers_list)


@log_wrapper
def download_deposit_server_log():
    """
        下载deposit_server日志文件
    """
    deposit_servers_list = server_constant.get_deposit_servers()
    download_deposit_server_log_job(deposit_servers_list)


@log_trading_wrapper
def start_server_strategy_am():
    """
        通过配置自动启动各服务器上的策略
    """
    cta_server_list = server_constant.get_cta_servers()
    from eod_aps.job.start_server_strategy_job import start_server_strategy_job
    start_server_strategy_job(cta_server_list)


@log_trading_wrapper
def start_server_strategy_pm():
    night_cta_server_list = server_constant.get_night_cta_servers()
    from eod_aps.job.start_server_strategy_job import start_server_strategy_job
    start_server_strategy_job(night_cta_server_list)


# # 获取、更新讯投仓位并检查是否更新成功
# @log_trading_wrapper
# def proxy_manager():
#     proxy_update_index()
#     account_check_job(['guoxin', ])


@log_trading_wrapper
def update_account_money():
    """
        更新华宝和中信的CTP账号持仓资金数据（如果MoneyManger有修改）
    """
    from eod_aps.job.update_account_money_job import update_account_money_job
    update_account_money_job()


@log_trading_wrapper
def main_contract_change():
    """
        处理主力合约修改流程
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    trade_servers_list = server_constant.get_trade_servers()

    for server_name in trade_servers_list:
        save_pf_position(server_name)
    time.sleep(5)

    main_contract_change_job(all_trade_servers_list)

    for server_name in trade_servers_list:
        save_pf_position(server_name)


@log_trading_wrapper
def index_return_update_job():
    """
        更新指数收益数据
    """
    index_return_calculation_job()


@log_trading_wrapper
def special_ticker_report():
    """
        今日需关注股票报告
    """
    stock_servers = server_constant.get_stock_servers()
    special_ticker_report_job(stock_servers)


@log_trading_wrapper
def index_constitute_report():
    """
        生成账户层面的股票仓位分指数构成报告
    """
    all_trade_servers_list = server_constant.get_all_trade_servers()
    index_constitute_report_job(all_trade_servers_list)


@log_trading_wrapper
def margin_ratio_check():
    """
        生成账户层面的股票仓位分指数构成报告
    """
    margin_ratio_check_job()


@log_trading_wrapper
def scattered_stock_repair():
    stock_servers = server_constant.get_stock_servers()
    for server_name in stock_servers:
        scattered_stock_tools = ScatteredStockTools(server_name)
        scattered_stock_tools.start_index()


@log_trading_wrapper
def server_risk_backup():
    from eod_aps.job.server_risk_backup_job import server_risk_backup_job
    server_risk_backup_job()


@log_trading_wrapper
def server_risk_report_daily():
    from eod_aps.job.server_risk_report_job import server_risk_report_daily_job
    server_risk_report_daily_job()


@log_trading_wrapper
def server_risk_report_week():
    from eod_aps.job.server_risk_report_job import server_risk_report_week_job
    server_risk_report_week_job()


@log_trading_wrapper
def upload_deposit_server_am():
    deposit_servers = server_constant.get_deposit_servers()
    from eod_aps.job.upload_deposit_server_job import upload_deposit_server_job
    upload_deposit_server_job(deposit_servers)


@log_trading_wrapper
def upload_deposit_server_pm():
    deposit_servers = server_constant.get_deposit_servers()
    from eod_aps.job.upload_deposit_server_job import upload_deposit_server_pm_job
    upload_deposit_server_pm_job(deposit_servers)


@log_trading_wrapper
def server_risk_report_daily():
    from eod_aps.job.server_risk_report_job import server_risk_report_daily_job
    server_risk_report_daily_job()


@log_wrapper
def server_risk_report_week():
    from eod_aps.job.server_risk_report_job import server_risk_report_week_job
    server_risk_report_week_job()


@log_wrapper
def server_risk_report_month():
    from eod_aps.job.server_risk_report_job import server_risk_report_month_job
    server_risk_report_month_job()


@log_trading_wrapper
def position_risk_report():
    from eod_aps.job.position_risk_report_job import position_risk_report_job
    position_risk_report_job()


@log_trading_wrapper
def update_deposit_server_db_am():
    deposit_servers = server_constant.get_deposit_servers()
    sql_library_list = ['common', 'portfolio']
    from eod_aps.job.update_deposit_server_db_job import update_deposit_server_db_job
    update_deposit_server_db_job(deposit_servers, sql_library_list)


@log_trading_wrapper
def update_deposit_server_db_pm():
    deposit_servers = server_constant.get_deposit_servers()
    sql_library_list = ['common', 'om', 'portfolio']
    from eod_aps.job.update_deposit_server_db_job import update_deposit_server_db_job
    update_deposit_server_db_job(deposit_servers, sql_library_list, 900)


@log_trading_wrapper
def oma_quota_build():
    oma_servers = server_constant.get_oma_servers()
    for server_name in oma_servers:
        oma_quota_build_job(server_name)


@log_trading_wrapper
def mc_order_report():
    """
        mc每日订单报告
    """
    oma_servers = server_constant.get_oma_servers()
    for server_name in oma_servers:
        mc_order_report_job(server_name)


@log_trading_wrapper
def nanhua_log_monitor():
    """
        定时检测南华服务器日志信息
    """
    from eod_aps.job.nanhua_log_monitor_job import nanhua_log_monitor_job
    nanhua_log_monitor_job('nanhua')


@log_trading_wrapper
def factordata_file_rebuild():
    factordata_file_rebuild_job()


@log_wrapper
def spider_cff_info():
    spider_cff_info_job()


# @log_trading_wrapper
# def tradeplat_init_local():
#     """
#         处理股票策略相关配置文件和调仓文件[交易服务器]
#     """
#     stock_servers = server_constant.get_stock_servers()
#     strategy_basket_info = StrategyBasketInfo(stock_servers[0], operation_enums.Change)
#     strategy_basket_info.ticker_index_report()
#
#     total_email_list1, total_email_list2, threads = [], [], []
#     for server_name in stock_servers:
#         server_model = server_constant.get_server_model(server_name)
#         if server_model.type != 'trade_server':
#             continue
#
#         t = threading.Thread(target=tradeplat_init_index_job, args=(server_name, total_email_list1, total_email_list2))
#         threads.append(t)
#
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()


# @log_trading_wrapper
# def tradeplat_init_deposit():
#     """
#         处理股票策略相关配置文件和调仓文件[托管服务器]
#     """
#     stock_servers = server_constant.get_stock_servers()
#
#     total_email_list1, total_email_list2, threads = [], [], []
#     for server_name in stock_servers:
#         server_model = server_constant.get_server_model(server_name)
#         if server_model.type != 'deposit_server':
#             continue
#
#         t = threading.Thread(target=tradeplat_init_index_job, args=(server_name, total_email_list1, total_email_list2))
#         threads.append(t)
#
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()
#
#     if len(total_email_list1) > 0:
#         email_title = '[Warning]Algo File Build Report'
#         email_utils9.send_email_group_all(email_title, ''.join(total_email_list1), 'html')
#
#     if len(total_email_list2) > 0:
#         email_title = '[Warning]Algo File Build Report_Detail'
#         email_utils2.send_email_group_all(email_title, ''.join(total_email_list2), 'html')


# def __tradeplat_init_index(server_name, total_email_list1, total_email_list2):
#     try:
#         print 'Server:%s TradePlat Init Start.' % server_name
#         strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Change)
#         email_trade_list, email_detail_list = strategy_basket_info.strategy_basket_file_build()
#         total_email_list1.extend(email_trade_list)
#         total_email_list2.extend(email_detail_list)
#
#         strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Add)
#         email_trade_list, email_detail_list = strategy_basket_info.strategy_basket_file_build()
#         total_email_list1.extend(email_trade_list)
#         total_email_list2.extend(email_detail_list)
#         print 'Server:%s TradePlat Init Stp1.' % server_name
#
#         account_list = const.INTRADAY_ACCOUNT_DICT[server_name]
#         tradeplat_init = TradeplatInit(server_name, account_list)
#         tradeplat_init.start_work()
#
#         strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Change)
#         strategy_basket_info.split_sigmavwap_ai()
#
#         error_message_list = strategy_basket_info.check_basket_file()
#         total_email_list2.extend(error_message_list)
#         print 'Server:%s TradePlat Init Stp2.' % server_name
#
#         server_model = server_constant.get_server_model(server_name)
#         if server_model.type == 'trade_server':
#             tensorflow_init = Tensorflow_init(server_name)
#             tensorflow_init.op_docker('restart', 'stkintraday_d1')
#             tensorflow_init.check_tensorflow_status()
#             tensorflow_init.check_server_proxy_status()
#             print 'Server:%s TradePlat Init Stp3.' % server_name
#
#             start_servers_tradeplat((server_name,))
#             print 'Server:%s TradePlat Init Stp4.' % server_name
#
#             # 重启后需要再发送一次策略启动命令
#             time.sleep(10)
#             from eod_aps.job.start_server_strategy_job import start_server_strategy_job
#             start_server_strategy_job((server_name,))
#         print 'Server:%s TradePlat Init Stp5.' % server_name
#         print 'Server:%s TradePlat Init Stop!' % server_name
#     except Exception:
#         error_msg = traceback.format_exc()
#         email_utils2.send_email_group_all('[Error]Running Error Job:tradeplat_init_index!', error_msg)


@log_trading_wrapper
def restart_mktdtcenter_service():
    stock_servers = server_constant.get_stock_servers()
    for server_name in stock_servers:
        server_model = server_constant.get_server_model(server_name)
        if server_model.type == 'trade_server':
            for service_name in ('MktDTCenter', 'HFCalculator'):
                restart_server_service(server_name, service_name)


@log_trading_wrapper
def account_position_report():
    all_trade_servers_list = server_constant.get_all_trade_servers()
    from eod_aps.job.account_position_report_job import account_position_report_job
    account_position_report_job(all_trade_servers_list)


@log_wrapper
def backtest_files_export_am():
    cta_server_list = server_constant.get_cta_servers()
    from eod_aps.job.backtest_files_export_job import backtest_files_export_job
    backtest_files_export_job(cta_server_list)


@log_trading_wrapper
def backtest_files_export_pm():
    cta_server_list = server_constant.get_cta_servers()
    from eod_aps.job.backtest_files_export_job import backtest_files_export_job
    backtest_files_export_job(cta_server_list)


@log_trading_wrapper
def account_position_repair():
    stock_servers2 = server_constant.get_stock_servers2()
    from eod_aps.job.account_position_repair_job import account_position_repair_job
    for server_name in stock_servers2:
        save_pf_position(server_name)
    time.sleep(5)

    for server_name in stock_servers2:
        account_position_repair_job(server_name)

    for server_name in stock_servers2:
        save_pf_position(server_name)


@log_trading_wrapper
def save_aggregator_message():
    from eod_aps.job.aggregator_msg_load_job import aggregator_msg_load_job
    aggregator_msg_load_job()


@log_trading_wrapper
def risk_calculation():
    from eod_aps.job.risk_calculation_job import RiskCalculationJob
    risk_calculation_job = RiskCalculationJob()
    risk_calculation_job.start_run()


@log_trading_wrapper
def ip_report():
    from eod_aps.job.ip_manager_job import ip_report_job
    ip_report_job()


@log_wrapper
def download_msci_file():
    from eod_aps.job.download_msci_file_job import download_msci_file_job
    download_msci_file_job()


@log_trading_wrapper
def reload_pickle_data():
    server_constant.reload_by_mmap()


@log_wrapper
def nas_files_backup():
    from eod_aps.job.nas_files_backup_job import nas_files_backup_job
    nas_files_backup_job()


@log_wrapper
def stkintraday_report():
    stock_deeplearning_list = server_constant.get_servers_by_strategy('Stock_DeepLearning')
    index_deeplearning_list = server_constant.get_servers_by_strategy('Index_DeepLearning')
    intraday_server_list = list(set(stock_deeplearning_list + index_deeplearning_list))
    for server_name in intraday_server_list:
        server_model = server_constant.get_server_model(server_name)
        if server_model.type != 'trade_server':
            continue

        account_list = const.INTRADAY_ACCOUNT_DICT[server_name] if server_name in const.INTRADAY_ACCOUNT_DICT else []
        stkintraday_report_job = StkintradayReport(server_name, account_list)
        stkintraday_report_job.report_index()


@log_wrapper
def exchange_notice_monitor1():
    monitor_item = ExchangeNoticeMonitor()
    monitor_item.start_work()


@log_wrapper
def exchange_notice_monitor2():
    monitor_item = ExchangeNoticeMonitor()
    monitor_item.start_work()


@log_trading_wrapper
def special_tickers_init():
    special_tickers_init_job = SpecialTickersInitJob()
    special_tickers_init_job.start_index()


@log_trading_wrapper
def alpha_calculation():
    from eod_aps.job.alpha_calculation_job import AlphaCalculationJob
    alpha_calculation_job = AlphaCalculationJob()
    alpha_calculation_job.start_run()


@log_trading_wrapper
def stock_index_future_position_report_job():
    stock_index_future_position_check_job()


@log_trading_wrapper
def unusual_order_notify():
    error_order_list = const.EOD_POOL['unusual_order_list']
    if error_order_list:
        table_title = 'Server,Account,Strategy,Symbol,Status,CreationT,TransactionT,Note'
        table_html_list = email_utils16.list_to_html(table_title, error_order_list)
        email_utils16.send_email_group_all('[Waning]Unusual Orders List', ''.join(table_html_list), 'html')


@log_trading_wrapper
def strategy_deeplearning_init():
    server_list1 = server_constant.get_servers_by_strategy('Stock_DeepLearning')
    for server_name in server_list1:
        stock_deeplearning_init_job(server_name)

    server_list2 = server_constant.get_servers_by_strategy('Index_DeepLearning')
    for server_name in server_list2:
        index_deeplearning_init_job(server_name)


@log_trading_wrapper
def strategy_multifactor_init():
    multi_factor_servers = server_constant.get_servers_by_strategy('Stock_MultiFactor')
    strategy_basket_info = StrategyBasketInfo(multi_factor_servers[0], operation_enums.Change)
    strategy_basket_info.ticker_index_report()

    email_list1, email_list2, threads = [], [], []
    for server_name in multi_factor_servers:
        t = threading.Thread(target=strategy_multifactor_init_job, args=(server_name, email_list1, email_list2))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if len(email_list1) > 0:
        email_title = 'Algo File Build Report'
        email_utils9.send_email_group_all(email_title, ''.join(email_list1), 'html')
    if len(email_list2) > 0:
        email_title = 'Algo File Build Report_Detail'
        email_utils2.send_email_group_all(email_title, ''.join(email_list2), 'html')


@log_trading_wrapper
def tradeplat_init_index():
    stock_servers = server_constant.get_stock_servers()
    table_list, threads = [], []

    for server_name in stock_servers:
        t = threading.Thread(target=tradeplat_init_index_job, args=(server_name, table_list))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if len(table_list) > 0:
        email_title = 'BasketFile Missing!'
        table_title = 'Server,Pf_Account,Content'
        table_html_list = email_utils9.list_to_html(table_title, table_list)
        email_utils9.send_email_group_all(email_title, ''.join(table_html_list), 'html')


@log_trading_wrapper
def vwap_cal():
    date_str = date_utils.get_today_str('%Y-%m-%d')
    vwap_cal_tools = VwapCalTools(date_str)
    error_messages = vwap_cal_tools.start_index()
    if error_messages:
        email_utils2.send_email_group_all('[Error]Vwap_Cal Missing Data!', ';'.join(error_messages), 'html')


if __name__ == '__main__':
    strategy_deeplearning_init()
    # 前一日,注意126的trade_server_list表中华宝增加Stock_DeepLearning
    # from eod_aps.job.upload_docker_models_job import UploadDockerModelFiles
    # upload_docker_model_files = UploadDockerModelFiles((server_name, ), 1)
    # upload_docker_model_files.upload_models_files()

    # 当日
    # from eod_aps.job.volume_profile_upload_job import volume_profile_upload_job
    # volume_profile_upload_job((server_name,))

    # stock_deeplearning_init_job(server_name)
    # index_deeplearning_init_job(server_name)
    #
    # tradeplat_init_index_job(server_name, [])

