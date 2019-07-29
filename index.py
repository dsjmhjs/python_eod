# -*- coding: utf-8 -*-
import random
from flask import render_template, request, jsonify, make_response
from flask import Flask
from flask_apscheduler import APScheduler
from flask_login import login_required
from flask_cors import CORS
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES
from eod_aps.task.maintain_server_task import *
from eod_aps.task.server_manage_task import *
from eod_aps.task.ysquant_manage_task import *
from eod_aps.task.eod_check_task import *
from cfg import *
from collections import OrderedDict


class Config(object):
    JOBS = [

        # -------------------------------------早盘前任务----------------------------------
        {'id': 'kill_aggregator', 'func': kill_aggregator, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '02', 'minute': "30", 'name': u'关闭Aggregator'},

        {'id': 'download_ctp_market_file_am', 'func': download_ctp_market_file_am, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '02', 'minute': "40", 'name': u'下载服务器CTP行情数据'},

        {'id': 'update_strategy_online_am', 'func': update_strategy_online_am, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '03', 'minute': "00", 'name': u'更新strategy_online[AM]'},

        {'id': 'backtest_files_export_am', 'func': backtest_files_export_am, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '03', 'minute': "05", 'name': u'回测文件导出[PM1]'},

        {'id': 'stop_service_am', 'func': stop_service_am, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '03', 'minute': "10", 'name': u'交易系统关闭'},

        {'id': 'build_history_date_am', 'func': build_history_date_am, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '03', 'minute': "15", 'name': u'生成历史数据(南华)'},

        {'id': 'update_strategy_state_check_am', 'func': update_strategy_state_check_am, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '05', 'minute': "50", 'name': u'更新CTA策略state并检查[AM]'},

        {'id': 'download_msci_file', 'func': download_msci_file, 'trigger': 'cron',
         'day_of_week': '0-6', 'hour': '05', 'minute': "01", 'name': u'msci_file文件下载'},

        # {'id': 'factordata_file_rebuild', 'func': factordata_file_rebuild, 'trigger': 'cron',
        #  'day_of_week': '0-6', 'hour': '07', 'minute': "30", 'name': u'整理HFCalculator配置文件'},

        {'id': 'spider_cff_info', 'func': spider_cff_info, 'trigger': 'cron',
         'day_of_week': '1-5', 'hour': '07', 'minute': "50", 'name': u'获取中金所网站持仓数据'},

        {'id': 'exchange_notice_monitor1', 'func': exchange_notice_monitor1, 'trigger': 'cron',
         'day_of_week': '0-6', 'hour': '08', 'minute': "00", 'name': u'监控公司公告1'},

        {'id': 'volume_profile_upload', 'func': volume_profile_upload, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "05", 'name': u'volume_profile文件上传'},

        {'id': 'db_pre_update_am', 'func': db_pre_update_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "10", 'name': u'数据库预更新'},

        {'id': 'history_date_file_check', 'func': history_date_file_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "25", 'name': u'历史quote数据生成检查(南华)'},

        {'id': 'reload_pickle_data', 'func': reload_pickle_data, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "28", 'name': u'缓存数据重新加载'},

        {'id': 'server_connection_monitor_am', 'func': server_connection_monitor_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "30", 'name': u'各服务器VPN检测'},

        {'id': 'position_risk_report', 'func': position_risk_report, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "32", 'name': u'A股持仓风险报告'},

        {'id': 'build_calendarma_transfer_parameter', 'func': build_calendarma_transfer_parameter,
         'trigger': 'cron', 'day_of_week': '0-4', 'hour': '08', 'minute': "33", 'name': u'更新换月参数'},

        {'id': 'order_check', 'func': order_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "34", 'name': u'隔夜单检查'},

        {'id': 'start_update_position', 'func': start_update_position, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "35", 'name': u'持仓更新'},

        {'id': 'start_update_future_price', 'func': start_update_future_price, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "37", 'name': u'行情更新[期货]'},

        {'id': 'start_update_stock_price', 'func': start_update_stock_price, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "39", 'name': u'行情更新[股票]'},

        {'id': 'db_check_am', 'func': db_check_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "43", 'name': u'早盘任务检查'},

        {'id': 'oma_quota_build', 'func': oma_quota_build, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "44", 'name': u'oma_quota数据生成'},

        {'id': 'start_server_am', 'func': start_server_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "45", 'name': u'交易系统启动'},

        {'id': 'start_aggregator_am', 'func': start_aggregator_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "46", 'name': u'Aggregator启动'},

        {'id': 'start_server_strategy_am', 'func': start_server_strategy_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "48", 'name': u'策略启动'},

        {'id': 'special_tickers_init', 'func': special_tickers_init, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "50", 'name': u'标记当日特殊股票'},

        {'id': 'strategy_deeplearning_init', 'func': strategy_deeplearning_init, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "52", 'name': u'日内策略初始化'},

        {'id': 'update_deposit_server_db_am', 'func': update_deposit_server_db_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "55", 'name': u'更新托管服务器数据库[AM]'},

        {'id': 'after_start_check_am', 'func': after_start_check_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "56", 'name': u'系统启动后检查'},

        {'id': 'strategy_multifactor_init', 'func': strategy_multifactor_init, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "57", 'name': u'多因子策略初始化'},

        {'id': 'tradeplat_init_index', 'func': tradeplat_init_index, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "00", 'name': u'Tradeplat配置初始化'},

        {'id': 'upload_deposit_server_am', 'func': upload_deposit_server_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "02", 'name': u'上传文件至托管服务器FTP'},

        {'id': 'special_ticker_report', 'func': special_ticker_report, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "03", 'name': u'今日需关注股票报告'},

        {'id': 'pf_real_position_check_am', 'func': pf_real_position_check_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "05", 'name': u'真实仓位和策略仓位比对'},

        {'id': 'save_aggregator_message', 'func': save_aggregator_message, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "10", 'name': u'缓存aggregator数据'},

        {'id': 'restart_mktdtcenter_service', 'func': restart_mktdtcenter_service, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "13", 'name': u'行情中心服务重启'},

        # {'id': 'mkt_center_log_check', 'func': mkt_center_log_check, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '09', 'minute': "15", 'name': u'国信日志校验'},

        {'id': 'server_status_check', 'func': server_status_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "20", 'name': u'服务器状态检查'},

        {'id': 'risk_calculation', 'func': risk_calculation, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "20", 'name': u'计算风控'},

        {'id': 'alpha_calculation', 'func': alpha_calculation, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "25", 'name': u'计算绩效'},

        # -------------------------------------下午收盘后任务----------------------------------
        {'id': 'unusual_order_notify', 'func': unusual_order_notify, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '14', 'minute': "30", 'name': u'异常订单通知'},

        {'id': 'scattered_stock_repair', 'func': scattered_stock_repair, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "10", 'name': u'自动处理零股'},

        # {'id': 'stkintraday_report', 'func': stkintraday_report, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '15', 'minute': "20", 'name': u'日内策略报告'},

        # {'id': 'update_account_money', 'func': update_account_money, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '15', 'minute': "25", 'name': u'更新华宝和中信的CTP账号持仓资金数据'},

        {'id': 'daily_update_afternoon', 'func': daily_update_afternoon, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "30", 'name': u'更新行情数据[PM]'},

        {'id': 'download_ctp_market_file_pm', 'func': download_ctp_market_file_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "35", 'name': u'下载服务器CTP行情数据[PM]'},

        {'id': 'upload_deposit_server_pm', 'func': upload_deposit_server_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "36", 'name': u'上传文件至托管服务器FTP[PM]'},

        {'id': 'main_contract_change', 'func': main_contract_change, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "40", 'name': u'主力合约换月流程'},

        {'id': 'account_position_repair', 'func': account_position_repair, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "42", 'name': u'真实仓位和实际仓位配平'},

        {'id': 'server_risk_backup', 'func': server_risk_backup, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "46", 'name': u'Risk备份任务'},

        {'id': 'download_mktcenter_file', 'func': download_mktcenter_file, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "50", 'name': u'下载行情文件'},

        {'id': 'server_risk_report_daily', 'func': server_risk_report_daily, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '15', 'minute': "55", 'name': u'Risk统计报告'},

        {'id': 'db_backup', 'func': db_backup, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "05", 'name': u'数据库备份'},

        {'id': 'order_trade_backup', 'func': order_trade_backup, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "20", 'name': u'交易流水备份'},

        {'id': 'stop_service_pm', 'func': stop_service_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "30", 'name': u'交易系统关闭[PM]'},

        {'id': 'build_history_date_pm', 'func': build_history_date_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "32", 'name': u'生成历史数据(南华)[PM]'},

        {'id': 'tar_tradeplat_log', 'func': tar_tradeplat_log, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "34", 'name': u'交易系统日志压缩'},

        {'id': 'main_contract_change_check', 'func': main_contract_change_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "35", 'name': u'主力合约修改检查'},

        {'id': 'update_deposit_server_db_pm', 'func': update_deposit_server_db_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "36", 'name': u'更新托管服务器数据库[PM]'},

        # {'id': 'db_compare', 'func': db_compare, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '16', 'minute': "40", 'name': u'服务器数据库比较'},

        {'id': 'backtest_files_export_pm', 'func': backtest_files_export_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "48", 'name': u'回测文件导出[PM1]'},

        {'id': 'basket_value_check', 'func': basket_value_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "50", 'name': u'各篮子股票期货价值'},

        {'id': 'update_strategy_online_pm', 'func': update_strategy_online_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "52", 'name': u'更新strategy_online[PM]'},

        {'id': 'account_position_report', 'func': account_position_report, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '16', 'minute': "55", 'name': u'期货账户可用资金报告'},

        {'id': 'switch_trading_day', 'func': switch_trading_day, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "00", 'name': u'切换交易日'},

        {'id': 'exchange_notice_monitor2', 'func': exchange_notice_monitor2, 'trigger': 'cron',
         'day_of_week': '0-6', 'hour': '17', 'minute': "00", 'name': u'监控公司公告2'},

        {'id': 'pf_real_position_check_pm1', 'func': pf_real_position_check_pm1, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "05", 'name': u'真实仓位和策略仓位比对[PM1]'},

        {'id': 'aggregation_analysis', 'func': aggregation_analysis, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "10", 'name': u'交易流水汇总'},

        {'id': 'download_deposit_server_log', 'func': download_deposit_server_log, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "12", 'name': u'托管服务器交易日志文件下载'},

        # {'id': 'upload_docker_models1', 'func': upload_docker_models1, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '17', 'minute': "12", 'name': u'上传日内models文件1'},

        {'id': 'ip_report', 'func': ip_report, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "15", 'name': u'IP检测报告'},

        {'id': 'index_return_update_job', 'func': index_return_update_job, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "20", 'name': u'更新指数收益数据'},

        {'id': 'kill_aggregator_pm', 'func': kill_aggregator_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "32", 'name': u'关闭Aggregator[PM]'},

        {'id': 'check_after_market_close', 'func': check_after_market_close, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "35", 'name': u'收盘后检查'},

        {'id': 'index_constitute_report', 'func': index_constitute_report, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "45", 'name': u'账户层面的股票仓位分指数构成报告'},

        {'id': 'asset_value_check', 'func': asset_value_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "46", 'name': u'check两个交易日前的净值是否入库'},

        {'id': 'stock_index_future_position_report_job', 'func': stock_index_future_position_report_job,
         'trigger': 'cron', 'day_of_week': '0-4', 'hour': '17', 'minute': "50", 'name': u'股指期货策略仓位报告'},

        {'id': 'update_strategy_state_check_pm', 'func': update_strategy_state_check_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '18', 'minute': "00", 'name': u'更新CTA策略state并检查[PM]'},

        {'id': 'vwap_cal', 'func': vwap_cal, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '19', 'minute': "50", 'name': u'vwap报告计算'},
        # -------------------------------------夜盘前任务----------------------------------
        {'id': 'server_connection_monitor_pm', 'func': server_connection_monitor_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "00", 'name': u'各服务器VPN检测'},

        # {'id': 'upload_docker_models2', 'func': upload_docker_models2, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '20', 'minute': "01", 'name': u'上传日内models文件2'},

        {'id': 'db_pre_update_pm', 'func': db_pre_update_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "10", 'name': u'数据库预更新[PM]'},

        {'id': 'start_server_pm', 'func': start_server_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "30", 'name': u'交易系统启动[PM]'},

        {'id': 'pf_real_position_check_pm2', 'func': pf_real_position_check_pm2, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "33", 'name': u'真实仓位和策略仓位比对[PM2]'},

        {'id': 'db_check_pm', 'func': db_check_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "34", 'name': u'任务检查[PM]'},

        {'id': 'start_aggregator_pm', 'func': start_aggregator_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "35", 'name': u'Aggregator启动[PM]'},

        {'id': 'start_server_strategy_pm', 'func': start_server_strategy_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "36", 'name': u'交易系统策略启动[PM]'},

        {'id': 'after_start_check_pm', 'func': after_start_check_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '20', 'minute': "37", 'name': u'系统启动后检查[PM]'},

        # -------------------------------------非日频任务----------------------------------
        {'id': 'server_risk_report_week', 'func': server_risk_report_week, 'trigger': 'cron',
         'day_of_week': '4', 'hour': '15', 'minute': "58", 'name': u'Risk统计报告[周]'},

        {'id': 'server_risk_report_month', 'func': server_risk_report_month, 'trigger': 'cron',
         'day': 'last', 'hour': '16', 'minute': "00", 'name': u'Risk统计报告[月]'},

        {'id': 'nas_files_backup', 'func': nas_files_backup, 'trigger': 'cron',
         'day': '1', 'hour': '12', 'minute': "00", 'name': u'Nas文件备份任务'},

        {'id': 'server_monitor', 'func': server_monitor, 'trigger': 'cron', 'misfire_grace_time': 30,
         'day_of_week': '0-4', 'hour': '9-11,13-14,21-24', 'second': '*/90', 'name': u'服务器状态巡检'},

        {'id': 'server_monitor_future', 'func': server_monitor_future, 'trigger': 'cron', 'misfire_grace_time': 30,
         'day_of_week': '1-5', 'hour': '0-2', 'second': '*/120', 'name': u'服务器状态巡检[PM]'},

        # {'id': 'server_order_monitor', 'func': server_order_monitor, 'trigger': 'cron', 'misfire_grace_time': 30,
        #  'day_of_week': '0-4', 'hour': '9-11,13-14', 'minute': '*/10', 'name': u'订单状态监测'},

        {'id': 'heart_beat_monitor', 'func': heart_beat_monitor, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '8-22', 'minute': '10', 'name': u'心跳程序'},

        {'id': 'server_risk_validate', 'func': server_risk_validate, 'trigger': 'cron',
         'day_of_week': '4', 'hour': '16', 'minute': '00', 'name': u'策略账户校验'},

        {'id': 'server_disk_clear', 'func': server_disk_clear, 'trigger': 'cron',
         'day_of_week': '4', 'hour': '16', 'minute': '45', 'name': u'服务器文件清理'},

        {'id': 'clear_deposit_ftp_job', 'func': clear_deposit_ftp_job, 'trigger': 'cron',
         'day_of_week': '4', 'hour': '17', 'minute': '30', 'name': u'清理托管服务器文件'},

        {'id': 'download_tradeplat_log', 'func': download_trade_server_log, 'trigger': 'cron',
         'day_of_week': '5', 'hour': '08', 'minute': '00', 'name': u'交易日志文件下载'},

        # -------------------------------------ysquant任务----------------------------------
        {'id': 'not_daily_reader', 'func': not_daily_reader, 'trigger': 'cron',
         'hour': '05', 'minute': "30", 'name': u'not_daily_reader[ysquant]'},

        {'id': 'daily_sharry_writer1', 'func': daily_sharry_writer1, 'trigger': 'cron',
         'day_of_week': '0-5', 'hour': '06', 'minute': "00", 'name': u'daily_sharry_writer1[ysquant]'},

        {'id': 'basic_sharry', 'func': basic_sharry, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '06', 'minute': "40", 'name': u'basic_sharry[ysquant]'},

        {'id': 'index_bin', 'func': index_bin, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '06', 'minute': "40", 'name': u'index_bin[ysquant]'},
    ]

    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': True,  # 积攒的任务只跑一次
        'max_instances': 1000,  # 支持1000个实例并发
        'misfire_grace_time': 600  # 600秒的任务超时容错
    }
    SCHEDULER_API_ENABLED = True


def err_listener(ev):
    try:
        if ev.code == EVENT_JOB_MAX_INSTANCES:
            error_msg = 'Job_Id:%s\n Max Instances Error!' % str(ev.job_id)
        elif ev.exception:
            error_msg = 'Scheduled_Run_Time:%s\nRetval:%s\nException:%s\nTraceback:%s Error.' \
                        % (str(ev.scheduled_run_time), str(ev.retval), str(ev.exception), str(ev.traceback))
        else:
            custom_log.log_error_task(ev)
            error_msg = 'Job_Id:%s\nScheduled_Run_Time:%s Unknown Error!' % (str(ev.job_id), str(ev.scheduled_run_time))
    except AttributeError:
        error_msg = traceback.format_exc()
    custom_log.log_error_task(error_msg)
    email_utils2.send_email_group_all('[ERROR]Apscheduler Job!', error_msg)


app = Flask(__name__)
CORS(app, supports_credentials=True)
app.secret_key = 'FYlKCBmQWwPzfDI4'

from templates.account import account as account_blueprint

app.register_blueprint(account_blueprint, url_prefix='/account')

from templates.eod import eod as eod_blueprint

app.register_blueprint(eod_blueprint, url_prefix='/eod')

from templates.critical_job import critical_job as critical_job_blueprint

app.register_blueprint(critical_job_blueprint, url_prefix='/critical_job')

from templates.report import report as report_blueprint

app.register_blueprint(report_blueprint, url_prefix='/report')

from templates.tool_list import tool as tool_blueprint

app.register_blueprint(tool_blueprint, url_prefix='/tool_list')

from templates.summary import summary as summary_blueprint

app.register_blueprint(summary_blueprint, url_prefix='/summary')

from templates.cta import cta as cta_blueprint

app.register_blueprint(cta_blueprint, url_prefix='/cta')

from templates.fund import fund as fund_blueprint

app.register_blueprint(fund_blueprint, url_prefix='/fund')

from templates.system import system as system_blueprint

app.register_blueprint(system_blueprint, url_prefix='/system')

from templates.display_module import display_module as display_module_blueprint

app.register_blueprint(display_module_blueprint, url_prefix='/display_module')

from templates.statistic_module import statistic_module as statistic_module

app.register_blueprint(statistic_module, url_prefix='/statistic_module')

app.config.from_object(Config())
app.config['SECRET_KEY'] = 'MY_KEY'

scheduler = APScheduler()
scheduler.init_app(app)
scheduler._logger = custom_log.get_logger('root')
scheduler.add_listener(err_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES)


# @app.route('/job', methods=['GET', 'POST'])
# @login_required
# def apscheduler_jobs():
#     jobs_array = scheduler.get_jobs()
#     jobs_lists = []
#
#     for obj in jobs_array:
#         dic = {'id': obj.id, 'name': obj.name}
#         func_name = obj.id
#         if func_name in const.JOB_START_TIME_DICT:
#             dic['start_time'] = const.JOB_START_TIME_DICT[func_name]
#
#         if func_name in const.JOB_END_TIME_DICT:
#             dic['end_time'] = const.JOB_END_TIME_DICT[func_name]
#
#         if hasattr(obj, 'next_run_time') and obj.next_run_time is not None:
#             dic['next_run_time'] = obj.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
#         else:
#             dic['next_run_time'] = '0000-00-00 00:00:00'
#         jobs_lists.append(dic)
#     jobs_lists.sort(key=lambda item: item['next_run_time'][11:])
#     return render_template('eod/jobs.html',
#                            jobs=jobs_lists,
#                            now_date_str=date_utils.get_today_str('%Y-%m-%d %H:%M:%S'),
#                            next_date_str=date_utils.get_next_trading_day('%Y-%m-%d'))


# ========================= manual task ==============================
@app.route('/task_manager', methods=['GET', 'POST'])
def task_manager():
    params = request.json
    task_id = params.get('task_id')
    option = params.get('option')

    custom_log.log_info_task('Job[%s]Manual Run.============================' % task_id)
    try:
        if option == 'pause':
            scheduler.pause_job(task_id)
        elif option == 'restart':
            scheduler.run_job(task_id)
        elif option == 'resume':
            scheduler.resume_job(task_id)
        complete_msg = 'Task:%s,Option:%s Success!' % (task_id, option)
    except Exception:
        custom_log.log_info_task(traceback.format_exc())
        complete_msg = 'Task:%s,Option:%s Fail!' % (task_id, option)
        return make_response(jsonify(code=100, message=complete_msg), 200)
    return make_response(jsonify(code=200, message=complete_msg), 200)


@app.route('/query_run_log')
@login_required
def query_run_log():
    query_log_path = 'log/eod_task.log'
    with open(query_log_path, 'rb') as fr:
        run_long_list = [x for x in fr.readlines()]
    return jsonify(run_long_list=run_long_list[-20:])


@app.route('/query_apscheduler_jobs', methods=['GET', 'POST'])
def query_apscheduler_jobs():
    params = request.json
    search_id = params.get('search_id')

    jobs_array = scheduler.get_jobs()
    apscheduler_job_lists = []

    for obj in jobs_array:
        if search_id and search_id not in obj.id:
            continue
        dic = {'id': obj.id, 'name': obj.name}
        func_name = obj.id
        if func_name in const.JOB_START_TIME_DICT:
            dic['start_time'] = const.JOB_START_TIME_DICT[func_name]

        if func_name in const.JOB_END_TIME_DICT:
            dic['end_time'] = const.JOB_END_TIME_DICT[func_name]

        if hasattr(obj, 'next_run_time') and obj.next_run_time is not None:
            dic['next_run_time'] = obj.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            dic['next_run_time'] = '0000-00-00 00:00:00'
        apscheduler_job_lists.append(dic)
    apscheduler_job_lists.sort(key=lambda item: item['next_run_time'][11:])
    query_result = {'data': apscheduler_job_lists}
    return make_response(jsonify(code=200, data=query_result), 200)


my_cookie = dict()

class MenuTree(object):
    id = None
    name = None
    url = None
    weight = 0
    parent_id = None
    sub_menu_list = None

    def __init__(self, id, name, url, weight, parent_id):
        self.id = id
        self.name = name
        self.url = url
        self.weight = int(weight)
        self.parent_id = parent_id
        self.sub_menu_list = []


@app.route('/login', methods=['GET', 'POST'])
def login():
    info = request.json
    if info is None:
        return make_response(jsonify(code=405, message="非法登陆!"))
    else:
        user_id = info.get('name')
        password = info.get('password')
        server_model = server_constant.get_server_model('host')
        session_job = server_model.get_db_session('jobs')
        query_sql = "select password, role_id from `jobs`.`user_list` where user_id='%s'" % user_id
        user_info_item = session_job.execute(query_sql).first()
        if not user_info_item or user_info_item[0] != password:
            return make_response(jsonify(code=404, message=u"用户名或密码错误，登陆失败!"))
        else:
            query_sql = "select menu_id_list from `jobs`.`role_list` where id='%s'" % user_info_item[1]
            menu_ids_str = session_job.execute(query_sql).first()[0]
            menu_id_list = menu_ids_str.split(';')
            menu_dict = dict()
            query_sql = "select `id`, `name`, `url`, `weight`, `parent_id` from jobs.menu_list"
            url_list = []
            for list_item in session_job.execute(query_sql):
                if list_item[2]:
                    url_list.append(list_item[2])
                menu_tree = MenuTree(list_item[0], list_item[1], list_item[2], list_item[3], list_item[4])
                menu_dict[list_item[0]] = menu_tree
            root_menu_ids = []

            query_sql = "select `id`, `parent_id` from jobs.menu_list where id in (%s) order by weight" % \
                        ','.join(menu_id_list)
            for menu_item in session_job.execute(query_sql):
                root_menu = __build_menu_tree(menu_item[0], menu_item[1], menu_dict)
                root_menu_ids.append(root_menu.id)

            root_menu_list = []
            for root_menu_id in list(set(root_menu_ids)):
                root_menu_list.append(menu_dict[root_menu_id])

            subject_list = __format_menu_list(root_menu_list)

            for subject_items in subject_list:
                temp_list = []
                for item in subject_items[2]:
                    if item not in temp_list:
                       temp_list.append(item)
                subject_items[2] = temp_list
            # print subject_list

            custom_key = hex(random.randint(268435456, 4294967295))
            key = custom_key[2:len(custom_key) - 1]
            key = '%s|%s' % (user_id, key)
            token = [key, subject_list]
            my_cookie[key] = url_list
            rst = {'token': token, 'role_id': user_info_item[1]}
            return make_response(jsonify(code=200, message=u"恭喜你，登陆成功!", data=rst))


def __format_menu_list(menu_list):
    result_list = []
    menu_list.sort(key=lambda item: item.weight)
    for menu_item in menu_list:
        sub_menu_list = __format_menu_list(menu_item.sub_menu_list) if menu_item.sub_menu_list else []
        result_list.append([menu_item.name, menu_item.url, sub_menu_list])
    return result_list


def __build_menu_tree(menu_id, parent_menu_id, menu_dict):
    menu = menu_dict[menu_id]
    parent_menu = menu_dict[parent_menu_id]
    parent_menu.sub_menu_list.append(menu)
    if parent_menu.parent_id is not None:
        root_menu = __build_menu_tree(parent_menu.id, parent_menu.parent_id, menu_dict)
    else:
        root_menu = parent_menu
    return root_menu


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    info = request.json
    if info is None:
        return make_response(jsonify(code=200, message=u"您已成功注销!"))
    else:
        login_key = info.get('key')

        if login_key in my_cookie:
            my_cookie.pop(login_key, None)
            return make_response(jsonify(code=200, message=u"您已成功注销!"))
        else:
            return make_response(jsonify(code=200, message=u"您已成功注销!"))


@app.route('/authentication', methods=['GET', 'POST'])
def authentication():
    info = request.json
    if info is None:
        return make_response(jsonify(code=401, message=u"对不起，您未登录!"))
    else:
        input_key = info.get('key')
        input_to = info.get('to')
        if input_key in my_cookie:
            routes = my_cookie[input_key]
            if input_to in routes:
                return make_response(jsonify(code=200, message=u"恭喜你，访问成功!"))
            else:
                return make_response(jsonify(code=404, message=u"对不起，访问权限不足!"))
        else:
            return make_response(jsonify(code=402, message=u"对不起，您未登录!"))


if __name__ == '__main__':
    # scheduler.start()
    custom_log.log_info_task("Running on http://0.0.0.0:10000/ (Press CTRL+C to quit)")
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False, threaded=True)

