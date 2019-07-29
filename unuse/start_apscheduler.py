# coding=utf-8
from datetime import datetime
from eod_aps.model.instrument import Instrument
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_EXECUTED
from eod_aps.task.server_manage_task import *
from eod_aps.task.maintain_server_task import *

jobstores = {
    'default': SQLAlchemyJobStore(url='mysql://admin:adminP@ssw0rd@172.16.10.126/jobs')
}
executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

apscheduler_logger = loggingUtils('apscheduler_run_message')
email_utils = EmailUtils(EmailUtils.group2)


def init_jobs():
    scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    # -------------------------------------以下为早盘任务----------------------------------
    # # 02:30关闭trademonitor的进程
    # scheduler.add_job(kill_trademonitor, 'cron', day_of_week='1-5', hour='02', minute="30", id='Kill Trademonitor')

    # 03:10关闭nanhua夜盘服务
    scheduler.add_job(stop_service_nanhua, 'cron', day_of_week='0-5', hour='03', minute='10', id='Stop Service NanHua')

    # # 03:20更新本地125数据库中pcf的tradingday信息
    # scheduler.add_job(update_local_pcf, 'cron', day_of_week='0-6', hour='03', minute='20', id='Update Local PCF')

    # scheduler.add_job(zip_log_file, 'cron', day_of_week='0-4', hour='07', minute='30', id='Zip Log File')

    # 03:30将南华和国信前一天的Log文件打包下载至本地
    # scheduler.add_job(log_zip_endofday, 'cron', day_of_week='0-4', hour='03', minute='30', id='Log_Zip_Endofday')

    # 04:00下载CTP行情数据
    scheduler.add_job(download_ctp_market_file, 'cron', day_of_week='0-5', hour='04', minute="00",
                      id='Download CTP Market File1')

    # 04:00下载除南华以外的其他服务器CTP行情数据
    scheduler.add_job(download_server_ctp_market_file, 'cron', day_of_week='0-5', hour='04', minute="00",
                      id='Download server CTP Market File2_1')

    # 04:30 生成历史数据
    scheduler.add_job(build_history_date, 'cron', day_of_week='0-5', hour='04', minute="30",
                      id='Build History Date1')

    # 05:30回测，获取参数更新数据库
    scheduler.add_job(backtest_automation, 'cron', day_of_week='0-5', hour='05', minute="30",
                      id='Backtest Automation1')

    # 06:00核对回测的state仓位与pf_position的仓位是否一致
    scheduler.add_job(trading_position_check, 'cron', day_of_week='0-4', hour='06', minute="00",
                      id='trading_position_check1')

    # 06:05对比检查实盘保存的state和回测计算得到的state是否一致
    scheduler.add_job(strategy_state_check, 'cron', day_of_week='0-4', hour='06', minute="05",
                      id='strategy_state_check1')

    # 08:10数据库预更新操作a.更新南华账号的enable值
    scheduler.add_job(db_pre_update, 'cron', day_of_week='0-4', hour='08', minute="10",
                      id='DB Pre Update1')

    # 08:30检查history_date文件是否正常生成
    scheduler.add_job(history_date_file_check, 'cron', day_of_week='0-4', hour='08', minute="30",
                      id='History Date File Check1')

    # 08:30检测各服务器VPN是否连接正常
    scheduler.add_job(server_connection_monitor, 'cron', day_of_week='0-4', hour='08', minute="30",
                      id='Server Connection Monitor')

    # 08:34启动持仓更新
    scheduler.add_job(start_update_position, 'cron', day_of_week='0-4', hour='08', minute="34",
                      id='Update Position1')

    # 08:35启动检查隔夜单任务
    scheduler.add_job(order_check, 'cron', day_of_week='0-4', hour='08', minute="35", id='Order Check')

    # 08:37启动南华真实仓位和策略仓位比对服务
    scheduler.add_job(pf_real_position_check, 'cron', day_of_week='0-4', hour='08', minute="37",
                      id='Check Account Position1')

    # 08:38启动持仓更新
    scheduler.add_job(start_update_price, 'cron', day_of_week='0-4', hour='08', minute="38", id='Update Price1')

    # 08:39 多因子策略修改华宝服务器参数及上传权重参数
    scheduler.add_job(stkintraday_file_build, 'cron', day_of_week='0-4', hour='08', minute='39',
                      id='Stkintraday File Build')

    # 08:41 中信，南华和网络数据进行比对，校验更新
    scheduler.add_job(db_check, 'cron', args=('nanhua_web,guoxin,huabao,zhongxin',), day_of_week='0-4', hour='08',
                      minute='41', id='DB Check1')

    # 08:45启动nanhua_web,guoxin,huabao的服务
    scheduler.add_job(start_server, 'cron', args=('nanhua_web,guoxin,huabao,zhongxin',), day_of_week='0-4', hour='08',
                      minute="45", id='Start Server1')

    # 08:48 获取并更新讯投仓位
    scheduler.add_job(proxy_manager, 'cron', day_of_week='0-4', hour='08', minute='48', id='proxy manager')

    # 08:48 自动启动南华和中信的策略
    scheduler.add_job(start_server_strategy, 'cron', day_of_week='0-4', hour='08', minute='48',
                      id='Start Server Strategy1')

    # # 09:03启动华宝，国信服务
    # scheduler.add_job(start_server, 'cron', args=('huabao',), day_of_week='0-4', hour='09', minute='03',
    #                   id='Start Server2')

    # # 08:50 aggregator启动
    # scheduler.add_job(start_aggregator, 'cron', day_of_week='0-4', hour='08', minute='50', id='Start Aggregator')

    # 08:52 系统启动后检查
    scheduler.add_job(after_start_check, 'cron', day_of_week='0-4', hour='08', minute='52', id='After Start Check')

    # 09:00 生成多因子策略的股票购买清单
    scheduler.add_job(algo_file_build, 'cron', day_of_week='0-4', hour='09', minute='00', id='Algo File Build')

    # 09:03 复牌股票校验
    scheduler.add_job(resume_ticker_check, 'cron', day_of_week='0-4', hour='09', minute='03', id='Resume Ticker Check')

    # # 09:04 华宝,国信和网络数据进行比对，校验更新
    # scheduler.add_job(db_check, 'cron', args=('huabao,guoxin',), day_of_week='0-4', hour='09', minute='04', id='DB Check2')

    # # 09:00 比较多因子策略购买清单和实际持仓清单比较
    # scheduler.add_job(algo_file_compare, 'cron', day_of_week='0-4', hour='09', minute='00', id='Algo File Compare')

    # 09:10比较几个数据库间的差异
    scheduler.add_job(db_compare, 'cron', day_of_week='0-4', hour='09', minute='10', id='DB Compare')

    # 09:15启动国信日志校验任务
    scheduler.add_job(mkt_center_log_check, 'cron', day_of_week='0-4', hour='09', minute="15", id='MktCtr Log Check')

    # 9:20检查服务器状态
    scheduler.add_job(server_status_check, 'cron', day_of_week='0-4', hour='09', minute="20", id='server_status_check')

    # -------------------------------------以下为下午收盘后任务----------------------------------

    # 15:10 测试国信网速
    scheduler.add_job(server_speed_test, 'cron', day_of_week='0-4', hour='15', minute='10', id='server_speed_test')

    # 15:15更新行情数据_下午
    scheduler.add_job(mc_order_report, 'cron', day_of_week='0-4', hour='15', minute='15', id='MC Order Report')

    # 15:20 下载一些时效性需求较高的文件
    scheduler.add_job(download_target_file, 'cron', day_of_week='0-4', hour='15', minute='20',
                      id='Download target file')

    # 15:25 更新华宝和中信的CTP账号持仓资金数据（如果MoneyManger有修改）
    scheduler.add_job(update_account_money, 'cron', day_of_week='0-4', hour='15', minute='25',
                      id='Update Account Money')

    # 15:30更新行情数据_下午
    scheduler.add_job(daily_update_afternoon, 'cron', day_of_week='0-4', hour='15', minute='30',
                      id='Daily Update Afternoon')

    # 15:35下载CTP行情数据
    scheduler.add_job(download_ctp_market_file, 'cron', day_of_week='0-4', hour='15', minute="35",
                      id='Download CTP Market File2')

    # 15:35下载除南华以外的其他服务器CTP行情数据
    scheduler.add_job(download_server_ctp_market_file, 'cron', day_of_week='0-4', hour='15', minute="35",
                      id='Download server CTP Market File2')

    # 15:40行情重建校验任务
    scheduler.add_job(mktcenter_rebuild_check, 'cron', day_of_week='0-4', hour='15', minute='40',
                      id='Mktcenter Rebuild Check')

    # 15:50下载国信和华宝行情文件
    scheduler.add_job(download_mktcenter_file, 'cron', day_of_week='0-4', hour='15', minute='50',
                      id='Download Mktcenter File')

    # 16:00将本交易日内的Log文件打包下载至本地
    # scheduler.add_job(log_zip_endofday, 'cron', day_of_week='0-4', hour='16', minute='00', id='log_zip_endofday')

    # 16:00 生成历史数据
    scheduler.add_job(build_history_date, 'cron', day_of_week='0-4', hour='16', minute="00",
                      id='Build History Date2')

    # 16:00 将换月信息保存的共享盘，供后续程序使用
    scheduler.add_job(get_future_main_contract_change_info, 'cron', day_of_week='0-4', hour='16', minute="00",
                      id='get future main contract change info')

    # 16:05启动数据库备份任务
    scheduler.add_job(db_backup, 'cron', day_of_week='0-4', hour='16', minute='05', id='DB BackUp')

    # 16:10关闭各服务器上的服务
    scheduler.add_job(stop_service, 'cron', day_of_week='0-4', hour='16', minute='10', id='Stop Service')

    # # 16:10关闭trademonitor的进程
    # scheduler.add_job(kill_trademonitor, 'cron', day_of_week='0-4', hour='16', minute="10", id='Kill Trademonitor2')

    # 16:20 将order和trade保存至对应的history表中
    scheduler.add_job(order_trade_backup, 'cron', day_of_week='0-4', hour='16', minute='20', id='Order Trade Backup')

    # 16:25 处理主力合约修改流程
    scheduler.add_job(main_contract_change, 'cron', day_of_week='0-4', hour='16', minute='25',
                      id='Main Contract Change')

    # 16:35 处理主力合约修改流程
    scheduler.add_job(main_contract_change_check, 'cron', day_of_week='0-4', hour='16', minute='35',
                      id='Main Contract Change Check')
    # # 16:30检查history_date文件是否正常生成
    # scheduler.add_job(history_date_file_check, 'cron', day_of_week='0-4', hour='16', minute="30",
    #                   id='History Date File Check2')

    # # 16:40 自动配平策略持仓和实际持仓间的差异（南华）
    # scheduler.add_job(close_position_automation, 'cron', day_of_week='0-4', hour='16', minute="40",
    #                   id='Close Position Automation')

    # 16:45 从各服务器同步持仓数据
    scheduler.add_job(aggregation_analysis, 'cron', day_of_week='0-4', hour='16', minute='45',
                      id='Aggregation Analysis')

    # 16:50 备份tradeplat日志文件
    scheduler.add_job(tar_tradeplat_log, 'cron', day_of_week='0-4', hour='16', minute='50', id='Tar Tradeplat Log')

    # 16:50 获取绩效计算需要的数据
    scheduler.add_job(get_pf_cal_sql_info, 'cron', day_of_week='0-4', hour='16', minute='50', id='Get Pf cal sql info')

    # # 下载wind数据
    # scheduler.add_job(wind_export_quote, 'cron', day_of_week='0-4', hour='16', minute='30', id='Wind Export Quote')
    # # 下载国信的CTP行情文件
    # scheduler.add_job(download_ctp_market_file, 'cron', day_of_week='0-4', hour='17', minute='00', id='Download CTP Market File')

    # 17:00回测DMI，获取参数只发送邮件
    scheduler.add_job(backtest_automation, 'cron', day_of_week='0-4', hour='17', minute="00",
                      id='Backtest Automation2')

    # 17:20 收盘后检查
    scheduler.add_job(index_return_update_job, 'cron', day_of_week='0-4', hour='17', minute='20',
                      id='index_return_update')

    # 17:30核对回测的state仓位与pf_position的仓位是否一致
    scheduler.add_job(trading_position_check, 'cron', day_of_week='0-4', hour='17', minute="30",
                      id='trading_position_check2')

    # 17:35对比检查实盘保存的state和回测计算得到的state是否一致
    scheduler.add_job(strategy_state_check, 'cron', day_of_week='0-4', hour='17', minute="35",
                      id='strategy_state_check2')

    # 17:35收盘后检查
    scheduler.add_job(check_after_market_close, 'cron', day_of_week='0-4', hour='17', minute='35',
                      id='check_after_market_close')

    # # 17:45每日统计各多因子策略的收益情况，并发送邮件通知
    # scheduler.add_job(daily_return_report, 'cron', day_of_week='0-4', hour='17', minute='45',
    #                   id='Daily Return Report')

    # 17:40 对比各篮子股票期货价值
    scheduler.add_job(basket_value_check, 'cron', day_of_week='0-4', hour='17', minute='40', id='Basket Value Check')

    # 17:45 生成账户层面的股票仓位分指数构成报告
    scheduler.add_job(index_constitute_report, 'cron', day_of_week='0-4', hour='17', minute='45', id='Index Constitute Report')
    # -------------------------------------以下为夜盘任务----------------------------------
    # 20:10数据库预更新操作a.更新期货账号的enable值
    scheduler.add_job(db_pre_update, 'cron', day_of_week='0-4', hour='20', minute="10", id='DB Pre Update2')

    # 20:30启动夜盘期货服务
    scheduler.add_job(start_server_future, 'cron', day_of_week='0-4', hour='20', minute="30",
                      id='Start Server Future')
    # 20:34 南华和网络数据进行比对，校验更新
    scheduler.add_job(db_check_future, 'cron', args=('nanhua_web,guoxin,zhongxin',), day_of_week='0-4', hour='20',
                      minute='34', id='DB Check3')
    # 20:35南华真实仓位和策略仓位比对服务
    scheduler.add_job(pf_real_position_check, 'cron', day_of_week='0-4', hour='20', minute="35",
                      id='Check Account Position2')

    # 20:36 自动启动南华和中信的策略
    scheduler.add_job(start_server_strategy, 'cron', day_of_week='0-4', hour='20', minute='36',
                      id='Start Server Strategy2')

    # # 20:38 aggregator启动
    # scheduler.add_job(start_aggregator_night, 'cron', day_of_week='0-4', hour='20', minute='38', id='Start Aggregator2')

    # 20:40 系统启动后检查
    scheduler.add_job(after_start_check, 'cron', day_of_week='0-4', hour='20', minute='40',
                      id='After Start Check2')

    # 22:00 行情重建ticker按照前日交易量进行分组
    scheduler.add_job(reset_mktdtctr_cfg_file, 'cron', day_of_week='0-4', hour='22', minute='00',
                      id='Reset Mktdtctr Cfg File')

    # 23:30上传volume_profile文件
    scheduler.add_job(volume_profile_upload, 'cron', day_of_week='0-4', hour='23', minute="30",
                      id='Volume Profile Upload')
    # -------------------------------------以下为非日频任务----------------------------------
    # 90s 服务器状态巡检
    scheduler.add_job(server_monitor, 'cron', day_of_week='0-4', hour='9-11,13-14,21-24', second='90',
                      id='Server Monitor')

    # 90s 服务器状态巡检
    scheduler.add_job(server_monitor_future, 'cron', day_of_week='1-5', hour='0-2', second='90', id='Server Monitor2')

    # # 30s 启动南华日志巡检任务
    # scheduler.add_job(nanhua_log_monitor, 'cron', day_of_week='0-4', hour='9-12,13-15', second='30',
    #                   id='NanHua Log Monitor')

    # 1h 定时访问数据库，避免丢失数据库链接
    scheduler.add_job(db_connection, 'cron', hour='*', id='DB Connection')

    # 每周五对各托管服务器上的文件进行清理
    scheduler.add_job(server_disk_clear, 'cron', day_of_week='4', hour='16', minute='00', id='Server Disk Clear')

    # 每周六下载tradeplat日志文件
    scheduler.add_job(download_tradeplat_log, 'cron', day_of_week='5', hour='08', minute='00',
                      id='Download Tradeplat Log')

    scheduler.start()


def db_connection():
    host_server_model = server_constant.get_server_model('host')
    session_common = host_server_model.get_db_session('common')
    query = session_common.query(Instrument)
    print query.count()
    host_server_model.close()

def err_listener(ev):
    if ev.exception:
        error_message = 'scheduled_run_time:%s\nretval:%s\nexception:%s\ntraceback:%s error.' % (
            str(ev.scheduled_run_time), str(ev.retval), str(ev.exception), str(ev.traceback))
    else:
        error_message = 'job_id:%s\nscheduled_run_time:%s error!' % (str(ev.job_id), str(ev.scheduled_run_time))
    apscheduler_logger.error(error_message)
    email_utils.send_email_group_all('[ERROR]Apscheduler Run!', error_message)


def run_listener(ev):
    run_message = 'job:%s run over!code:%s' % (str(ev.job_id), str(ev.code))
    apscheduler_logger.info(run_message)


def start_jobs():
    try:
        scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
        scheduler.add_listener(err_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        scheduler.add_listener(run_listener, EVENT_JOB_EXECUTED)
        scheduler.start()
    except Exception, e:
        traceback.print_exc()
        error_msg = traceback.format_exc()
        task_logger.error('Scheduler has some error,will stop!error_msg:%s' % error_msg)
        scheduler.shutdown()


def get_jobs():
    scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    jobs_array = scheduler.get_jobs()
    for job_info in jobs_array:
        print job_info.id, job_info.name, job_info.next_run_time


# def pause_job(job_id):
#     scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
#     jobs_info = scheduler.get_job(job_id)
#     jobs_info.pause()


if __name__ == '__main__':
    init_jobs()
    # start_jobs()
    # get_jobs()
    # pause_job('Stop Service')
