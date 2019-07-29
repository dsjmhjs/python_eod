# -*- coding: utf-8 -*-
from xmlrpclib import ServerProxy
from eod_aps.job.tensorflow_init import Tensorflow_init
from eod_aps.job.update_server_db_job import *
from eod_aps.job.del_expire_instrument_job import del_expire_instrument_job
from eod_aps.job.build_history_data_job import build_history_data_job
from eod_aps.job.daily_db_check_job import account_check_job
from eod_aps.job.fair_price_calculation_job import fair_price_calculation_job
from eod_aps.job.server_manage_job import start_servers_tradeplat, stop_servers_tradeplat
from eod_aps.job.update_server_instrument_job import start_update_instrument, \
    start_update_instrument_pm, start_update_etf, start_update_instrument_pandas
from eod_aps.job.update_delist_stock_job import update_delist_stock_job
from eod_aps.job.download_server_file_job import instrument_files_backup
from eod_aps.job.upload_docker_models_job import UploadDockerModelFiles
from eod_aps.job.aggregator_manager_job import start_aggregator_day, start_aggregator_night, stop_aggregator
from eod_aps.tools.instrument_check_tools import InstrumentCheckTools
from eod_aps.tools.server_manage_tools import *
from eod_aps.job.asset_value_check_job import asset_value_check_job
from eod_logger import log_trading_wrapper, log_wrapper
from eod_aps.job.update_price_job import update_stock_instrument_job, update_etf_instrument_job, \
    update_future_instrument_job

date_utils = DateUtils()
TS_WSDL_ADDRESS = const.EOD_CONFIG_DICT['ts_wsdl_address']


@log_trading_wrapper
def start_aggregator_am():
    """
        aggregator启动[am]
    """
    start_aggregator_day()


@log_trading_wrapper
def start_aggregator_pm():
    """
        aggregator启动[pm]
    """
    start_aggregator_night()


@log_wrapper
def kill_aggregator():
    """
        aggregator关闭
    """
    stop_aggregator()


@log_wrapper
def kill_aggregator_pm():
    """
        aggregator关闭[pm]
    """
    stop_aggregator()

    # 关闭142和50上的TS
    # for ts_wsdl_str in TS_WSDL_ADDRESS.split(';'):
    #     s = ServerProxy(ts_wsdl_str)
    #     s.kill_trade_station_job()


@log_trading_wrapper
def start_server_am():
    """
        系统启动
    """
    cta_server_list = server_constant.get_cta_servers()
    trade_servers_list = server_constant.get_trade_servers()
    check_server = cta_server_list[0]
    instrument_check_tools = InstrumentCheckTools(check_server)
    check_result = instrument_check_tools.check_index()

    if check_result:
        start_servers_tradeplat(trade_servers_list)
    else:
        email_utils2.send_email_group_all('[Error]start_server_am!', u'行情更新异常，交易系统未启动', 'html')


@log_trading_wrapper
def start_server_pm():
    """
        系统启动[pm]
    """
    # 预更新，设置prev_close
    all_servers_list = server_constant.get_all_servers()
    night_session_server_list = server_constant.get_night_session_servers()

    pre_update_market_job(all_servers_list)
    update_position_job(night_session_server_list)
    start_update_future_price()
    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(night_session_server_list)

    start_servers_tradeplat(night_session_server_list)

    instrument_check_tools = InstrumentCheckTools()
    instrument_check_tools.check_index()


@log_wrapper
def stop_service_am():
    """
        系统关闭
    """
    night_session_server_list = server_constant.get_night_session_servers()
    stop_servers_tradeplat(night_session_server_list)


@log_trading_wrapper
def stop_service_pm():
    """
        系统关闭[pm]
    """
    trade_servers_list = server_constant.get_trade_servers()
    intraday_server_list = server_constant.get_servers_by_strategy('Stock_DeepLearning')

    stop_servers_tradeplat(trade_servers_list)
    for server_name in intraday_server_list:
        server_model = server_constant.get_server_model(server_name)
        if server_model.type != 'trade_server':
            continue

        tensorflow_init = Tensorflow_init(server_name)
        tensorflow_init.op_docker('stop', 'stkintraday_d1')


@log_trading_wrapper
def switch_trading_day():
    """
       切换交易日
    """
    trade_servers_list = server_constant.get_trade_servers()
    calendar_server_list = server_constant.get_calendar_servers()
    all_local_servers = server_constant.get_all_local_servers()

    # 生成pf_position表下一个交易日的数据
    from eod_aps.job.pf_position_rebuild_job import pf_position_rebuild_job
    pf_position_rebuild_job(trade_servers_list)
    # 更新strategy_parameter表的值
    from eod_aps.job.strategy_parameter_update_job import strategy_parameter_update_job
    strategy_parameter_update_job(calendar_server_list)
    # 如果common.instrument存在新增，需要在account_trade_restrictions表插入数据
    from eod_aps.job.account_trade_retrictions_update_job import account_trade_restrictions_update_job
    account_trade_restrictions_update_job(trade_servers_list)

    from eod_aps.job.instrument_extend_update_job import instrument_switch_trading_day
    instrument_switch_trading_day(all_local_servers)


@log_trading_wrapper
def start_update_position():
    """
       持仓更新
    """
    trade_servers_list = server_constant.get_trade_servers()

    update_position_job(trade_servers_list)
    account_check_job(trade_servers_list)


@log_trading_wrapper
def start_update_price():
    """
       行情更新
    """
    trade_servers_list = server_constant.get_trade_servers()

    start_update_instrument()
    start_update_etf()
    fair_price_calculation_job()

    # 更新common.instrument表volume_tdy字段
    from eod_aps.job.instrument_extend_update_job import update_volume_tdy
    update_volume_tdy()

    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(trade_servers_list)

    # 如果common.instrument存在新增，需要在account_trade_restrictions表插入数据
    from eod_aps.job.account_trade_retrictions_update_job import account_trade_restrictions_update_job
    account_trade_restrictions_update_job(trade_servers_list)

    instrument_check_tools = InstrumentCheckTools()
    instrument_check_tools.check_index()

    # 文件备份
    instrument_files_backup()


@log_trading_wrapper
def start_update_future_price():
    trade_servers_list = server_constant.get_trade_servers()
    update_future_instrument_job()

    # 更新common.instrument表volume_tdy字段(期货)
    from eod_aps.job.instrument_extend_update_job import update_volume_tdy
    update_volume_tdy()

    # 更新服务器common.instrument表期货数据
    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(trade_servers_list, const.INSTRUMENT_TYPE_ENUMS.Future)


@log_trading_wrapper
def start_update_stock_price():
    trade_servers_list = server_constant.get_trade_servers()
    update_stock_instrument_job()
    update_etf_instrument_job()
    fair_price_calculation_job()

    # 更新服务器common.instrument表股票数据
    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(trade_servers_list, const.INSTRUMENT_TYPE_ENUMS.CommonStock)

    instrument_check_tools = InstrumentCheckTools()
    instrument_check_tools.check_index()

    # 文件备份
    instrument_files_backup()


@log_trading_wrapper
def daily_update_afternoon():
    """
       日盘收盘后更新
    """
    all_servers_list = server_constant.get_all_servers()
    fix_server_list = server_constant.get_fix_servers()
    trade_servers_list = server_constant.get_trade_servers()

    # 关闭国信的OrdGROUP，因FIX只能单点登录
    for server_name in fix_server_list:
        stop_server_service(server_name, 'OrdGROUP')
    update_position_job(trade_servers_list)

    trade_servers_list = server_constant.get_trade_servers()
    update_stock_instrument_job()

    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(trade_servers_list, const.INSTRUMENT_TYPE_ENUMS.CommonStock)

    update_future_instrument_job()
    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(trade_servers_list, const.INSTRUMENT_TYPE_ENUMS.Future)

    # 删除instrument表过期的数据
    del_expire_instrument_job(all_servers_list)
    update_delist_stock_job(all_servers_list)
    account_check_job(trade_servers_list)


@log_wrapper
def build_history_date_am():
    """
        生成历史数据
    """
    calendar_server_list = server_constant.get_calendar_servers()
    build_history_data_job(calendar_server_list)


@log_wrapper
def build_history_date_pm():
    calendar_server_list = server_constant.get_calendar_servers()
    build_history_data_job(calendar_server_list)


# # 获取绩效计算需要的文件
# @log_trading_wrapper
# def get_pl_cal_sql_info():
#     get_pl_cal_sql_info_job()

@log_trading_wrapper
def start_update_price_pandas():
    trade_servers_list = server_constant.get_trade_servers()

    start_update_instrument_pandas()
    start_update_etf()
    fair_price_calculation_job()

    # 更新common.instrument表volume_tdy字段
    from eod_aps.job.instrument_extend_update_job import update_volume_tdy
    update_volume_tdy()

    from eod_aps.job.update_server_instrument_job import update_server_instrument_job
    update_server_instrument_job(trade_servers_list)

    # 如果common.instrument存在新增，需要在account_trade_restrictions表插入数据
    from eod_aps.job.account_trade_retrictions_update_job import account_trade_restrictions_update_job
    account_trade_restrictions_update_job(trade_servers_list)

    # # 文件备份
    # instrument_files_backup()


@log_wrapper
def upload_docker_models1():
    intraday_server_list = server_constant.get_servers_by_strategy('Stock_DeepLearning')
    index_num = 1
    upload_docker_model_files = UploadDockerModelFiles(intraday_server_list, index_num)
    upload_docker_model_files.upload_models_files()


@log_wrapper
def upload_docker_models2():
    intraday_server_list = server_constant.get_servers_by_strategy('Stock_DeepLearning')
    index_num = 2
    upload_docker_model_files = UploadDockerModelFiles(intraday_server_list, index_num)
    upload_docker_model_files.upload_models_files()


@log_trading_wrapper
def asset_value_check():
    asset_value_check_job()


if __name__ == '__main__':
    # start_servers_tradeplat(trade_servers_list)
    # kill_aggregator_pm()
    print server_constant.get_future_market_server()
