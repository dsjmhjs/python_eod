# -*- coding: utf-8 -*-
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.server_constans import server_constant
from cfg import custom_log

date_utils = DateUtils()

email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])
email_utils3 = EmailUtils(const.EMAIL_DICT['group3'])
email_utils4 = EmailUtils(const.EMAIL_DICT['group4'])
email_utils5 = EmailUtils(const.EMAIL_DICT['group5'])
email_utils6 = EmailUtils(const.EMAIL_DICT['group6'])
email_utils7 = EmailUtils(const.EMAIL_DICT['group7'])
email_utils8 = EmailUtils(const.EMAIL_DICT['group8'])
email_utils9 = EmailUtils(const.EMAIL_DICT['group9'])
email_utils10 = EmailUtils(const.EMAIL_DICT['group10'])
email_utils11 = EmailUtils(const.EMAIL_DICT['group11'])
email_utils12 = EmailUtils(const.EMAIL_DICT['group12'])
email_utils13 = EmailUtils(const.EMAIL_DICT['group13'])
email_utils15 = EmailUtils(const.EMAIL_DICT['group15'])
email_utils16 = EmailUtils(const.EMAIL_DICT['group16'])
email_utils17 = EmailUtils(const.EMAIL_DICT['group17'])
email_utils18 = EmailUtils(const.EMAIL_DICT['group18'])

DATA_SHARE_FOLDER = const.EOD_CONFIG_DICT['data_share_folder']
ETF_FILE_BACKUP_FOLDER = '%s/index_weight' % DATA_SHARE_FOLDER
ETF_FILE_BACKUP_FOLDER2 = const.EOD_CONFIG_DICT['etf_file_backup_folder_beijing']
PRICE_FILES_BACKUP_FOLDER = '%s/price_files' % DATA_SHARE_FOLDER

ETF_FILE_PATH = const.EOD_CONFIG_DICT['etf_file_path']

DAILY_FILES_FOLDER = const.EOD_CONFIG_DICT['daily_files_folder']
DAILY_FILES_TEMP_FOLDER = '%s/temp' % DAILY_FILES_FOLDER

UPDATE_PRICE_PICKLE = const.EOD_CONFIG_DICT['update_price_pickle']
EOD_PROJECT_FOLDER = const.EOD_CONFIG_DICT['eod_project_folder']
RESTRICTIONS_FILE_PATH_TEMPLATE = '%s/cfg/account_trade_restrictions_%%s.csv' % EOD_PROJECT_FOLDER
STRUCTUREFUND_FILE_PATH = '%s/cfg/structurefund.xlsx' % EOD_PROJECT_FOLDER

MKTDTCTR_CFG_FOLDER = const.EOD_CONFIG_DICT['mktdtctr_cfg_folder']

SERVER_DAILY_FILES_FOLDER_TEMPLATE = const.EOD_CONFIG_DICT['server_daily_files_folder_template']
# 服务器sql文件存放目录
DEPOSIT_SERVER_SQL_FILE_FOLDER_TEMPLATE = SERVER_DAILY_FILES_FOLDER_TEMPLATE + '/db'
# 行情重建校验结果文件
MKT_CHECK_FILE_FOLDER_TEMPLATE = SERVER_DAILY_FILES_FOLDER_TEMPLATE + '/mkt_check_files'
# 每日订单成交分析结果目录
ORDER_STATISTICS_REPORT_FOLDER_TEMPLATE = SERVER_DAILY_FILES_FOLDER_TEMPLATE + '/order'
# 策略相关TradePlat配置文件
TRADEPLAT_FILE_FOLDER_TEMPLATE = SERVER_DAILY_FILES_FOLDER_TEMPLATE + '/TradePlat'

PARAMETER_DICT_FILE_PATH = const.EOD_CONFIG_DICT['parameter_dict_file_path']

STOCK_SELECTION_FOLDER = const.EOD_CONFIG_DICT['stock_selection_folder']
STOCK_SELECTION_CONFIG_FILE = '%s/algo_config.txt' % STOCK_SELECTION_FOLDER

STOCK_INTRADAY_FOLDER = const.EOD_CONFIG_DICT['stock_intraday_folder']
STOCK_INTRADAY_BACKUP_FOLDER = '%s/cfg' % STOCK_INTRADAY_FOLDER
LEADLAG_PARAMETER_FILE_PATH = '%s/LeadLag_Parameter.csv' % STOCK_INTRADAY_FOLDER

CTP_DATA_BACKUP_PATH = const.EOD_CONFIG_DICT['ctp_data_backup_path']

LOG_BACKUP_FOLDER_TEMPLATE = const.EOD_CONFIG_DICT['log_backup_folder_template']

BLACKLIST_FOLDER = const.EOD_CONFIG_DICT['blacklist_folder']

CFF_INFO_FOLDER = const.EOD_CONFIG_DICT['cff_info_folder']

DATAFETCHER_MESSAGEFILE_FOLDER = const.EOD_CONFIG_DICT['datafetcher_messagefile_folder']

UPDATE_PRICE_PICKLE = const.EOD_CONFIG_DICT['update_price_pickle']

STRATEGYLOADER_FILE_PATH = const.EOD_CONFIG_DICT['strategyloader_file_folder']

IPO_FILE_PATH = const.EOD_CONFIG_DICT['ipo_file_path']

RISK_REPORT_FOLDER = const.EOD_CONFIG_DICT['risk_report_folder']

MAIN_CONTRACT_CHANGE_FILE_FOLDER = const.EOD_CONFIG_DICT['main_contract_change_file_folder']

DATA_FILE_FOLDER = const.EOD_CONFIG_DICT['data_file_folder']
TRANSACTIONS_FILE_PATH_TEMPLATE = '%s/wind/stock/%%s/transaction/summary.csv' % DATA_FILE_FOLDER
BACKTEST_DATA_FOLDER = '%s/future/backtest/' % DATA_FILE_FOLDER
VOLUME_PROFILE_FOLDER = '%s/daily/stock/volume_profile' % DATA_FILE_FOLDER
VOLUME_MEAN_FILE_TEMPLATE = '%s/daily/stock/volume_profile/%%s/volume_mean.csv' % DATA_FILE_FOLDER

BACKTEST_RESULT_FILE_FOLDER = const.EOD_CONFIG_DICT['backtest_result_file_folder']
BACKTEST_RESULT_HISTORY_FOLDER = '%s/history' % BACKTEST_RESULT_FILE_FOLDER
SOURCE_BACKTEST_DATA_PATH_BASE = const.EOD_CONFIG_DICT['source_backtest_data_path_base']
SOURCE_BACKTEST_INFO_PATH = const.EOD_CONFIG_DICT['source_backtest_info_path']
CHANGE_MONTH_INFO_PATH = const.EOD_CONFIG_DICT['change_month_info_path']
BACKTEST_RESULT_FOLDER_PATH_BASE = const.EOD_CONFIG_DICT['backtest_result_folder_path_base']
STATE_INSERT_SQL_SAVE_FOLDER_PATH_BASE = const.EOD_CONFIG_DICT['state_insert_sql_save_folder_path_base']
BARRA_REPORT_PATH = const.EOD_CONFIG_DICT['barra_report_path']

MULTIFACTOR_PARAMETER_BASE_FOLDER = const.EOD_CONFIG_DICT['multifactor_parameter_base_folder']
LEADLAG_PARAMETER_FILE_FOLDER = '%s/IntradaySignal/LeadLag_Corr' % MULTIFACTOR_PARAMETER_BASE_FOLDER

MULTIFACTOR_PARAMETER_FILE_FOLDER = const.EOD_CONFIG_DICT['multifactor_parameter_file_folder']
MULTIFACTOR_PARAMETER_FILE_PATH_TEMPLATE = const.EOD_CONFIG_DICT['multifactor_parameter_file_path_template']

STRATEGY_INTRADAYSIGNAL_FOLDER = const.EOD_CONFIG_DICT['Strategy_IntradaySignal_Folder']
VWAP_PARAMETER_FILE_FOLDER = '%s/MultiFactorWeightData_yansheng-ss7' % STRATEGY_INTRADAYSIGNAL_FOLDER
VWAP_PARAMETER_PRODUCT_FOLDER = '%s/product' % STRATEGY_INTRADAYSIGNAL_FOLDER

MULTIFACTOR_RESULT_FOLDER = const.EOD_CONFIG_DICT['multifactor_result_folder']
EVENTDRIVEN_RESULT_FOLDER = const.EOD_CONFIG_DICT['eventdriven_result_folder']
EVENTREAL_RESULT_FOLDER = const.EOD_CONFIG_DICT['eventreal_result_folder']
STRATEGY_FILE_PATH_DICT = {'MultiFactor': MULTIFACTOR_RESULT_FOLDER,
                           'EventDriven': EVENTDRIVEN_RESULT_FOLDER,
                           'Event_Real': EVENTREAL_RESULT_FOLDER}

BACKTEST_STATE_INSERT_FOLDER = const.EOD_CONFIG_DICT['backtest_state_insert_folder']
BACKTEST_INFO_FOLDER = const.EOD_CONFIG_DICT['backtest_info_folder']
BACKTEST_PARAMETER_STR_FOLDER_PATH = '%s/backtest_parameter_str/' % BACKTEST_INFO_FOLDER
BACKTEST_INFO_STR_FOLDER_PATH = '%s/backtest_info_str/' % BACKTEST_INFO_FOLDER
STRATEGY_NAME_LIST_FOLD_PATH = '%s/strategy_name_list/' % BACKTEST_INFO_FOLDER
STRATEGY_GROUP_STR_FOLD_PATH = '%s/strategy_group_str/' % BACKTEST_INFO_FOLDER
STRATEGY_SERVER_PARAMETER_FOLDER_PATH = '%s/server_parameter/' % BACKTEST_INFO_FOLDER
STRATEGY_ONLINE_OFFLINE_PATH = const.EOD_CONFIG_DICT['strategy_online_offline_path']

# PL_CAL_INFO_FOLDER = const.EOD_CONFIG_DICT['pl_cal_info_folder']

PHONE_TRADE_FOLDER = const.EOD_CONFIG_DICT['phone_trade_folder']

SSH_ID_RSA_PATH = const.EOD_CONFIG_DICT['ssh_id_rsa_path']

BACKTEST_BASE_PATH_TEMPLATE = const.EOD_CONFIG_DICT['backtest_base_path_template']

# INTRADAY_DEEP_LEARNING_PATH = const.EOD_CONFIG_DICT['intraday_deep_learning_path']
# BASE_STKINTRADAY_MODEL_FOLDER = '%s/model' % INTRADAY_DEEP_LEARNING_PATH
# BASE_STKINTRADAY_CONFIG_FOLDER = '%s/config' % INTRADAY_DEEP_LEARNING_PATH
# BASE_VWAP_CONFIG_VWAP_FOLDER = '%s/config_vwap' % INTRADAY_DEEP_LEARNING_PATH
# BASE_VWAP_CONFIG_COMBINE_FOLDER = '%s/config_combine' % INTRADAY_DEEP_LEARNING_PATH
# BASE_PARAMETER_DICT_FILE_PATH = '%s/parameter_dict.csv' % INTRADAY_DEEP_LEARNING_PATH

INTRADAY_DEEP_LEARNING_PATH = const.EOD_CONFIG_DICT['intraday_stock_path']
BASE_STKINTRADAY_MODEL_FOLDER = '%s/model' % INTRADAY_DEEP_LEARNING_PATH
BASE_STKINTRADAY_CONFIG_FOLDER = '%s/config' % INTRADAY_DEEP_LEARNING_PATH
BASE_PARAMETER_DICT_FILE_PATH = '%s/parameter_dict.csv' % INTRADAY_DEEP_LEARNING_PATH

INTRADAY_INDEX_FUTURE_PATH = const.EOD_CONFIG_DICT['intraday_index_future_path']
BASE_INTRADAY_INDEX_MODEL_FOLDER = '%s/model' % INTRADAY_INDEX_FUTURE_PATH
BASE_INTRADAY_INDEX_CONFIG_FOLDER = '%s/config' % INTRADAY_INDEX_FUTURE_PATH
# --------------------常用字典数据----------------------------------------
ORDER_ROUTE_LOG_DICT = {'guoxin': 'logon fail|Login CTP order route fail',
                        'huabao': 'Login LTS Order Route fail|Login CTP order route fail',
                        'nanhua': 'Login CTP order route fail',
                        'zhongxin': 'Login CTP order route fail'}

custom_enum_utils = CustomEnumUtils()
Instrument_Type_Enums = const.INSTRUMENT_TYPE_ENUMS
Exchange_Type_Enums = const.EXCHANGE_TYPE_ENUMS
Direction_Enums = const.DIRECTION_ENUMS
Trade_Type_Enums = const.TRADE_TYPE_ENUMS
Hedge_Flag_Type_Enums = const.HEDGEFLAG_TYPE_ENUMS
IO_Type_Enums = const.IO_TYPE_ENUMS
