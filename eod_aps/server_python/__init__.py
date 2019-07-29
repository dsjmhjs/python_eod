# -*- coding: utf-8 -*-
from eod_aps.model.server_constans_local import server_constant_local
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.eod_const import const

date_utils = DateUtils()

server_host = server_constant_local.get_server_model('host')
lOCAL_SERVER_NAME = server_host.server_name

# 本地和托管服务器均使用的目录
ETF_FILE_PATH = const.EOD_CONFIG_DICT['etf_file_path']
DATAFETCHER_MESSAGEFILE_FOLDER = const.EOD_CONFIG_DICT['datafetcher_messagefile_folder']

if 'tradeplat_project_folder' in const.EOD_CONFIG_DICT:
    TRADEPLAT_PROJECT_FOLDER = const.EOD_CONFIG_DICT['tradeplat_project_folder']
    MKTDTCTR_PROJECT_FOLDER = const.EOD_CONFIG_DICT['mktdtctr_project_folder']
    DATAFETCHER_PROJECT_FOLDER = const.EOD_CONFIG_DICT['datafetcher_project_folder']
    EOD_PROJECT_FOLDER = const.EOD_CONFIG_DICT['eod_project_folder']

if 'market_file_folder' in const.EOD_CONFIG_DICT:
    MARKET_FILE_FOLDER = const.EOD_CONFIG_DICT['market_file_folder']
    HISTORY_DATA_FOLDER = const.EOD_CONFIG_DICT['history_data_folder']
    QUOTES_BASE_FILE_FOLDER = '%s/quotes_base' % HISTORY_DATA_FOLDER
    QUOTES_FILE_FOLDER = '%s/quotes' % HISTORY_DATA_FOLDER
    MINBAR_FILE_FOLDER = '%s/bars' % HISTORY_DATA_FOLDER

if 'etf_mount_folder' in const.EOD_CONFIG_DICT:
    ETF_MOUNT_FOLDER = const.EOD_CONFIG_DICT['etf_mount_folder']





