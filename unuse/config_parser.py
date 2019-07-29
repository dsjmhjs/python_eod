# -*- coding: utf-8 -*-
import os
import ConfigParser
from eod_aps.model.server_model import ServerModel
from eod_aps.model.eod_const import const


def get_config_server_list():
    cp = ConfigParser.SafeConfigParser()
    path = os.path.dirname(__file__)

    cp.read(path + '/cfg/config.txt')
    for server_name in cp.sections():
        if server_name == 'base_server_config':
            port = cp.get(server_name, 'port')
            user = cp.get(server_name, 'user')
            pwd = cp.get(server_name, 'pwd')
            db_port = cp.get(server_name, 'db_port')
            db_user = cp.get(server_name, 'db_user')
            db_password = cp.get(server_name, 'db_password')
            # 常用目录
            home_folder = cp.get(server_name, 'home_folder')
            eod_project_folder = cp.get(server_name, 'eod_project_folder')
            tradeplat_project_folder = cp.get(server_name, 'tradeplat_project_folder')
            datafetcher_project_folder = cp.get(server_name, 'datafetcher_project_folder')
            mktdtctr_project_folder = cp.get(server_name, 'mktdtctr_project_folder')
            etf_upload_folder = cp.get(server_name, 'etf_upload_folder')
            db_backup_folder = cp.get(server_name, 'db_backup_folder')
            break

    config_server_list = []
    for server_name in cp.sections():
        if server_name == 'base_server_config':
            continue
        elif server_name == 'config':
            for config_name in cp.options(server_name):
                const.EOD_CONFIG_DICT[config_name] = cp.get(server_name, config_name)
        else:
            type = cp.get(server_name, 'type')
            if type == 'local_host':
                server_model = ServerModel(server_name)
                server_model.type = type
                server_model.server_name = cp.get(server_name, 'server_name')
                server_model.db_ip = cp.get(server_name, 'db_ip')
                server_model.db_user = cp.get(server_name, 'db_user')
                server_model.db_password = cp.get(server_name, 'db_password')
                server_model.db_port = cp.get(server_name, 'db_port')
            elif type == 'server_host':
                server_model = ServerModel(server_name)
                server_model.type = type
                server_model.server_name = cp.get(server_name, 'server_name')
                server_model.db_ip = cp.get(server_name, 'db_ip')
                server_model.db_user = cp.get(server_name, 'db_user')
                server_model.db_password = cp.get(server_name, 'db_password')
                server_model.db_port = cp.get(server_name, 'db_port')

                for option in cp.options(server_name):
                    if 'folder' not in option and 'path' not in option:
                        continue
                    server_model.server_path_dict[option] = cp.get(server_name, option)
            elif type == 'trader_server':
                server_model = ServerModel(server_name)
                server_model.type = type
                server_model.ip = cp.get(server_name, 'ip')
                server_model.db_ip = cp.get(server_name, 'db_ip')
                server_model.db_port = int(__get_value(cp, server_name, 'db_port', db_port))
                server_model.db_user = __get_value(cp, server_name, 'db_user', db_user)
                server_model.db_password = __get_value(cp, server_name, 'db_password', db_password)

                if cp.has_option(server_name, 'night_session') and cp.get(server_name, 'night_session') == 'True':
                    server_model.night_session = True
                if cp.has_option(server_name, 'cta_server') and cp.get(server_name, 'cta_server') == 'True':
                    server_model.cta_server = True
                if cp.has_option(server_name, 'calendar_server') and cp.get(server_name, 'calendar_server') == 'True':
                    server_model.calendar_server = True
                if cp.has_option(server_name, 'stock_server') and cp.get(server_name, 'stock_server') == 'True':
                    server_model.stock_server = True
                if cp.has_option(server_name, 'market_type_stock'):
                    server_model.market_type_stock = cp.get(server_name, 'market_type_stock')
                if cp.has_option(server_name, 'market_type_future'):
                    server_model.market_type_future = cp.get(server_name, 'market_type_future')
                if cp.has_option(server_name, 'fix_server') and cp.get(server_name, 'fix_server') == 'True':
                    server_model.fix_server = True
                if cp.has_option(server_name, 'tdf_server') and cp.get(server_name, 'tdf_server') == 'True':
                    server_model.tdf_server = True
                if cp.has_option(server_name, 'oma_server') and cp.get(server_name, 'oma_server') == 'True':
                    server_model.oma_server = True
                if cp.has_option(server_name, 'commodity_future_server') and cp.get(server_name, 'commodity_future_server') == 'True':
                    server_model.commodity_future_server = True

                server_model.port = int(__get_value(cp, server_name, 'port', port))
                server_model.userName = __get_value(cp, server_name, 'user', user)
                server_model.passWord = __get_value(cp, server_name, 'pwd', pwd)
                server_model.check_port_list = cp.get(server_name, 'check_port_list')

                server_model.server_path_dict['home_folder'] = __get_value(cp, server_name, 'home_folder', home_folder)
                server_model.server_path_dict['eod_project_folder'] = __get_value(cp, server_name, 'eod_project_folder', eod_project_folder)
                server_model.server_path_dict['tradeplat_project_folder'] = __get_value(cp, server_name, 'tradeplat_project_folder', tradeplat_project_folder)
                server_model.server_path_dict['datafetcher_project_folder'] = __get_value(cp, server_name, 'datafetcher_project_folder', datafetcher_project_folder)
                server_model.server_path_dict['mktdtctr_project_folder'] = __get_value(cp, server_name, 'mktdtctr_project_folder', mktdtctr_project_folder)
                server_model.server_path_dict['etf_upload_folder'] = __get_value(cp, server_name, 'etf_upload_folder', etf_upload_folder)
                server_model.server_path_dict['db_backup_folder'] = __get_value(cp, server_name, 'db_backup_folder', db_backup_folder)

                if cp.has_option(server_name, 'mktcenter_local_save_path'):
                    server_model.server_path_dict['mktcenter_local_save_path'] = cp.get(server_name, 'mktcenter_local_save_path')

                if cp.has_option(server_name, 'etf_mount_folder'):
                    server_model.server_path_dict['etf_mount_folder'] = cp.get(server_name, 'etf_mount_folder')

                if cp.has_option(server_name, 'history_data_file_path'):
                    server_model.server_path_dict['history_data_file_path'] = cp.get(server_name, 'history_data_file_path')

                if cp.has_option(server_name, 'omaproxy_project_folder'):
                    server_model.server_path_dict['omaproxy_project_folder'] = cp.get(server_name, 'omaproxy_project_folder')

                if cp.has_option(server_name, 'mktcenter_file_template'):
                    mktcenter_file_template_str = cp.get(server_name, 'mktcenter_file_template')
                    for mktcenter_file_template in mktcenter_file_template_str.split(','):
                        server_model.mktcenter_file_template_list.append(mktcenter_file_template)
                server_model.build_file_path()
                # server_model.init_file_path()
            elif type == 'deposit_server':
                # 代管服务器
                server_model = ServerModel(server_name)
                server_model.type = type
                server_model.ip = cp.get(server_name, 'ip')
                server_model.db_ip = cp.get(server_name, 'db_ip')
                server_model.db_user = cp.get(server_name, 'db_user')
                server_model.db_password = cp.get(server_name, 'db_password')
                server_model.db_port = cp.get(server_name, 'db_port')
                server_model.server_path_dict['ftp_wsdl_address'] = cp.get(server_name, 'ftp_wsdl_address')
                server_model.server_path_dict['ftp_upload_folder'] = cp.get(server_name, 'ftp_upload_folder')
                server_model.server_path_dict['ftp_download_folder'] = cp.get(server_name, 'ftp_download_folder')

                if cp.has_option(server_name, 'night_session') and cp.get(server_name, 'night_session') == 'True':
                    server_model.night_session = True
                if cp.has_option(server_name, 'cta_server') and cp.get(server_name, 'cta_server') == 'True':
                    server_model.cta_server = True
                if cp.has_option(server_name, 'calendar_server') and cp.get(server_name, 'calendar_server') == 'True':
                    server_model.calendar_server = True
                if cp.has_option(server_name, 'notify_monitor') and cp.get(server_name, 'notify_monitor') == 'True':
                    server_model.notify_monitor = True
                if cp.has_option(server_name, 'stock_server') and cp.get(server_name, 'stock_server') == 'True':
                    server_model.stock_server = True
            elif type == 'db_server':
                server_model = ServerModel(server_name)
                server_model.type = type
                server_model.db_ip = cp.get(server_name, 'db_ip')
                server_model.db_user = cp.get(server_name, 'db_user')
                server_model.db_password = cp.get(server_name, 'db_password')
                server_model.db_port = cp.get(server_name, 'db_port')
            elif type == 'local_server':
                server_model = ServerModel(server_name)
                server_model.type = type
                server_model.ip = cp.get(server_name, 'ip')
                server_model.port = int(cp.get(server_name, 'port'))
                server_model.userName = cp.get(server_name, 'user')
                server_model.passWord = cp.get(server_name, 'pwd')
            else:
                continue
            config_server_list.append(server_model)
    const.CONFIG_SERVER_LIST.extend(config_server_list)


def __get_value(cp, server_name, item_value, default_value):
    if cp.has_option(server_name, item_value):
        result_value = cp.get(server_name, item_value)
    else:
        result_value = default_value
    return result_value


if __name__ == '__main__':
     get_config_server_list()