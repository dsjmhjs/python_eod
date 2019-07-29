# -*- coding: utf-8 -*-
import os
import ConfigParser
from eod_aps.model.schema_history import HolidayInfo
from eod_aps.model.server_model import HostModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from eod_aps.model.schema_jobs import LocalParameters, ProjectDict
from eod_aps.model.eod_const import const


def get_config_server_list():
    cp = ConfigParser.SafeConfigParser()
    path = os.path.dirname(__file__)
    cp.read(path + '/../../cfg/config.txt')

    db_ip = cp.get('host', 'db_ip')
    db_port = cp.get('host', 'db_port')
    db_user = cp.get('host', 'db_user')
    db_password = cp.get('host', 'db_password')
    db_name = 'jobs'
    Session = sessionmaker()
    db_connect_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8;compress=true' % \
                        (db_user, db_password, db_ip, db_port, db_name)
    engine = create_engine(db_connect_string, echo=False, poolclass=NullPool)
    Session.configure(bind=engine)
    session_job = Session()

    # ----------------------加载本地相关信息------------------------
    # 邮件相关配置
    local_parameters = session_job.query(LocalParameters).first()
    project_dict = session_job.query(ProjectDict)
    for project_item in project_dict:
        if project_item.dict_type == 'Email_dict':
            const.EMAIL_DICT[project_item.dict_name] = project_item.dict_value.split(',')
        else:
            const.EOD_CONFIG_DICT[project_item.dict_name] = project_item.dict_value

    server_host = HostModel('host')
    server_host.load_parameter(local_parameters, project_dict)
    const.CONFIG_SERVER_LIST.append(server_host)
    session_job.close()

    holiday_list = []
    session_history = server_host.get_db_session('history')
    for holiday_info_db in session_history.query(HolidayInfo):
        holiday_list.append(holiday_info_db.holiday.strftime('%Y-%m-%d'))
    session_history.close()
    const.EOD_CONFIG_DICT['holiday_list'] = holiday_list

    # db_name = 'common'
    # db_connect_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8;compress=true' % \
    #                     (db_user, db_password, db_ip, db_port, db_name)
    # engine = create_engine(db_connect_string, echo=False, poolclass=NullPool)
    # Session.configure(bind=engine)
    # session_common = Session()
    # services_list = []
    # for service_name_item in session_common.query(AppInfo.app_name).group_by(AppInfo.app_name):
    #     services_list.append(service_name_item[0])
    # const.EOD_CONFIG_DICT['service_list'] = services_list
    # session_common.close()


if __name__ == '__main__':
    print get_config_server_list()
