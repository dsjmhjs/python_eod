#!/usr/bin/env python
# _*_ coding:utf-8 _*_
from eod_aps.model.schema_jobs import UserList, EodMessage
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils


def save_msg(title, content, role_id):
    date_utils = DateUtils()
    create_time = str(date_utils.get_now())
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    user_obj_list = session.query(UserList).filter(UserList.role_id == role_id).all()
    for user_obj in user_obj_list:
        obj_msg = EodMessage(title=title, content=content, create_time=create_time)
        obj_msg.user_id = user_obj.id
        session.add(obj_msg)
    session.commit()
    session.close()


def query_msg(user_name, session, read_flag=0):
    # host_server_model = server_constant.get_server_model('host')
    # session = host_server_model.get_db_session('jobs')
    user_obj = session.query(UserList).filter(UserList.user_id == user_name).all()[0]

    if read_flag != 0 and read_flag != 1:
        msg_list = session.query(EodMessage).filter(EodMessage.user_id == user_obj.id).all()
    else:
        msg_list = session.query(EodMessage).filter(EodMessage.read_flag == read_flag).filter(
            EodMessage.user_id == user_obj.id).all()
    # session.close()
    return msg_list


def read_msg(user_name, msg_id=None):
    host_server_model = server_constant.get_server_model('host')
    session = host_server_model.get_db_session('jobs')
    user_obj = session.query(UserList).filter(UserList.user_id == user_name).all()[0]
    if not msg_id:
        msg_objs = session.query(EodMessage).filter(EodMessage.user_id == user_obj.id).all()
    else:
        msg_objs = session.query(EodMessage).filter(EodMessage.user_id == user_obj.id).filter(
            EodMessage.id == msg_id).all()
    for msg_obj in msg_objs:
        msg_obj.read_flag = 1
    session.commit()
    session.close()
    return True


if __name__ == '__main__':
    print save_msg('test', 'lllll', 43)
