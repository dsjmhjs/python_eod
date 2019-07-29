# -*- coding: utf-8 -*-
# 删除过期的期货或期权
import threading
import sys
import time
import traceback

from eod_aps.model.schema_common import Instrument
from eod_aps.job import *

warning_message_list = []


def compare_time(start_t, end_t):
    s_time = time.mktime(start_t.timetuple())
    e_time = time.mktime(end_t.timetuple())

    if float(s_time) >= float(e_time):
        return False
    return True


def __del_expire_instrument(server_name):
    try:
        custom_log.log_info_job('Server:%s del_expire_instrument' % server_name)

        server_model = server_constant.get_server_model(server_name)
        validate_time = long(date_utils.get_today_str('%H%M%S'))
        if validate_time < 150500:
            custom_log.log_error_job('Please execute this after 15:05.')
            raise Exception('time error', validate_time)

        type_list = [Instrument_Type_Enums.Future, Instrument_Type_Enums.Option]
        session = server_model.get_db_session('common')
        query = session.query(Instrument)
        for instrument_db in query.filter(Instrument.type_id.in_(type_list), Instrument.del_flag == 0):
            if instrument_db.expire_date is None or instrument_db.expire_date == '':
                warning_message_list.append(
                    'Server_name:%s, Instrument:%s Expire_date Is Empty!' % (server_name, instrument_db.ticker))
                continue
            expire_date = instrument_db.expire_date.strftime('%Y-%m-%d') + ' 15:00:00'
            if compare_time(date_utils.string_toDatetime(expire_date, '%Y-%m-%d %H:%M:%S'), date_utils.get_now()):
                instrument_db.del_flag = 1
                session.merge(instrument_db)
                warning_message_list.append(
                    'Server_name:%s, Expire Instrument:%s Update.' % (server_name, instrument_db.ticker))
        session.commit()
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__del_expire_instrument:%s.' % server_name, error_msg)


def del_expire_instrument_job(server_name_tuple):
    global warning_message_list
    warning_message_list = []

    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__del_expire_instrument, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # if len(warning_message_list) > 0:
    #     email_utils2.send_email_group_all('Expire Instrument Info!', '\n'.join(warning_message_list))


if __name__ == '__main__':
    all_servers_list = server_constant.get_all_servers()
    del_expire_instrument_job(all_servers_list)
