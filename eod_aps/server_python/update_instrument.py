#!/usr/bin/env python
# _*_ coding:utf-8 _*_
#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import os
import pickle
import time
from eod_aps.model.schema_common import Instrument
from eod_aps.server_python import *


def update_db():
    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    today_str = date_utils.get_today_str(format_str='%Y-%m-%d')
    file_name = 'INSTRUMENT_' + today_str + '.pickle'
    daily_instrument_obj_list_file = '%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, file_name)
    if os.path.exists(daily_instrument_obj_list_file):
        with open(daily_instrument_obj_list_file, 'rb') as f:
            daily_instrument_obj_list = pickle.load(f)
        for update_sql in daily_instrument_obj_list:
            session_common.execute(update_sql)
        session_common.commit()
    session_common.close()


if __name__ == '__main__':
    update_db()
