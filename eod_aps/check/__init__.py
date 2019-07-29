# -*- coding: utf-8 -*-
from eod_aps.model.eod_const import const
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.server_constans import server_constant
from cfg import custom_log

date_utils = DateUtils()

email_utils1 = EmailUtils(const.EMAIL_DICT['group1'])
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


DATA_FILE_FOLDER = const.EOD_CONFIG_DICT['data_file_folder']
VOLUME_PROFILE_FOLDER = '%s/daily/stock/volume_profile' % DATA_FILE_FOLDER
