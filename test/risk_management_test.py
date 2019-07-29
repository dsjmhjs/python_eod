# -*- coding: utf-8 -*-
import urllib
import urllib2
import datetime
import requests
from bs4 import BeautifulSoup
from eod_aps.model.eod_const import const
from eod_aps.model.schema_jobs import RiskManagement
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.email_utils import EmailUtils

email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


def risk_management():
    server_constant = ServerConstant()
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')

    email_content_list = []
    for risk_management_db in session_jobs.query(RiskManagement):
        email_content_list.append('Title:%s' % risk_management_db.monitor_index)
        email_content_list.append('Describe:%s' % risk_management_db.describe)

        temp_email_content = []
        if risk_management_db.fund_risk_list != '':
            for fund_risk_item in risk_management_db.fund_risk_list.split(';'):
                temp_email_content.append(fund_risk_item.split('|'))
        html_table_list = email_utils2.list_to_html('FundName,Warn_Line,Error_Line', temp_email_content)
        email_content_list.append(''.join(html_table_list))
        email_content_list.append('<br><br>')
    email_utils2.send_email_group_all('RiskManagementInfo', '<br>'.join(email_content_list), 'html')


if __name__ == '__main__':
    risk_management()
