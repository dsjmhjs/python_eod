# -*- coding: utf-8 -*-
import MySQLdb
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.instrument import Instrument
from decimal import Decimal

from eod_aps.tools.getConfig import getConfig


def test():
    # host_server_model = ServerConstant().get_server_model('host')
    # session = host_server_model.get_db_session('om')
    # file_path = 'E:/report/screenlog_MainFrame_20170822-084557_8kC000ee.log'
    #
    # with open(file_path) as fr:
    #     for line in fr.readlines():
    #         replace_index = line.index('replace into')
    #         if replace_index == 0:
    #             continue
    #         sql_str = line[replace_index:]
    #         print 'sql:', sql_str
    #         session.execute(sql_str)
    #         break
    # session.commit()
    # host_server_model.close()

    cfg_dict = getConfig()
    try:
        conn = MySQLdb.connect( \
            host=cfg_dict['host'], user=cfg_dict['db_user'], passwd=cfg_dict['db_password'], \
            db='common', charset='utf8')
        print 'db ip:', cfg_dict['host']

        cursor = conn.cursor()

        file_path = 'E:/report/screenlog_MainFrame_20170822-084557_8kC000ee.log'
        with open(file_path) as fr:
            for line in fr.readlines():
                replace_index = line.index('replace into')
                if replace_index == 0:
                    continue
                sql_str = line[replace_index:]
                sql_title, sql_value_info = sql_str.split('values')

                sql_items = sql_value_info.split('),(')
                index = 0
                vaLue_list = []
                for sql_item in sql_items:
                    if 'not-a-date-time' in sql_item:
                        continue
                    sql_item = sql_item.replace('(', '').replace(')', '')
                    vaLue_list.append('(%s)' % sql_item)
                print len(vaLue_list)
                sql_str = '%s values %s' % (sql_title, ','.join(vaLue_list))
                cursor.execute(sql_str)
                break
 #        sql_str = """
 #        replace INTO om.`order`
 # (`ID`, `SYS_ID`,`ACCOUNT`,`SYMBOL`,`DIRECTION`,`TYPE`,`TRADE_TYPE`,`STATUS`,`OP_STATUS`,`PROPERTY`,`CREATE_TIME`,`TRANSACTION_TIME`,`USER_ID`,`STRATEGY_ID`,`PARENT_ORD_ID`,`QTY`,`PRICE`,`EX_QTY`,`EX_PRICE`,`HEDGEFLAG`,ALGO_TYPE) values
 # ('8kE01163','','198800888077-TS-xhhm02-','601111 CG',1, 1, 0, 2, 2, 0, '2017-08-22 13:01:01.707253', '2017-08-22 13:07:31.183752', 'xhhm02', 'Event_Real.Earning_01', '8kC0003a', 100, 9.11, 100, 9.11, 0, 0)
 #        """
 #        cursor.execute(sql_str)
        cursor.close()
        conn.commit()
        conn.close()
    except Exception, e:
        print e


if __name__ == '__main__':
    test()



