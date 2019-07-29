# -*- coding: utf-8 -*-
import os

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class SQLAlchemyUtils:
    server_ip = None
    db_user = 'admin'
    db_password = 'adminP@ssw0rd'
    db_port = 3306
    conn = None
    cursor = None

    def __init__(self, server_ip):
        self.server_ip = server_ip

    def db_session(self, db_name):
        db_connect_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % (
            self.db_user, self.db_password, self.server_ip, self.db_port, db_name)
        # echo参数控制是否打印sql日志
        engine = create_engine(db_connect_string, echo=False)
        DB_Session = sessionmaker(bind=engine)
        return DB_Session()
