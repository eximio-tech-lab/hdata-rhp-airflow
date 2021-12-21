import os

import cx_Oracle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def connect_rhp():
    connect_rhp_simulacao = 'oracle+cx_oracle://' + 'hdata' + ':' + 'hhhdatadata22191' + '@' + 'ORCL'
    return connect_rhp_simulacao

def connect_rhp_2():
    os.environ["NLS_LANG"] = ".UTF8"
    dsn_tns = cx_Oracle.makedsn('10.0.38.7', 1522, service_name='dbamv')  # 172.17.0.1
    return cx_Oracle.connect('hdata', 'hhhdatadata22191', dsn_tns)

def engine():
    engine = create_engine(connect_rhp(), max_identifier_length=128)
    return engine

def engine_rhp():
    engine_rhp = create_engine(connect_rhp(), max_identifier_length=128)
    return engine_rhp

Session = sessionmaker(bind=engine)
session = Session()

Session_engine = sessionmaker(bind=engine_rhp)
session_engine = Session_engine()