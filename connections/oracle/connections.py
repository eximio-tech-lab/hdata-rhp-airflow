import os

import cx_Oracle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def connect_rhp():
    os.environ["NLS_LANG"] = ".UTF8"
    dsn_tns = cx_Oracle.makedsn('10.0.38.7', 1521, service_name='sml')  # 172.17.0.1
    return cx_Oracle.connect('hdata', 'hhhdatadata22191', dsn_tns)

def connect_rhp_hdata():
    os.environ["NLS_LANG"] = ".UTF8"
    dsn_tns = cx_Oracle.makedsn('orclstage-1.cxp7emb18yqw.us-east-2.rds.amazonaws.com', 61521, service_name='orcl')
    return cx_Oracle.connect('mv_rhp', 'zaDU0$qd', dsn_tns, encoding='UTF-8')

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