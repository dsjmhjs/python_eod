# -*- coding: utf-8 -*-
from eod_aps.model.schema_common import Instrument
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.model.eod_const import const


Instrument_Type_Enum = const.INSTRUMENT_TYPE_ENUMS
Exchange_Type_Enum = const.EXCHANGE_TYPE_ENUMS

custom_enum_utils = CustomEnumUtils()
exchange_type_inversion_dict = custom_enum_utils.enum_to_dict(Exchange_Type_Enum, inversion_flag=True)


def query_instrument_list(server_name, type_list=None):
    instrument_list = []
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    if type_list is None:
        for instrument_db in query.filter(Instrument.del_flag == 0):
            instrument_list.append(instrument_db)
    else:
        for instrument_db in query.filter(Instrument.type_id.in_(type_list), Instrument.del_flag == 0):
            instrument_list.append(instrument_db)
    server_model.close()
    return instrument_list


def query_all_instrument_dict(server_name, type_list=None):
    instrument_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    if type_list is None:
        for instrument_db in query.filter(Instrument.del_flag == 0):
            instrument_dict[instrument_db.ticker] = instrument_db
    else:
        for instrument_db in query.filter(Instrument.type_id.in_(type_list)):
            instrument_dict[instrument_db.ticker] = instrument_db
    server_model.close()
    return instrument_dict


def query_instrument_dict(server_name, type_list=None):
    instrument_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    if type_list is None:
        for instrument_db in query.filter(Instrument.del_flag == 0):
            instrument_dict[instrument_db.ticker] = instrument_db
    else:
        for instrument_db in query.filter(Instrument.type_id.in_(type_list), Instrument.del_flag == 0):
            instrument_dict[instrument_db.ticker] = instrument_db
    server_model.close()
    return instrument_dict


def query_use_instrument_dict(server_name):
    instrument_list = []
    exchange_list = [Exchange_Type_Enum.CG, Exchange_Type_Enum.CS, Exchange_Type_Enum.SHF,
                     Exchange_Type_Enum.DCE, Exchange_Type_Enum.ZCE, Exchange_Type_Enum.CFF,
                     Exchange_Type_Enum.INE
                     ]
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.del_flag == 0,
                                      Instrument.exchange_id.in_(exchange_list)):
        instrument_list.append(instrument_db)
    return instrument_list


def query_exchange_name(exchange_id):
    if exchange_id in exchange_type_inversion_dict:
        return exchange_type_inversion_dict[exchange_id]
    else:
        raise Exception("Error exchange_id:%s" % exchange_id)


if __name__ == '__main__':
    print query_exchange_name(35)