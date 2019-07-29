# -*- coding: utf-8 -*-
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.tradeplat_message_tools import socket_init, send_phone_trade_request_msg
from eod_aps.model.eod_const import CustomEnumUtils, const

date_utils = DateUtils()
email_utils = EmailUtils(const.EMAIL_DICT['group2'])
custom_enum_utils = CustomEnumUtils()
inversion_direction_dict = custom_enum_utils.enum_to_dict(const.DIRECTION_ENUMS, inversion_flag=True)
inversion_trade_type_dict = custom_enum_utils.enum_to_dict(const.TRADE_TYPE_ENUMS, inversion_flag=True)
inversion_hedge_flag_dict = custom_enum_utils.enum_to_dict(const.HEDGEFLAG_TYPE_ENUMS, inversion_flag=True)
inversion_io_type_dict = custom_enum_utils.enum_to_dict(const.IO_TYPE_ENUMS, inversion_flag=True)

direction_dict = custom_enum_utils.enum_to_dict(const.DIRECTION_ENUMS)
trade_type_dict = custom_enum_utils.enum_to_dict(const.TRADE_TYPE_ENUMS)
io_type_dict = custom_enum_utils.enum_to_dict(const.IO_TYPE_ENUMS)

table_title = 'fund,strategy1,symbol,direction,trade_type,hedge_flag,exprice,exqty,io_type,strategy2,\
              connect_address'


def send_phone_trade(server_name, phone_trade_list):
    """
       发送phone_trade
    :param server_name:
    :param phone_trade_list:
    :return:
    """
    if not phone_trade_list:
        return

    server_socket = socket_init(server_name)
    send_phone_trade_request_msg(server_socket, phone_trade_list)

    __save_phone_trade_list(phone_trade_list)


def save_phone_trade_file(file_save_path, phone_trade_list, notify_flag=True):
    """
        保存phone_trade文件，并邮件通知
    :param file_save_path:
    :param phone_trade_list:
    :param notify_flag:
    :return:
    """
    if not phone_trade_list:
        return

    save_message_list = __build_phone_trade_message(phone_trade_list)
    with open(file_save_path, 'w+') as fr:
        fr.write('\n'.join(save_message_list))

    __save_phone_trade_list(phone_trade_list)
    if notify_flag:
        notify_phone_trade_list(phone_trade_list, file_save_path)


def notify_phone_trade_list(phone_trade_list, file_save_path=None):
    """
       邮件通知phone_trade
    :param phone_trade_list:
    :param file_save_path:
    """
    phone_trade_message_list = __build_phone_trade_message(phone_trade_list)
    table_list = [item.split(',') for item in phone_trade_message_list]
    html_list = email_utils.list_to_html(table_title, table_list)
    if file_save_path:
        html_list.insert(0, 'File Path:%s' % file_save_path)
    email_utils.send_email_group_all('Phone Trade Info', ''.join(html_list), 'html')


def __save_phone_trade_list(phone_trade_list):
    server_host = server_constant.get_server_model('host')
    session_history = server_host.get_db_session('history')
    for phone_trade_info in phone_trade_list:
        phone_trade_info.update_time = date_utils.get_now()
        session_history.add(phone_trade_info)
    session_history.commit()


def __build_phone_trade_message(phone_trade_list):
    connect_address = server_constant.get_connect_address(phone_trade_list[0].server_name)
    connect_address = connect_address.replace('tcp://', '')

    phone_trade_message_list = []
    for phone_trade_info in phone_trade_list:
        direction = inversion_direction_dict[phone_trade_info.direction]
        trade_type = inversion_trade_type_dict[phone_trade_info.tradetype]
        hedge_flag = inversion_hedge_flag_dict[phone_trade_info.hedgeflag]
        io_type = inversion_io_type_dict[phone_trade_info.iotype]
        save_message_str = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
                           (phone_trade_info.fund, phone_trade_info.strategy1, phone_trade_info.symbol, direction,
                            trade_type, hedge_flag, phone_trade_info.exprice, phone_trade_info.exqty, io_type,
                            phone_trade_info.strategy2, connect_address)
        phone_trade_message_list.append(save_message_str)
    return phone_trade_message_list


# def send_by_phone_trade_file(server_name, phone_trade_file_path):
#     phone_trade_list = []
#     with open(phone_trade_file_path) as fr:
#         for line in fr.readlines():
#             line_item = line.replace('\n', '').split(',')
#             phone_trade_info = PhoneTradeInfo()
#             phone_trade_info.server_name = server_name
#             phone_trade_info.fund = line_item[0]
#             phone_trade_info.strategy1 = line_item[1]
#
#             phone_trade_info.symbol = line_item[2]
#             phone_trade_info.exqty = line_item[7]
#             phone_trade_info.exprice = line_item[6]
#
#             phone_trade_info.direction = direction_dict[line_item[3]]
#             phone_trade_info.tradetype = trade_type_dict[line_item[4]]
#
#             phone_trade_info.hedgeflag = const.HEDGEFLAG_TYPE_ENUMS.Speculation
#             # phone_trade_info.strategy2 = line_item[9]
#             phone_trade_info.iotype = io_type_dict[line_item[8]]
#             phone_trade_list.append(phone_trade_info)
#     send_phone_trade(server_name, phone_trade_list)


if __name__ == '__main__':
    # send_by_phone_trade_file('huabao', 'E:/phonetrade_1.csv')
    pass
