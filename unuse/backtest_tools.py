# -*- coding: utf-8 -*-
import six
import zmq
import time
import zlib
from eod_aps.model.AllProtoMsg_pb2 import ServerParameterChangeRequestMsg, TradeInfoRequestMsg, TradeInfoResponseMsg


def __send_control_message(strategy_cmd_str):
    context = zmq.Context().instance()
    print "Connecting to aggregator server:%s" % '172.16.11.127'
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b'172.16.11.127_test')

    socket.connect('tcp://172.16.11.127:17101')


    msg = ServerParameterChangeRequestMsg()
    msg.Command = strategy_cmd_str
    msg_str = msg.SerializeToString()
    msg_list = [six.int2byte(13), msg_str]
    socket.send_multipart(msg_list)

    i = 0
    while True:
        i = i + 1
        print i
        msg2 = TradeInfoRequestMsg()
        msg2_str = msg2.SerializeToString()
        msg2_list = [six.int2byte(17), msg2_str]
        socket.send_multipart(msg2_list)

        recv_message = socket.recv_multipart()
        print recv_message
        recv_result = six.byte2int(recv_message[1])
        print recv_result

        recv_message = socket.recv_multipart()
        instrument_info_msg = TradeInfoResponseMsg()
        instrument_info_msg.ParseFromString(zlib.decompress(recv_message[1]))
        time.sleep(1)


if __name__ == '__main__':
    strategy_cmd_str = 'BackTest --StratsName=AMASkew --WatchList=rb1710 --Parameter=[Account]1:0:0;[NumDevs]17.000000:0:0;[tq.All_Weather_2.max_long_position]1:0:0;[skewLen]26:0:0;[AMA_Slow_len]30:0:0;[tq.All_Weather_3.max_long_position]1:0:0;[AMA_Fast_len]2:0:0;[tq.All_Weather_3.qty_per_trade]2:0:0;[tq.All_Weather_1.qty_per_trade]2:0:0;[StopLossPoint]60.000000:0:0;[tq.steady_return.max_long_position]1:0:0;[tq.All_Weather_1.max_long_position]1:0:0;[tq.absolute_return.max_short_position]0:0:0;[AMA_Eff_len]10:0:0;[tq.absolute_return.qty_per_trade]2:0:0;[tq.All_Weather_2.qty_per_trade]2:0:0;[tq.steady_return.max_short_position]1:0:0;[tq.steady_return.qty_per_trade]2:0:0;[LengthSTD]125:0:0;[tq.All_Weather_2.max_short_position]1:0:0;[DevX]1.100000:0:0;[Length]125:0:0;[tq.absolute_return.max_long_position]0:0:0;[tq.All_Weather_3.max_short_position]1:0:0;[tq.All_Weather_1.max_short_position]1:0:0;[max_slippage]3:0:0;[positionCtrlFlag]0:0:0;[BarDurationMin]5:0:0 --StartDate=2017-03-21 --EndDate=2017-03-23 --StartTime=23:00:00  --AssemblyName=AMASkew_strategy --Parallel=0'
    __send_control_message(strategy_cmd_str)
