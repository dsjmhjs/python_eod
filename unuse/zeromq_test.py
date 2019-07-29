import six
import zmq
import AllProtoMsg_pb2
import zlib
import datetime
import time

from algo import bcl_pb2

def get_market_info():
    context = zmq.Context().instance()
    # Socket to talk to server
    print "Connecting to hello world server"
    socket = context.socket(zmq.DEALER)

    # identity_str = addresess.First(p=>p.Contains(".")) + "-" + DateTime.Now.ToString() + "."
    identity_str = b'172.16.11.68'
    socket.setsockopt(zmq.IDENTITY, identity_str)
    # socket.bind_to_random_port('tcp://172.16.11.68')
    # socket.bind('tcp://1172.16.11.68-2017/4/10 16:05:46.')

    # socket.connect("tcp://172.16.12.118:17000")
    # socket.connect("tcp://172.16.10.126:10000")
    socket.connect("tcp://172.16.11.106:10000")

    msg_list = [six.int2byte(100), bytearray('login')]
    socket.send_multipart(msg_list, copy=False, track=True)

    message = socket.recv_multipart()
    print 'login recv:', message

    # SendZMQMsg(100, System.Text.Encoding.ASCII.GetBytes("login"))

    # Do 10 requests, waiting each time for a response
    msg = AllProtoMsg_pb2.InstrumentInfoRequestMsg()
    msg.IsAll = True
    msg.IncludeStaticInfo = True
    # msg.LastUpdate = time.time()

    msg_str = msg.SerializeToString()
    msg_list = [six.int2byte(2), msg_str]
    socket.send_multipart(msg_list)
    # Get the reply.
    message = socket.recv_multipart()
    print 'market recv:',message
    print 'type:', six.byte2int(message[0])

    recv_message = AllProtoMsg_pb2.InstrumentInfoResponseMsg()
    recv_message.ParseFromString(zlib.decompress(message[1]))

    instrument_dict = dict()
    for instrument_msg in recv_message.Targets:
        instrument_dict[instrument_msg.id] = instrument_msg

    for market_msg in recv_message.Infos:
        instrument_info = instrument_dict[market_msg.ID]
        market_args = market_msg.Args
        print instrument_info.ticker, market_args.LastPrice, __GetDateTime(market_args.UpdateTime)


def __GetDateTime(input_value):
    Jan1st1970 = datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

    value = input_value.value
    if input_value.scale == bcl_pb2.DateTime().TICKS:
        return Jan1st1970 + datetime.timedelta(microseconds=value/10)
    elif input_value.scale == bcl_pb2.DateTime().MILLISECONDS:
        return Jan1st1970 + datetime.timedelta(milliseconds=value)
    elif input_value.scale == bcl_pb2.DateTime().SECONDS:
        return Jan1st1970 + datetime.timedelta(seconds=value)
    elif input_value.scale == bcl_pb2.DateTime().MINUTES:
        return Jan1st1970 + datetime.timedelta(minutes=value)
    elif input_value.scale == bcl_pb2.DateTime().HOURS:
        return Jan1st1970 + datetime.timedelta(hours=value)
    elif input_value.scale == bcl_pb2.DateTime().DAYS:
        return Jan1st1970 + datetime.timedelta(days=value)
    return Jan1st1970

if __name__ == '__main__':
    # test_date_time = bcl_pb2.DateTime()
    # test_date_time.scale = bcl_pb2.DateTime().TICKS
    # test_date_time.value = -179874432000000000
    # print __GetDateTime(test_date_time)
    get_market_info()


