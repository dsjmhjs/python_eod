import six
import zmq
import AllProtoMsg_pb2


def start_strategy_job():
    context = zmq.Context().instance()
    print "Connecting to aggregator server"
    socket = context.socket(zmq.DEALER)

    socket.setsockopt(zmq.IDENTITY, b'172.16.11.68')

    socket.connect("tcp://172.16.12.118:17000")

    msg = AllProtoMsg_pb2.StrategyParameterChangeRequestMsg()
    msg.Name = 'CalendarMA.SU'
    msg.IsEnable = False

    msg_str = msg.SerializeToString()

    msg_list = [six.int2byte(16), msg_str]

    print "Send StrategyParameterChangeRequestMsg Message."
    socket.send_multipart(msg_list)

    recv_message = socket.recv_multipart()
    print 'Recv Message Type:', six.byte2int(recv_message[0])



if __name__ == '__main__':
    start_strategy_job()