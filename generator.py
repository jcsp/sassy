import ctypes
import struct
import zmq
from common import PORT, FORMAT
import time

class Generator(object):
    def run(self):
        ctx = zmq.Context()
        skt = ctx.socket(zmq.PUSH)
        skt.connect("tcp://127.0.0.1:%s" % PORT)

        server_count = 1000
        stats_per_server = 100
        time_iterations = 1000
        start = time.time()
        sent = 0
        format_length = struct.calcsize(FORMAT)
        for t in xrange(0, time_iterations):
            for server in xrange(0, server_count):
                buffer = ctypes.create_string_buffer(format_length * stats_per_server)
                for stat in xrange(0, stats_per_server):
                    val = 0
                    struct.pack_into(FORMAT, buffer, stat * format_length, server, stat, t, val)
                skt.send(buffer)
                sent += stats_per_server

        end = time.time()
        print "Issue rate: %.1f/s" % (float(sent) / (end - start))
        msg_size = struct.calcsize(FORMAT)
        #msg_size = len(STATIC_PAYLOAD)
        print "Bandwidth: %.1fMB/s" % (float(sent * msg_size) / (1024.0*1024.0) / (end - start))
        print "Total sent: %.1fMB" % (float(sent * msg_size) / (1024.0 * 1024.0))



Generator().run()
