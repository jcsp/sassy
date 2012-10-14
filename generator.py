import ctypes
import struct
import math
import zmq
from common import PORT, FORMAT
import time

class Generator(object):
    def run(self):
        ctx = zmq.Context()
        skt = ctx.socket(zmq.PUSH)
        skt.connect("tcp://127.0.0.1:%s" % PORT)

        RAW_PERIOD = 10
        server_count = 10
        stats_per_server = 10
        time_iterations = (3600 * 24) / RAW_PERIOD
        #time_iterations = (1200) / RAW_PERIOD
        start = time.time()
        sent = 0
        format_length = struct.calcsize(FORMAT)
        for t_counter in xrange(0, time_iterations):
            t = t_counter * RAW_PERIOD
            val = 0.0
            # test signal: superposition of sine waves
            SINE_PERIODS = ((0.1, 60), (0.4, 3600), (1.0, 3600 * 24))
            for magnitude, period in SINE_PERIODS:
                val += magnitude * math.sin((float(t % period) / float(period) * math.pi))

            for server in xrange(0, server_count):
                buffer = ctypes.create_string_buffer(format_length * stats_per_server)
                for stat in xrange(0, stats_per_server):
                    id = (server << 16) + stat
                    struct.pack_into(FORMAT, buffer, stat * format_length, id, t, val)
                skt.send(buffer)
                sent += stats_per_server

        end = time.time()
        print "Issue rate: %.1f/s" % (float(sent) / (end - start))
        msg_size = struct.calcsize(FORMAT)
        #msg_size = len(STATIC_PAYLOAD)
        print "Bandwidth: %.1fMB/s" % (float(sent * msg_size) / (1024.0*1024.0) / (end - start))
        print "Total sent: %.1fMB" % (float(sent * msg_size) / (1024.0 * 1024.0))



Generator().run()
