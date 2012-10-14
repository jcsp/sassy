from collections import defaultdict
import struct
import zmq
from common import PORT, FORMAT

journal = []
series = defaultdict(list)
series_files = {}

class Journaller(object):
    def run(self):
        ctx = zmq.Context()
        skt = ctx.socket(zmq.PULL)
        skt.bind("tcp://127.0.0.1:%s" % PORT)

        file = open('journal.bin', 'w')

        while True:
            msg = skt.recv()
            journal.append(msg)
            file.write(msg)

            for i in range(0, len(msg), struct.calcsize(FORMAT)):
                server_id, stat_id, t, val = struct.unpack_from(FORMAT, msg, i)
                stat_tag = (server_id, stat_id)
                series[stat_tag].append((t, val))
                if not server_id in series_files:
                    series_files[server_id] = open('journal_%s.bin' % (server_id), 'w')
                series_files[server_id].write(struct.pack("IQ", t, val))


Journaller().run()