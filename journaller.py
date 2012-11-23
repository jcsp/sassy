from collections import defaultdict
import struct
import time
import zmq
from common import PORT, FORMAT, DEFAULT_PERIODS, RAW_PERIOD
import pymongo
import bson

journal = []
series_files = {}


# Options for your journal:
#  - none: die horribly when restarting service (ok if you're writing out often)
#  - raw disk: very fast, requires handy local access
#  - mongodb capped collection: about half as fast as raw disk

# Options for your persistent output:
#  - mongodb capped collections (requires up-front decision about how long to keep)
#  - mongodb regular collections
#  - mysql tables
#  - ... others?


# There is a global horizon
# When it is PERIOD * 2 ahead of the last
# data frozen for PERIOD, then we can go back
# and freeze.  To freeze, we need the data from
# the next-most-fine resolution

# e.g. last freeze for 60s data was at t=300
# when t > 420, we will freeze the region from
# 300 to 360, and delete the raw data from 300-360
# from memory (or at least put it somewhere that
# it can be aged out safely)




conn = pymongo.Connection()
db = conn.journaldb
#db.drop_collection('journal')
#db.create_collection('journal', capped = True, size = 1024 * 1024 * 1024 * 2)
#journal_collection = db.journal

for period in DEFAULT_PERIODS:
    collection_name = "rollup_%s" % period
    db.drop_collection(collection_name)
    db.create_collection(collection_name)

class SeriesState(object):
    # These periods are not written out as they come in,
    # they are written in bulk when the next period up
    # is written

    IN_MEMORY_PERIODS = (0, 60,)
    ACCUMULATOR_PERIODS = DEFAULT_PERIODS
    def __init__(self, id):
        self._id = id
        self.raw = []
        self.accumulators = dict([(p, list()) for p in self.ACCUMULATOR_PERIODS])
        self.next_rollup_gate = {}
        self.last_t = None

    def _try_rollup(self, t):
        if self.last_t is None:
            self.last_t = t
            for period in self.ACCUMULATOR_PERIODS[1:]:
                rounded = (t / period) * period
                self.next_rollup_gate[period] = rounded + period * 2
        else:
            if not t > self.next_rollup_gate[self.ACCUMULATOR_PERIODS[1]]:
                return
            for period_index, p in enumerate(self.ACCUMULATOR_PERIODS[1:]):
                if t > self.next_rollup_gate[p]:
                    self.next_rollup_gate[p] += p
                    t_start = self.next_rollup_gate[p] - p * 2
                    t_end = t_start + p
                    data = self.accumulators[self.ACCUMULATOR_PERIODS[period_index]]
                    sum = 0.0
                    count = 0
                    latest = None
                    for i, d in enumerate(data):
                        if d[0] >= t_start and d[0] < t_end:
                            latest = i
                            sum += d[1]
                            count += 1
                    if latest is not None:
                        collection_name = "rollup_%s" % p
                        mid_point_t = t_start + p / 2
                        mid_point_val = sum / float(count)
                        if not p in self.IN_MEMORY_PERIODS:
                            # FIXME: no need to do an individual insert here, can
                            # stash them all until the end of processing an
                            # incoming message
                            db[collection_name].insert({
                                't': mid_point_t,
                                'v': mid_point_val,
                                's_id': self._id
                            })
                        self.accumulators[p].append((mid_point_t, mid_point_val))

                        if self.ACCUMULATOR_PERIODS[period_index] in self.IN_MEMORY_PERIODS:
                            # FIXME this is going to write out the raw data every time
                            # I write out a 60s rollup, really I want to wait until the
                            # 300s rollup before writing out either.
                            records = []
                            for datapoint in data[:latest + 1]:
                                records.append({
                                    't': datapoint[0],
                                    'v': datapoint[1],
                                    's_id': self._id
                                })
                            collection_name = "rollup_%s" % self.ACCUMULATOR_PERIODS[period_index]
                            db[collection_name].insert(records)

                        self.accumulators[self.ACCUMULATOR_PERIODS[period_index]] = data[latest + 1:]

    def insert(self, t, val, rollup = False):
        if rollup:
            self._try_rollup(t)

        self.accumulators[RAW_PERIOD].append((t, val))

series = {}


class Journaller(object):
    def run(self):
        ctx = zmq.Context()
        skt = ctx.socket(zmq.PULL)
        skt.bind("tcp://127.0.0.1:%s" % PORT)

        file = open('journal.bin', 'w')

        byte_count = 0
        start = time.time()
        datapoint_count = 0
        stats_time = 0
        while True:
            msg = skt.recv()
            byte_count += len(msg)
            #journal.append(msg)
            file.write(msg)
            #journal_collection.insert({'b': bson.Binary(msg)})

            if byte_count > 1000000:
                t = time.time()
                duration = t - start
                print "At stats time %s" % stats_time
                print "Issue rate: %.1f/s" % (float(datapoint_count) / duration)
                print "Bandwidth: %.1fMB/s" % (float(byte_count) / duration / (1024.0 * 1024.0))
                byte_count = 0
                datapoint_count = 0
                start = t

            for i in range(0, len(msg), struct.calcsize(FORMAT)):
                id, t, val = struct.unpack_from(FORMAT, msg, i)
                try:
                    series_state = series[id]
                except KeyError:
                    series_state = SeriesState(id)
                    series[id] = series_state
                series_state.insert(t, val, True)
                datapoint_count += 1
                stats_time = max(stats_time, t)

Journaller().run()