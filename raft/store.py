import os
import errno
import uuid

import msgpack  # we're using it anyway...

def read_state():
    sfile = '/tmp/raft-state'
    try:
        with open(sfile) as r:
            return msgpack.unpackb(r.read())
    except IOError as e:
        if not e.errno == errno.ENOENT:
            raise
    # no state file exists; initialize with fresh values
    return 0, None, [], [], uuid.uuid4().hex

def write_state(term, voted, log, peers, uuid):
    sfile = '/tmp/raft-state'
    with open(sfile, 'w') as w:
        w.write(msgpack.packb((term, voted, log, peers, uuid)))
