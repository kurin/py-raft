import uuid

import msgpack

import raft.udp as udp

class RaftClient(object):
    def __init__(self):
        self.udp = udp.UDP()
        self.udp.start()

    def sendquery(self, addr, query):
        rpc = self.cq_rpc(query)
        self.udp.send(rpc, addr)
        msg, addr = self.udp.recv(None)  # in reality, have it time out and retry
        msg = msgpack.unpackb(msg)
        if msg['type'] == 'cr_rdr':
            addr = msg['master']
            addr = tuple(addr)
            print "got redirected to %s" % (addr,)
            self.udp.send(rpc, addr)
            msg, addr = self.udp.recv(None)
            msg = msgpack.unpackb(msg)
        return msg['data']

    def cq_rpc(self, query):
        # client query rpc
        rpc = {
            'type': 'cq',
            'id': uuid.uuid4().hex,
            'data': query
        }
        return msgpack.packb(rpc)
