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
        ans = self.udp.recv(0.25)  # in reality, have it time out and retry
        if not ans:
            return
        msg, _ = ans
        msg = msgpack.unpackb(msg)
        if msg['type'] == 'cr_rdr':
            addr = msg['master']
            addr = tuple(addr)
            print "got redirected to %s" % (addr,)
            self.udp.send(rpc, addr)
            ans = self.udp.recv(0.25)
            if not ans:
                return
            msg, _ = ans
            msg = msgpack.unpackb(msg)
        return msg['data']

    def update_hosts(self, config, addr):
        rpc = self.pu_rpc(config)
        self.udp.send(rpc, addr)

    def cq_rpc(self, query):
        # client query rpc
        rpc = {
            'type': 'cq',
            'id': uuid.uuid4().hex,
            'data': query
        }
        return msgpack.packb(rpc)

    def pu_rpc(self, config):
        # protocol update rpc
        rpc = {
            'type': 'pu',
            'id': uuid.uuid4().hex,
            'config': config
        }
        return msgpack.packb(rpc)
