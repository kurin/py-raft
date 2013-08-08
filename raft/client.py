import uuid

import msgpack

import raft.tcp as tcp

class RaftClient(object):
    def __init__(self):
        self.tcp = tcp.TCP(0, 'client')
        self.tcp.start()
        self.tcp.connect(('localhost', 9990))
        self.leader = 'a'

    def sendquery(self, addr, query):
        rpc = self.cq_rpc(query)
        self.tcp.send(rpc, self.leader)
        ans = self.tcp.recv(0.5)
        if not ans:
            return
        for a in ans:
            uid, msgs = a
            for msg in msgs:
                msg = msgpack.unpackb(msg, use_list=False)
                if msg['type'] == 'cr_rdr':
                    if not msg['addr']:
                        return
                    self.leader = msg['leader']
                    self.tcp.connect(msg['addr'])
                    self.tcp.send(rpc, self.leader)
                    ans = self.tcp.recv(0.5)
                else:
                    print msg

    def update_hosts(self, config, addr):
        rpc = self.pu_rpc(config)
        self.tcp.send(rpc, addr)

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
