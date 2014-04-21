from __future__ import print_function
import uuid

import msgpack

import raft.tcp as tcp


class NoConnection(Exception):
    pass


class RaftClient(object):
    def __init__(self, server):
        self.tcp = tcp.TCP(0, 'client')
        self.tcp.start()
        self.msgs = {}
        self.tcp.connect(server)
        if not self.tcp.u2c:
            # wait 2 seconds to connect
            self.tcp.recv(0.5)
        if not self.tcp.u2c:
            raise NoConnection
        self.leader = next(iter(self.tcp.u2c.keys()))

    def _send(self, rpc, msgid):
        self.tcp.send(rpc, self.leader)
        msgids = self.poll(0.5)
        if not msgids or not msgid in msgids:
            return  # XXX put real recovery logic here
        msg = self.msgs[msgid][0]
        if msg['type'] == 'cr_rdr':
            self.leader = msg['leader']
            print("redirected to %s! %s" % (self.leader, msg['addr']))
            self.tcp.connect(msg['addr'])
            del self.msgs[msgid]
            return self._send(rpc, msgid)

    def poll(self, timeout=0):
        ans = self.tcp.recv(timeout)
        if not ans:
            return
        msgids = set()
        for _, msgs in ans:
            for msg in msgs:
                msg = msgpack.unpackb(msg, use_list=False)
                msgid = msg['id']
                msgids.add(msgid)
                ums = self.msgs.get(msgid, [])
                ums.append(msg)
                self.msgs[msgid] = ums
        return msgids

    def send(self, data):
        msgid = uuid.uuid4().hex
        rpc = self.cq_rpc(data, msgid)
        self._send(rpc, msgid)
        return msgid

    def update_hosts(self, config):
        msgid = uuid.uuid4().hex
        rpc = self.pu_rpc(config, msgid)
        self._send(rpc, msgid)
        return msgid

    def cq_rpc(self, data, msgid):
        # client query rpc
        rpc = {
            'type': 'cq',
            'id': msgid,
            'data': data
        }
        return msgpack.packb(rpc)

    def pu_rpc(self, config, msgid):
        # protocol update rpc
        rpc = {
            'type': 'pu',
            'id': msgid,
            'config': config
        }
        return msgpack.packb(rpc)
