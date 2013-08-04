import time
import uuid

import msgpack

import raft.store as store
import raft.udp as udp


class Server(object):
    def __init__(self, port=9289):
        self.load()
        self.role = 'follower'
        self.udp = udp.UDP(port)
        self.last_update = time.time()

    def load(self):
        self.term, self.voted, self.log, self.peers, self.uuid = store.read_state()

    def save(self):
        store.write_state(self.term, self.voted, self.log, self.peers, self.uuid)

    def run(self):
        while True:
            msg, addr = self.udp.recv()
            if msg is not None:
                self.handle_message(msg, addr)
            else:
                self.handle_nomessage()

    def handle_nomessage(self):
        now = time.time()
        if now - self.last_update > 0.5 and self.role == 'follower':
            self.call_election()

    def call_election(self):
        self.term += 1
        self.voted = self.uuid
        self.save()
        self.cronies = set()
        self.cronies.add(self.uuid)
        self.role = 'candidate'
        rpc = self.rv_rpc()
        for _, addr in self.peers.iteritems():
            self.udp.send(rpc, addr)

    def rv_rpc(self):
        try:
            last_log = self.log[-1]
        except IndexError:
            last_log = (0, 0, '')
        log_index = last_log[0]
        log_term = last_log[1]
        rpc = {
            'type': 'rv',
            'term': self.term,
            'candidate': self.uuid,
            'log_index': log_index,
            'log_term': log_term,
        }
        return msgpack.packb(rpc)
