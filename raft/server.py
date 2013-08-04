import time
import uuid
import random

import msgpack

import raft.store as store
import raft.udp as udp
from raft.log import RaftLog


class Server(object):
    def __init__(self, port=9289):
        self.load()
        self.role = 'follower'
        self.udp = udp.UDP(port)
        self.last_update = time.time()

    def load(self):
        self.term, self.voted, log, self.peers, self.uuid = store.read_state()
        self.log = RaftLog(log)

    def save(self):
        store.write_state(self.term, self.voted,
                self.log.dump(), self.peers, self.uuid)

    def run(self):
        while True:
            msg, addr = self.udp.recv()
            if msg is not None:
                self.handle_message(msg)
            else:
                self.handle_nomessage()

    def handle_message(self, msg):
        msg = msgpack.unpackb(msg)
        mtype = msg['type']
        term = msg['term']
        # no matter what, if our term is old, update and step down
        if term > self.term:
            self.term = term
            self.voted = None
            self.role = 'follower'
        mname = 'handle_msg_%s_%s' % (self.role, mtype)
        if not hasattr(self, mname):
            return  # nothing to do
        getattr(self, mname)(msg)

    def handle_msg_candidate_rv(self, msg):
        # don't vote for a different candidate!
        uuid = msg[b'id']
        if self.uuid == uuid:
            # huh
            return
        addr = self.peers[uuid]
        rpc = self.rv_rpc_reply(False)
        self.udp.send(rpc, addr)

    def handle_msg_follower_rv(self, msg):
        term = msg['term']
        uuid = msg['id']
        olog = {msg['log_index']: (msg['log_term'], None, None)}
        olog = RaftLog(olog)
        addr = self.peers[uuid]
        if term < self.term:
            # someone with a smaller term wants to get elected
            # as if
            rpc = self.rv_rpc_reply(False)
            self.udp.send(rpc, addr)
            return
        if (self.voted is None or self.voted == uuid) \
            and self.log <= olog:
            # we can vote for this guy
            self.voted = uuid
            self.save()
            rpc = self.rv_rpc_reply(True)
            self.last_update = time.time()
            self.udp.send(rpc, addr)
            return
        # we probably voted for somebody else, or the log is old
        rpc = self.rv_rpc_reply(False)
        self.udp.send(rpc, addr)

    def handle_msg_candidate_rv_reply(self, msg):
        uuid = msg['id']
        voted = msg['voted']
        if voted:
            self.cronies.add(uuid)
        else:
            self.refused.add(uuid)
        if len(self.cronies) - 1 > len(self.peers)/2:
            # won the election
            self.role = 'leader'

    def handle_nomessage(self):
        now = time.time()
        if now - self.last_update > 0.5 and self.role == 'follower':
            # got no heartbeats; leader is probably dead
            # establish candidacy and run for election
            self.call_election()
        elif self.role == 'candidate' and \
           now - self.selection_start < self.election_timeout:
            # we're in an election and haven't won, but the
            # timeout isn't expired.  repoll peers that haven't
            # responded yet
            self.campaign()
        elif self.role == 'candidate':
            # the election timeout *has* expired, and we *still*
            # haven't won or lost.  call a new election.
            self.call_election()
        elif self.role == 'leader':
            # send a heartbeat
            pass

    def call_election(self):
        self.term += 1
        self.voted = self.uuid
        self.save()
        self.cronies = set()
        self.refused = set()
        self.cronies.add(self.uuid)
        self.election_start = time.time()
        self.election_timeout = 0.5 * random.random() + 0.5
        self.role = 'candidate'
        self.campaign()

    def campaign(self):
        voted = self.cronies.union(self.refused)  # everyone who voted
        remaining = set(self.peers).difference(voted)  # peers who haven't
        rpc = self.rv_rpc()
        for uuid in remaining:
            addr = self.peers[uuid]
            self.udp.send(rpc, addr)

    def rv_rpc(self):
        log_index, log_term = self.get_max_index_term()
        rpc = {
            'type': 'rv',
            'term': self.term,
            'id': self.uuid,
            'log_index': log_index,
            'log_term': log_term,
        }
        return msgpack.packb(rpc)

    def rv_rpc_reply(self, voted):
        rpc = {
            'type': 'rv_reply',
            'id': self.uuid,
            'term': self.term,
            'voted': voted
        }
        return msgpack.packb(rpc)

    def ae_rpc(self, peeruuid, append=[]):
        previdx = self.next_index[peeruuid]
        rpc = {
            'type': 'ae',
            'term': self.term,
            'id': self.uuid,
            'previdx': previdx,
            'prevterm': self.log[previdx][1],
            'entries': append,
            'commitidx': self.commitidx,
        }
