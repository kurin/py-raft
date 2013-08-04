import time
import uuid
import random

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
            self.role = 'follower'
        mname = 'handle_msg_%s' % mtype
        if not hasattr(self, mname):
            return  # drop malformed(?) rpc
        getattr(self, mname)(msg)

    def handle_msg_rv(self, msg):
        term = msg['term']
        uuid = msg['id']
        olog = msg['log_index'], msg['log_term']
        addr = self.peers[uuid]
        if term < self.term:
            # someone with a smaller term wants to get elected
            # as if
            rpc = self.rv_rpc_reply(False)
            self.udp.send(rpc, addr)
            return
        if (self.voted is None or self.voted == uuid) \
            and not self.other_log_older(olog):
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

    def handle_msg_rv_reply(self, msg):
        if self.role == 'follower':
            # we probably stepped down in the middle of an election
            return
        uuid = msg['id']
        voted = msg['voted']
        if voted:
            self.cronies.add(uuid)
        else:
            self.refused.add(uuid)

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
            self.campaign
        elif self.role == 'candidate':
            # the election timeout *has* expired, and we *still*
            # haven't won or lost.  call a new election.
            self.call_election()
        elif self.role == 'leader':
            # send a heartbeat

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

    def get_log_status(self):
        try:
            last_log = self.log[-1]
        except IndexError:
            last_log = (0, 0, '')
        return last_log[0], last_log[1]

    def other_log_older(self, otherlog):
        my_index, my_term = self.get_log_status()
        o_index, o_term = otherlog
        if o_term < my_term:
            return True
        if o_index < my_index:
            return True
        return False

    def rv_rpc(self):
        log_index, log_term = self.get_log_status()
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
            'term': self.term,
            'voted': voted
        }
        return msgpack.packb(rpc)
