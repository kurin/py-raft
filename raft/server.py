import time
import uuid
import random

import msgpack

import raft.store as store
import raft.udp as udp
from raft.log import RaftLog


class Server(object):
    def __init__(self, port=9289, *fakes):
        if not fakes:
            self.load()
        else:
            self.term, self.voted, log, self.peers, self.uuid = fakes
            self.log = RaftLog(log)
        self.role = 'follower'
        self.addpeers = set()
        self.delpeers = set()
        self.udp = udp.UDP(port)
        self.last_update = time.time()

    def load(self):
        self.term, self.voted, log, self.peers, self.uuid = store.read_state()
        self.log = RaftLog(log)

    def save(self):
        store.write_state(self.term, self.voted,
                self.log.dump(), self.peers, self.uuid)

    def run(self):
        self.udp.start()
        while True:
            print self.role, self.log.maxindex()
            if self.role == 'leader':
                print self.next_index
            ans = self.udp.recv()
            if ans is not None:
                msg, addr = ans
                self.handle_message(msg, addr)
            else:
                self.handle_nomessage()

    def handle_message(self, msg, addr):
        msg = msgpack.unpackb(msg)
        mtype = msg['type']
        term = msg.get('term', None)
        msg['src'] = addr
        # no matter what, if our term is old, update and step down
        if term and term > self.term:
            self.term = term
            self.voted = None
            self.role = 'follower'
        mname = 'handle_msg_%s_%s' % (self.role, mtype)
        if not hasattr(self, mname):
            return  # nothing to do
        getattr(self, mname)(msg)
        if self.role == 'leader':
            # send heartbeats when handling messages as well
            self.send_ae()

    def handle_msg_leader_ae_reply(self, msg):
        success = msg['success']
        uuid = msg['id']
        print msg
        if success:
            self.next_index[uuid] = msg['index']
        else:
            self.next_index[uuid] = msg['index'] - 1

    def handle_msg_follower_ae(self, msg):
        self.last_update = time.time()
        self.leader = msg['id']
        logs = msg['entries']
        addr = self.peers[self.leader]
        previdx = msg['previdx']
        prevterm = msg['prevterm']
        if not self.log.exists(previdx, prevterm):
            rpc = self.ae_rpc_reply(previdx, False)
            self.udp.send(rpc, addr)
            return
        if not logs:
            # just a heartbeat
            return
        for ent in sorted(logs):
            self.log.add(logs[ent])
        rpc = self.ae_rpc_reply(self.log.maxindex(), True)
        self.udp.send(rpc, addr)

    def handle_msg_candidate_ae(self, msg):
        # someone else was elected during our candidacy
        term = msg['term']
        if term < self.term:
            # illegitimate, toss it
            return
        self.role = 'follower'
        self.handle_msg_follower_ae(msg)

    def handle_msg_follower_cq(self, msg):
        try:
            rpc = self.cr_rdr_rpc()
            src = msg['src']
            self.udp.send(rpc, src)
        except:
            return

    def handle_msg_leader_cq(self, msg):
        logentry = {
            'term': self.term,
            'msgid': msg['id'],
            'committed': False,
            'msg': msg
        }
        self.log.add(logentry)
        rpc = self.cr_rpc(msg['id'], msg['data'])
        src = msg['src']
        self.udp.send(rpc, src)

    def handle_msg_candidate_rv(self, msg):
        # don't vote for a different candidate!
        uuid = msg['id']
        if self.uuid == uuid:
            # huh
            return
        addr = self.peers[uuid]
        rpc = self.rv_rpc_reply(False)
        self.udp.send(rpc, addr)

    def handle_msg_follower_rv(self, msg):
        term = msg['term']
        uuid = msg['id']
        olog = {msg['log_index']: {
                    'index': msg['log_index'],
                    'term': msg['log_term'],
                    'msgid': '',
                    'msg': {}}}
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
        if len(self.cronies) - 1 >= len(self.peers)/2:
            # won the election
            self.role = 'leader'
            self.next_index = {}
            for uuid in self.peers:
                self.next_index[uuid] = self.log.maxindex()
            self.commitidx = self.log.get_commit_index()

    def handle_nomessage(self):
        now = time.time()
        if now - self.last_update > 0.5 and self.role == 'follower':
            # got no heartbeats; leader is probably dead
            # establish candidacy and run for election
            self.call_election()
        elif self.role == 'candidate' and \
           now - self.election_start < self.election_timeout:
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
            self.send_ae()

    def send_ae(self):
        for uuid in self.peers:
            if uuid == self.uuid:
                continue
            logs = self.log.logs_after_index(self.next_index[uuid])
            rpc = self.ae_rpc(uuid, logs)
            addr = self.peers[uuid]
            self.udp.send(rpc, addr)

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
        log_index, log_term = self.log.get_max_index_term()
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

    def ae_rpc(self, peeruuid, append={}):
        previdx = self.next_index[peeruuid]
        rpc = {
            'type': 'ae',
            'term': self.term,
            'id': self.uuid,
            'previdx': previdx,
            'prevterm': self.log.get_term_of(previdx),
            'entries': append,
            'commitidx': self.commitidx,
        }
        return msgpack.packb(rpc)

    def ae_rpc_reply(self, index, success):
        rpc = {
            'type': 'ae_reply',
            'term': self.term,
            'id': self.uuid,
            'index': index,
            'success': success
        }
        return msgpack.packb(rpc)

    def cr_rpc(self, qid, ans):
        # client response RPC
        # qid = query id, ans is arbitrary data
        rpc = {
            'type': 'cr',
            'id': qid,
            'data': ans
        }
        return msgpack.packb(rpc)

    def cr_rdr_rpc(self):
        # client response redirect; just point them
        # at the master
        rpc = {
            'type': 'cr_rdr',
            'master': self.peers[self.leader]
        }
        return msgpack.packb(rpc)
