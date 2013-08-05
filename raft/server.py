import time
import uuid
import copy
import random

import msgpack

import raft.store as store
#try:
#    import raft.snake as transport
#except ImportError:
import raft.udp as transport
import raft.log as log


class Server(object):
    def __init__(self, port=9289, *fakes):
        if not fakes:
            self.load()
        else:
            self.term, self.voted, llog, self.peers, self.uuid = fakes
            self.log = log.RaftLog(llog)
        self.role = 'follower'
        self.transport = transport.start(port)
        self.last_update = time.time()
        self.commitidx = 0
        self.maxmsgsize = 65536
        self.update_uuid = None
        self.leader = None
        self.newpeers = None
        self.oldpeers = None

    def load(self):
        self.term, self.voted, llog, self.peers, self.uuid = store.read_state()
        self.log = log.RaftLog(llog)

    def save(self):
        store.write_state(self.term, self.voted,
                self.log.dump(), self.peers, self.uuid)

    def run(self):
        self.running = True
        while self.running:
            print self.term, self.role, self.peers.keys()
            ans = self.transport.recv()
            if ans is not None:
                msg, addr = ans
                self.handle_message(msg, addr)
            else:
                self.handle_nomessage()

    def handle_message(self, msg, addr):
        msg = msgpack.unpackb(msg, use_list=False)
        mtype = msg['type']
        term = msg.get('term', None)
        msg['src'] = addr
        uuid = msg.get('id', None)
        # no matter what, if our term is old, update and step down
        if term and term > self.term:
            # okay, well, only if it's from a valid source
            if uuid and (uuid in self.peers or uuid in self.newpeers):
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
        if not uuid in self.peers and not uuid in self.newpeers:
            return
        index = msg['index']
        if success:
            self.next_index[uuid] = index
            if self.log.get_commit_index() < index:
                # if it is already committed, then we don't need to worry
                # about any of this
                term = msg['term']
                index = msg['index']
                self.log.add_ack(index, term)
                if self.log.num_acked(index) >= (len(self.peers)/2) + 1 and \
                   term == self.term:
                    self.log.commit(index, term)
                    assert index >= self.commitidx
                    self.commitidx = index
                    if self.update_uuid:
                        self.possible_update_commit()
        else:
            self.next_index[uuid] = msg['index'] - 1

    def handle_msg_follower_ae(self, msg):
        uuid = msg['id']
        if not uuid in self.peers:
            return  # bogus
        self.last_update = time.time()
        self.leader = msg['id']
        logs = msg['entries']
        addr = self.peers.get(self.leader, None)
        if not addr:
            return
        previdx = msg['previdx']
        prevterm = msg['prevterm']
        if not self.log.exists(previdx, prevterm):
            rpc = self.ae_rpc_reply(previdx, False)
            self.transport.send(rpc, addr)
            return
        if not logs:
            # just a heartbeat; update some values
            cidx = msg['commitidx']
            if cidx > self.commitidx:  # don't lower the commit index
                self.commitidx = cidx
                self.log.force_commit(cidx)
            return
        for ent in sorted(logs):
            val = logs[ent]
            self.process_possible_update(val)
            self.log.add(val)
        cidx = msg['commitidx']
        if cidx > self.commitidx:
            self.commitidx = cidx
            self.log.force_commit(cidx)
        rpc = self.ae_rpc_reply(self.log.maxindex(), True)
        self.transport.send(rpc, addr)

    def handle_msg_candidate_ae(self, msg):
        # someone else was elected during our candidacy
        term = msg['term']
        uuid = msg['id']
        if not uuid in self.peers:
            return
        if term < self.term:
            # illegitimate, toss it
            return
        self.role = 'follower'
        self.handle_msg_follower_ae(msg)

    def handle_msg_follower_cq(self, msg):
        try:
            rpc = self.cr_rdr_rpc()
            src = msg['src']
            self.transport.send(rpc, src)
        except:
            return

    def handle_msg_leader_cq(self, msg):
        logentry = log.logentry(self.term, msg['id'], msg)
        self.log.add(logentry)
        rpc = self.cr_rpc(msg['id'], msg['data'])
        src = msg['src']
        self.transport.send(rpc, src)

    def handle_msg_candidate_rv(self, msg):
        # don't vote for a different candidate!
        uuid = msg['id']
        if self.uuid == uuid:
            # huh
            return
        if not uuid in self.peers:
            # who is this?
            return
        addr = self.peers.get(uuid, None)
        if not addr:
            return
        rpc = self.rv_rpc_reply(False)
        self.transport.send(rpc, addr)

    def handle_msg_follower_rv(self, msg):
        term = msg['term']
        uuid = msg['id']
        if not uuid in self.peers:
            return
        olog = {msg['log_index']: {
                    'index': msg['log_index'],
                    'term': msg['log_term'],
                    'msgid': '',
                    'msg': {}}}
        olog = log.RaftLog(olog)
        addr = self.peers.get(uuid, None)
        if not addr:
            return
        if term < self.term:
            # someone with a smaller term wants to get elected
            # as if
            rpc = self.rv_rpc_reply(False)
            self.transport.send(rpc, addr)
            return
        if (self.voted is None or self.voted == uuid) \
            and self.log <= olog:
            # we can vote for this guy
            self.voted = uuid
            self.save()
            rpc = self.rv_rpc_reply(True)
            self.last_update = time.time()
            self.transport.send(rpc, addr)
            return
        # we probably voted for somebody else, or the log is old
        rpc = self.rv_rpc_reply(False)
        self.transport.send(rpc, addr)

    def handle_msg_candidate_rv_reply(self, msg):
        uuid = msg['id']
        if not uuid in self.peers:
            return
        voted = msg['voted']
        if voted:
            self.cronies.add(uuid)
        else:
            self.refused.add(uuid)
        if len(self.cronies) - 1 >= len(self.peers)/2:
            # won the election
            self.role = 'leader'
            self.next_index = {}
            self.commitidx = self.log.get_commit_index()
            maxidx = self.log.maxindex()
            for uuid in self.peers:
                # just start by pretending everyone is caught up,
                # they'll let us know if not
                self.next_index[uuid] = maxidx

    def handle_msg_leader_pu(self, msg):
        if self.update_uuid:
            # we're either already in the middle of this, or we're
            # in the middle of something *else*, so piss off
            return
        uuid = msg['id']
        self.update_uuid = uuid
        # got a new update request
        # it will consist of machines to add and to remove
        # here we perform the first phase of the update, by
        # telling clients to add the new machines to their
        # existing peer set.
        msg['phase'] = 1
        self.newpeers = msg['config']  # adopt the new config right away
        logentry = log.logentry(self.term, uuid, msg)
        self.log.add(logentry)

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
            ni = self.next_index.get(uuid, self.log.maxindex())
            logs = self.log.logs_after_index(ni)
            rpc = self.ae_rpc(uuid, logs)
            addr = self.peers[uuid]
            try:
                self.transport.send(rpc, addr)
            except transport.TooBig:
                # the message was too big; try with half the message size
                self.maxmsgsize /= 2
                self.send_ae()

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
            addr = self.peers.get(uuid, None)
            if addr is None:
                continue
            self.transport.send(rpc, addr)

    def process_possible_update(self, msg):
        if not 'msg' in msg:
            return
        data = msg['msg']
        if not 'type' in data:
            return
        if data['type'] != 'pu':
            return
        phase = data['phase']
        uuid = data['id']
        self.update_uuid = uuid  # in case we become leader during this debacle
        if phase == 1:
            self.newpeers = data['config']
        elif phase == 2:
            self.oldpeers = self.peers
            self.peers = self.newpeers
            self.newpeers = None

    def possible_update_commit(self):
        # we're in an update; see if the update msg
        # has committed, and go to phase 2 or finish
        umsg = self.log.get_by_uuid(self.update_uuid)
        if umsg['index'] > self.commitidx:  # it hasn't
            return
        data = copy.deepcopy(umsg['msg'])
        if data['phase'] == 1:
            # the *first* phase of the update has been committed
            # new leaders are guaranteed to be in the union of the
            # old and new configs.  now update the configuration
            # to the new one only.
            data['phase'] = 2
            newid = uuid.uuid4().hex
            self.update_uuid = newid
            data['id'] = newid
            self.oldpeers = self.peers
            self.peers = self.newpeers
            self.newpeers = None
            logentry = log.logentry(self.term, newid, data)
            self.log.add(logentry)
        else:
            # the *second* phase is now committed.  drop
            # the old config entirely and, if necessary, step down
            self.oldpeers = None
            self.update_uuid = None
            if not self.uuid in self.peers:
                self.running = False

    def all_peers(self):
        for host, addr in self.peers:
            yield host, addr
        if self.newpeers:
            for host, addr in self.newpeers:
                yield host, addr
        if self.oldpeers:
            for host, addr in self.oldpeers:
                yield host, addr

    def valid_peer(self, uuid):
        if uuid in self.peers:
            return True
        if self.newpeers and uuid in self.newpeers:
            return True
        if self.oldpeers and uuid in self.oldpeers:
            return True
        return False

    def get_peer_addr(self, uuid):
        if uuid in self.peers:
            return self.peers[uuid]
        if self.newpeers and uuid in self.newpeers:
            return self.newpeers[uuid]
        if self.oldpeers and uuid in self.oldpeers:
            return self.oldpeers[uuid]

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
        previdx = self.next_index.get(peeruuid, self.log.maxindex())
        while True:  # the only way out is to get your message size down
            rpc = {
                'type': 'ae',
                'term': self.term,
                'id': self.uuid,
                'previdx': previdx,
                'prevterm': self.log.get_term_of(previdx),
                'entries': append,
                'commitidx': self.commitidx,
            }
            packed = msgpack.packb(rpc)
            if len(packed) < self.maxmsgsize:
                # msgsize is acceptable
                return packed
            # msgsize is not; pop off the biggest dictionary entry
            nappend = {}
            size = len(append)/2
            for x in sorted(append.keys())[:size]:  # this could be faster
                nappend[x] = append[x]
            append = nappend

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
