import time
import uuid
import copy
import random
import logging

import msgpack

import raft.store as store
import raft.tcp as channel
import raft.log as log


class Server(object):
    def __init__(self, port=9289, *fakes):
        self.port = port
        if not fakes:
            self.load()
        else:
            self.term, self.voted, llog, self.peers, self.uuid = fakes
            self.log = log.RaftLog(llog)
        self.role = 'follower'
        self.channel = channel.start(port, self.uuid)
        self.last_update = time.time()
        self.commitidx = 0
        self.update_uuid = None
        self.leader = None
        self.newpeers = None
        self.oldpeers = None

    #
    ## startup and state methods
    #

    def load(self):
        self.term, self.voted, llog, self.peers, self.uuid = store.read_state(self.port)
        self.log = log.RaftLog(llog)

    def save(self):
        store.write_state(self.port, self.term, self.voted,
                self.log.dump(), self.peers, self.uuid)

    def run(self):
        self.running = True
        self.next_index = None
        while self.running:
            print self.uuid, self.role, self.term, self.log.maxindex(), self.channel.u2c.keys(), self.next_index
            for peer in self.peers:
                if not peer in self.channel and peer != self.uuid:
                    self.channel.connect(self.peers[peer])
            channelans = self.channel.recv(0.25)
            if channelans:
                for peer, msgs in channelans:
                    for msg in msgs:
                        self.handle_message(msg, peer)
            else:
                self.housekeeping()

    #
    ## message handling
    #

    def handle_message(self, msg, addr):
        # got a new message
        # update our term if applicable, and dispatch the message
        # to the appropriate handler.  finally, if we are still
        # (or have become) the leader, send out heartbeats
        msg = msgpack.unpackb(msg, use_list=False)
        mtype = msg['type']
        term = msg.get('term', None)
        msg['src'] = addr
        uuid = msg.get('id', None)
        # no matter what, if our term is old, update and step down
        if term and term > self.term and self.valid_peer(uuid):
            # okay, well, only if it's from a valid source
            self.term = term
            self.voted = None
            self.role = 'follower'
        mname = 'handle_msg_%s_%s' % (self.role, mtype)
        if hasattr(self, mname):
            getattr(self, mname)(msg)
        if self.role == 'leader':
            # send heartbeats when handling messages as well
            self.send_ae()

    def handle_msg_leader_ae_reply(self, msg):
        # we are a leader who has received an ae ack
        # if the update was rejected, it's because the follower
        # has an incorrect log entry, so send an update for that
        # log entry as well
        # if the update succeeded, record that in the log and,
        # if the log has been recorded by enough followers, mark
        # it committed.
        uuid = msg['id']
        if not self.valid_peer(uuid):
            return
        success = msg['success']
        index = msg['index']
        if success:
            self.next_index[uuid] = index
            if self.log.get_commit_index() < index:
                self.msg_recorded(msg)
        else:
            self.next_index[uuid] = 0

    def handle_msg_follower_ae(self, msg):
        # we are a follower who just got an append entries rpc
        # reset the timeout counter
        uuid = msg['id']
        if not self.valid_peer(uuid):
            return
        term = msg['term']
        if term < self.term:
            return
        self.last_update = time.time()
        self.leader = msg['id']
        logs = msg['entries']
        previdx = msg['previdx']
        prevterm = msg['prevterm']
        if not self.log.exists(previdx, prevterm):
            rpc = self.ae_rpc_reply(previdx, False)
            self.send_to_peer(rpc, self.leader)
            return
        cidx = msg['commitidx']
        if cidx > self.commitidx:  # don't lower the commit index
            self.commitidx = cidx
            self.log.force_commit(cidx)
            if self.update_uuid:
                self.check_update_committed()
        if not logs:
            # heartbeat
            return
        for ent in sorted(logs):
            val = logs[ent]
            self.process_possible_update(val)
            self.log.add(val)
        rpc = self.ae_rpc_reply(self.log.maxindex(), True)
        self.send_to_peer(rpc, self.leader)

    def handle_msg_candidate_ae(self, msg):
        # someone else was elected during our candidacy
        term = msg['term']
        uuid = msg['id']
        if not self.valid_peer(uuid):
            return
        if term < self.term:
            # illegitimate, toss it
            return
        self.role = 'follower'
        self.handle_msg_follower_ae(msg)

    def handle_msg_follower_cq(self, msg):
        try:
            rpc = self.cr_rdr_rpc(msg['id'])
            src = msg['src']
            self.send_to_peer(rpc, src)
        except:
            return

    def handle_msg_leader_cq(self, msg):
        src = msg['src']
        self.add_to_log(msg)
        rpc = self.cr_rpc_ack(msg['id'])
        self.send_to_peer(rpc, src)

    def handle_msg_candidate_rv(self, msg):
        # don't vote for a different candidate!
        uuid = msg['id']
        if self.uuid == uuid:
            # huh
            return
        if not self.valid_peer(uuid):
            return
        rpc = self.rv_rpc_reply(False)
        self.send_to_peer(rpc, uuid)

    def handle_msg_follower_rv(self, msg):
        term = msg['term']
        uuid = msg['id']
        if not self.valid_peer(uuid):
            return
        olog = {msg['log_index']: {
                    'index': msg['log_index'],
                    'term': msg['log_term'],
                    'msgid': '',
                    'msg': {}}}
        olog = log.RaftLog(olog)
        if term < self.term:
            # someone with a smaller term wants to get elected
            # as if
            rpc = self.rv_rpc_reply(False)
            self.send_to_peer(rpc, uuid)
            return
        if (self.voted is None or self.voted == uuid) \
            and self.log <= olog:
            # we can vote for this guy
            self.voted = uuid
            self.save()
            rpc = self.rv_rpc_reply(True)
            self.last_update = time.time()
            self.send_to_peer(rpc, uuid)
            return
        # we probably voted for somebody else, or the log is old
        rpc = self.rv_rpc_reply(False)
        self.send_to_peer(rpc, uuid)

    def handle_msg_candidate_rv_reply(self, msg):
        uuid = msg['id']
        if not self.valid_peer(uuid):
            return
        voted = msg['voted']
        if voted:
            self.cronies.add(uuid)
        else:
            self.refused.add(uuid)
        if len(self.cronies) >= self.quorum():
            # won the election
            self.role = 'leader'
            self.next_index = {}
            self.commitidx = self.log.get_commit_index()
            maxidx = self.log.maxindex()
            for uuid in self.all_peers():
                # just start by pretending everyone is caught up,
                # they'll let us know if not
                self.next_index[uuid] = maxidx

    def handle_msg_leader_pu(self, msg):
        if self.update_uuid:
            # we're either already in the middle of this, or we're
            # in the middle of something *else*, so piss off
            return
        uuid = msg['id']
        # got a new update request
        # it will consist of machines to add and to remove
        # here we perform the first phase of the update, by
        # telling clients to add the new machines to their
        # existing peer set.
        msg['phase'] = 1
        self.newpeers = msg['config']  # adopt the new config right away
        if not self.newpeers:
            return
        self.update_uuid = uuid
        self.add_to_log(msg)

    def housekeeping(self):
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

    #
    ## convenience methods
    #

    def send_ae(self):
        for uuid in self.all_peers():
            if uuid == self.uuid:  # no selfies
                continue
            ni = self.next_index.get(uuid, self.log.maxindex())
            logs = self.log.logs_after_index(ni)
            rpc = self.ae_rpc(uuid, logs)
            self.send_to_peer(rpc, uuid)

    def call_election(self):
        self.term += 1
        self.voted = self.uuid
        self.save()
        self.cronies = set()
        self.refused = set()
        self.cronies.add(self.uuid)
        self.election_start = time.time()
        self.election_timeout = 0.15 * random.random() + 0.15
        self.role = 'candidate'
        self.campaign()

    def campaign(self):
        voted = self.cronies.union(self.refused)  # everyone who voted
        voters = set(self.peers)
        if self.newpeers:
            voters = voters.union(set(self.newpeers))
        remaining = voters.difference(voted)  # peers who haven't
        rpc = self.rv_rpc()
        for uuid in remaining:
            self.send_to_peer(rpc, uuid)

    def check_update_committed(self):
        # we (a follower) just learned that one or more
        # logs were committed, *and* we are in the middle of an
        # update.  check to see if that was phase 2 of the update,
        # and remove old hosts if so
        umsg = self.log.get_by_uuid(self.update_uuid)
        if umsg['index'] > self.commitidx:
            # isn't yet committed
            return
        data = umsg['msg']
        if data['phase'] == 2:
            self.oldpeers = None
            self.update_uuid = None
            if not self.uuid in self.all_peers():
                self.running = False

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
        if self.update_uuid == uuid:
            # we've already done this
            return
        self.update_uuid = uuid  # in case we become leader during this debacle
        if phase == 1:
            self.newpeers = data['config']
        elif phase == 2 and self.newpeers:
            self.oldpeers = self.peers
            self.peers = self.newpeers
            self.newpeers = None

    def possible_update_commit(self):
        # we're in an update; see if the update msg
        # has committed, and go to phase 2 or finish
        if not self.log.is_committed_by_uuid(self.update_uuid):
            # it hasn't
            return
        umsg = self.log.get_by_uuid(self.update_uuid)
        data = copy.deepcopy(umsg['msg'])
        if data['phase'] == 1 and self.newpeers:
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
            # the *second* phase is now committed.  tell all our
            # current peers about the successful commit, drop
            # the old config entirely and, if necessary, step down
            self.send_ae()  # send this to peers who might be about to dispeer
            self.oldpeers = None
            self.update_uuid = None
            if not self.uuid in self.peers:
                self.running = False

    def all_peers(self):
        for host in self.peers:
            yield host
        if self.newpeers:
            for host in self.newpeers:
                yield host
        if self.oldpeers:
            for host in self.oldpeers:
                yield host

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

    def send_to_peer(self, rpc, uuid):
        self.channel.send(rpc, uuid)

    def quorum(self):
        peers = set(self.peers)
        if self.newpeers:
            peers.union(set(self.newpeers))
        # oldpeers don't get a vote
        # use sets because there could be dupes
        np = len(peers)
        return np/2 + 1

    def msg_recorded(self, msg):
        # we're a leader and we just got an ack from
        # a follower who might have been the one to
        # commit an entry
        term = msg['term']
        index = msg['index']
        uuid = msg['id']
        self.log.add_ack(index, term, uuid)
        if self.log.num_acked(index) >= self.quorum() and term == self.term:
            self.log.commit(index, term)
            assert index >= self.commitidx
            oldidx = self.commitidx
            self.commitidx = index
            if self.update_uuid:
                # if there's an update going on, see if our commit
                # is actionable
                self.possible_update_commit()
            # otherwise just see what messages are now runnable
            self.run_committed_messages(oldidx)

    def add_to_log(self, msg):
        uuid = msg['id']
        logentry = log.logentry(self.term, uuid, msg)
        index = self.log.add(logentry)
        self.save()
        self.log.add_ack(index, self.term, self.uuid)

    def run_committed_messages(self, oldidx):
        pass

    #
    ## rpc methods
    #

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

    def cr_rpc_ack(self, qid):
        # client response RPC
        # qid = query id, ans is arbitrary data
        rpc = {
            'type': 'cr_ack',
            'id': qid,
        }
        return msgpack.packb(rpc)

    def cr_rdr_rpc(self, msgid):
        # client response redirect; just point them
        # at the master
        rpc = {
            'type': 'cr_rdr',
            'id': msgid,
            'addr': self.get_peer_addr(self.leader),
            'leader': self.leader
        }
        return msgpack.packb(rpc)
