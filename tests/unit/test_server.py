import pytest
from mock import Mock, MagicMock

import msgpack

@pytest.fixture
def server(monkeypatch):
    import raft.server as srv
    store = Mock()
    udp = Mock()
    tcp = MagicMock()
    monkeypatch.setattr(srv, 'store', store)
    monkeypatch.setattr(srv, 'udp', udp)
    monkeypatch.setattr(srv, 'tcp', tcp)
    store.read_state.return_value = (27, None,
                {32: dict(index=32, term=25, committed=True, msgid='one', msg={}),
                 33: dict(index=33, term=26, committed=False, msgid='two', msg={})},
                {'otherobj': ('1.2.3.4', 5678)},
                'thisobj')
    server = srv.Server()
    return server, store, udp

def mk_rv_rpc(term, uuid, log_index, log_term):
    rpc = {
        'type': 'rv',
        'term': term,
        'id': uuid,
        'log_index': log_index,
        'log_term': log_term,
    }
    return msgpack.packb(rpc)

def mk_rv_rpc_reply(uuid, term, voted):
    rpc = {
        'type': 'rv_reply',
        'id': uuid,
        'term': term,
        'voted': voted
    }
    return msgpack.packb(rpc)

def arbrpc(**kwargs):
    return msgpack.packb(kwargs)

def test_handle_message_1(server, monkeypatch):
    # test that handle_message dispactches correctly
    server, _, _ = server
    msg = dict(term=27, id='uuid', type='ae')
    rpc = arbrpc(**msg)
    server.role = 'candidate'
    hmca = Mock()
    monkeypatch.setattr(server, 'handle_msg_candidate_ae', hmca)
    server.handle_message(rpc, None)
    msg['src'] = None  # it should also always pick this up
    hmca.assert_called_with(msg)

def test_handle_message_2(server):
    # test that terms get updated correctly
    server, _, _ = server
    assert server.role == 'follower'
    # first, send a message that should *not* update anything
    msg = dict(term=29, id='uuid', type='null')
    rpc = arbrpc(**msg)
    server.handle_message(rpc, None)
    assert server.term == 27
    # okay, now send a message that should update
    server.role = 'leader'
    msg['id'] = 'otherobj'
    rpc = arbrpc(**msg)
    server.handle_message(rpc, None)
    assert server.term == 29
    assert server.voted == None
    assert server.role == 'follower'

def test_handle_message_3(server, monkeypatch):
    # test that heartbeats go out
    server, _, _ = server
    msg = dict(term=27, id='uuid', type='ae')
    sa = Mock()
    monkeypatch.setattr(server, 'send_ae', sa)
    server.role = 'leader'
    rpc = arbrpc(**msg)
    server.handle_message(rpc, None)
    assert sa.called == True

def test_handle_msg_leader_ae_reply_0(server):
    # bad uuids get rejected
    server, _, _ = server
    msg = dict(id='badid')
    assert server.handle_msg_leader_ae_reply(msg) == None
    msg['id'] = 'otherobj'
    with pytest.raises(KeyError):
        server.handle_msg_leader_ae_reply(msg)

def test_handle_msg_leader_ae_reply_1(server, monkeypatch):
    # test that failures decease our msg index
    server, _, _ = server
    server.next_index = {}
    server.next_index['otherobj'] = 12
    msg = dict(id='otherobj', success=False, index=12)
    server.handle_msg_leader_ae_reply(msg)
    assert server.next_index['otherobj'] == 11

def test_handle_msg_leader_ae_reply_1(server, monkeypatch):
    # success updates the msg index
    server, _, _ = server
    server.next_index = {}
    server.next_index['otherobj'] = 12
    msg = dict(id='otherobj', success=True, index=14)
    server.handle_msg_leader_ae_reply(msg)
    server.log = Mock()
    server.msg_recorded = mr = Mock()
    server.log.get_commit_index.return_value = 55
    server.handle_msg_leader_ae_reply(msg)
    assert server.next_index['otherobj'] == 14
    assert mr.called == False
    msg = dict(id='otherobj', success=True, index=82)
    server.handle_msg_leader_ae_reply(msg)
    assert server.next_index['otherobj'] == 82
    assert mr.called == True

def test_handle_msg_follower_ae0(server):
    # return if bad uuid
    server, _, _ = server
    msg = dict(id='randomobj')
    assert server.handle_msg_follower_ae(msg) == None   

def test_handle_msg_follower_ae1(server):
    # return if term < current term
    # we test that by making sure that the
    # server.last_update variable is never updated
    server, _, _ = server
    server.last_update = None
    msg = dict(term=25, id='otherobj', entries=[],
               previdx=32, prevterm=25, commitidx=5)
    server.handle_msg_follower_ae(msg)
    assert server.last_update == None

def test_handle_msg_follower_ae2(server):
    # now test that server.last_update *IS* updated
    server, _, _ = server
    server.last_update = None
    msg = dict(term=27, id='otherobj', entries=[],
               previdx=32, prevterm=25, commitidx=5)
    server.handle_msg_follower_ae(msg)
    assert server.last_update != None

def test_handle_msg_follower_ae3(server):
    # failure when log entries are bad
    server, _, _ = server
    msg = dict(term=27, id='otherobj', entries=[],
               previdx=32, prevterm=27, commitidx=5)
    rpc = server.ae_rpc_reply(32, False)
    server.send_to_peer = stp = Mock()
    server.handle_msg_follower_ae(msg)
    stp.assert_called_with(rpc, 'otherobj')

def test_handle_msg_follower_ae4(server):
    # commitidx is handled correctly
    server, _, _ = server
    msg = dict(term=27, id='otherobj', entries=[],
               previdx=32, prevterm=25, commitidx=5)
    server.commitidx = 8
    server.log.force_commit = fc = Mock()
    server.handle_msg_follower_ae(msg)
    assert fc.called == False
    msg['commitidx'] = 12
    server.handle_msg_follower_ae(msg)
    fc.assert_called_with(12)

def test_handle_msg_follower_ae5(server):
    # each log is recorded
    server, _, _ = server
    logs = {
            34: dict(index=34, term=27, committed=True, msgid='one', msg={}),
            35: dict(index=35, term=27, committed=True, msgid='two', msg={}),
            36: dict(index=36, term=27, committed=True, msgid='three', msg={}),
            37: dict(index=37, term=27, committed=True, msgid='four', msg={}),
        }
    msg = dict(term=27, id='otherobj', entries=logs,
               previdx=33, prevterm=26, commitidx=5)
    server.log.add = a = Mock()
    server.handle_msg_follower_ae(msg)
    for l in logs:
        a.assert_any_call(logs[l])

def test_handle_msg_rv_1(server):
    server, _, udp = server
    # we're a candidate, and we get a solicitation from another
    # candidate in the same term.  do not vote
    server.role = 'candidate'
    msg = mk_rv_rpc(27, 'otherobj', 33, 26)
    rply = mk_rv_rpc_reply('thisobj', 27, False)
    server.handle_message(msg, None)
    udp.start().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rv_2(server):
    server, _, udp = server
    # we're a candidate, and we get a solicitation from another
    # candidate in the next term, with an equal most recent log.
    # bump our term and vote for the other candidate.
    server.role = 'candidate'
    msg = mk_rv_rpc(28, 'otherobj', 33, 26)
    assert server.term == 27
    rply = mk_rv_rpc_reply('thisobj', 28, True)  # term bumpted and voted
    server.handle_message(msg, None)
    udp.start().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rv_3(server):
    server, _, udp = server
    # again a candidate, this time we get an old solicitation
    # ignore it, *even though* its term is greater than ours
    server.role = 'candidate'
    msg = mk_rv_rpc(28, 'otherobj', 33, 25)
    assert server.term == 27
    rply = mk_rv_rpc_reply('thisobj', 28, False)
    server.handle_message(msg, None)
    udp.start().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rv_4(server):
    server, _, udp = server
    # reject candidates whose terms are less than ours
    server.role = 'candidate'
    msg = mk_rv_rpc(26, 'otherobj', 33, 26)
    assert server.term == 27
    rply = mk_rv_rpc_reply('thisobj', 27, False)
    server.handle_message(msg, None)
    udp.start().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rev_reply_1(server):
    server, _, _ = server
    # we get a vote
    server.role = 'candidate'
    msg = mk_rv_rpc_reply('otherobj', 27, True)
    server.cronies = set()
    server.handle_message(msg, None)
    assert 'otherobj' in server.cronies

def test_handle_sg_rev_reply_2(server):
    server, _, _ = server
    # we get denied
    server.role = 'candidate'
    msg = mk_rv_rpc_reply('otherobj', 27, False)
    server.cronies = set()
    server.refused = set()
    server.handle_message(msg, None)
    assert 'otherobj' not in server.cronies
    assert 'otherobj' in server.refused

def test_handle_msg_rev_reply_3(server):
    server, _, _ = server
    # we win the election
    server.role = 'candidate'
    msg = mk_rv_rpc_reply('otherobj', 27, True)
    server.cronies = set(['thisobj'])
    server.refused = set()
    server.handle_message(msg, None)
    assert server.role == 'leader'

def test_rv_rpc_reply(server):
    server, _, _ = server
    assert server.rv_rpc_reply(False) == mk_rv_rpc_reply('thisobj', 27, False)
