import pytest
from mock import Mock

import msgpack

@pytest.fixture
def server(monkeypatch):
    import raft.server as srv
    store = Mock()
    udp = Mock()
    monkeypatch.setattr(srv, 'store', store)
    monkeypatch.setattr(srv, 'udp', udp)
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

def test_handle_msg_rv_1(server):
    server, _, udp = server
    # we're a candidate, and we get a solicitation from another
    # candidate in the same term.  do not vote
    server.role = 'candidate'
    msg = mk_rv_rpc(27, 'otherobj', 33, 26)
    rply = mk_rv_rpc_reply('thisobj', 27, False)
    server.handle_message(msg, None)
    udp.UDP().send.assert_called_with(rply, ('1.2.3.4', 5678))

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
    udp.UDP().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rv_3(server):
    server, _, udp = server
    # again a candidate, this time we get an old solicitation
    # ignore it, *even though* its term is greater than ours
    server.role = 'candidate'
    msg = mk_rv_rpc(28, 'otherobj', 33, 25)
    assert server.term == 27
    rply = mk_rv_rpc_reply('thisobj', 28, False)
    server.handle_message(msg, None)
    udp.UDP().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rv_4(server):
    server, _, udp = server
    # reject candidates whose terms are less than ours
    server.role = 'candidate'
    msg = mk_rv_rpc(26, 'otherobj', 33, 26)
    assert server.term == 27
    rply = mk_rv_rpc_reply('thisobj', 27, False)
    server.handle_message(msg, None)
    udp.UDP().send.assert_called_with(rply, ('1.2.3.4', 5678))

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
