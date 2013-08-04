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
                {32: (25, True, 'one'),
                 33: (26, False, 'two')},
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

def mk_rv_rpc_reply(term, voted):
    rpc = {
        'type': 'rv_reply',
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
    rply = mk_rv_rpc_reply(27, False)
    server.handle_message(msg)
    udp.UDP().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_handle_msg_rv_2(server):
    server, _, udp = server
    # we're a candidate, and we get a solicitation from another
    # candidate in the next term, with an equal most recent log.
    # bump our term and vote for the other candidate.
    server.role = 'candidate'
    udp.reset_mock
    msg = mk_rv_rpc(28, 'otherobj', 33, 26)
    assert server.term == 27
    rply = mk_rv_rpc_reply(28, True)  # term bumpted and voted
    server.handle_message(msg)
    udp.UDP().send.assert_called_with(rply, ('1.2.3.4', 5678))

def test_rv_rpc_reply(server):
    server, _, _ = server
    assert server.rv_rpc_reply(False) == mk_rv_rpc_reply(27, False)
