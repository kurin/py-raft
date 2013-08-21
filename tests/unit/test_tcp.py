import pytest
from mock import Mock

@pytest.fixture
def tcp(monkeypatch):
    import raft.tcp as tcp
    socket = Mock()
    thread = Mock()
    monkeypatch.setattr(tcp, 'socket', socket)
    monkeypatch.setattr(tcp, 'thread', thread)
    tcpobj = tcp.TCP(9990, 'uuid')
    return tcpobj, socket, thread

def test_start(tcp):
    tcpo, sock, thread = tcp
    tcpo.start()
    assert sock.socket().bind.called == True    
