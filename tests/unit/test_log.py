import pytest

from raft import log

def mle(index, term, committed=False, msgid='', msg={}):
    return dict(index=index, term=term, committed=committed,
                msgid=msgid, msg=msg)

def test_le():
    # a's term is greater than b's
    a = {1: mle(1, 2),
         2: mle(2, 2),
         3: mle(3, 4)}
    b = {1: mle(1, 2),
         2: mle(2, 2),
         3: mle(3, 3)}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert ra > rb
    # terms are equal
    a = {1: mle(1, 2),
         2: mle(2, 2),
         3: mle(3, 4)}
    b = {1: mle(1, 2),
         2: mle(2, 2),
         3: mle(3, 4)}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert a <= b
    assert b <= a
    # terms equal but more commits in b
    a = {1: mle(1, 2),
         2: mle(2, 2),
         3: mle(3, 4)}
    b = {1: mle(1, 2),
         2: mle(2, 2),
         3: mle(3, 4),
         4: mle(4, 4)}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert rb > ra

def test_dump():
    rl = log.RaftLog(None)
    dump = {0: {'term': 0, 'msgid': '', 'committed': True,
                'acked': [], 'msg': {}, 'index': 0}}
    assert rl.dump() == dump

def test_get_max_index_term():
    rl = log.RaftLog(None)
    le = log.logentry(2, 'abcd', {})
    rl.add(le)
    assert rl.get_max_index_term() == (1, 2)
    le = log.logentry(6, 'abcdefg', {})
    rl.add(le)
    assert rl.get_max_index_term() == (2, 6)

def test_has_uuid():
    rl = log.RaftLog(None)
    le = log.logentry(2, 'abcd', {})
    rl.add(le)
    assert rl.has_uuid('abcd') == True
    assert rl.has_uuid('dcba') == False

def test_maxindex():
    rl = log.RaftLog(None)
    assert rl.maxindex() == 0
    le = log.logentry(2, 'abcd', {})
    rl.add(le)
    assert rl.maxindex() == 1
    le = log.logentry(2, 'abcde', {})
    le['index'] = 12
    rl.add(le)
    assert rl.maxindex() == 12
    le = log.logentry(2, 'abcdef', {})
    le['index'] = 5
    rl.add(le)
    assert rl.maxindex() == 5
