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

def test_get():
    rl = log.RaftLog(None)
    assert rl.get(2) == None
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(2, 'abcde', {})
    rl.add(le1)
    rl.add(le2)
    assert rl.get(2) == le2
    assert rl.get_by_uuid('abcd') == le1
    assert rl.get_by_index(1) == le1

def test_get_term():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(4, 'abcde', {})
    rl.add(le1)
    rl.add(le2)
    assert rl.get_term_of(0) == 0
    assert rl.get_term_of(1) == 2
    assert rl.get_term_of(2) == 4

def test_remove():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(4, 'abcde', {})
    rl.add(le1)
    rl.add(le2)
    rl.remove(1)
    assert rl.get_by_uuid('abcd') == None
    assert rl.get_by_index(1) == None

def test_add():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(4, 'abcde', {})
    rl.add(le1)
    rl.add(le2)
    assert le1['index'] == 1
    assert le2['index'] == 2
    assert rl.get_by_uuid('abcd') == le1
    le = log.logentry(6, 'xyz', {})
    le['index'] = 1
    rl.add(le)
    assert rl.get_by_uuid('abcd') == None
    assert rl.get_by_uuid('abcde') == None

def test_add_ack():
    rl = log.RaftLog(None)
    le = log.logentry(6, 'xyz', {})
    rl.add(le)
    rl.add_ack(1, 6, 'f')
    assert 'f' in rl.get_by_uuid('xyz')['acked']

def test_num_acked():
    rl = log.RaftLog(None)
    le = log.logentry(6, 'xyz', {})
    rl.add(le)
    assert rl.num_acked(1) == 0
    rl.add_ack(1, 6, 'f')
    assert rl.num_acked(1) == 1
    # double acks don't increase our count
    rl.add_ack(1, 6, 'f')
    assert rl.num_acked(1) == 1
    rl.add_ack(1, 6, 'g')
    assert rl.num_acked(1) == 2

def test_commit():
    rl = log.RaftLog(None)
    le = log.logentry(6, 'xyz', {})
    rl.add(le)
    assert le['committed'] == False
    rl.commit(1, 6)
    assert le['committed'] == True
    with pytest.raises(AssertionError):
        rl.commit(1, 8)

def test_force_commit():
    rl = log.RaftLog(None)
    le = log.logentry(6, 'xyz', {})
    rl.add(le)
    assert le['committed'] == False
    rl.force_commit(1)
    assert le['committed'] == True
    rl.force_commit(5) # bad indices do nothing

def test_is_committed():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(2, 'abcde', {})
    le3 = log.logentry(4, 'abcdef', {})
    rl.add(le1)
    rl.add(le2)
    rl.add(le3)
    rl.commit(2, 2)
    assert rl.is_committed(1, 2) == True
    assert rl.is_committed(2, 2) == True
    assert rl.is_committed(3, 4) == False

def test_committed_by_uuid():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(2, 'abcde', {})
    le3 = log.logentry(4, 'abcdef', {})
    rl.add(le1)
    rl.add(le2)
    rl.add(le3)
    rl.commit(2, 2)
    assert rl.is_committed_by_uuid('abcd') == True
    assert rl.is_committed_by_uuid('abcdef') == False
    assert rl.is_committed_by_uuid('abcde') == True

def test_logs_after_index():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(2, 'abcde', {})
    le3 = log.logentry(4, 'abcdef', {})
    rl.add(le1)
    rl.add(le2)
    rl.add(le3)
    assert rl.logs_after_index(1) == {2: le2, 3: le3}

def test_committed_logs_after_index():
    rl = log.RaftLog(None)
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(2, 'abcde', {})
    le3 = log.logentry(4, 'abcdef', {})
    rl.add(le1)
    rl.add(le2)
    rl.add(le3)
    rl.commit(2, 2)
    assert rl.committed_logs_after_index(1) == {2: le2}

def test_get_commit_index():
    rl = log.RaftLog(None)
    assert rl.get_commit_index() == 0
    le1 = log.logentry(2, 'abcd', {})
    le2 = log.logentry(2, 'abcde', {})
    le3 = log.logentry(4, 'abcdef', {})
    rl.add(le1)
    rl.add(le2)
    rl.add(le3)
    assert rl.get_commit_index() == 0
    rl.commit(2, 2)
    assert rl.get_commit_index() == 2
