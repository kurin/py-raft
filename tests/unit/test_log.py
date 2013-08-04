import pytest

from raft import log

def logentry(index, term, committed=False, msgid='', msg={}):
    return dict(index=index, term=term, committed=committed,
                msgid=msgid, msg=msg)

def test_le():
    # a's term is greater than b's
    a = {1: logentry(1, 2),
         2: logentry(2, 2),
         3: logentry(3, 4)}
    b = {1: logentry(1, 2),
         2: logentry(2, 2),
         3: logentry(3, 3)}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert ra > rb
    # terms are equal
    a = {1: logentry(1, 2),
         2: logentry(2, 2),
         3: logentry(3, 4)}
    b = {1: logentry(1, 2),
         2: logentry(2, 2),
         3: logentry(3, 4)}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert a <= b
    assert b <= a
    # terms equal but more commits in b
    a = {1: logentry(1, 2),
         2: logentry(2, 2),
         3: logentry(3, 4)}
    b = {1: logentry(1, 2),
         2: logentry(2, 2),
         3: logentry(3, 4),
         4: logentry(4, 4)}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert rb > ra

