import pytest

from raft import log

def test_le():
    # a's term is greater than b's
    a = {1: (2, True, ''),
         2: (2, True, ''),
         3: (4, False, '')}
    b = {1: (2, True, ''),
         2: (2, True, ''),
         3: (3, True, '')}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert ra > rb
    # terms are equal
    a = {1: (2, True, ''),
         2: (2, True, ''),
         3: (4, False, '')}
    b = {1: (2, True, ''),
         2: (2, True, ''),
         3: (4, True, '')}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert a <= b
    # terms equal but more commits in b
    a = {1: (2, True, ''),
         2: (2, True, ''),
         3: (4, False, '')}
    b = {1: (2, True, ''),
         2: (2, True, ''),
         3: (4, True, ''),
         4: (4, True, '')}
    ra = log.RaftLog(a)
    rb = log.RaftLog(b)
    assert b > a

