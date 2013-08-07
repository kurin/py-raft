import pytest
from raft.bijectivemap import create_map

def test_keypairs():
    a, b = create_map()
    a[1] = 'a'
    assert b['a'] == 1

def test_overwrite_key():
    a, b = create_map()
    a[1] = 'a'
    a[1] = 'b'
    assert b['b'] == 1
    assert 'a' not in b

def test_overwrite_value():
    a, b = create_map()
    a[1] = 'a'
    a[2] = 'a'
    assert b['a'] == 2
    assert 1 not in a

def test_del():
    a, b = create_map()
    a[1] = 'a'
    assert b['a'] == 1
    del a[1]
    assert 'a' not in b
