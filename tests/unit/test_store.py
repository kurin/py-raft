import pytest
from mock import Mock, MagicMock

def test_write(monkeypatch):
    import os
    import msgpack
    import raft.store as store

    mock_open = MagicMock()
    monkeypatch.setattr(store, 'open', mock_open, raising=False)
    fobj = MagicMock()
    mock_open().__enter__.return_value = fobj

    vals = (25,
            'mr excalibur',
            [(1, 1, 'a msg'), (2, 1, 'another msg')],
            {'mr excalibur': ('192.168.0.15', 2995)},
            'conan')
    packed = msgpack.packb(vals)
    store.write_state(0, *vals)
    fobj.write.assert_called_with(packed)
