import pytest

def test_protocol_is_open_gap():
    from src.sensorgrab import protocol
    with pytest.raises(NotImplementedError):
        protocol.process({"id": "x"})

def test_backfill_is_open_gap():
    from src.sensorgrab import backfill
    with pytest.raises(NotImplementedError):
        backfill.process({"id": "x"})
