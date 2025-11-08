import pytest
from importlib import import_module


def test_should_delete_guard_dry_run():
    mod = import_module(__name__)
    assert mod.should_delete(True, True) is False
    assert mod.should_delete(True, False) is False
    assert mod.should_delete(False, False) is False
    assert mod.should_delete(False, True) is True


def test_human_size_rounding():
    mod = import_module(__name__)
    assert mod.human_size(0) == "0.0B"
    assert mod.human_size(1023) == "1023.0B"
    assert mod.human_size(1024) == "1.0KB"


def test_parse_size_from_fetch_ok():
    mod = import_module(__name__)
    resp = b"1 (RFC822.SIZE 12345)"
    assert mod.parse_size_from_fetch(resp) == 12345


def test_parse_size_from_fetch_bad():
    mod = import_module(__name__)
    assert mod.parse_size_from_fetch(b"garbage") == 0
    
