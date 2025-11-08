"""Test suite for IMAP delete tool."""
import pytest
from imap_delete.utils import human_size, parse_size_from_fetch, should_delete
from imap_delete.config import DELETED_FLAG


def test_should_delete_guard_dry_run():
    assert should_delete(True, True) is False
    assert should_delete(True, False) is False
    assert should_delete(False, False) is False
    assert should_delete(False, True) is True


def test_human_size_rounding():
    assert human_size(0) == "0.0B"
    assert human_size(1023) == "1023.0B"
    assert human_size(1024) == "1.0KB"
    assert human_size(1048576) == "1.0MB"


def test_parse_size_from_fetch_ok():
    resp = b"1 (RFC822.SIZE 12345)"
    assert parse_size_from_fetch(resp) == 12345


def test_parse_size_from_fetch_bad():
    assert parse_size_from_fetch(b"garbage") == 0


def test_deleted_flag_format():
    assert DELETED_FLAG == r"\Deleted"
