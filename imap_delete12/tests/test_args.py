import pytest
import types as _types
import builtins as _builtins


def _import_module_from_canvas():
    import importlib
    return importlib.import_module(__name__)


def test_argbuilder_discovers_add_methods(monkeypatch):
    mod = _import_module_from_canvas()
    ab = mod.ArgBuilder()
    adders = ab._adder_methods()
    names = {fn.__name__ for fn in adders}
    assert "add_server" in names
    assert "add_verbose" in names


def test_parser_parses_core_flags(monkeypatch):
    mod = _import_module_from_canvas()
    p = mod.build_parser()
    ns = p.parse_args([
        "--user", "u", "--password", "p", "--server", "s",
        "--port", "143", "--mailbox", "X", "--query", "ALL",
        "--dry-run", "-v",
    ])
    assert ns.user == "u"
    assert ns.password == "p"
    assert ns.server == "s"
    assert ns.port == 143
    assert ns.mailbox == "X"
    assert ns.query == "ALL"
    assert ns.dry_run is True
    assert ns.verbose is True
