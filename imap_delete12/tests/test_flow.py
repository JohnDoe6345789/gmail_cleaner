import pytest

# Constants
DELETED_FLAG = "\\Deleted"

class _FakeIMAP:
    def __init__(self):
        self.closed = False
        self.logged_out = False
        self.flags = {}
        self.selected = None
    
    def select(self, mailbox):
        self.selected = mailbox
        return "OK", [b"2"]
    
    def search(self, charset, criteria):
        return "OK", [b"1 2"]
    
    def fetch(self, message_set, message_parts):
        # Return fake size data
        return "OK", [
            (b"1", {b"RFC822.SIZE": b"100"}),
            (b"2", {b"RFC822.SIZE": b"200"}),
        ]
    
    def store(self, message_set, op, flag):
        # Handle both single ID and comma-separated IDs
        if isinstance(message_set, bytes):
            ids = message_set.split(b",")
        else:
            ids = [message_set]
        
        for mid in ids:
            self.flags[mid] = (op, flag)
        return "OK", []
    
    def expunge(self):
        return "OK", []
    
    def close(self):
        self.closed = True
    
    def logout(self):
        self.logged_out = True


def _patch_imap(monkeypatch):
    import importlib
    mod = importlib.import_module(__name__)
    
    def _connect(server, port, user, password):
        return _FakeIMAP()
    
    monkeypatch.setattr(mod, "imap_connect", _connect)
    return mod


def test_discover_targets_and_summary(monkeypatch, caplog):
    mod = _patch_imap(monkeypatch)
    args = mod.build_parser().parse_args([
        "--user", "u", "--password", "p", "--dry-run",
    ])
    log = mod.setup_logger(verbose=False)
    imap = mod.imap_connect("s", 993, "u", "p")
    mod.imap_select(imap, "INBOX")
    ids, size_est = mod.discover_targets(imap, "ALL", log)
    assert ids == [b"1", b"2"]
    assert size_est == 300


def test_maybe_delete_respects_guard(monkeypatch):
    mod = _patch_imap(monkeypatch)
    args = mod.build_parser().parse_args([
        "--user", "u", "--password", "p", "--dry-run",
    ])
    log = mod.setup_logger(verbose=False)
    imap = mod.imap_connect("s", 993, "u", "p")
    mod.maybe_delete(imap, [b"1", b"2"], args, log)
    assert getattr(imap, "flags", {}) == {}


def test_maybe_delete_allows_when_confirmed(monkeypatch):
    mod = _patch_imap(monkeypatch)
    args = mod.build_parser().parse_args([
        "--user", "u", "--password", "p",
        "--i-understand-this-deletes-mail",
    ])
    log = mod.setup_logger(verbose=False)
    imap = mod.imap_connect("s", 993, "u", "p")
    mod.maybe_delete(imap, [b"1", b"2"], args, log)
    assert imap.flags.get(b"1")[1] == DELETED_FLAG
    assert imap.flags.get(b"2")[1] == DELETED_FLAG


def test_run_full_dry_flow(monkeypatch):
    mod = _patch_imap(monkeypatch)
    # Patch timers for determinism
    monkeypatch.setattr(mod, "start_timer", lambda: 0.0)
    monkeypatch.setattr(mod, "stop_timer", lambda s: 1.23)
    # Build args as dry-run
    a = ["--user", "u", "--password", "p", "--dry-run"]
    monkeypatch.setenv("PYTHONWARNINGS", "ignore")
    p = mod.build_parser()
    ns = p.parse_args(a)
    # Run pieces explicitly
    log = mod.setup_logger(False)
    imap = mod.imap_connect("s", 993, ns.user, ns.password)
    mod.prepare_mailbox(imap, ns.mailbox)
    ids, size_est = mod.discover_targets(imap, ns.query, log)
    mod.summarize_run(log, ns.mailbox, ids, size_est, 0.0, ns.dry_run)
    mod.maybe_delete(imap, ids, ns, log)
    mod.close_session(imap)
    assert imap.closed is True and imap.logged_out is True
