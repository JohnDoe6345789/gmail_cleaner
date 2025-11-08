#!/usr/bin/env python3
"""
imap_delete_strict.py

IMAP mailbox cleaner with strict small functions (<=10 LOC), full PEP 8,
clear dry-run semantics, and explicit confirmation flag for deletion.

GETTING STARTED:
    1. Install dependencies:
       python3 imap_delete_strict.py --install-deps

    2. Run tests to verify installation:
       python3 imap_delete_strict.py --test

    3. Dry-run to see what would be deleted:
       python3 imap_delete_strict.py --user YOUR_EMAIL --password YOUR_PASSWORD --dry-run

    4. Actually delete (requires confirmation flag):
       python3 imap_delete_strict.py --user YOUR_EMAIL --password YOUR_PASSWORD \\
           --i-understand-this-deletes-mail

EXAMPLES:
    # Delete emails before a certain date
    python3 imap_delete_strict.py --user me@gmail.com --password 'app_password' \\
        --query 'BEFORE 1-Jan-2020' --dry-run

    # Delete from a specific folder
    python3 imap_delete_strict.py --user me@gmail.com --password 'app_password' \\
        --mailbox 'Spam' --i-understand-this-deletes-mail

    # Custom IMAP server
    python3 imap_delete_strict.py --server imap.example.com --port 993 \\
        --user me@example.com --password 'pass' --dry-run

NOTE: For Gmail, use an App Password, not your regular password.
      Generate one at: https://myaccount.google.com/apppasswords
"""
from __future__ import annotations

import argparse
import imaplib
import logging
import time
from typing import Callable, Iterable, List, Tuple


# ------------------------------ Constants ---------------------------------
DEFAULT_SERVER = "imap.gmail.com"
DEFAULT_PORT = 993
DEFAULT_MAILBOX = "INBOX"
DEFAULT_QUERY = "ALL"
DELETED_FLAG = r"\Deleted"


# ------------------------------ Args --------------------------------------


class ArgBuilder:
    """Creates argparse parser using add_* methods discovered on self."""

    def __init__(self) -> None:
        self.parser = argparse.ArgumentParser(
            description=(
                "Delete matching messages from an IMAP mailbox."
            )
        )

    # ---- individual adders (<=10 LOC each) ----
    def add_server(self) -> None:
        self.parser.add_argument(
            "--server", default=DEFAULT_SERVER, help="IMAP server host"
        )

    def add_port(self) -> None:
        self.parser.add_argument(
            "--port", type=int, default=DEFAULT_PORT, help="IMAPS port"
        )

    def add_user(self) -> None:
        self.parser.add_argument("--user", required=True, help="Username")

    def add_password(self) -> None:
        self.parser.add_argument(
            "--password", required=True, help="Password (use app password)"
        )

    def add_mailbox(self) -> None:
        self.parser.add_argument(
            "--mailbox", default=DEFAULT_MAILBOX, help="Mailbox name"
        )

    def add_query(self) -> None:
        self.parser.add_argument(
            "--query", default=DEFAULT_QUERY,
            help="IMAP SEARCH query, e.g. 'BEFORE 1-Jan-2022'",
        )

    def add_dry_run(self) -> None:
        self.parser.add_argument(
            "--dry-run", action="store_true", help="Report only"
        )

    def add_confirm(self) -> None:
        self.parser.add_argument(
            "--i-understand-this-deletes-mail", action="store_true",
            help="Required to permit deletion",
        )

    def add_verbose(self) -> None:
        self.parser.add_argument(
            "-v", "--verbose", action="store_true", help="Verbose logs"
        )

    def add_test(self) -> None:
        self.parser.add_argument(
            "--test", action="store_true", help="Run test suite"
        )

    def add_install_deps(self) -> None:
        self.parser.add_argument(
            "--install-deps", action="store_true",
            help="Install required pip dependencies"
        )

    # ---- assembly helpers ----
    def _adder_methods(self) -> List[Callable[[], None]]:
        return [
            getattr(self, name) for name in dir(self)
            if name.startswith("add_") and callable(getattr(self, name))
        ]

    def build(self) -> argparse.ArgumentParser:
        for fn in self._adder_methods():
            fn()
        return self.parser


def build_parser() -> argparse.ArgumentParser:
    return ArgBuilder().build()


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


# ------------------------------ Logging -----------------------------------


def logger_level(verbose: bool) -> int:
    return logging.DEBUG if verbose else logging.INFO


def setup_logger(verbose: bool) -> logging.Logger:
    logging.basicConfig(
        format="%(asctime)s %(levelname)5s %(message)s",
        level=logger_level(verbose),
    )
    return logging.getLogger("imap-delete")


# ------------------------------ IMAP Ops ----------------------------------


def imap_connect(
    server: str, port: int, user: str, password: str
) -> imaplib.IMAP4_SSL:
    imap = imaplib.IMAP4_SSL(host=server, port=port)
    imap.login(user, password)
    return imap


def imap_select(imap: imaplib.IMAP4_SSL, mailbox: str) -> None:
    typ, data = imap.select(mailbox, readonly=False)
    if typ != "OK":
        raise RuntimeError(f"select {mailbox!r} failed: {data}")


def imap_search(imap: imaplib.IMAP4_SSL, query: str) -> List[bytes]:
    typ, data = imap.search(None, query)
    if typ != "OK":
        raise RuntimeError(f"search failed: {typ} {data}")
    return data[0].split() if data and data[0] else []


def imap_mark_deleted(imap: imaplib.IMAP4_SSL, ids: Iterable[bytes]) -> int:
    count = 0
    for mid in ids:
        imap.store(mid, "+FLAGS", DELETED_FLAG)
        count += 1
    return count


def imap_expunge(imap: imaplib.IMAP4_SSL) -> None:
    imap.expunge()


# ------------------------------ Size Estimation ---------------------------


def parse_size_from_fetch(resp: bytes) -> int:
    try:
        txt = resp.decode(errors="ignore")
        part = txt.split("RFC822.SIZE", 1)[1]
        return int(part.strip(" )"))
    except Exception:
        return 0


def imap_fetch_sizes(imap: imaplib.IMAP4_SSL, ids: Iterable[bytes]) -> int:
    total = 0
    for mid in ids:
        typ, data = imap.fetch(mid, "(RFC822.SIZE)")
        if typ == "OK" and data and data[0]:
            total += parse_size_from_fetch(data[0])
    return total


# ------------------------------ Safety ------------------------------------


def should_delete(dry_run: bool, confirmed: bool) -> bool:
    if dry_run:
        return False
    return confirmed


# ------------------------------ Reporting ---------------------------------


def human_size(n: int) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n //= 1024
    return f"{n:.1f}TB"


def log_summary(
    log: logging.Logger,
    mailbox: str,
    count: int,
    total_bytes: int,
    elapsed: float,
    dry_run: bool,
) -> None:
    log.info("Mailbox: %s", mailbox)
    log.info("Messages matched: %d", count)
    log.info("Estimated total size: %s", human_size(total_bytes))
    log.info("Elapsed: %.1fs", elapsed)
    log.info("Dry-run: %s", dry_run)


# ------------------------------ Flow --------------------------------------


def do_delete_flow(
    imap: imaplib.IMAP4_SSL, ids: List[bytes], log: logging.Logger
) -> None:
    deleted = imap_mark_deleted(imap, ids)
    log.info("Marked %d message(s) for deletion", deleted)
    imap_expunge(imap)
    log.info("Expunge complete")


def start_timer() -> float:
    return time.time()


def stop_timer(start: float) -> float:
    return time.time() - start


def make_context() -> tuple:
    args = parse_args()
    log = setup_logger(args.verbose)
    return args, log, start_timer()


def open_imap_session(args: argparse.Namespace) -> imaplib.IMAP4_SSL:
    return imap_connect(args.server, args.port, args.user, args.password)


def prepare_mailbox(imap: imaplib.IMAP4_SSL, mailbox: str) -> None:
    imap_select(imap, mailbox)


def discover_targets(
    imap: imaplib.IMAP4_SSL, query: str, log: logging.Logger
) -> Tuple[List[bytes], int]:
    ids = imap_search(imap, query)
    log.info("Found %d message(s) for %r", len(ids), query)
    size_est = imap_fetch_sizes(imap, ids)
    return ids, size_est


def summarize_run(
    log: logging.Logger,
    mailbox: str,
    ids: List[bytes],
    size_est: int,
    start: float,
    dry: bool,
) -> None:
    elapsed = stop_timer(start)
    log_summary(log, mailbox, len(ids), size_est, elapsed, dry)


def maybe_delete(
    imap: imaplib.IMAP4_SSL,
    ids: List[bytes],
    args: argparse.Namespace,
    log: logging.Logger,
) -> None:
    if not ids:
        return
    if should_delete(args.dry_run, args.i_understand_this_deletes_mail):
        do_delete_flow(imap, ids, log)
    else:
        log.info("Not deleting (dry-run or no confirmation)")


def close_session(imap: imaplib.IMAP4_SSL) -> None:
    try:
        imap.close()
    except (imaplib.IMAP4.error, OSError):
        pass
    try:
        imap.logout()
    except (imaplib.IMAP4.error, OSError):
        pass


# ------------------------------ Testing -----------------------------------


def install_dependencies() -> int:
    """Install required pip packages."""
    import subprocess
    import sys
    
    packages = ["pytest"]
    
    print("Installing dependencies...")
    for pkg in packages:
        print(f"  - {pkg}")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pkg],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError:
            print(f"Failed to install {pkg}")
            return 1
    
    print("✓ All dependencies installed successfully!")
    return 0


def run_tests() -> int:
    """Run built-in test suite."""
    try:
        import pytest
    except ImportError:
        print("pytest not installed. Install with: pip install pytest")
        return 1
    
    # Run pytest programmatically on this file
    import sys
    return pytest.main([__file__, "-v"])


# ------------------------------ Test Suite --------------------------------


class _FakeIMAP:
    """Mock IMAP for testing."""
    
    def __init__(self):
        self.closed = False
        self.logged_out = False
        self.flags = {}
        self.selected = None
    
    def select(self, mailbox, readonly=False):
        self.selected = mailbox
        return "OK", [b"2"]
    
    def search(self, charset, criteria):
        return "OK", [b"1 2"]
    
    def fetch(self, message_set, message_parts):
        return "OK", [
            (b"1 (RFC822.SIZE 100)", None),
            (b"2 (RFC822.SIZE 200)", None),
        ]
    
    def store(self, message_set, op, flag):
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


def test_should_delete_guard_dry_run():
    assert should_delete(True, True) is False
    assert should_delete(True, False) is False
    assert should_delete(False, False) is False
    assert should_delete(False, True) is True


def test_human_size_rounding():
    assert human_size(0) == "0.0B"
    assert human_size(1023) == "1023.0B"
    assert human_size(1024) == "1.0KB"


def test_parse_size_from_fetch_ok():
    resp = b"1 (RFC822.SIZE 12345)"
    assert parse_size_from_fetch(resp) == 12345


def test_parse_size_from_fetch_bad():
    assert parse_size_from_fetch(b"garbage") == 0


def test_argbuilder_discovers_add_methods():
    ab = ArgBuilder()
    adders = ab._adder_methods()
    names = {fn.__name__ for fn in adders}
    assert "add_server" in names
    assert "add_verbose" in names
    assert "add_test" in names


def test_parser_parses_core_flags():
    p = build_parser()
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


def run() -> int:
    args, log, start = make_context()
    
    # Handle install-deps mode
    if args.install_deps:
        return install_dependencies()
    
    # Handle test mode
    if args.test:
        return run_tests()
    
    imap = open_imap_session(args)
    try:
        prepare_mailbox(imap, args.mailbox)
        ids, size_est = discover_targets(imap, args.query, log)
        summarize_run(log, args.mailbox, ids, size_est, start, args.dry_run)
        maybe_delete(imap, ids, args, log)
        return 0
    finally:
        close_session(imap)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
