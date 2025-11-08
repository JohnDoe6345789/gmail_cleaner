#!/usr/bin/env python3
"""
Generate IMAP Delete Tool project structure in a single shot.
- Functions ≤10 lines each, single responsibility.
- PEP8 (≈79 cols), mypy-friendly typing.
- All explanations live as code comments.
Usage: python3 generate_project.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import sys

# ------------------------------ templates ------------------------------
# Keep long text in module-level constants so functions stay tiny.

T_README = """# IMAP Delete Tool

Graceful IMAP mailbox cleaner with batching, rate limiting, and excellent
user feedback.

## Quick Start

```bash
cd imap-delete
python3 generate_project.py  # You already did this!
pip install -r requirements.txt
python3 -m imap_delete --help
```

## Usage

```bash
# Dry run (safe, no changes)
python3 -m imap_delete --user you@gmail.com --password 'app_pass' --dry-run

# Delete emails before 2020
python3 -m imap_delete --user you@gmail.com --password 'app_pass' \
    --query 'BEFORE 1-Jan-2020' --i-understand-this-deletes-mail

# Custom server
python3 -m imap_delete --server imap.example.com --port 993 \
    --user you@example.com --password 'pass' --dry-run
```

## Run Tests

```bash
pytest -q
```

## Features

- ✅ Batch operations (50 emails at a time)
- ✅ Rate limiting (0.1s between batches)
- ✅ Automatic retries (3 attempts)
- ✅ Connection timeouts
- ✅ Progress feedback
- ✅ Dry-run mode
- ✅ Size estimation

## Gmail Setup

Use an App Password, not your regular password.
Generate one at: https://myaccount.google.com/apppasswords
"""

T_REQUIREMENTS = """pytest>=7.0.0
mypy>=1.10.0
"""

T_SETUP = """from setuptools import setup, find_packages

setup(
    name="imap-delete",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[],
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "imap-delete=imap_delete.main:main",
        ],
    },
)
"""

T_INIT = """"""IMAP Delete Tool - Graceful mailbox cleaner."""\
"""
__all__ = ["__version__"]
__version__ = "1.0.0"
"""

T_CONFIG = """"""Configuration constants."""

DEFAULT_SERVER = "imap.gmail.com"
DEFAULT_PORT = 993
DEFAULT_MAILBOX = "INBOX"
DEFAULT_QUERY = "ALL"
DELETED_FLAG = r"\\Deleted"
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 0.1
CONNECTION_TIMEOUT = 30
MAX_RETRIES = 3
"""

T_BATCH = """"""Batch helpers."""
from __future__ import annotations

from typing import List
from .config import BATCH_SIZE


def calculate_total_batches(total: int) -> int:
    """Number of batches for a total count."""
    return (total + BATCH_SIZE - 1) // BATCH_SIZE


def make_message_set(batch: List[bytes]) -> bytes:
    """IMAP message set from a batch of ids."""
    return b",".join(batch)
"""

T_DELETION = """"""Deletion operations - marking and expunging."""
from __future__ import annotations

import imaplib
import time
import logging
from typing import Iterable, List

from .config import DELETED_FLAG, BATCH_SIZE, RATE_LIMIT_DELAY, MAX_RETRIES
from .batch import calculate_total_batches, make_message_set


def _store(
    imap: imaplib.IMAP4_SSL, msg_set: bytes
) -> None:
    """Single STORE call, isolated for retry wrapper."""
    imap.store(msg_set, "+FLAGS", DELETED_FLAG)


def _retry_store(
    imap: imaplib.IMAP4_SSL, msg_set: bytes, log: logging.Logger,
    batch_num: int, total_batches: int
) -> None:
    """Retry STORE with basic backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            _store(imap, msg_set)
            return
        except (imaplib.IMAP4.abort, OSError) as err:
            if attempt == MAX_RETRIES - 1:
                log.error(
                    "✗ Batch %d/%d failed after %d attempts",
                    batch_num, total_batches, MAX_RETRIES,
                )
                raise RuntimeError(f"Failed after {MAX_RETRIES}: {err}")
            log.warning(
                "Retry %d/%d for batch %d...",
                attempt + 1, MAX_RETRIES, batch_num,
            )
            time.sleep(1)


def _mark_batch(
    imap: imaplib.IMAP4_SSL, batch: List[bytes], batch_num: int,
    total_batches: int, log: logging.Logger
) -> int:
    """Mark a single batch as deleted."""
    msg_set = make_message_set(batch)
    log.info(
        "Batch %d/%d: Marking %d messages...",
        batch_num, total_batches, len(batch),
    )
    _retry_store(imap, msg_set, log, batch_num, total_batches)
    log.info("✓ Batch %d/%d complete", batch_num, total_batches)
    return len(batch)


def imap_mark_deleted(
    imap: imaplib.IMAP4_SSL, ids: Iterable[bytes], log: logging.Logger
) -> int:
    """Mark ids as deleted in rate-limited batches."""
    id_list = list(ids)
    count = 0
    total_batches = calculate_total_batches(len(id_list))
    for bnum, i in enumerate(range(0, len(id_list), BATCH_SIZE), 1):
        batch = id_list[i:i + BATCH_SIZE]
        count += _mark_batch(imap, batch, bnum, total_batches, log)
        if i + BATCH_SIZE < len(id_list):
            time.sleep(RATE_LIMIT_DELAY)
    return count


def imap_expunge(imap: imaplib.IMAP4_SSL) -> None:
    """Expunge deleted messages."""
    try:
        imap.expunge()
    except imaplib.IMAP4.abort as err:
        raise RuntimeError(f"Expunge failed (server may be slow): {err}")
"""

T_UTILS = """"""Utility functions."""
from __future__ import annotations

import logging


def human_size(n: int) -> str:
    """Convert bytes to a compact human-readable string."""
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(n)
    for u in units:
        if size < 1024 or u == "TB":
            return f"{size:.1f}{u}"
        size /= 1024
    return f"{size:.1f}TB"


def parse_size_from_fetch(resp: bytes) -> int:
    """Parse RFC822.SIZE from IMAP fetch response; return 0 if absent."""
    try:
        txt = resp.decode(errors="ignore")
        part = txt.split("RFC822.SIZE", 1)[1]
        return int(part.strip(" )"))
    except Exception:
        return 0


def should_delete(dry_run: bool, confirmed: bool) -> bool:
    """Deletion only when not dry-run and confirmation provided."""
    return (not dry_run) and confirmed


def setup_logger(verbose: bool) -> logging.Logger:
    """Configure root logger and return named logger."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)5s %(message)s", level=level
    )
    return logging.getLogger("imap-delete")
"""

T_IMAP_OPS = """"""IMAP operations - connection and selection."""
from __future__ import annotations

import imaplib
from typing import List
from .config import CONNECTION_TIMEOUT


def imap_connect(
    server: str, port: int, user: str, password: str
) -> imaplib.IMAP4_SSL:
    """Connect and log in to IMAP server."""
    imap = imaplib.IMAP4_SSL(host=server, port=port, timeout=CONNECTION_TIMEOUT)
    imap.login(user, password)
    return imap


def imap_select(imap: imaplib.IMAP4_SSL, mailbox: str) -> None:
    """Select mailbox (rw)."""
    typ, data = imap.select(mailbox, readonly=False)
    if typ != "OK":  # keep raw server data for debugging
        raise RuntimeError(f"select {mailbox!r} failed: {data}")


def _warn_large(count: int) -> None:
    """Emit STDERR warning when result set is very large."""
    if count <= 1000:
        return
    import sys as _sys
    print(
        f"WARNING: Found {count} messages. Consider a narrower query.",
        file=_sys.stderr,
    )


def imap_search(imap: imaplib.IMAP4_SSL, query: str) -> List[bytes]:
    """Run IMAP SEARCH and return a list of ids (bytes)."""
    typ, data = imap.search(None, query)
    if typ != "OK":
        raise RuntimeError(f"search failed: {typ} {data}")
    result = data[0].split() if data and data[0] else []
    _warn_large(len(result))
    return result


def close_imap_mailbox(imap: imaplib.IMAP4_SSL) -> None:
    """Close mailbox gently; ignore errors."""
    try:
        imap.close()
    except (imaplib.IMAP4.error, OSError):
        pass


def logout_imap(imap: imaplib.IMAP4_SSL) -> None:
    """Logout gently; ignore errors."""
    try:
        imap.logout()
    except (imaplib.IMAP4.error, OSError):
        pass
"""

T_CLI = """"""Command-line argument parsing."""
from __future__ import annotations

import argparse
from .config import (
    DEFAULT_SERVER, DEFAULT_PORT, DEFAULT_MAILBOX, DEFAULT_QUERY,
)


def _add_server(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--server", default=DEFAULT_SERVER, help="IMAP host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="IMAPS port")


def _add_auth(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument("--password", required=True,
                        help="Password (app password)")


def _add_mailbox(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--mailbox", default=DEFAULT_MAILBOX,
                        help="Mailbox name")
    parser.add_argument("--query", default=DEFAULT_QUERY,
                        help="IMAP SEARCH query e.g. 'BEFORE 1-Jan-2022'")


def _add_safety(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dry-run", action="store_true", help="Report only")
    parser.add_argument(
        "--i-understand-this-deletes-mail", action="store_true",
        help="Required to permit deletion",
    )


def _add_logging(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose logs")


def build_parser() -> argparse.ArgumentParser:
    """Assembler for the CLI parser."""
    p = argparse.ArgumentParser(
        description="Delete matching messages from an IMAP mailbox.")
    _add_server(p)
    _add_auth(p)
    _add_mailbox(p)
    _add_safety(p)
    _add_logging(p)
    return p
"""

T_LOGGING = """"""Logging and output helpers."""
from __future__ import annotations

from .utils import human_size


def _line(log) -> None:
    log.info("=" * 60)


def print_header(log) -> None:
    log.info("")
    _line(log)
    log.info("IMAP DELETE TOOL")
    _line(log)
    log.info("")


def log_summary(log, mailbox: str, count: int, total: int,
                elapsed: float, dry_run: bool) -> None:
    log.info("")
    _line(log)
    log.info("SUMMARY")
    _line(log)
    log.info("Mailbox:              %s", mailbox)
    log.info("Messages matched:     %d", count)
    log.info("Estimated total size: %s", human_size(total))
    log.info("Time elapsed:         %.1fs", elapsed)
    mode = "DRY-RUN (no changes made)" if dry_run else "LIVE"
    log.info("Mode:                 %s", mode)
    _line(log)
    log.info("")


def log_deletion_start(log, count: int) -> None:
    log.info("")
    log.info("🗑️  STARTING DELETION PROCESS")
    log.info("Processing %d message(s) in batches of 50", count)
    log.info("")


def log_deletion_complete(log, count: int) -> None:
    log.info("")
    log.info("✓ All messages marked for deletion (%d total)", count)
    log.info("Running expunge (permanently removing messages)...")
    log.info("This may take a moment for large batches...")


def log_expunge_complete(log) -> None:
    log.info("✓ Expunge complete - messages permanently deleted!")
    log.info("")


def log_dry_run_info(log) -> None:
    log.info("")
    log.info("ℹ️  DRY-RUN MODE: No messages were deleted")
    log.info("   To actually delete, remove --dry-run and add:")
    log.info("   --i-understand-this-deletes-mail")
    log.info("")


def log_missing_confirmation(log) -> None:
    log.info("")
    log.info("⚠️  DELETION BLOCKED: Missing confirmation flag")
    log.info("   Add this flag to confirm deletion:")
    log.info("   --i-understand-this-deletes-mail")
    log.info("")
"""

T_SIZING = """"""Fetch and estimate sizes for a set of ids."""
from __future__ import annotations

import imaplib
from typing import Iterable
from .utils import parse_size_from_fetch


def imap_fetch_sizes(
    imap: imaplib.IMAP4_SSL, ids: Iterable[bytes], log
) -> int:
    """Fetch RFC822.SIZE for each id and sum them."""
    total = 0
    for mid in ids:
        typ, data = imap.fetch(mid, b"(RFC822.SIZE)")
        if typ == "OK" and data and isinstance(data[0], tuple):
            total += parse_size_from_fetch(data[0][1])
    return total
"""

T_WORKFLOW = """"""High-level workflow orchestration."""
from __future__ import annotations

from typing import List
from .imap_ops import imap_connect, imap_select, imap_search
from .sizing import imap_fetch_sizes
from .deletion import imap_mark_deleted, imap_expunge
from .logging import (
    log_deletion_start, log_deletion_complete, log_expunge_complete,
    log_dry_run_info, log_missing_confirmation,
)
from .utils import human_size


def do_delete_flow(imap, ids: List[bytes], log) -> None:
    log_deletion_start(log, len(ids))
    deleted = imap_mark_deleted(imap, ids, log)
    log_deletion_complete(log, deleted)
    imap_expunge(imap)
    log_expunge_complete(log)


def handle_no_deletion(args, log) -> None:
    if args.dry_run:
        log_dry_run_info(log)
    else:
        log_missing_confirmation(log)


def connect_and_auth(args, log):
    log.info("🔌 Connecting to %s:%d...", args.server, args.port)
    imap = imap_connect(args.server, args.port, args.user, args.password)
    log.info("✓ Connected and authenticated")
    return imap


def select_mailbox(imap, mailbox: str, log) -> None:
    log.info("📬 Selecting mailbox '%s'...", mailbox)
    imap_select(imap, mailbox)
    log.info("✓ Mailbox selected")


def search_messages(imap, query: str, log) -> List[bytes]:
    log.info("🔍 Searching for messages matching: %r", query)
    ids = imap_search(imap, query)
    log.info("✓ Found %d message(s)", len(ids))
    return ids


def calculate_sizes(imap, ids: List[bytes], log) -> int:
    log.info("")
    size_est = imap_fetch_sizes(imap, ids, log)
    log.info("✓ Size calculation complete: %s total", human_size(size_est))
    return size_est
"""

T_SESSION = """"""Session cleanup helpers."""
from __future__ import annotations

from .imap_ops import close_imap_mailbox, logout_imap


def close_session(imap, log) -> None:
    """Close and logout, always log intent."""
    log.info("Closing mailbox and logging out...")
    close_imap_mailbox(imap)
    logout_imap(imap)
    log.info("✓ Disconnected")
"""

T_MAIN = """"""Main entry point."""
from __future__ import annotations

import time
from .cli import build_parser
from .utils import should_delete, setup_logger
from .session import close_session
from .logging import log_summary, print_header
from .workflow import (
    connect_and_auth, select_mailbox, search_messages,
    calculate_sizes, do_delete_flow, handle_no_deletion,
)


def run() -> int:
    parser = build_parser()
    args = parser.parse_args()
    log = setup_logger(args.verbose)
    start = time.time()

    print_header(log)
    imap = connect_and_auth(args, log)

    try:
        select_mailbox(imap, args.mailbox, log)
        ids = search_messages(imap, args.query, log)
        if not ids:
            log.info("No messages to process!")
            return 0
        size_est = calculate_sizes(imap, ids, log)
        elapsed = time.time() - start
        log_summary(log, args.mailbox, len(ids), size_est, elapsed,
                    args.dry_run)
        if should_delete(args.dry_run, args.i_understand_this_deletes_mail):
            do_delete_flow(imap, ids, log)
        else:
            handle_no_deletion(args, log)
        return 0
    finally:
        close_session(imap, log)


def main() -> None:
    raise SystemExit(run())
"""

T_TESTS = """"""Test suite for IMAP delete tool."""
import pytest
from imap_delete.utils import human_size, parse_size_from_fetch, should_delete
from imap_delete.config import DELETED_FLAG


def test_should_delete_guard_dry_run() -> None:
    assert should_delete(True, True) is False
    assert should_delete(True, False) is False
    assert should_delete(False, False) is False
    assert should_delete(False, True) is True


def test_human_size_rounding() -> None:
    assert human_size(0) == "0.0B"
    assert human_size(1023) == "1023.0B"
    assert human_size(1024) == "1.0KB"
    assert human_size(1048576) == "1.0MB"


def test_parse_size_from_fetch_ok() -> None:
    resp = b"1 (RFC822.SIZE 12345)"
    assert parse_size_from_fetch(resp) == 12345


def test_parse_size_from_fetch_bad() -> None:
    assert parse_size_from_fetch(b"garbage") == 0


def test_deleted_flag_format() -> None:
    assert DELETED_FLAG == r"\\Deleted"
"""

T_GITIGNORE = """__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
.pytest_cache/
.coverage
htmlcov/
.env
venv/
ENV/
"""

T_PYPROJECT = """[tool.mypy]
python_version = "3.9"
ignore_missing_imports = true
strict = true
warn_return_any = true
warn_unused_configs = true
warn_redundant_casts = true
warn_unused_ignores = true
warn_no_return = true
warn_unreachable = true

[tool.pytest.ini_options]
addopts = "-q"
"""

# ------------------------------ generator ------------------------------

def _root() -> Path:
    """Project root path."""
    return Path("imap-delete")


def _write(path: Path, content: str) -> None:
    """Create parent dirs and write content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"✓ Created {path.as_posix()}")


def _pkg_path(*parts: str) -> Path:
    """Helper for package file paths."""
    return _root().joinpath("imap_delete", *parts)


def _tests_path(*parts: str) -> Path:
    """Helper for tests file paths."""
    return _root().joinpath("tests", *parts)


def _files() -> List[Tuple[Path, str]]:
    """All files to generate as (path, content) tuples."""
    return [
        (_root().joinpath("README.md"), T_README),
        (_root().joinpath("requirements.txt"), T_REQUIREMENTS),
        (_root().joinpath("setup.py"), T_SETUP),
        (_pkg_path("__init__.py"), T_INIT),
        (_pkg_path("config.py"), T_CONFIG),
        (_pkg_path("batch.py"), T_BATCH),
        (_pkg_path("deletion.py"), T_DELETION),
        (_pkg_path("utils.py"), T_UTILS),
        (_pkg_path("imap_ops.py"), T_IMAP_OPS),
        (_pkg_path("cli.py"), T_CLI),
        (_pkg_path("logging.py"), T_LOGGING),
        (_pkg_path("sizing.py"), T_SIZING),
        (_pkg_path("workflow.py"), T_WORKFLOW),
        (_pkg_path("session.py"), T_SESSION),
        (_pkg_path("main.py"), T_MAIN),
        (_tests_path("__init__.py"), ""),
        (_tests_path("test_imap_delete.py"), T_TESTS),
        (_root().joinpath(".gitignore"), T_GITIGNORE),
        (_root().joinpath("pyproject.toml"), T_PYPROJECT),
    ]


def generate_project() -> None:
    """Write the entire project tree."""
    for path, content in _files():
        _write(path, content)
    print("\n" + "=" * 60)
    print("✓ Project structure generated successfully!")
    print("=" * 60)
    print("\nNext steps:")
    print("  cd imap-delete")
    print("  pip install -r requirements.txt")
    print("  python3 -m imap_delete --help")
    print("  pytest")


def main() -> None:
    """CLI entry: generate on invocation."""
    generate_project()


if __name__ == "__main__":
    main()
