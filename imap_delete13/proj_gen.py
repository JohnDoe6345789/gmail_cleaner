#!/usr/bin/env python3
"""
Generate IMAP Delete Tool project structure.
Usage: python3 generate_project.py
"""
import os
from pathlib import Path


def create_file(path: str, content: str) -> None:
    """Create a file with given content."""
    filepath = Path(path)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(content)
    print(f"✓ Created {path}")


def generate_project():
    """Generate complete project structure."""
    
    # ======================== README.md ========================
    create_file("imap-delete/README.md", '''# IMAP Delete Tool

Graceful IMAP mailbox cleaner with batching, rate limiting, and excellent user feedback.

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
python3 -m imap_delete --user you@gmail.com --password 'app_pass' \\
    --query 'BEFORE 1-Jan-2020' --i-understand-this-deletes-mail

# Custom server
python3 -m imap_delete --server imap.example.com --port 993 \\
    --user you@example.com --password 'pass' --dry-run
```

## Run Tests

```bash
pytest tests/
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
''')

    # =================== requirements.txt ===================
    create_file("imap-delete/requirements.txt", '''pytest>=7.0.0
''')

    # ====================== setup.py ========================
    create_file("imap-delete/setup.py", '''from setuptools import setup, find_packages

setup(
    name="imap-delete",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "imap-delete=imap_delete.main:main",
        ],
    },
)
''')

    # ================= imap_delete/__init__.py ================
    create_file("imap-delete/imap_delete/__init__.py", '''"""IMAP Delete Tool - Graceful mailbox cleaner."""
__version__ = "1.0.0"
''')

    # =================== imap_delete/config.py =================
    create_file("imap-delete/imap_delete/config.py", '''"""Configuration constants."""

DEFAULT_SERVER = "imap.gmail.com"
DEFAULT_PORT = 993
DEFAULT_MAILBOX = "INBOX"
DEFAULT_QUERY = "ALL"
DELETED_FLAG = r"\\Deleted"
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 0.1
CONNECTION_TIMEOUT = 30
MAX_RETRIES = 3
''')

    # ================= imap_delete/deletion.py ===================
    create_file("imap-delete/imap_delete/deletion.py", '''"""Deletion operations - marking and expunging."""
import imaplib
import time
import logging
from typing import Iterable, List

from .config import DELETED_FLAG, BATCH_SIZE, RATE_LIMIT_DELAY, MAX_RETRIES
from .batch import calculate_total_batches, make_message_set


def retry_store_operation(
    imap: imaplib.IMAP4_SSL, msg_set: bytes, log: logging.Logger,
    batch_num: int, total_batches: int
) -> None:
    """Retry STORE operation with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            imap.store(msg_set, "+FLAGS", DELETED_FLAG)
            return
        except (imaplib.IMAP4.abort, OSError) as e:
            if attempt == MAX_RETRIES - 1:
                log.error(
                    "✗ Batch %d/%d failed after %d attempts",
                    batch_num, total_batches, MAX_RETRIES
                )
                raise RuntimeError(
                    f"Failed after {MAX_RETRIES} attempts: {e}"
                )
            log.warning(
                "Retry %d/%d for batch %d...",
                attempt + 1, MAX_RETRIES, batch_num
            )
            time.sleep(1)


def mark_batch_deleted(
    imap: imaplib.IMAP4_SSL, batch: List[bytes], batch_num: int,
    total_batches: int, log: logging.Logger
) -> int:
    """Mark a single batch of messages as deleted."""
    msg_set = make_message_set(batch)
    log.info(
        "Batch %d/%d: Marking %d messages...",
        batch_num, total_batches, len(batch)
    )
    retry_store_operation(imap, msg_set, log, batch_num, total_batches)
    log.info(
        "✓ Batch %d/%d complete",
        batch_num, total_batches
    )
    return len(batch)


def imap_mark_deleted(
    imap: imaplib.IMAP4_SSL, ids: Iterable[bytes], log: logging.Logger
) -> int:
    """Mark emails as deleted in batches."""
    id_list = list(ids)
    count = 0
    total_batches = calculate_total_batches(len(id_list))
    
    for batch_num, i in enumerate(range(0, len(id_list), BATCH_SIZE), 1):
        batch = id_list[i:i + BATCH_SIZE]
        count += mark_batch_deleted(
            imap, batch, batch_num, total_batches, log
        )
        if i + BATCH_SIZE < len(id_list):
            time.sleep(RATE_LIMIT_DELAY)
    
    return count


def imap_expunge(imap: imaplib.IMAP4_SSL) -> None:
    """Expunge deleted messages."""
    try:
        imap.expunge()
    except imaplib.IMAP4.abort as e:
        raise RuntimeError(f"Expunge failed (server may be slow): {e}")
''')
    create_file("imap-delete/imap_delete/utils.py", '''"""Utility functions."""
import logging


def human_size(n: int) -> str:
    """Convert bytes to human-readable format."""
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n //= 1024
    return f"{n:.1f}TB"


def parse_size_from_fetch(resp: bytes) -> int:
    """Parse RFC822.SIZE from IMAP fetch response."""
    try:
        txt = resp.decode(errors="ignore")
        part = txt.split("RFC822.SIZE", 1)[1]
        return int(part.strip(" )"))
    except Exception:
        return 0


def should_delete(dry_run: bool, confirmed: bool) -> bool:
    """Determine if deletion should proceed."""
    if dry_run:
        return False
    return confirmed


def setup_logger(verbose: bool) -> logging.Logger:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)5s %(message)s",
        level=level,
    )
    return logging.getLogger("imap-delete")
''')

    # ================== imap_delete/imap_ops.py ==================
    create_file("imap-delete/imap_delete/imap_ops.py", '''"""IMAP operations - connection and selection."""
import imaplib
from typing import List

from .config import CONNECTION_TIMEOUT


def imap_connect(
    server: str, port: int, user: str, password: str
) -> imaplib.IMAP4_SSL:
    """Connect to IMAP server."""
    imap = imaplib.IMAP4_SSL(
        host=server, port=port, timeout=CONNECTION_TIMEOUT
    )
    imap.login(user, password)
    return imap


def imap_select(imap: imaplib.IMAP4_SSL, mailbox: str) -> None:
    """Select IMAP mailbox."""
    typ, data = imap.select(mailbox, readonly=False)
    if typ != "OK":
        raise RuntimeError(f"select {mailbox!r} failed: {data}")


def warn_large_result_set(count: int) -> None:
    """Warn if search returned too many results."""
    if count > 1000:
        import sys
        print(
            f"WARNING: Found {count} messages. "
            "This may take a while. Consider a more specific query.",
            file=sys.stderr
        )


def imap_search(imap: imaplib.IMAP4_SSL, query: str) -> List[bytes]:
    """Search for messages matching query."""
    typ, data = imap.search(None, query)
    if typ != "OK":
        raise RuntimeError(f"search failed: {typ} {data}")
    result = data[0].split() if data and data[0] else []
    warn_large_result_set(len(result))
    return result


def close_imap_mailbox(imap: imaplib.IMAP4_SSL) -> None:
    """Close IMAP mailbox safely."""
    try:
        imap.close()
    except (imaplib.IMAP4.error, OSError):
        pass


def logout_imap(imap: imaplib.IMAP4_SSL) -> None:
    """Logout from IMAP safely."""
    try:
        imap.logout()
    except (imaplib.IMAP4.error, OSError):
        pass
''')

    # ==================== imap_delete/cli.py =======================
    create_file("imap-delete/imap_delete/cli.py", '''"""Command-line argument parsing."""
import argparse

from .config import DEFAULT_SERVER, DEFAULT_PORT, DEFAULT_MAILBOX, DEFAULT_QUERY


def add_server_args(parser: argparse.ArgumentParser) -> None:
    """Add server connection arguments."""
    parser.add_argument(
        "--server", default=DEFAULT_SERVER, help="IMAP server host"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="IMAPS port"
    )


def add_auth_args(parser: argparse.ArgumentParser) -> None:
    """Add authentication arguments."""
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument(
        "--password", required=True, help="Password (use app password)"
    )


def add_mailbox_args(parser: argparse.ArgumentParser) -> None:
    """Add mailbox operation arguments."""
    parser.add_argument(
        "--mailbox", default=DEFAULT_MAILBOX, help="Mailbox name"
    )
    parser.add_argument(
        "--query", default=DEFAULT_QUERY,
        help="IMAP SEARCH query, e.g. 'BEFORE 1-Jan-2022'"
    )


def add_safety_args(parser: argparse.ArgumentParser) -> None:
    """Add safety and confirmation arguments."""
    parser.add_argument("--dry-run", action="store_true", help="Report only")
    parser.add_argument(
        "--i-understand-this-deletes-mail", action="store_true",
        help="Required to permit deletion"
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    """Add logging arguments."""
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose logs"
    )


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser."""
    parser = argparse.ArgumentParser(
        description="Delete matching messages from an IMAP mailbox."
    )
    add_server_args(parser)
    add_auth_args(parser)
    add_mailbox_args(parser)
    add_safety_args(parser)
    add_logging_args(parser)
    return parser
''')

    # =================== imap_delete/logging.py ====================
    create_file("imap-delete/imap_delete/logging.py", '''"""Logging and output functions."""
import logging

from .utils import human_size


def print_summary_header(log) -> None:
    """Print summary section header."""
    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)


def print_summary_footer(log) -> None:
    """Print summary section footer."""
    log.info("=" * 60)
    log.info("")


def log_summary(
    log, mailbox: str, count: int, total_bytes: int,
    elapsed: float, dry_run: bool
) -> None:
    """Print summary of operation."""
    print_summary_header(log)
    log.info("Mailbox:              %s", mailbox)
    log.info("Messages matched:     %d", count)
    log.info("Estimated total size: %s", human_size(total_bytes))
    log.info("Time elapsed:         %.1fs", elapsed)
    mode = "DRY-RUN (no changes made)" if dry_run else "LIVE"
    log.info("Mode:                 %s", mode)
    print_summary_footer(log)


def log_deletion_start(log, count: int) -> None:
    """Log start of deletion process."""
    log.info("")
    log.info("🗑️  STARTING DELETION PROCESS")
    log.info("Processing %d message(s) in batches of 50", count)
    log.info("")


def log_deletion_complete(log, count: int) -> None:
    """Log completion of deletion."""
    log.info("")
    log.info("✓ All messages marked for deletion (%d total)", count)
    log.info("Running expunge (permanently removing messages)...")
    log.info("This may take a moment for large batches...")


def log_expunge_complete(log) -> None:
    """Log expunge completion."""
    log.info("✓ Expunge complete - messages permanently deleted!")
    log.info("")


def log_dry_run_info(log) -> None:
    """Log dry-run mode information."""
    log.info("")
    log.info("ℹ️  DRY-RUN MODE: No messages were deleted")
    log.info("   To actually delete, remove --dry-run and add:")
    log.info("   --i-understand-this-deletes-mail")
    log.info("")


def log_missing_confirmation(log) -> None:
    """Log missing confirmation flag."""
    log.info("")
    log.info("⚠️  DELETION BLOCKED: Missing confirmation flag")
    log.info("   Add this flag to confirm deletion:")
    log.info("   --i-understand-this-deletes-mail")
    log.info("")


def print_header(log) -> None:
    """Print application header."""
    log.info("")
    log.info("=" * 60)
    log.info("IMAP DELETE TOOL")
    log.info("=" * 60)
    log.info("")
''')

    # ================== imap_delete/workflow.py ====================
    create_file("imap-delete/imap_delete/workflow.py", '''"""High-level workflow functions."""
from typing import List

from .imap_ops import imap_connect, imap_select, imap_search
from .sizing import imap_fetch_sizes
from .deletion import imap_mark_deleted, imap_expunge
from .logging import (
    log_deletion_start, log_deletion_complete, log_expunge_complete,
    log_dry_run_info, log_missing_confirmation
)
from .utils import human_size


def do_delete_flow(imap, ids: List[bytes], log) -> None:
    """Execute deletion flow."""
    log_deletion_start(log, len(ids))
    deleted = imap_mark_deleted(imap, ids, log)
    log_deletion_complete(log, deleted)
    imap_expunge(imap)
    log_expunge_complete(log)


def handle_no_deletion(args, log) -> None:
    """Handle case where deletion is not performed."""
    if args.dry_run:
        log_dry_run_info(log)
    else:
        log_missing_confirmation(log)


def connect_and_auth(args, log):
    """Connect and authenticate to IMAP server."""
    log.info("🔌 Connecting to %s:%d...", args.server, args.port)
    imap = imap_connect(args.server, args.port, args.user, args.password)
    log.info("✓ Connected and authenticated")
    return imap


def select_mailbox(imap, mailbox: str, log) -> None:
    """Select target mailbox."""
    log.info("📬 Selecting mailbox '%s'...", mailbox)
    imap_select(imap, mailbox)
    log.info("✓ Mailbox selected")


def search_messages(imap, query: str, log) -> List[bytes]:
    """Search for matching messages."""
    log.info("🔍 Searching for messages matching: %r", query)
    ids = imap_search(imap, query)
    log.info("✓ Found %d message(s)", len(ids))
    return ids


def calculate_sizes(imap, ids: List[bytes], log) -> int:
    """Calculate total size of messages."""
    log.info("")
    size_est = imap_fetch_sizes(imap, ids, log)
    log.info("✓ Size calculation complete: %s total", human_size(size_est))
    return size_est
''')

    # =================== imap_delete/main.py ====================
    create_file("imap-delete/imap_delete/main.py", '''"""Main entry point."""
import time

from .cli import build_parser
from .utils import should_delete, setup_logger
from .session import close_session
from .logging import log_summary, print_header
from .workflow import (
    connect_and_auth, select_mailbox, search_messages,
    calculate_sizes, do_delete_flow, handle_no_deletion
)


def run() -> int:
    """Main execution flow."""
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
        log_summary(
            log, args.mailbox, len(ids), size_est, elapsed, args.dry_run
        )
        
        if should_delete(args.dry_run, args.i_understand_this_deletes_mail):
            do_delete_flow(imap, ids, log)
        else:
            handle_no_deletion(args, log)
        
        return 0
    finally:
        close_session(imap, log)


def main() -> None:
    """Entry point."""
    raise SystemExit(run())


if __name__ == "__main__":
    main()
''')

    # ================== tests/__init__.py ===================
    create_file("imap-delete/tests/__init__.py", "")

    # ================ tests/test_imap_delete.py ================
    create_file("imap-delete/tests/test_imap_delete.py", '''"""Test suite for IMAP delete tool."""
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
    assert DELETED_FLAG == r"\\Deleted"
''')

    # ==================== .gitignore =======================
    create_file("imap-delete/.gitignore", '''__pycache__/
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
''')

    print("\n" + "=" * 60)
    print("✓ Project structure generated successfully!")
    print("=" * 60)
    print("\nNext steps:")
    print("  cd imap-delete")
    print("  pip install -r requirements.txt")
    print("  python3 -m imap_delete --help")
    print("  pytest tests/")
    print()


if __name__ == "__main__":
    generate_project()

