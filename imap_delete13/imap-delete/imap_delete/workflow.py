"""High-level workflow functions."""
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
