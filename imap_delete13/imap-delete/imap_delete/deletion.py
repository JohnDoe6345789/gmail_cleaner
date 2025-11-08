"""Deletion operations - marking and expunging."""
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
