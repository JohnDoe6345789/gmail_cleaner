"""Logging and output functions."""
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
