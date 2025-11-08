"""Utility functions."""
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
