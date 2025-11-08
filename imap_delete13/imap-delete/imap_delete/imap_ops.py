"""IMAP operations - connection and selection."""
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
