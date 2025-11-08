#!/usr/bin/env python3
"""
gmail_imap_delete.py

Carefully delete Gmail messages over IMAP in small batches without hammering the server.

Features
- Proper IMAP quoting and modified UTF-7 handling of mailbox names.
- Small batch UID deletion with pauses and exponential backoff.
- Safe processing order: labels first -> All Mail -> Trash/Spam purge.
- Dry-run by default; requires explicit destructive confirmation flag.
- Include/exclude mailbox filters.
- Optional XOAUTH2 support (if you already have an access token).
- Structured logging and graceful shutdown.

Usage (App Password recommended):
  python gmail_imap_delete.py --user you@gmail.com --password YOUR_APP_PASSWORD --dry-run true
  python gmail_imap_delete.py --user you@gmail.com --password YOUR_APP_PASSWORD \
      --dry-run false --i-understand-this-deletes-mail

Usage (XOAUTH2: you supply an access token):
  python gmail_imap_delete.py --user you@gmail.com --xoauth2-access-token ya29.a0Af... --dry-run true
"""

from __future__ import annotations

import argparse
import imaplib
import logging
import os
import re
import signal
import socket
import sys
import time
from typing import Iterable, List, Sequence, Tuple, Optional, Set

# ---- Constants --------------------------------------------------------------

GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT_SSL = 993

SPECIAL_FLAG_MAP = {
    br'\All': 'all',
    br'\Trash': 'trash',
    br'\Junk': 'spam',
    br'\Spam': 'spam',
}

MAILBOX_LINE_RE = re.compile(
    rb'^\((?P<flags>[^)]*)\)\s+"(?P<sep>[^"]+)"\s+(?P<name>.+)$'
)

DEFAULT_BATCH_SIZE = 50
DEFAULT_PAUSE = 0.5
MAX_RETRIES = 5
BASE_BACKOFF = 0.8
BACKOFF_JITTER = 0.25  # seconds

# global stop flag for graceful shutdown
STOP_REQUESTED = False


# ---- Utilities --------------------------------------------------------------

def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def chunked(seq: Sequence[bytes], size: int) -> Iterable[Sequence[bytes]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def uid_str(uids: Sequence[bytes]) -> str:
    return ",".join(u.decode() if isinstance(u, (bytes, bytearray)) else str(u) for u in uids)


def imap_quote_mailbox(name: str | bytes) -> str:
    """
    Quote and escape a mailbox name for IMAP commands.
    Input may be bytes (modified UTF-7) or str; output is a quoted str.
    """
    if isinstance(name, (bytes, bytearray)):
        # Gmail LIST returns modified UTF-7; decode to str
        name = name.decode('imap4-utf-7', errors='strict')
    # Escape backslash and double-quote per RFC
    name = name.replace('\\', '\\\\').replace('"', r'\"')
    return f'"{name}"'


def parse_list_line(raw: bytes) -> Tuple[Set[bytes], bytes, bytes]:
    """
    Parse IMAP LIST raw line: returns (flags, delimiter, name) as bytes.
    """
    m = MAILBOX_LINE_RE.match(raw)
    if not m:
        # Fall back to a lightweight parse
        return set(), b'/', raw.strip().strip(b'"')
    flags = set(m.group('flags').split())
    name = m.group('name').strip()
    if name.startswith(b'"') and name.endswith(b'"'):
        name = name[1:-1]
    return flags, m.group('sep'), name


def classify_mailbox(flags: Set[bytes]) -> Optional[str]:
    for f in flags:
        if f in SPECIAL_FLAG_MAP:
            return SPECIAL_FLAG_MAP[f]
    return None


def backoff_sleep(attempt: int, base: float = BASE_BACKOFF) -> None:
    # jittered exponential backoff
    delay = (base * (2 ** attempt)) + (BACKOFF_JITTER * attempt)
    time.sleep(delay)


def install_signal_handlers():
    def _handler(signum, frame):
        global STOP_REQUESTED
        STOP_REQUESTED = True
        logging.warning("Stop requested (signal %s). Finishing current batch, then exiting…", signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except Exception:
            pass  # Not all platforms support all signals


# ---- IMAP helpers with retries ---------------------------------------------

def imap_call_with_retry(M: imaplib.IMAP4_SSL, cmd: str, *args, max_retries: int = MAX_RETRIES):
    """
    Call IMAP command with retries on transient errors.
    Returns (typ, data). Raises on persistent failure.
    """
    for attempt in range(max_retries + 1):
        try:
            logging.debug("IMAP %s %s", cmd, " ".join(map(str, args)))
            method = getattr(M, cmd)
            typ, data = method(*args)
            if typ == 'OK':
                return typ, data
            logging.warning("IMAP %s returned %s; data=%s", cmd, typ, data)
        except (imaplib.IMAP4.abort, socket.timeout, OSError) as e:
            logging.warning("IMAP %s exception: %s", cmd, e)

        if attempt < max_retries:
            backoff_sleep(attempt)
        else:
            raise imaplib.IMAP4.error(f"{cmd} failed after retries")


def imap_uid_with_retry(M: imaplib.IMAP4_SSL, *args, max_retries: int = MAX_RETRIES):
    for attempt in range(max_retries + 1):
        try:
            logging.debug("IMAP UID %s", " ".join(map(str, args)))
            typ, data = M.uid(*args)
            if typ == 'OK':
                return typ, data
            logging.warning("IMAP UID returned %s; data=%s", typ, data)
        except (imaplib.IMAP4.abort, socket.timeout, OSError) as e:
            logging.warning("IMAP UID exception: %s", e)

        if attempt < max_retries:
            backoff_sleep(attempt)
        else:
            raise imaplib.IMAP4.error("UID command failed after retries")


# ---- Core deletion logic ----------------------------------------------------

def delete_in_mailbox(
    M: imaplib.IMAP4_SSL,
    mailbox_name_bytes: bytes | str,
    batch_size: int,
    dry_run: bool,
    pause: float,
    max_messages: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Select mailbox, search UIDs, and delete in batches.
    Returns (total_seen, total_deleted).
    """
    mbox_quoted = imap_quote_mailbox(mailbox_name_bytes)
    typ, _ = imap_call_with_retry(M, "select", mbox_quoted, readonly=dry_run)
    if typ != 'OK':
        logging.error("Cannot select %s", mbox_quoted)
        return 0, 0

    typ, data = imap_uid_with_retry(M, 'SEARCH', None, 'ALL')
    if typ != 'OK' or data is None:
        logging.error("Search failed in %s", mbox_quoted)
        imap_call_with_retry(M, "close")
        return 0, 0

    uids = data[0].split() if data and data[0] else []
    total = len(uids)
    if max_messages is not None:
        uids = uids[:max_messages]

    if dry_run:
        logging.info("[dry-run] %s: %d messages", safe_display_name(mailbox_name_bytes), total)
        imap_call_with_retry(M, "close")
        return total, 0

    deleted = 0
    for batch in chunked(uids, batch_size):
        if STOP_REQUESTED:
            logging.warning("Stop requested; ending early in %s", safe_display_name(mailbox_name_bytes))
            break

        us = uid_str(batch)
        typ, _ = imap_uid_with_retry(M, 'STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
        if typ != 'OK':
            logging.warning("STORE failed in %s; continuing with backoff", safe_display_name(mailbox_name_bytes))
            time.sleep(max(pause * 2, 2.0))
            continue

        typ, _ = imap_call_with_retry(M, 'expunge')
        if typ != 'OK':
            logging.warning("EXPUNGE failed in %s; continuing with backoff", safe_display_name(mailbox_name_bytes))
            time.sleep(max(pause * 2, 2.0))
            continue

        deleted += len(batch)
        logging.info("[%s] deleted %d / %d", safe_display_name(mailbox_name_bytes), deleted, total)
        time.sleep(pause)

    imap_call_with_retry(M, "close")
    logging.info("[done] %s: deleted %d/%d", safe_display_name(mailbox_name_bytes), deleted, total)
    return total, deleted


def safe_display_name(name: bytes | str) -> str:
    try:
        if isinstance(name, (bytes, bytearray)):
            return name.decode('imap4-utf-7')
        return name
    except Exception:
        return str(name)


# ---- Authentication ---------------------------------------------------------

def imap_login(
    user: str,
    password: Optional[str],
    xoauth2_access_token: Optional[str],
    server: str,
    port: int,
    timeout: float,
) -> imaplib.IMAP4_SSL:
    socket.setdefaulttimeout(timeout)
    M = imaplib.IMAP4_SSL(server, port)
    if xoauth2_access_token:
        # XOAUTH2 flow where you already possess a valid access token.
        # Construct SASL XOAUTH2 string as documented by Google.
        auth_string = f'user={user}\x01auth=Bearer {xoauth2_access_token}\x01\x01'

        def _auth_cb(response):
            return auth_string.encode()

        typ, data = M.authenticate('XOAUTH2', _auth_cb)
        if typ != 'OK':
            raise imaplib.IMAP4.error(f"XOAUTH2 authenticate failed: {data}")
        logging.info("Authenticated via XOAUTH2")
    else:
        if not password:
            raise ValueError("Either --password (App Password) or --xoauth2-access-token is required.")
        typ, data = M.login(user, password)
        if typ != 'OK':
            raise imaplib.IMAP4.error(f"Login failed: {data}")
        logging.info("Authenticated with password (App Password recommended).")
    return M


# ---- Mailbox discovery / ordering ------------------------------------------

def discover_mailboxes(
    M: imaplib.IMAP4_SSL,
    include_filters: List[str],
    exclude_filters: List[str],
) -> List[Tuple[Optional[str], bytes]]:
    """
    Returns list of (kind, name_bytes) where kind ∈ {None, 'all','trash','spam'}.
    """
    typ, boxes = imap_call_with_retry(M, "list")
    if typ != 'OK' or boxes is None:
        raise imaplib.IMAP4.error("Could not list mailboxes")

    mailboxes: List[Tuple[Optional[str], bytes]] = []
    for raw in boxes:
        flags, _, name = parse_list_line(raw)
        kind = classify_mailbox(flags)
        nstr = safe_display_name(name).lower()

        if include_filters and not any(sub.lower() in nstr for sub in include_filters):
            continue
        if exclude_filters and any(sub.lower() in nstr for sub in exclude_filters):
            continue

        mailboxes.append((kind, name))

    # Order:
    # safe => normal labels first, then All Mail, then Trash, then Spam
    normals = [(k, n) for (k, n) in mailboxes if k not in ('all', 'trash', 'spam')]
    all_mail = [(k, n) for (k, n) in mailboxes if k == 'all']
    trash = [(k, n) for (k, n) in mailboxes if k == 'trash']
    spam = [(k, n) for (k, n) in mailboxes if k == 'spam']

    ordered = normals + all_mail + trash + spam

    # De-dup just in case
    seen: Set[Tuple[Optional[str], bytes]] = set()
    result: List[Tuple[Optional[str], bytes]] = []
    for item in ordered:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result


# ---- Main -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Delete Gmail messages over IMAP carefully, in small batches.")
    auth = ap.add_argument_group("Authentication")
    auth.add_argument("--user", required=True, help="Gmail address")
    auth.add_argument("--password", help="Use a Google App Password (recommended).")
    auth.add_argument("--xoauth2-access-token", help="If provided, authenticate via XOAUTH2 using this access token.")

    perf = ap.add_argument_group("Performance & Safety")
    perf.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="UIDs per batch (keep small).")
    perf.add_argument("--pause", type=float, default=DEFAULT_PAUSE, help="Seconds to sleep between batches.")
    perf.add_argument("--max-messages", type=int, default=None, help="Optional cap per mailbox for testing.")
    perf.add_argument("--dry-run", type=lambda x: x.lower() in {"1", "true", "yes"}, default=True,
                      help="true/false: list counts only, no deletions.")
    perf.add_argument("--i-understand-this-deletes-mail", action="store_true",
                      help="Required to run with --dry-run false.")
    perf.add_argument("--timeout", type=float, default=60.0, help="Socket timeout in seconds.")
    perf.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging verbosity (-v, -vv).")

    filt = ap.add_argument_group("Mailbox Selection")
    filt.add_argument("--include", action="append", default=[], help="Substring filter (can repeat).")
    filt.add_argument("--exclude", action="append", default=[], help="Substring exclude (can repeat).")

    net = ap.add_argument_group("Network")
    net.add_argument("--server", default=GMAIL_IMAP_HOST)
    net.add_argument("--port", type=int, default=GMAIL_IMAP_PORT_SSL)

    args = ap.parse_args()
    setup_logging(args.verbose)
    install_signal_handlers()

    if not args.dry_run and not args.i_understand_this_deletes_mail:
        ap.error("Destructive run requires --i-understand-this-deletes-mail")

    # Connect & authenticate
    try:
        M = imap_login(
            user=args.user,
            password=args.password,
            xoauth2_access_token=args.xoauth2_access_token,
            server=args.server,
            port=args.port,
            timeout=args.timeout,
        )
    except imaplib.IMAP4.error as e:
        logging.error("Login/authentication failed: %s", e)
        sys.exit(1)

    total_seen = 0
    total_deleted = 0
    try:
        mailboxes = discover_mailboxes(M, args.include, args.exclude)
        if not mailboxes:
            logging.warning("No mailboxes matched the filters.")
        for kind, name in mailboxes:
            if STOP_REQUESTED:
                logging.warning("Stop requested; halting before mailbox %s", safe_display_name(name))
                break

            logging.info("Processing mailbox: %s (kind=%s)", safe_display_name(name), kind or "normal")
            t, d = delete_in_mailbox(
                M,
                name,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
                pause=max(0.0, args.pause),
                max_messages=args.max_messages,
            )
            total_seen += t
            total_deleted += d
            # Gentle extra pause between folders
            time.sleep(max(args.pause, 0.5))
    finally:
        try:
            M.logout()
        except Exception:
            pass

    if args.dry_run:
        print(f"[dry-run complete] Total messages seen across selected folders: {total_seen}")
    else:
        print(f"[complete] Deleted {total_deleted} messages across selected folders.")


if __name__ == "__main__":
    main()
