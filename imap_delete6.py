#!/usr/bin/env python3
"""
gmail_imap_delete_backwards.py

Delete Gmail messages over IMAP in small batches, starting from TODAY and moving
BACKWARD in time, until each mailbox is EMPTY.

Design:
- Windowed UID searches (e.g., SINCE <start> BEFORE <end>) in reverse, newest→older.
- After every window, re-check the mailbox EXISTS count; when it reaches 0, stop.
- Dry-run by default; destructive runs require --i-understand-this-deletes-mail.
- Safe order: normal labels -> All Mail -> Trash -> Spam.
- Retries with backoff, batch-sized deletes, and gentle pauses.
- Avoids imaplib's 1,000,000-byte response limit by never using UID SEARCH ALL.

Examples:
  Dry run:
    python gmail_imap_delete_backwards.py --user you@gmail.com --password APP_PASSWORD --dry-run true -v

  Destructive (weekly windows):
    python gmail_imap_delete_backwards.py --user you@gmail.com --password APP_PASSWORD \
      --dry-run false --i-understand-this-deletes-mail --batch-size 50 --pause 0.5 -v

  Destructive (monthly windows) on Inbox only:
    python gmail_imap_delete_backwards.py --user you@gmail.com --password APP_PASSWORD \
      --include inbox --window months:1 \
      --dry-run false --i-understand-this-deletes-mail -v
"""

import argparse
import datetime as dt
import imaplib
import logging
import re
import signal
import socket
import sys
import time
from typing import Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union

# -------------------- Constants --------------------

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

# Limits to avoid infinite backfill loops if something goes odd:
DEFAULT_MAX_WINDOWS = 2000   # plenty for ~38 years of weeks
DEFAULT_MAX_YEARS_BACK = 30  # hard stop

STOP_REQUESTED = False

# -------------------- Logging ---------------------

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

# -------------------- Helpers ---------------------

def chunked(seq: Sequence[bytes], size: int) -> Iterable[Sequence[bytes]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def uid_str(uids: Sequence[bytes]) -> str:
    return ",".join(u.decode() if isinstance(u, (bytes, bytearray)) else str(u) for u in uids)

def imap_quote_mailbox(name: Union[bytes, str]) -> str:
    """
    Safely quote a mailbox for IMAP SELECT.
    If 'name' is bytes (as returned by LIST), keep raw bytes; if str, encode.
    Escape backslashes and double quotes, then wrap in quotes.
    """
    if isinstance(name, (bytes, bytearray)):
        b = bytes(name)
    else:
        b = name.encode('utf-8', errors='backslashreplace')
    b = b.replace(b'\\', b'\\\\').replace(b'"', b'\\"')
    # imaplib wants str; latin-1 round-trips bytes 1:1
    return '"' + b.decode('latin-1') + '"'

def safe_display_name(name: Union[bytes, str]) -> str:
    if isinstance(name, (bytes, bytearray)):
        return name.decode('ascii', errors='backslashreplace')
    return name

def parse_list_line(raw: bytes) -> Tuple[Set[bytes], bytes, bytes]:
    """
    Parse IMAP LIST raw line: returns (flags, delimiter, name) as bytes.
    """
    m = MAILBOX_LINE_RE.match(raw)
    if not m:
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

def backoff_sleep(attempt: int) -> None:
    delay = (BASE_BACKOFF * (2 ** attempt)) + (BACKOFF_JITTER * attempt)
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
            pass

# --------- IMAP wrappers with retries -----------

def imap_call_with_retry(M: imaplib.IMAP4_SSL, cmd: str, *args, max_retries: int = MAX_RETRIES, **kwargs):
    """
    Generic IMAP call retry wrapper.
    Supports keyword args so we can use SELECT(..., readonly=True).
    """
    for attempt in range(max_retries + 1):
        try:
            logging.debug("IMAP %s %s %s", cmd, " ".join(map(str, args)), kwargs if kwargs else "")
            method = getattr(M, cmd)
            typ, data = method(*args, **kwargs)
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

# --------------- Date utilities (backwards windows) ------------------

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

def imap_date(d: dt.date) -> str:
    """Return IMAP date literal like 01-Jan-2024 (English month names)."""
    return f"{d.day:02d}-{MONTH_NAMES[d.month-1]}-{d.year}"

def iter_day_windows_backward(end_exclusive: dt.date, span_days: int, hard_stop: dt.date,
                              max_windows: int) -> Iterator[Tuple[dt.date, dt.date]]:
    """
    Yield windows [start, end) going backward by span_days, starting from end_exclusive.
    Stops at hard_stop (inclusive) or max_windows.
    """
    produced = 0
    end = end_exclusive
    while produced < max_windows and end > hard_stop:
        start = end - dt.timedelta(days=span_days)
        if start < hard_stop:
            start = hard_stop
        yield (start, end)
        end = start
        produced += 1

def first_of_month(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)

def month_add(d: dt.date, months: int) -> dt.date:
    """Add (or subtract if negative) months, clamped to the 1st of the month."""
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return dt.date(y, m, 1)

def iter_month_windows_backward(end_exclusive: dt.date, months_per_window: int,
                                hard_stop: dt.date, max_windows: int) -> Iterator[Tuple[dt.date, dt.date]]:
    """
    Yield month windows [start, end) going backward, aligned to month boundaries.
    The first window captures the partial current month up to end_exclusive.
    """
    produced = 0
    # Start with a partial month (from the 1st of current month)
    cur_end = end_exclusive
    cur_start = first_of_month(cur_end)
    while produced < max_windows and cur_end > hard_stop:
        start = max(cur_start, hard_stop)
        yield (start, cur_end)
        produced += 1
        # Step back by N whole months
        cur_end = first_of_month(cur_start)  # previous boundary
        cur_start = month_add(cur_end, -months_per_window)

# --------------- Core deletion (backwards) ------------------

def search_uids_in_window(
    M: imaplib.IMAP4_SSL,
    start: dt.date,
    end: dt.date,
) -> List[bytes]:
    """
    Run a windowed UID SEARCH: SINCE start BEFORE end.
    Returns a list of UID bytes for that window.
    """
    since_s = imap_date(start)
    before_s = imap_date(end)
    logging.debug("SEARCH window SINCE %s BEFORE %s", since_s, before_s)
    typ, data = imap_uid_with_retry(M, 'SEARCH', None, 'SINCE', since_s, 'BEFORE', before_s)
    if typ != 'OK' or not data or data[0] is None:
        return []
    return data[0].split()

def select_and_get_exists(M: imaplib.IMAP4_SSL, mbox_quoted: str, readonly: bool) -> int:
    """SELECT (or read-only) and return EXISTS count."""
    typ, data = imap_call_with_retry(M, "select", mbox_quoted, readonly=readonly)
    if typ != 'OK' or not data:
        return 0
    try:
        return int(data[0])
    except Exception:
        # Some servers return [b'123'] or similar; be defensive.
        try:
            return int(re.findall(rb'\d+', data[0])[0])  # type: ignore[index]
        except Exception:
            return 0

def delete_in_mailbox(
    M: imaplib.IMAP4_SSL,
    mailbox_name: Union[bytes, str],
    batch_size: int,
    dry_run: bool,
    pause: float,
    window_kind: str,
    window_size: int,
    max_windows: int,
    hard_stop_years: int,
) -> Tuple[int, int]:
    """
    Process a mailbox starting from TODAY backwards until it's empty (EXISTS == 0)
    or hard stop is reached. Returns (total_seen, total_deleted) across all windows.
    """
    mbox_quoted = imap_quote_mailbox(mailbox_name)

    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)  # BEFORE tomorrow includes today's mail
    hard_stop_date = dt.date(max(1970, today.year - hard_stop_years), 1, 1)

    # initial exists (read-only so we don't alter anything on dry-run)
    exists = select_and_get_exists(M, mbox_quoted, readonly=True)
    logging.info("[%s] initial messages: %d", safe_display_name(mailbox_name), exists)
    if exists == 0:
        imap_call_with_retry(M, "close")
        return 0, 0

    total_seen = 0
    total_deleted = 0

    # Build backward windows iterator
    if window_kind == "months":
        windows_iter = iter_month_windows_backward(tomorrow, window_size, hard_stop_date, max_windows)
    else:
        windows_iter = iter_day_windows_backward(tomorrow, window_size, hard_stop_date, max_windows)

    # For destructive runs, re-open read-write once at start
    if not dry_run:
        imap_call_with_retry(M, "select", mbox_quoted, readonly=False)

    for start, end in windows_iter:
        if STOP_REQUESTED:
            logging.warning("Stop requested; ending early in %s", safe_display_name(mailbox_name))
            break

        # Find UIDs in this backward window
        uids = search_uids_in_window(M, start, end)
        count = len(uids)
        total_seen += count

        if dry_run:
            logging.info("[dry-run] %s: %4d messages in %s..%s",
                         safe_display_name(mailbox_name), count, start.isoformat(), end.isoformat())
        else:
            # Delete in small batches
            for batch in chunked(uids, batch_size):
                if STOP_REQUESTED:
                    logging.warning("Stop requested mid-batch in %s", safe_display_name(mailbox_name))
                    break
                if not batch:
                    continue
                us = uid_str(batch)
                typ, _ = imap_uid_with_retry(M, 'STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
                if typ != 'OK':
                    logging.warning("STORE failed in %s; backing off and continuing", safe_display_name(mailbox_name))
                    time.sleep(max(pause * 2, 2.0))
                    continue
                typ, _ = imap_call_with_retry(M, 'expunge')
                if typ != 'OK':
                    logging.warning("EXPUNGE failed in %s; backing off and continuing", safe_display_name(mailbox_name))
                    time.sleep(max(pause * 2, 2.0))
                    continue
                total_deleted += len(batch)
                logging.info("[%s] deleted %d (+%d) in window up to %s",
                             safe_display_name(mailbox_name), total_deleted, len(batch), end.isoformat())
                time.sleep(pause)

        # Re-check EXISTS to know if we're done (read-only to avoid surprises)
        exists = select_and_get_exists(M, mbox_quoted, readonly=True)
        logging.info("[%s] messages remaining: %d", safe_display_name(mailbox_name), exists)
        if exists == 0:
            imap_call_with_retry(M, "close")
            logging.info("[done] %s is empty", safe_display_name(mailbox_name))
            return total_seen, total_deleted

        # Re-open read-write for next window if destructive
        if not dry_run:
            imap_call_with_retry(M, "select", mbox_quoted, readonly=False)

    # Hard stop reached; close up
    imap_call_with_retry(M, "close")
    logging.info("[stop] %s reached hard stop (date or window count). Messages may remain: %d",
                 safe_display_name(mailbox_name), exists)
    return total_seen, total_deleted

# -------- Mailbox discovery / ordering ---------

def discover_mailboxes(
    M: imaplib.IMAP4_SSL,
    include_filters: List[str],
    exclude_filters: List[str],
) -> List[Tuple[Optional[str], bytes]]:
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

    normals = [(k, n) for (k, n) in mailboxes if k not in ('all', 'trash', 'spam')]
    all_mail = [(k, n) for (k, n) in mailboxes if k == 'all']
    trash = [(k, n) for (k, n) in mailboxes if k == 'trash']
    spam = [(k, n) for (k, n) in mailboxes if k == 'spam']

    ordered = normals + all_mail + trash + spam

    seen: Set[Tuple[Optional[str], bytes]] = set()
    result: List[Tuple[Optional[str], bytes]] = []
    for item in ordered:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result

# ---------------- Authentication ---------------

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

# -------------------- Main ---------------------

def main():
    ap = argparse.ArgumentParser(description="Delete Gmail messages over IMAP, newest→oldest windows until empty.")
    auth = ap.add_argument_group("Authentication")
    auth.add_argument("--user", required=True, help="Gmail address")
    auth.add_argument("--password", help="Use a Google App Password (recommended).")
    auth.add_argument("--xoauth2-access-token", help="Authenticate via XOAUTH2 using this access token.")

    perf = ap.add_argument_group("Performance & Safety")
    perf.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="UIDs per delete batch (keep small).")
    perf.add_argument("--pause", type=float, default=DEFAULT_PAUSE, help="Seconds to sleep between delete batches.")
    perf.add_argument("--dry-run", type=lambda x: x.lower() in {"1", "true", "yes"}, default=True,
                      help="true/false: list counts only, no deletions.")
    perf.add_argument("--i-understand-this-deletes-mail", action="store_true",
                      help="Required to run with --dry-run false.")
    perf.add_argument("--timeout", type=float, default=60.0, help="Socket timeout in seconds.")
    perf.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging verbosity (-v, -vv).")

    filt = ap.add_argument_group("Mailbox Selection")
    filt.add_argument("--include", action="append", default=[], help="Substring filter (can repeat).")
    filt.add_argument("--exclude", action="append", default=[], help="Substring exclude (can repeat).")

    win = ap.add_argument_group("Windowing (backwards)")
    win.add_argument("--window", default="days:7",
                     help="Backwards window, e.g. 'days:7' or 'months:1'")
    win.add_argument("--max-windows", type=int, default=DEFAULT_MAX_WINDOWS,
                     help="Safety cap on number of windows to traverse (default 2000).")
    win.add_argument("--max-years-back", type=int, default=DEFAULT_MAX_YEARS_BACK,
                     help="Hard stop going back more than N years (default 30).")

    net = ap.add_argument_group("Network")
    net.add_argument("--server", default=GMAIL_IMAP_HOST)
    net.add_argument("--port", type=int, default=GMAIL_IMAP_PORT_SSL)

    args = ap.parse_args()
    setup_logging(args.verbose)
    install_signal_handlers()

    if not args.dry_run and not args.i_understand_this_deletes_mail:
        ap.error("Destructive run requires --i-understand-this-deletes-mail")

    # Parse window spec
    try:
        kind, size_s = args.window.split(":")
        kind = kind.lower()
        wsize = int(size_s)
        if kind not in ("months", "days") or wsize < 1:
            raise ValueError
    except Exception:
        ap.error("--window must look like 'months:1' or 'days:7' with size>=1")
        return

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

        for kind_lbl, name in mailboxes:
            if STOP_REQUESTED:
                logging.warning("Stop requested; halting before mailbox %s", safe_display_name(name))
                break

            logging.info("Processing mailbox: %s (kind=%s)", safe_display_name(name), kind_lbl or "normal")
            t, d = delete_in_mailbox(
                M,
                name,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
                pause=max(0.0, args.pause),
                window_kind=kind,
                window_size=wsize,
                max_windows=max(1, args.max_windows),
                hard_stop_years=max(1, args.max_years_back),
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
        print(f"[dry-run complete] Total messages seen across selected folders (backwards): {total_seen}")
    else:
        print(f"[complete] Deleted {total_deleted} messages across selected folders (backwards).")

if __name__ == "__main__":
    main()
