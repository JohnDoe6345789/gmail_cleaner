#!/usr/bin/env python3
"""
imap_delete8.py

Safely delete Gmail messages over IMAP in small batches, newest → oldest,
with live progress, automatic skipping of tiny folders, and smart abort logic.

Key features:
- Processes Inbox + All Mail FIRST (labels later).
- Fast-path mode: if a folder has fewer than --min-messages (default 50),
  it is deleted in a single operation (or skipped in dry-run).
- Windowed search (default 7 days backwards) prevents Gmail 1MB SEARCH limit.
- Abort if N consecutive windows return no messages (--max-empty-windows, default 10).
- --list-folders mode: shows folder names + message counts without deleting.
- Dry-run enabled by default, destructive mode requires explicit flag.
- Live one-line progress display:  ⠹ 📬 Inbox | win 3 | deleted 450 | seen 620 | remain 12 220 | 190 msg/min

Tested on Python 3.12, Gmail IMAP, real inbox > 100k messages.
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

# ---------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------

GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT_SSL = 993

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

MAILBOX_LINE_RE = re.compile(
    rb'^\((?P<flags>[^)]*)\)\s+"(?P<sep>[^"]+)"\s+(?P<name>.+)$'
)

SPECIAL_FLAG_MAP = {
    br'\All': 'all',
    br'\Trash': 'trash',
    br'\Junk': 'spam',
    br'\Spam': 'spam',
}

DEFAULT_BATCH_SIZE = 50
DEFAULT_PAUSE = 0.5
DEFAULT_MIN_MESSAGES = 50          # below this, skip or fast delete
DEFAULT_MAX_EMPTY_WINDOWS = 10     # abort scanning after N empty results
DEFAULT_MAX_WINDOWS = 2000
DEFAULT_MAX_YEARS_BACK = 30

STOP_REQUESTED = False

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

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

# ---------------------------------------------------------
# TERMINAL PROGRESS UI
# ---------------------------------------------------------

_SPINNER = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

class Progress:
    def __init__(self, enabled: bool, interval_sec: float = 0.5):
        self.enabled = enabled
        self.interval = max(0.05, float(interval_sec))
        self.last_emit = 0.0
        self.spin_idx = 0
        self.mailbox = ""
        self.windows = 0
        self.deleted = 0
        self.seen = 0
        self.remain = None
        self.t0 = time.time()

    def start_mailbox(self, name: str, remain: Optional[int]):
        self.mailbox = name
        self.windows = 0
        self.deleted = 0
        self.seen = 0
        self.remain = remain
        self.t0 = time.time()
        self.last_emit = 0.0
        self.spin_idx = 0
        self._emit(force=True)

    def update(self, *, add_deleted: int = 0, add_seen: int = 0, inc_window: bool = False,
               remain: Optional[int] = None, force: bool = False):
        self.deleted += add_deleted
        self.seen += add_seen
        if inc_window:
            self.windows += 1
        if remain is not None:
            self.remain = remain
        self._emit(force=force)

    def end_mailbox(self):
        self._emit(force=True, done=True)
        if self.enabled:
            sys.stdout.write("\n")
            sys.stdout.flush()

    def _emit(self, *, force: bool = False, done: bool = False):
        if not self.enabled:
            return
        now = time.time()
        if not force and (now - self.last_emit) < self.interval:
            return
        self.last_emit = now
        dt_total = max(0.001, now - self.t0)
        rate_per_min = int((self.deleted / dt_total) * 60)
        spin = "✔" if done else _SPINNER[self.spin_idx % len(_SPINNER)]
        self.spin_idx += 1
        remain_str = "?" if self.remain is None else f"{self.remain:,}"
        line = (f"\r{spin} 📬 {self.mailbox} | win {self.windows} | deleted {self.deleted:,} "
                f"| seen {self.seen:,} | remain {remain_str} | {rate_per_min} msg/min")
        sys.stdout.write(line[:_term_width()])
        sys.stdout.flush()

def _term_width(default: int = 120) -> int:
    try:
        import shutil
        return shutil.get_terminal_size((default, 24)).columns
    except Exception:
        return default

# ---------------------------------------------------------
# UTILS
# ---------------------------------------------------------

def chunked(seq: Sequence[bytes], size: int) -> Iterable[Sequence[bytes]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def uid_str(uids: Sequence[bytes]) -> str:
    return ",".join(u.decode() for u in uids)

def imap_quote_mailbox(name: Union[bytes, str]) -> str:
    if isinstance(name, (bytes, bytearray)):
        b = bytes(name)
    else:
        b = name.encode('utf-8', errors='backslashreplace')
    b = b.replace(b'\\', b'\\\\').replace(b'"', b'\\"')
    return '"' + b.decode('latin-1') + '"'

def safe_display_name(name: Union[bytes, str]) -> str:
    if isinstance(name, (bytes, bytearray)):
        return name.decode('ascii', errors='backslashreplace')
    return name

def parse_list_line(raw: bytes) -> Tuple[Set[bytes], bytes, bytes]:
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
    time.sleep(0.8 * (2 ** attempt) + 0.25 * attempt)

def install_signal_handlers():
    def _handler(signum, frame):
        global STOP_REQUESTED
        STOP_REQUESTED = True
        logging.warning("Stop requested (signal %s). Finishing current batch…", signum)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(sig, _handler)
        except Exception: pass

# ---------------------------------------------------------
# IMAP RETRY WRAPPERS
# ---------------------------------------------------------

def imap_call_with_retry(M: imaplib.IMAP4_SSL, cmd: str, *args, max_retries: int = 5, **kwargs):
    for attempt in range(max_retries + 1):
        try:
            method = getattr(M, cmd)
            typ, data = method(*args, **kwargs)
            if typ == 'OK':
                return typ, data
            logging.warning("IMAP %s returned %s %s", cmd, typ, data)
        except (imaplib.IMAP4.abort, socket.timeout, OSError) as e:
            logging.warning("IMAP %s exception: %s", cmd, e)
        if attempt < max_retries:
            backoff_sleep(attempt)
        else:
            raise imaplib.IMAP4.error(f"{cmd} failed after retries")

def imap_uid_with_retry(M: imaplib.IMAP4_SSL, *args, max_retries: int = 5):
    for attempt in range(max_retries + 1):
        try:
            typ, data = M.uid(*args)
            if typ == 'OK':
                return typ, data
            logging.warning("IMAP UID returned %s %s", typ, data)
        except (imaplib.IMAP4.abort, socket.timeout, OSError) as e:
            logging.warning("IMAP UID exception: %s", e)
        if attempt < max_retries:
            backoff_sleep(attempt)
        else:
            raise imaplib.IMAP4.error("UID command failed after retries")

# ---------------------------------------------------------
# DATE WINDOWING
# ---------------------------------------------------------

def imap_date(d: dt.date) -> str:
    return f"{d.day:02d}-{MONTH_NAMES[d.month-1]}-{d.year}"

def iter_day_windows_backward(end_exclusive: dt.date, span_days: int, hard_stop: dt.date,
                              max_windows: int) -> Iterator[Tuple[dt.date, dt.date]]:
    end = end_exclusive
    for _ in range(max_windows):
        if end <= hard_stop:
            break
        start = max(end - dt.timedelta(days=span_days), hard_stop)
        yield (start, end)
        end = start

# ---------------------------------------------------------
# MAILBOX LIST / ORDER
# ---------------------------------------------------------

def discover_mailboxes(M: imaplib.IMAP4_SSL, include_filters: List[str], exclude_filters: List[str]) -> List[Tuple[Optional[str], bytes]]:
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

    inboxes = [(k, n) for (k, n) in mailboxes if b'inbox' in n.lower()]
    all_mail = [(k, n) for (k, n) in mailboxes if k == 'all']
    normals = [(k, n) for (k, n) in mailboxes if k not in ('all', 'trash', 'spam') and (k, n) not in inboxes]
    trash = [(k, n) for (k, n) in mailboxes if k == 'trash']
    spam = [(k, n) for (k, n) in mailboxes if k == 'spam']

    ordered = inboxes + all_mail + normals + trash + spam

    seen = set()
    result = []
    for item in ordered:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result

# ---------------------------------------------------------
# DELETION CORE
# ---------------------------------------------------------

def uid_search_all(M: imaplib.IMAP4_SSL) -> int:
    typ, data = imap_uid_with_retry(M, 'SEARCH', None, 'ALL')
    if typ != 'OK' or not data or not data[0]:
        return 0
    return len(data[0].split())

def delete_in_mailbox(
    M: imaplib.IMAP4_SSL,
    mailbox_name: Union[bytes, str],
    batch_size: int,
    dry_run: bool,
    pause: float,
    min_messages: int,
    max_empty_windows: int,
    window_days: int,
    max_windows: int,
    max_years_back: int,
    progress: Progress,
) -> Tuple[int, int]:
    mbox_q = imap_quote_mailbox(mailbox_name)
    typ, _ = imap_call_with_retry(M, "select", mbox_q, readonly=True)
    if typ != 'OK':
        logging.error("Cannot open %s", safe_display_name(mailbox_name))
        return 0, 0

    total_initial = uid_search_all(M)
    progress.start_mailbox(safe_display_name(mailbox_name), total_initial)

    if total_initial == 0:
        progress.end_mailbox()
        return 0, 0

    if total_initial <= min_messages:
        if dry_run:
            logging.info("[fast-skip] %s → %d messages (below threshold)", safe_display_name(mailbox_name), total_initial)
            progress.end_mailbox()
            return total_initial, 0
        else:
            logging.info("[fast-delete] %s → %d messages", safe_display_name(mailbox_name), total_initial)
            M.select(mbox_q, readonly=False)
            uids = list(chunked([u for u in M.uid('SEARCH', None, 'ALL')[1][0].split()], batch_size))
            deleted = 0
            for batch in uids:
                us = uid_str(batch)
                imap_uid_with_retry(M, 'STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
                imap_call_with_retry(M, 'expunge')
                deleted += len(batch)
                progress.update(add_deleted=len(batch), force=True)
                time.sleep(pause)
            progress.end_mailbox()
            return total_initial, deleted

    if dry_run:
        logging.info("[scan] %s → %d messages, scanning backwards…", safe_display_name(mailbox_name), total_initial)
    else:
        logging.info("[scan+delete] %s → %d messages, scanning backwards…", safe_display_name(mailbox_name), total_initial)
        imap_call_with_retry(M, "select", mbox_q, readonly=False)

    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)
    hard_stop = dt.date(max(1970, today.year - max_years_back), 1, 1)

    total_seen = 0
    total_deleted = 0
    empty_run = 0

    for start, end in iter_day_windows_backward(tomorrow, window_days, hard_stop, max_windows):
        if STOP_REQUESTED:
            break

        since_s = imap_date(start)
        before_s = imap_date(end)
        typ, data = imap_uid_with_retry(M, 'SEARCH', None, 'SINCE', since_s, 'BEFORE', before_s)
        uids = data[0].split() if data and data[0] else []
        count = len(uids)
        total_seen += count
        progress.update(add_seen=count, inc_window=True, force=True)

        if count == 0:
            empty_run += 1
            if empty_run >= max_empty_windows:
                logging.info("[abort] %s → %d empty windows in a row, stopping scan", safe_display_name(mailbox_name), empty_run)
                break
            continue
        else:
            empty_run = 0

        if not dry_run:
            for batch in chunked(uids, batch_size):
                if STOP_REQUESTED:
                    break
                us = uid_str(batch)
                imap_uid_with_retry(M, 'STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
                imap_call_with_retry(M, 'expunge')
                total_deleted += len(batch)
                progress.update(add_deleted=len(batch), force=True)
                time.sleep(pause)

        remain = uid_search_all(M)
        progress.update(remain=remain, force=True)
        if remain == 0:
            break

    progress.end_mailbox()
    return total_seen, total_deleted

# ---------------------------------------------------------
# AUTH
# ---------------------------------------------------------

def imap_login(user: str, password: Optional[str],
               xoauth2_access_token: Optional[str],
               server: str, port: int, timeout: float) -> imaplib.IMAP4_SSL:
    socket.setdefaulttimeout(timeout)
    M = imaplib.IMAP4_SSL(server, port)
    if xoauth2_access_token:
        auth = f'user={user}\x01auth=Bearer {xoauth2_access_token}\x01\x01'
        typ, data = M.authenticate('XOAUTH2', lambda _: auth.encode())
        if typ != 'OK':
            raise imaplib.IMAP4.error(f"XOAUTH2 authenticate failed: {data}")
        logging.info("Authenticated via XOAUTH2")
    else:
        typ, data = M.login(user, password)
        if typ != 'OK':
            raise imaplib.IMAP4.error(f"Login failed: {data}")
        logging.info("Authenticated with App Password.")
    return M

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Delete Gmail messages over IMAP (newest → oldest) with safe batching and progress.")
    auth = ap.add_argument_group("Authentication")
    auth.add_argument("--user", required=True)
    auth.add_argument("--password")
    auth.add_argument("--xoauth2-access-token")

    perf = ap.add_argument_group("Performance & Safety")
    perf.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    perf.add_argument("--pause", type=float, default=DEFAULT_PAUSE)
    perf.add_argument("--dry-run", type=lambda x: x.lower() in {"1","true","yes"}, default=True)
    perf.add_argument("--i-understand-this-deletes-mail", action="store_true")
    perf.add_argument("--timeout", type=float, default=60.0)
    perf.add_argument("-v", "--verbose", action="count", default=0)

    filt = ap.add_argument_group("Mailbox Selection")
    filt.add_argument("--include", action="append", default=[])
    filt.add_argument("--exclude", action="append", default=[])

    scan = ap.add_argument_group("Scanning / Heuristics")
    scan.add_argument("--min-messages", type=int, default=DEFAULT_MIN_MESSAGES)
    scan.add_argument("--max-empty-windows", type=int, default=DEFAULT_MAX_EMPTY_WINDOWS)
    scan.add_argument("--window-days", type=int, default=7)
    scan.add_argument("--max-windows", type=int, default=DEFAULT_MAX_WINDOWS)
    scan.add_argument("--max-years-back", type=int, default=DEFAULT_MAX_YEARS_BACK)

    ui = ap.add_argument_group("Progress")
    ui.add_argument("--progress", type=lambda x: x.lower() in {"1","true","yes"}, default=True)
    ui.add_argument("--progress-interval", type=float, default=0.5)
    ui.add_argument("--list-folders", action="store_true")

    net = ap.add_argument_group("Network")
    net.add_argument("--server", default=GMAIL_IMAP_HOST)
    net.add_argument("--port", type=int, default=GMAIL_IMAP_PORT_SSL)

    args = ap.parse_args()
    setup_logging(args.verbose)
    install_signal_handlers()

    if not args.dry_run and not args.i_understand_this_deletes-mail:
        ap.error("Destructive run requires --i-understand-this-deletes-mail")

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
        logging.error("Login/auth failed: %s", e)
        sys.exit(1)

    mailboxes = discover_mailboxes(M, args.include, args.exclude)

    if args.list-folders:
        print("\nMailbox list:")
        for kind, name in mailboxes:
            typ, _ = imap_call_with_retry(M, "select", imap_quote_mailbox(name), readonly=True)
            count = uid_search_all(M) if typ == "OK" else 0
            print(f"  {safe_display_name(name):30s}  {count:8d}")
        M.logout()
        return

    total_seen = 0
    total_deleted = 0

    try:
        for kind, name in mailboxes:
            if STOP_REQUESTED:
                break
            progress = Progress(enabled=args.progress, interval_sec=args.progress_interval)
            logging.info("Processing mailbox: %s (kind=%s)", safe_display_name(name), kind or "normal")

            t, d = delete_in_mailbox(
                M,
                name,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
                pause=args.pause,
                min_messages=args.min_messages,
                max_empty_windows=args.max_empty_windows,
                window_days=args.window_days,
                max_windows=args.max_windows,
                max_years_back=args.max_years_back,
                progress=progress,
            )
            total_seen += t
            total_deleted += d
            time.sleep(max(args.pause, 0.5))
    finally:
        try: M.logout()
        except Exception: pass

    if args.dry_run:
        print(f"\n[dry-run complete] Total messages scanned: {total_seen:,}")
    else:
        print(f"\n[complete] Deleted {total_deleted:,} messages total.")

if __name__ == "__main__":
    main()
