#!/usr/bin/env python3
"""
gmail_imap_delete_backwards_progress.py

Delete Gmail messages over IMAP in small batches, starting from TODAY and moving
BACKWARD in time, until each mailbox is EMPTY — with live progress feedback.

What you get:
- Windowed UID searches (e.g., SINCE <start> BEFORE <end>) newest→older to avoid
  imaplib's 1 MB line limit from UID SEARCH ALL.
- After every window (and often after each batch), we re-check EXISTS; when it
  hits 0, we stop for that mailbox.
- Live single-line progress ticker (no dependencies), e.g.:
    📬 Inbox | win 3 | deleted 450 | seen 620 | remain 2,931 | 180 msg/min
- Dry-run by default; destructive runs require --i-understand-this-deletes-mail.
- Safe order: normal labels -> All Mail -> Trash -> Spam.
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

# Limits to avoid infinite loops if something is odd:
DEFAULT_MAX_WINDOWS = 2000
DEFAULT_MAX_YEARS_BACK = 30

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

# -------------------- Progress UI ---------------------

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
        self.remain = None  # type: Optional[int]
        self.t0 = time.time()
        self.t_last = self.t0

    def start_mailbox(self, name: str, remain: Optional[int]):
        self.mailbox = name
        self.windows = 0
        self.deleted = 0
        self.seen = 0
        self.remain = remain
        self.t0 = time.time()
        self.t_last = self.t0
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

        # rate since start
        dt_total = max(0.001, now - self.t0)
        rate_per_min = int((self.deleted / dt_total) * 60)

        remain_str = "?" if self.remain is None else f"{self.remain:,}"
        spin = "✔" if done else _SPINNER[self.spin_idx % len(_SPINNER)]
        self.spin_idx += 1

        line = f"\r{spin} 📬 {self.mailbox} | win {self.windows} | deleted {self.deleted:,} | seen {self.seen:,} | remain {remain_str} | {rate_per_min} msg/min"
        sys.stdout.write(line[:term_width()])
        sys.stdout.flush()

def term_width(default: int = 120) -> int:
    try:
        import shutil
        cols = shutil.get_terminal_size((default, 24)).columns
        return max(40, cols)
    except Exception:
        return default

# -------------------- Helpers ---------------------

def chunked(seq: Sequence[bytes], size: int) -> Iterable[Sequence[bytes]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def uid_str(uids: Sequence[bytes]) -> str:
    return ",".join(u.decode() if isinstance(u, (bytes, bytearray)) else str(u) for u in uids)

def imap_quote_mailbox(name: Union[bytes, str]) -> str:
    """Safely quote a mailbox for IMAP SELECT."""
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
    """Parse IMAP LIST raw line: returns (flags, delimiter, name) as bytes."""
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
    return f"{d.day:02d}-{MONTH_NAMES[d.month-1]}-{d.year}"

def iter_day_windows_backward(end_exclusive: dt.date, span_days: int, hard_stop: dt.date,
                              max_windows: int) -> Iterator[Tuple[dt.date, dt.date]]:
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
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return dt.date(y, m, 1)

def iter_month_windows_backward(end_exclusive: dt.date, months_per_window: int,
                                hard_stop: dt.date, max_windows: int) -> Iterator[Tuple[dt.date, dt.date]]:
    produced = 0
    cur_end = end_exclusive
    cur_start = first_of_month(cur_end)
    while produced < max_windows and cur_end > hard_stop:
        start = max(cur_start, hard_stop)
        yield (start, cur_end)
        produced += 1
        cur_end = first_of_month(cur_start)
        cur_start = month_add(cur_end, -months_per_window)

# --------------- Core deletion (backwards) ------------------

def search_uids_in_window(M: imaplib.IMAP4_SSL, start: dt.date, end: dt.date) -> List[bytes]:
    since_s = imap_date(start)
    before_s = imap_date(end)
    logging.debug("SEARCH window SINCE %s BEFORE %s", since_s, before_s)
    typ, data = imap_uid_with_retry(M, 'SEARCH', None, 'SINCE', since_s, 'BEFORE', before_s)
    if typ != 'OK' or not data or data[0] is None:
        return []
    return data[0].split()

def select_and_get_exists(M: imaplib.IMAP4_SSL, mbox_quoted: str, readonly: bool) -> int:
    typ, data = imap_call_with_retry(M, "select", mbox_quoted, readonly=readonly)
    if typ != 'OK' or not data:
        return 0
    try:
        return int(data[0])
    except Exception:
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
    progress: Progress,
) -> Tuple[int, int]:
    """
    Process a mailbox starting from TODAY backwards until it's empty (EXISTS == 0)
    or hard stop is reached. Returns (total_seen, total_deleted).
    """
    mbox_quoted = imap_quote_mailbox(mailbox_name)

    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)  # BEFORE tomorrow includes today's mail
    hard_stop_date = dt.date(max(1970, today.year - hard_stop_years), 1, 1)

    initial = select_and_get_exists(M, mbox_quoted, readonly=True)
    progress.start_mailbox(safe_display_name(mailbox_name), initial)

    if initial == 0:
        imap_call_with_retry(M, "close")
        progress.end_mailbox()
        return 0, 0

    total_seen = 0
    total_deleted = 0

    if window_kind == "months":
        windows_iter = iter_month_windows_backward(tomorrow, window_size, hard_stop_date, max_windows)
    else:
        windows_iter = iter_day_windows_backward(tomorrow, window_size, hard_stop_date, max_windows)

    if not dry_run:
        imap_call_with_retry(M, "select", mbox_quoted, readonly=False)

    for start, end in windows_iter:
        if STOP_REQUESTED:
            logging.warning("Stop requested; ending early in %s", safe_display_name(mailbox_name))
            break

        uids = search_uids_in_window(M, start, end)
        count = len(uids)
        total_seen += count
        progress.update(add_seen=count, inc_window=True, force=True)

        if dry_run:
            # Show a tick even for dry-run so the user sees movement.
            remain = select_and_get_exists(M, mbox_quoted, readonly=True)
            progress.update(remain=remain, force=True)
        else:
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
                # Update progress after each batch
                progress.update(add_deleted=len(batch), force=True)
                time.sleep(pause)

            # After the window, check remaining
            remain = select_and_get_exists(M, mbox_quoted, readonly=True)
            progress.update(remain=remain, force=True)

            if remain == 0:
                imap_call_with_retry(M, "close")
                progress.end_mailbox()
                logging.info("[done] %s is empty", safe_display_name(mailbox_name))
                return total_seen, total_deleted

            # Re-open read-write for next window
            imap_call_with_retry(M, "select", mbox_quoted, readonly=False)

    # Hard stop reached or STOP requested
    imap_call_with_retry(M, "close")
    progress.end_mailbox()
    return total_seen, total_deleted

# -------- Mailbox discovery / ordering ---------

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

    normals = [(k, n) for (k, n) in mailboxes if k not in ('all', 'trash', 'spam')]
    all_mail = [(k, n) for (k, n) in mailboxes if k == 'all']
    trash = [(k, n) for (k, n) in mailboxes if k == 'trash']
    spam = [(k, n) for (k, n) in mailboxes if k == 'spam']

    ordered = normals + all_mail + trash + spam

    # De-dup
    seen: Set[Tuple[Optional[str], bytes]] = set()
    result: List[Tuple[Optional[str], bytes]] = []
    for item in ordered:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result

# ---------------- Authentication ---------------

def imap_login(user: str, password: Optional[str], xoauth2_access_token: Optional[str],
               server: str, port: int, timeout: float) -> imaplib.IMAP4_SSL:
    socket.setdefaulttimeout(timeout)
    M = imaplib.IMAP4_SSL(server, port)
    if xoauth2_access_token:
        auth_string = f'user={user}\x01auth=Bearer {xoauth2_access_token}\x01\x01'
        def _auth_cb(_):
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
    ap = argparse.ArgumentParser(description="Delete Gmail messages over IMAP, newest→oldest windows until empty (with progress).")
    auth = ap.add_argument_group("Authentication")
    auth.add_argument("--user", required=True, help="Gmail address")
    auth.add_argument("--password", help="Use a Google App Password (recommended).")
    auth.add_argument("--xoauth2-access-token", help="Authenticate via XOAUTH2 using this access token.")

    perf = ap.add_argument_group("Performance & Safety")
    perf.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="UIDs per delete batch (keep small).")
    perf.add_argument("--pause", type=float, default=DEFAULT_PAUSE, help="Seconds to sleep between delete batches.")
    perf.add_argument("--dry-run", type=lambda x: x.lower() in {"1","true","yes"}, default=True,
                      help="true/false: list counts only, no deletions.")
    perf.add_argument("--i-understand-this-deletes-mail", action="store_true",
                      help="Required to run with --dry-run false.")
    perf.add_argument("--timeout", type=float, default=60.0, help="Socket timeout in seconds.")
    perf.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging verbosity (-v, -vv).")

    filt = ap.add_argument_group("Mailbox Selection")
    filt.add_argument("--include", action="append", default=[], help="Substring filter (can repeat).")
    filt.add_argument("--exclude", action="append", default=[], help="Substring exclude (can repeat).")

    win = ap.add_argument_group("Windowing (backwards)")
    win.add_argument("--window", default="days:7", help="Backwards window, e.g. 'days:7' or 'months:1'")
    win.add_argument("--max-windows", type=int, default=DEFAULT_MAX_WINDOWS, help="Safety cap on number of windows (default 2000).")
    win.add_argument("--max-years-back", type=int, default=DEFAULT_MAX_YEARS_BACK, help="Hard stop going back N years (default 30).")

    ui = ap.add_argument_group("Progress")
    ui.add_argument("--progress", type=lambda x: x.lower() in {"1","true","yes"}, default=True, help="Live progress line (default true).")
    ui.add_argument("--progress-interval", type=float, default=0.5, help="Seconds between progress updates (default 0.5).")

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

    # Connect & auth
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

            progress = Progress(enabled=args.progress, interval_sec=args.progress_interval)
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
                progress=progress,
            )
            total_seen += t
            total_deleted += d
            time.sleep(max(args.pause, 0.5))  # gentle pause between folders
    finally:
        try:
            M.logout()
        except Exception:
            pass

    if args.dry_run:
        print(f"\n[dry-run complete] Total messages seen across selected folders (backwards): {total_seen}")
    else:
        print(f"\n[complete] Deleted {total_deleted} messages across selected folders (backwards).")

if __name__ == "__main__":
    main()
