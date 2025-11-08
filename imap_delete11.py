#!/usr/bin/env python3
"""
imap_delete11.py

Gmail IMAP bulk deletion, newest → oldest, safe small-batch deletes with:
- Live progress line
- EXISTS-based counts (never hits 1MB UID overflow)
- Fast-skip/fast-delete for tiny folders
- Adaptive windowing: doubles window size after repeated empty windows
- Clean skip for non-selectable virtual parents (e.g. “[Gmail]”)

Defaults chosen for large, gappy inboxes.
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
DEFAULT_MIN_MESSAGES = 50
DEFAULT_MAX_EMPTY_WINDOWS = 10
DEFAULT_MAX_WINDOWS = 2000
DEFAULT_MAX_YEARS_BACK = 30
DEFAULT_WINDOW_DAYS = 30           # better default for big inboxes
DEFAULT_MAX_WINDOW_DAYS = 365
DEFAULT_ADAPTIVE_WINDOWS = True

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
        self.remain = None
        self.t0 = time.time()

    def start_mailbox(self, name: str, remain: Optional[int]):
        self.mailbox = name
        self.windows = 0
        self.deleted = 0
        self.seen = 0
        self.remain = remain
        self.t0 = time.time()
        self._emit(force=True)

    def update(self, *, add_deleted: int = 0, add_seen: int = 0,
               inc_window: bool = False, remain: Optional[int] = None,
               force: bool = False):
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

        remain_str = "?" if self.remain is None else f"{self.remain:,}"
        spin = "✔" if done else _SPINNER[self.spin_idx % len(_SPINNER)]
        self.spin_idx += 1

        line = (f"\r{spin} 📬 {self.mailbox} | win {self.windows} | "
                f"deleted {self.deleted:,} | seen {self.seen:,} | "
                f"remain {remain_str} | {rate_per_min} msg/min")
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
    def _handler(signum, _):
        global STOP_REQUESTED
        STOP_REQUESTED = True
        logging.warning("Stop requested (signal %s) — finishing current batch", signum)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except Exception:
            pass

# --------- IMAP wrappers with retries -----------

def imap_call_with_retry(M: imaplib.IMAP4_SSL, cmd: str, *args,
                         max_retries: int = 5, **kwargs):
    for attempt in range(max_retries + 1):
        try:
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
            return 'NO', [b'IMAP call failed after retries']

def imap_uid_with_retry(M: imaplib.IMAP4_SSL, *args, max_retries: int = 5):
    for attempt in range(max_retries + 1):
        try:
            typ, data = M.uid(*args)
            if typ == 'OK':
                return typ, data
            logging.warning("IMAP UID returned %s; data=%s", typ, data)
        except (imaplib.IMAP4.abort, socket.timeout, OSError) as e:
            logging.warning("IMAP UID exception: %s", e)
        if attempt < max_retries:
            backoff_sleep(attempt)
        else:
            return 'NO', [b'UID call failed after retries']

# --------------- Date utilities ------------------

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

def imap_date(d: dt.date) -> str:
    return f"{d.day:02d}-{MONTH_NAMES[d.month-1]}-{d.year}"

def iter_day_windows_backward(end_exclusive: dt.date, span_days: int,
                              hard_stop: dt.date, max_windows: int):
    produced = 0
    end = end_exclusive
    while produced < max_windows and end > hard_stop:
        start = end - dt.timedelta(days=span_days)
        if start < hard_stop:
            start = hard_stop
        yield (start, end)
        end = start
        produced += 1

# --------------- Core deletion logic ------------------

def try_select_exists_once(M: imaplib.IMAP4_SSL, mbox_quoted: str, readonly: bool=True) -> int:
    """Single-attempt SELECT to quickly determine selectability + EXISTS count.
    Returns:
        >=0 : EXISTS count
        -1  : non-selectable (NO/BAD)
    """
    try:
        typ, data = M.select(mbox_quoted, readonly=readonly)
    except Exception as e:
        logging.warning("SELECT threw exception on %s: %s", mbox_quoted, e)
        return -1
    if typ != 'OK' or not data:
        return -1
    try:
        return int(data[0])
    except Exception:
        return -1

def search_uids_in_window(M: imaplib.IMAP4_SSL, start: dt.date, end: dt.date) -> List[bytes]:
    typ, data = imap_uid_with_retry(
        M, 'SEARCH', None, 'SINCE', imap_date(start), 'BEFORE', imap_date(end)
    )
    if typ != 'OK' or not data or data[0] is None:
        return []
    return data[0].split()

def delete_in_mailbox(
    M: imaplib.IMAP4_SSL,
    mailbox_name: Union[bytes, str],
    mailbox_kind: Optional[str],
    batch_size: int,
    dry_run: bool,
    pause: float,
    window_days: int,
    max_empty_windows: int,
    max_windows: int,
    min_messages: int,
    max_years_back: int,
    adaptive_windows: bool,
    max_window_days: int,
    progress: Progress,
) -> Tuple[int, int]:

    mbox_quoted = imap_quote_mailbox(mailbox_name)
    name_disp = safe_display_name(mailbox_name)

    # Quick select to see if this mailbox even exists/selects
    exists = try_select_exists_once(M, mbox_quoted, readonly=True)
    if exists < 0:
        logging.info("[skip] %s is not selectable (likely a virtual parent).", name_disp)
        return 0, 0

    progress.start_mailbox(name_disp, exists)

    if exists == 0:
        progress.end_mailbox()
        return 0, 0

    # Fast path for tiny folders
    if exists <= min_messages:
        if dry_run:
            logging.info("[fast-skip] %s → %d messages (<%d)", name_disp, exists, min_messages)
            progress.end_mailbox()
            return exists, 0
        else:
            logging.info("[fast-delete] %s → deleting %d in single batch", name_disp, exists)
            typ, data = imap_call_with_retry(M, "select", mbox_quoted, readonly=False)
            if typ == 'OK':
                typ, data = imap_uid_with_retry(M, 'SEARCH', None, 'ALL')
                uids = data[0].split() if (typ == 'OK' and data and data[0]) else []
                deleted = 0
                for batch in chunked(uids, batch_size):
                    us = uid_str(batch)
                    imap_uid_with_retry(M, 'STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
                    imap_call_with_retry(M, 'expunge')
                    deleted += len(batch)
                    progress.update(add_deleted=len(batch), force=True)
                    time.sleep(pause)
                imap_call_with_retry(M, 'close')
                progress.end_mailbox()
                return exists, deleted
            else:
                logging.warning("[skip] Could not open %s read-write.", name_disp)
                progress.end_mailbox()
                return 0, 0

    # Read-write if destructive; read-only if dry-run
    if dry_run:
        imap_call_with_retry(M, "select", mbox_quoted, readonly=True)
    else:
        imap_call_with_retry(M, "select", mbox_quoted, readonly=False)

    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)
    hard_stop = dt.date(max(1970, today.year - max_years_back), 1, 1)

    total_seen = 0
    total_deleted = 0
    empty_streak = 0
    nonempty_seen_once = False

    cur_window_days = max(1, window_days)
    priority_box = (mailbox_kind in ('all',) or 'inbox' in name_disp.lower())

    windows = iter_day_windows_backward(
        end_exclusive=tomorrow,
        span_days=cur_window_days,
        hard_stop=hard_stop,
        max_windows=max_windows,
    )

    for (start, end) in windows:
        if STOP_REQUESTED:
            logging.warning("Stop requested; quitting %s", name_disp)
            break

        uids = search_uids_in_window(M, start, end)
        count = len(uids)
        total_seen += count
        progress.update(add_seen=count, inc_window=True, force=True)

        if count == 0:
            empty_streak += 1

            # Adaptive growth: if we keep hitting empties, expand window size
            if adaptive_windows and (empty_streak >= max_empty_windows):
                # Only adapt for INBOX/All Mail aggressively; for labels we can still adapt but maybe less important
                new_days = min(cur_window_days * 2, max_window_days)
                if new_days > cur_window_days:
                    logging.info("[adapt] %s → %d empty windows; increasing window %d→%d days",
                                 name_disp, empty_streak, cur_window_days, new_days)
                    cur_window_days = new_days
                    empty_streak = 0  # reset streak
                    # Rebuild generator with larger windows, from current 'end' backwards
                    windows = iter_day_windows_backward(
                        end_exclusive=start,           # continue from where we are
                        span_days=cur_window_days,
                        hard_stop=hard_stop,
                        max_windows=max_windows,
                    )
                    continue

            # Only abort if we've seen at least one non-empty OR we've scanned a lot already
            abort_allowed = nonempty_seen_once or (progress.windows >= max(5, max_empty_windows))
            if empty_streak >= max_empty_windows and abort_allowed:
                logging.info("[abort] %s → %d empty windows in a row, stopping scan",
                             name_disp, empty_streak)
                break
            continue

        # Reset on non-empty, mark we found mail
        nonempty_seen_once = True
        empty_streak = 0

        if not dry_run:
            for batch in chunked(uids, batch_size):
                if STOP_REQUESTED:
                    logging.warning("Stop requested mid-batch %s", name_disp)
                    break
                if not batch:
                    continue
                us = uid_str(batch)
                imap_uid_with_retry(M, 'STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
                imap_call_with_retry(M, 'expunge')
                total_deleted += len(batch)
                progress.update(add_deleted=len(batch), force=True)
                time.sleep(pause)

        # Update remain after each non-empty window
        remain = try_select_exists_once(M, mbox_quoted, readonly=True)
        progress.update(remain=remain if remain >= 0 else None, force=True)
        if remain == 0:
            break

    imap_call_with_retry(M, "close")
    progress.end_mailbox()
    return total_seen, total_deleted

# -------- Mailbox discovery / ordering ---------

def discover_mailboxes(M: imaplib.IMAP4_SSL,
                       include_filters: List[str],
                       exclude_filters: List[str],
                       only_important: bool) -> List[Tuple[Optional[str], bytes]]:

    typ, boxes = imap_call_with_retry(M, "list")
    if typ != 'OK' or boxes is None:
        raise imaplib.IMAP4.error("Could not list mailboxes")

    mailboxes = []
    for raw in boxes:
        flags, _, name = parse_list_line(raw)
        kind = classify_mailbox(flags)
        nstr = safe_display_name(name).lower()

        # Filter include/exclude
        if include_filters and not any(sub.lower() in nstr for sub in include_filters):
            continue
        if exclude_filters and any(sub.lower() in nstr for sub in exclude_filters):
            continue

        # Skip obvious virtual parent "[Gmail]" exactly
        if nstr.strip() in ('[gmail]',):
            continue

        mailboxes.append((kind, name))

    if only_important:
        wanted = []
        for kind, name in mailboxes:
            s = safe_display_name(name).lower()
            if "inbox" in s or kind == 'all' or "all mail" in s:
                wanted.append((kind, name))
        # INBOX first, All Mail next
        inboxes = [(k, n) for (k, n) in wanted if 'inbox' in safe_display_name(n).lower()]
        alls    = [(k, n) for (k, n) in wanted if k == 'all' or 'all mail' in safe_display_name(n).lower()]
        others  = [(k, n) for (k, n) in wanted if (k, n) not in inboxes + alls]
        return inboxes + alls + others

    # Default ordering: Inbox first, then All Mail, then normals, then Trash/Spam
    inboxes = [(k, n) for (k, n) in mailboxes if 'inbox' in safe_display_name(n).lower()]
    all_mail = [(k, n) for (k, n) in mailboxes if k == 'all' or 'all mail' in safe_display_name(n).lower()]
    normals = [(k, n) for (k, n) in mailboxes if k not in ('all', 'trash', 'spam') and (k, n) not in inboxes]
    trash = [(k, n) for (k, n) in mailboxes if k == 'trash']
    spam = [(k, n) for (k, n) in mailboxes if k == 'spam']

    ordered = inboxes + all_mail + normals + trash + spam

    # De-dup preserve order
    seen = set()
    result = []
    for item in ordered:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result

# ---------------- Authentication ---------------

def imap_login(user: str, password: Optional[str],
               xoauth2_access_token: Optional[str],
               server: str, port: int, timeout: float) -> imaplib.IMAP4_SSL:
    socket.setdefaulttimeout(timeout)
    M = imaplib.IMAP4_SSL(server, port)
    if xoauth2_access_token:
        auth_string = f'user={user}\x01auth=Bearer {xoauth2_access_token}\x01\x01'
        def _auth_cb(_): return auth_string.encode()
        typ, data = M.authenticate('XOAUTH2', _auth_cb)
        if typ != 'OK':
            raise imaplib.IMAP4.error(f"XOAUTH2 failed: {data}")
        logging.info("Authenticated via XOAUTH2")
    else:
        if not password:
            raise ValueError("Password or XOAUTH2 token required.")
        typ, data = M.login(user, password)
        if typ != 'OK':
            raise imaplib.IMAP4.error(f"Login failed: {data}")
        logging.info("Authenticated with App Password.")
    return M

# -------------------- Main ---------------------

def main():
    ap = argparse.ArgumentParser(description="Gmail IMAP delete tool (newest→oldest, small batches, adaptive windows).")

    auth = ap.add_argument_group("Auth")
    auth.add_argument("--user", required=True)
    auth.add_argument("--password")
    auth.add_argument("--xoauth2-access-token")

    perf = ap.add_argument_group("Performance")
    perf.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    perf.add_argument("--pause", type=float, default=DEFAULT_PAUSE)
    perf.add_argument("--min-messages", type=int, default=DEFAULT_MIN_MESSAGES)
    perf.add_argument("--max-empty-windows", type=int, default=DEFAULT_MAX_EMPTY_WINDOWS)
    perf.add_argument("--max-windows", type=int, default=DEFAULT_MAX_WINDOWS)
    perf.add_argument("--max-years-back", type=int, default=DEFAULT_MAX_YEARS_BACK)
    perf.add_argument("--dry-run", type=lambda x: x.lower() in {"1","true","yes"}, default=True)
    perf.add_argument("--i-understand-this-deletes-mail", action="store_true")
    perf.add_argument("--timeout", type=float, default=60.0)
    perf.add_argument("-v","--verbose", action="count", default=0)

    filt = ap.add_argument_group("Mailbox Filtering")
    filt.add_argument("--include", action="append", default=[])
    filt.add_argument("--exclude", action="append", default=[])
    filt.add_argument("--only-important", type=lambda x: x.lower() in {"1","true","yes"}, default=False,
                      help="Only process Inbox + All Mail (ignore other labels).")

    win = ap.add_argument_group("Windowing")
    win.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS, help="Initial days per window (default 30)")
    win.add_argument("--adaptive-windows", type=lambda x: x.lower() in {"1","true","yes"}, default=DEFAULT_ADAPTIVE_WINDOWS,
                     help="If too many empty windows, double window size instead of aborting.")
    win.add_argument("--max-window-days", type=int, default=DEFAULT_MAX_WINDOW_DAYS, help="Upper cap for adaptive window size")

    ui = ap.add_argument_group("Progress")
    ui.add_argument("--progress", type=lambda x: x.lower() in {"1","true","yes"}, default=True)
    ui.add_argument("--progress-interval", type=float, default=0.5)

    misc = ap.add_argument_group("Misc")
    misc.add_argument("--list-folders", type=lambda x: x.lower() in {"1","true","yes"}, default=False)
    misc.add_argument("--server", default=GMAIL_IMAP_HOST)
    misc.add_argument("--port", type=int, default=GMAIL_IMAP_PORT_SSL)

    args = ap.parse_args()
    setup_logging(args.verbose)
    install_signal_handlers()

    if not args.dry_run and not args.i_understand_this_deletes_mail:
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
        logging.error("Auth failed: %s", e)
        sys.exit(1)

    if args.list_folders:
        print("\n[Folder list + message counts]\n")
        mailboxes = discover_mailboxes(M, args.include, args.exclude, args.only_important)
        for kind, name in mailboxes:
            mbox_quoted = imap_quote_mailbox(name)
            cnt = try_select_exists_once(M, mbox_quoted, readonly=True)
            sel = "selectable" if cnt >= 0 else "NON-SELECTABLE"
            print(f"{safe_display_name(name):40}  {max(0,cnt):8}  {sel:16}  kind={kind or 'normal'}")
        M.logout()
        return

    total_seen = 0
    total_deleted = 0
    try:
        mailboxes = discover_mailboxes(M, args.include, args.exclude, args.only_important)
        if not mailboxes:
            logging.warning("No matching folders.")
        for kind, name in mailboxes:
            if STOP_REQUESTED:
                logging.warning("Stop requested; halting before %s", safe_display_name(name))
                break
            progress = Progress(enabled=args.progress, interval_sec=args.progress_interval)
            logging.info("Processing mailbox: %s (kind=%s)", safe_display_name(name), kind or "normal")
            t, d = delete_in_mailbox(
                M,
                name,
                mailbox_kind=kind,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
                pause=max(0.0, args.pause),
                window_days=max(1, args.window_days),
                max_empty_windows=max(1, args.max_empty_windows),
                max_windows=max(1, args.max_windows),
                min_messages=max(0, args.min_messages),
                max_years_back=max(1, args.max_years_back),
                adaptive_windows=bool(args.adaptive_windows),
                max_window_days=max(1, args.max_window_days),
                progress=progress,
            )
            total_seen += t
            total_deleted += d
            time.sleep(max(args.pause, 0.5))
    finally:
        try: M.logout()
        except Exception: pass

    if args.dry_run:
        print(f"\n[dry-run complete] Total messages seen: {total_seen}")
    else:
        print(f"\n[complete] Deleted {total_deleted} messages.")

if __name__ == "__main__":
    main()
