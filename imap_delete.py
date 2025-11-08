#!/usr/bin/env python3
import argparse
import imaplib
import time
import re
import sys

# ---- Helpers ---------------------------------------------------------------

SPECIAL_FLAG_MAP = {
    br'\All': 'all',
    br'\Trash': 'trash',
    br'\Junk': 'spam',        # Gmail often exposes Spam as \Junk
    br'\Spam': 'spam',        # Some servers use \Spam
}

MAILBOX_LINE_RE = re.compile(rb'^\((?P<flags>[^)]*)\)\s+"(?P<sep>[^"]+)"\s+(?P<name>.+)$')

def parse_list_line(raw: bytes):
    """
    Parse an IMAP LIST response line into (flags:set(bytes), delim:bytes, name:bytes).
    """
    m = MAILBOX_LINE_RE.match(raw)
    if not m:
        return set(), b'/', raw.strip().strip(b'"')
    flags = set(m.group('flags').split())
    name = m.group('name').strip()
    # Names can be quoted or not; strip quotes if present
    if name.startswith(b'"') and name.endswith(b'"'):
        name = name[1:-1]
    return flags, m.group('sep'), name

def classify_mailbox(flags:set[bytes]) -> str | None:
    for f in flags:
        if f in SPECIAL_FLAG_MAP:
            return SPECIAL_FLAG_MAP[f]
    return None

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def uid_str(uids):
    return ",".join(u.decode() if isinstance(u, bytes) else str(u) for u in uids)

# ---- Core ------------------------------------------------------------------

def delete_in_mailbox(M, mailbox: bytes, batch_size: int, dry_run: bool, pause: float):
    typ, _ = M.select(mailbox, readonly=dry_run)  # readonly=True for dry-run counts
    if typ != 'OK':
        print(f"[skip] Cannot select {mailbox.decode(errors='ignore')}", file=sys.stderr)
        return 0, 0

    typ, data = M.uid('SEARCH', None, 'ALL')
    if typ != 'OK':
        print(f"[skip] Search failed in {mailbox.decode(errors='ignore')}", file=sys.stderr)
        M.close()
        return 0, 0

    uids = data[0].split()
    total = len(uids)
    deleted = 0

    if dry_run:
        # Just report counts
        M.close()
        print(f"[dry-run] {mailbox.decode(errors='ignore')}: {total} messages")
        return total, 0

    # Delete in small batches
    for batch in chunked(uids, batch_size):
        us = uid_str(batch)
        # Mark \Deleted silently
        typ, _ = M.uid('STORE', us, '+FLAGS.SILENT', r'(\Deleted)')
        if typ != 'OK':
            print(f"[warn] STORE failed in {mailbox.decode(errors='ignore')} on batch; backing off…", file=sys.stderr)
            time.sleep(max(pause*2, 2.0))
            continue
        # Expunge the batch
        typ, _ = M.expunge()
        if typ != 'OK':
            print(f"[warn] EXPUNGE failed in {mailbox.decode(errors='ignore')} on batch; backing off…", file=sys.stderr)
            time.sleep(max(pause*2, 2.0))
            continue

        deleted += len(batch)
        time.sleep(pause)

    M.close()
    print(f"[done] {mailbox.decode(errors='ignore')}: deleted {deleted}/{total}")
    return total, deleted

def main():
    ap = argparse.ArgumentParser(description="Delete Gmail messages over IMAP carefully, in small batches.")
    ap.add_argument("--user", required=True, help="Your Gmail address")
    ap.add_argument("--password", required=True,
                    help="App Password (recommended) or your password (if IMAP-only test account).")
    ap.add_argument("--server", default="imap.gmail.com")
    ap.add_argument("--batch-size", type=int, default=50, help="UIDs per batch (keep this small)")
    ap.add_argument("--pause", type=float, default=0.5, help="Seconds to sleep between batches")
    ap.add_argument("--dry-run", type=lambda x: x.lower() in {"1","true","yes"}, default=True,
                    help="true/false: list counts only, do not delete")
    ap.add_argument("--include", action="append", default=[],
                    help="Optional substring filter for mailbox names; can be repeated")
    ap.add_argument("--exclude", action="append", default=[],
                    help="Optional substring exclude filter for mailbox names; can be repeated")
    ap.add_argument("--process-order", choices=["safe","aggressive"], default="safe",
                    help="'safe' = labels first, All Mail last, then Trash/Spam. 'aggressive' = All Mail first.")
    args = ap.parse_args()

    # Connect
    M = imaplib.IMAP4_SSL(args.server, 993)
    try:
        M.login(args.user, args.password)  # Use an App Password if you have 2FA
    except imaplib.IMAP4.error as e:
        print(f"Login failed: {e}", file=sys.stderr)
        sys.exit(1)

    # List mailboxes
    typ, boxes = M.list()
    if typ != 'OK' or boxes is None:
        print("Could not list mailboxes.", file=sys.stderr)
        sys.exit(1)

    mailboxes = []
    all_mail = None
    trash = None
    spam = None

    for raw in boxes:
        flags, _, name = parse_list_line(raw)
        kind = classify_mailbox(flags)
        # Gmail often exposes non-ASCII label names; keep bytes
        target = name

        # Skip non-selectable if advertised (rare on Gmail)
        # (We rely on SELECT to tell us if we can enter.)
        if kind == 'all':
            all_mail = target
        elif kind == 'trash':
            trash = target
        elif kind == 'spam':
            spam = target

        # Apply include/exclude filters on mailbox name (bytes -> lower str)
        nstr = target.decode(errors='ignore').lower()
        if args.include and not any(sub.lower() in nstr for sub in args.include):
            continue
        if args.exclude and any(sub.lower() in nstr for sub in args.exclude):
            continue

        mailboxes.append((kind, target))

    # Ensure unique and sensible order
    # SAFE: remove labels first (avoids touching All Mail until last), then All Mail, then purge Trash/Spam.
    # AGGRESSIVE: All Mail first (moves everything to Trash), then purge Trash/Spam, then any stragglers.
    ordered = []

    def add_if_present(k):
        for kk, nm in mailboxes:
            if kk == k:
                ordered.append((kk, nm))

    # Add non-special first/last depending on mode
    normals = [(k, n) for (k, n) in mailboxes if k not in ('all','trash','spam')]
    if args.process_order == "safe":
        ordered.extend(normals)       # remove labels
        if all_mail: ordered.append(('all', all_mail))
        if trash:    ordered.append(('trash', trash))
        if spam:     ordered.append(('spam', spam))
    else:
        if all_mail: ordered.append(('all', all_mail))
        if trash:    ordered.append(('trash', trash))
        if spam:     ordered.append(('spam', spam))
        ordered.extend(normals)

    total_all = 0
    deleted_all = 0

    # De-dupe while preserving order
    seen = set()
    deduped = []
    for k, n in ordered:
        key = (k, n)
        if key not in seen:
            deduped.append((k, n))
            seen.add(key)

    # Process each mailbox
    for kind, name in deduped:
        t, d = delete_in_mailbox(M, name, args.batch_size, args.dry_run, args.pause)
        total_all += t
        deleted_all += d
        # Gentle extra pause between folders
        time.sleep(max(args.pause, 0.5))

    M.logout()

    if args.dry_run:
        print(f"\n[dry-run complete] Total messages seen across selected folders: {total_all}")
    else:
        print(f"\n[complete] Deleted {deleted_all} messages across selected folders.")

if __name__ == "__main__":
    main()
