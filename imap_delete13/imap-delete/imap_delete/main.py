"""Main entry point."""
import time

from .cli import build_parser
from .utils import should_delete, setup_logger
from .session import close_session
from .logging import log_summary, print_header
from .workflow import (
    connect_and_auth, select_mailbox, search_messages,
    calculate_sizes, do_delete_flow, handle_no_deletion
)


def run() -> int:
    """Main execution flow."""
    parser = build_parser()
    args = parser.parse_args()
    log = setup_logger(args.verbose)
    start = time.time()
    
    print_header(log)
    imap = connect_and_auth(args, log)
    
    try:
        select_mailbox(imap, args.mailbox, log)
        ids = search_messages(imap, args.query, log)
        
        if not ids:
            log.info("No messages to process!")
            return 0
        
        size_est = calculate_sizes(imap, ids, log)
        elapsed = time.time() - start
        log_summary(
            log, args.mailbox, len(ids), size_est, elapsed, args.dry_run
        )
        
        if should_delete(args.dry_run, args.i_understand_this_deletes_mail):
            do_delete_flow(imap, ids, log)
        else:
            handle_no_deletion(args, log)
        
        return 0
    finally:
        close_session(imap, log)


def main() -> None:
    """Entry point."""
    raise SystemExit(run())


if __name__ == "__main__":
    main()
