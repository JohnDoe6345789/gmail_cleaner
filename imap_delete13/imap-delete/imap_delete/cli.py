"""Command-line argument parsing."""
import argparse

from .config import DEFAULT_SERVER, DEFAULT_PORT, DEFAULT_MAILBOX, DEFAULT_QUERY


def add_server_args(parser: argparse.ArgumentParser) -> None:
    """Add server connection arguments."""
    parser.add_argument(
        "--server", default=DEFAULT_SERVER, help="IMAP server host"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="IMAPS port"
    )


def add_auth_args(parser: argparse.ArgumentParser) -> None:
    """Add authentication arguments."""
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument(
        "--password", required=True, help="Password (use app password)"
    )


def add_mailbox_args(parser: argparse.ArgumentParser) -> None:
    """Add mailbox operation arguments."""
    parser.add_argument(
        "--mailbox", default=DEFAULT_MAILBOX, help="Mailbox name"
    )
    parser.add_argument(
        "--query", default=DEFAULT_QUERY,
        help="IMAP SEARCH query, e.g. 'BEFORE 1-Jan-2022'"
    )


def add_safety_args(parser: argparse.ArgumentParser) -> None:
    """Add safety and confirmation arguments."""
    parser.add_argument("--dry-run", action="store_true", help="Report only")
    parser.add_argument(
        "--i-understand-this-deletes-mail", action="store_true",
        help="Required to permit deletion"
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    """Add logging arguments."""
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose logs"
    )


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser."""
    parser = argparse.ArgumentParser(
        description="Delete matching messages from an IMAP mailbox."
    )
    add_server_args(parser)
    add_auth_args(parser)
    add_mailbox_args(parser)
    add_safety_args(parser)
    add_logging_args(parser)
    return parser
