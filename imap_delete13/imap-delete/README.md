# IMAP Delete Tool

Graceful IMAP mailbox cleaner with batching, rate limiting, and excellent user feedback.

## Quick Start

```bash
cd imap-delete
python3 generate_project.py  # You already did this!
pip install -r requirements.txt
python3 -m imap_delete --help
```

## Usage

```bash
# Dry run (safe, no changes)
python3 -m imap_delete --user you@gmail.com --password 'app_pass' --dry-run

# Delete emails before 2020
python3 -m imap_delete --user you@gmail.com --password 'app_pass' \
    --query 'BEFORE 1-Jan-2020' --i-understand-this-deletes-mail

# Custom server
python3 -m imap_delete --server imap.example.com --port 993 \
    --user you@example.com --password 'pass' --dry-run
```

## Run Tests

```bash
pytest tests/
```

## Features

- ✅ Batch operations (50 emails at a time)
- ✅ Rate limiting (0.1s between batches)
- ✅ Automatic retries (3 attempts)
- ✅ Connection timeouts
- ✅ Progress feedback
- ✅ Dry-run mode
- ✅ Size estimation

## Gmail Setup

Use an App Password, not your regular password.
Generate one at: https://myaccount.google.com/apppasswords
