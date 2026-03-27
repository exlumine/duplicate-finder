#!/usr/bin/env python3
"""
duplicate_finder — find and remove duplicate files based on SHA-256 hash.

Phases:
  1. SCAN     — walk directory tree, record file metadata
  2. HASH     — compute SHA-256 for every file
  3. ANALYSE  — mark oldest copy 'keep', all others 'delete'
  4. VERIFY   — safety check: every 'delete' hash exists in 'keep'
  5. CONFIRM  — show summary, require explicit user confirmation
  6. DELETE   — remove files marked 'delete' (skipped in --dry-run)
"""

import argparse
import sys
from pathlib import Path

from db import Database
from scanner import scan
from analyser import analyse
from verifier import verify
from cleaner import confirm_and_delete


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="duplicate_finder",
        description="Identify and delete duplicate files by SHA-256 hash.",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Root directory to scan recursively.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("duplicates.db"),
        help="Path to the SQLite database file (default: ./duplicates.db).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all phases including confirmation prompt, but do not delete anything.",
    )
    parser.add_argument(
        "--phase",
        choices=["scan", "hash", "analyse", "verify", "delete", "all"],
        default="all",
        help=(
            "Run only a specific phase, or 'all' to run the full pipeline (default: all). "
            "Useful for resuming after an interrupted run."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    scan_root = args.path.resolve()
    db_path = args.db.resolve()

    if not scan_root.is_dir():
        print(f"ERROR: '{scan_root}' is not a directory or does not exist.", file=sys.stderr)
        sys.exit(1)

    # Guard: prevent the DB file from sitting inside the scan root and being catalogued.
    try:
        db_path.relative_to(scan_root)
        print(
            f"ERROR: The database file '{db_path}' is inside the scan root '{scan_root}'.\n"
            "Use --db to place it somewhere outside the scanned directory.",
            file=sys.stderr,
        )
        sys.exit(1)
    except ValueError:
        pass  # db_path is not inside scan_root — good.

    if args.dry_run:
        print("*** DRY-RUN MODE — no files will be deleted ***\n")

    db = Database(db_path)
    db.initialise()

    run = args.phase

    # ── Phase 1 & 2: SCAN + HASH ──────────────────────────────────────────────
    if run in ("scan", "all"):
        print("── Phase 1: SCAN ──────────────────────────────────────────")
        scan(db, scan_root, hash_inline=False)

    if run in ("hash", "all"):
        print("\n── Phase 2: HASH ──────────────────────────────────────────")
        scan(db, scan_root, hash_inline=True, hash_only=True)

    # ── Phase 3: ANALYSE ──────────────────────────────────────────────────────
    if run in ("analyse", "all"):
        print("\n── Phase 3: ANALYSE ───────────────────────────────────────")
        analyse(db)

    # ── Phase 4: VERIFY ───────────────────────────────────────────────────────
    if run in ("verify", "all"):
        print("\n── Phase 4: VERIFY ────────────────────────────────────────")
        ok = verify(db)
        if not ok:
            print("VERIFICATION FAILED — aborting. No files have been deleted.", file=sys.stderr)
            sys.exit(2)

    # ── Phase 5 & 6: CONFIRM + DELETE ─────────────────────────────────────────
    if run in ("delete", "all"):
        print("\n── Phase 5 & 6: CONFIRM & DELETE ──────────────────────────")
        confirm_and_delete(db, dry_run=args.dry_run)

    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
