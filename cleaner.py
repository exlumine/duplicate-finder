"""
cleaner.py — Phase 5 & 6: confirm with the user, then delete.
"""

import os
import sys
from db import Database


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PiB"


def confirm_and_delete(db: Database, dry_run: bool = False) -> None:
    s = db.summary()

    if s["delete_count"] == 0:
        print("  Nothing to delete — no duplicates were found.")
        return

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("  ┌─ Deletion summary ──────────────────────────────────────────┐")
    print(f"  │  Files scanned        : {s['total']:>10,}                       │")
    print(f"  │  Unique content hashes: {s['unique_hashes']:>10,}                       │")
    print(f"  │  Files to keep        : {s['keep_count']:>10,}                       │")
    print(f"  │  Files to DELETE      : {s['delete_count']:>10,}                       │")
    print(f"  │  Space to be freed    : {_fmt_bytes(s['bytes_to_free']):>14}                   │")
    if dry_run:
        print("  │                                                              │")
        print("  │  *** DRY-RUN — no files will actually be deleted ***         │")
    print("  └──────────────────────────────────────────────────────────────┘")
    print()

    if dry_run:
        print("  Dry-run complete. The following files WOULD be deleted:\n")
        for _fid, path in db.iter_delete_paths():
            print(f"    {path}")
        print(f"\n  Total: {s['delete_count']:,} files, {_fmt_bytes(s['bytes_to_free'])} freed.")
        print("  (Re-run without --dry-run to perform actual deletion.)")
        return

    # ── Confirmation prompt ───────────────────────────────────────────────────
    print("  This operation is IRREVERSIBLE.")
    print("  Type  DELETE  (all caps) and press Enter to confirm, or Ctrl+C to abort.")
    print()

    try:
        answer = input("  > ").strip()
    except KeyboardInterrupt:
        print("\n  Aborted by user. No files deleted.")
        return

    if answer != "DELETE":
        print("  Input did not match 'DELETE'. Aborting — no files deleted.")
        return

    # ── Deletion ──────────────────────────────────────────────────────────────
    print()
    deleted = 0
    failed = 0

    for file_id, path in db.iter_delete_paths():
        try:
            os.remove(path)
            db.mark_deleted(file_id)
            deleted += 1
        except OSError as exc:
            print(f"  WARN  could not delete '{path}': {exc}", file=sys.stderr)
            db.set_status_error(file_id)
            failed += 1

        if deleted % 500 == 0 and deleted > 0:
            db.commit()
            print(f"  … {deleted:,} deleted", end="\r")

    db.commit()
    print(f"  Deletion complete: {deleted:,} files removed, {failed} failures.")
    if failed:
        print(
            f"  {failed} file(s) could not be deleted (status set to 'error' in DB).",
            file=sys.stderr,
        )
