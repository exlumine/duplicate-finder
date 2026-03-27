"""
scanner.py — recursive directory walk, metadata collection, and SHA-256 hashing.

Two modes controlled by flags:
  hash_inline=False  → Phase 1: walk and record metadata only (fast)
  hash_inline=True, hash_only=True → Phase 2: hash files that still need it (slow)
"""

import hashlib
import os
import sys
from pathlib import Path

try:
    from tqdm import tqdm
    _TQDM = True
except ImportError:
    _TQDM = False

from db import Database

_CHUNK = 1024 * 1024  # 1 MiB read chunks for hashing


# ── Public entry point ────────────────────────────────────────────────────────

def scan(
    db: Database,
    root: Path,
    hash_inline: bool = False,
    hash_only: bool = False,
) -> None:
    """
    Walk *root* recursively.

    Phase 1 (hash_inline=False, hash_only=False):
        Insert / update file metadata. SHA-256 left as NULL.

    Phase 2 (hash_inline=True, hash_only=True):
        Only process rows that already exist in the DB with sha256 IS NULL.
        Does NOT re-walk the filesystem — reads the pending list from the DB.
    """
    if hash_only:
        _hash_pending(db)
    else:
        _walk_and_record(db, root, hash_inline=hash_inline)


# ── Phase 1: walk ─────────────────────────────────────────────────────────────

def _walk_and_record(db: Database, root: Path, hash_inline: bool) -> None:
    print(f"Scanning: {root}")
    count = 0
    errors = 0

    for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
        for fname in filenames:
            fpath = Path(dirpath) / fname
            try:
                stat = fpath.stat()
            except OSError as exc:
                print(f"  WARN  cannot stat '{fpath}': {exc}", file=sys.stderr)
                errors += 1
                continue

            if not fpath.is_file():
                continue  # skip symlinks, sockets, etc.

            db.upsert_file_meta(
                path=str(fpath),
                filename=fname,
                size_bytes=stat.st_size,
                mtime=stat.st_mtime,
            )
            count += 1

            if count % 5000 == 0:
                db.commit()
                print(f"  … {count:,} files recorded", end="\r")

    db.commit()
    print(f"  Scan complete: {count:,} files recorded, {errors} errors.")


# ── Phase 2: hash ─────────────────────────────────────────────────────────────

def _hash_pending(db: Database) -> None:
    total = db.count_unhashed()
    if total == 0:
        print("  Nothing to hash — all files already have a SHA-256.")
        return

    print(f"  Hashing {total:,} files …")

    rows = list(db.iter_unhashed())  # materialise so tqdm knows the total
    errors = 0
    done = 0

    iterator = (
        tqdm(rows, unit="file", dynamic_ncols=True) if _TQDM else rows
    )

    for row in iterator:
        file_id: int = row["id"]
        path: str = row["path"]
        try:
            digest = _sha256(path)
            db.set_sha256(file_id, digest)
        except OSError as exc:
            print(f"\n  WARN  cannot hash '{path}': {exc}", file=sys.stderr)
            db.set_status_error(file_id)
            errors += 1

        done += 1
        if done % 500 == 0:
            db.commit()

    db.commit()
    print(f"  Hashing complete. Errors: {errors}.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()
