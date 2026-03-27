"""
analyser.py — Phase 3: mark duplicates for deletion, keep the oldest copy.
"""

import sys
from db import Database


def analyse(db: Database) -> None:
    db.reset_keep_delete()

    # Step A: all files with a unique hash → keep automatically.
    db.mark_all_unique_as_keep()
    db.commit()

    # Step B: for every duplicate group, keep oldest (lowest mtime), delete rest.
    groups_processed = 0
    files_to_delete = 0

    for group in db.iter_duplicate_groups():
        # group is already sorted by mtime ASC (oldest first)
        oldest = group[0]
        db.mark_keep(oldest["id"])

        for duplicate in group[1:]:
            db.mark_delete(duplicate["id"])
            files_to_delete += 1

        groups_processed += 1
        if groups_processed % 1000 == 0:
            db.commit()

    db.commit()

    print(
        f"  Duplicate groups: {groups_processed:,}  |  "
        f"Files marked for deletion: {files_to_delete:,}"
    )

    # Warn about any rows still 'pending' (unhashed / errored) that weren't classified.
    pending = db.conn.execute(
        "SELECT COUNT(*) FROM files WHERE status = 'pending'"
    ).fetchone()[0]
    if pending:
        print(
            f"  WARNING: {pending:,} files remain in 'pending' state "
            "(likely hashing errors). They will not be deleted.",
            file=sys.stderr,
        )
