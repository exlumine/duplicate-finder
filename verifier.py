"""
verifier.py — Phase 4: safety check before any deletion.

Rule: every SHA-256 that appears among 'delete' rows MUST also appear
among 'keep' rows.  If any hash would be lost entirely, abort.
"""

from db import Database


def verify(db: Database) -> bool:
    orphans = db.orphaned_delete_hashes()

    if orphans:
        print(f"  VERIFICATION FAILED — {len(orphans)} hash(es) have no 'keep' copy:")
        for h in orphans[:20]:  # show at most 20 to avoid flooding the terminal
            print(f"    {h}")
        if len(orphans) > 20:
            print(f"    … and {len(orphans) - 20} more.")
        return False

    s = db.summary()
    print(f"  VERIFICATION PASSED ✓")
    print(f"    Total files   : {s['total']:>10,}")
    print(f"    Unique hashes : {s['unique_hashes']:>10,}")
    print(f"    Marked keep   : {s['keep_count']:>10,}")
    print(f"    Marked delete : {s['delete_count']:>10,}")
    print(f"    Errors skipped: {s['error_count']:>10,}")
    return True
