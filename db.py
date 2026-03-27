"""
db.py — SQLite database schema and all data-access operations.
"""

import sqlite3
from pathlib import Path
from typing import Iterator


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT    UNIQUE NOT NULL,   -- absolute path
    filename    TEXT    NOT NULL,
    sha256      TEXT,                      -- NULL until Phase 2 completes for this row
    size_bytes  INTEGER NOT NULL,
    mtime       REAL    NOT NULL,          -- Unix timestamp (float)
    status      TEXT    NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending', 'keep', 'delete', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_status ON files(status);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialise(self) -> None:
        self._conn = sqlite3.connect(self._path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "Database.initialise() has not been called."
        return self._conn

    # ── Phase 1: SCAN ─────────────────────────────────────────────────────────

    def upsert_file_meta(
        self,
        path: str,
        filename: str,
        size_bytes: int,
        mtime: float,
    ) -> None:
        """Insert a new file record, or update metadata if the path already exists.
        The sha256 and status are intentionally left untouched on conflict so that
        a re-scan does not reset hashing progress."""
        self.conn.execute(
            """
            INSERT INTO files (path, filename, size_bytes, mtime)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                filename   = excluded.filename,
                size_bytes = excluded.size_bytes,
                mtime      = excluded.mtime
            WHERE files.sha256 IS NULL   -- don't clobber already-hashed rows
            """,
            (path, filename, size_bytes, mtime),
        )

    def commit(self) -> None:
        self.conn.commit()

    # ── Phase 2: HASH ─────────────────────────────────────────────────────────

    def iter_unhashed(self) -> Iterator[sqlite3.Row]:
        """Yield all rows that still need hashing (sha256 IS NULL, status='pending')."""
        cur = self.conn.execute(
            "SELECT id, path FROM files WHERE sha256 IS NULL AND status = 'pending'"
        )
        yield from cur

    def set_sha256(self, file_id: int, sha256: str) -> None:
        self.conn.execute(
            "UPDATE files SET sha256 = ? WHERE id = ?",
            (sha256, file_id),
        )

    def set_status_error(self, file_id: int) -> None:
        self.conn.execute(
            "UPDATE files SET status = 'error' WHERE id = ?",
            (file_id,),
        )

    def count_unhashed(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM files WHERE sha256 IS NULL AND status = 'pending'"
        ).fetchone()
        return row[0]

    # ── Phase 3: ANALYSE ──────────────────────────────────────────────────────

    def reset_keep_delete(self) -> None:
        """Reset all keep/delete marks so analyse can be re-run cleanly."""
        self.conn.execute(
            "UPDATE files SET status = 'pending' WHERE status IN ('keep', 'delete')"
        )
        self.conn.commit()

    def iter_duplicate_groups(self) -> Iterator[list[sqlite3.Row]]:
        """Yield lists of rows that share the same sha256, only where count > 1."""
        hashes_cur = self.conn.execute(
            """
            SELECT sha256
            FROM files
            WHERE sha256 IS NOT NULL AND status != 'error'
            GROUP BY sha256
            HAVING COUNT(*) > 1
            """
        )
        for (sha256,) in hashes_cur:
            rows_cur = self.conn.execute(
                "SELECT id, path, mtime FROM files WHERE sha256 = ? ORDER BY mtime ASC",
                (sha256,),
            )
            yield list(rows_cur)

    def mark_keep(self, file_id: int) -> None:
        self.conn.execute("UPDATE files SET status = 'keep' WHERE id = ?", (file_id,))

    def mark_delete(self, file_id: int) -> None:
        self.conn.execute("UPDATE files SET status = 'delete' WHERE id = ?", (file_id,))

    def mark_all_unique_as_keep(self) -> None:
        """Files with a unique sha256 (no duplicates) are automatically kept."""
        self.conn.execute(
            """
            UPDATE files SET status = 'keep'
            WHERE status = 'pending'
              AND sha256 IS NOT NULL
              AND sha256 NOT IN (
                  SELECT sha256 FROM files
                  WHERE sha256 IS NOT NULL
                  GROUP BY sha256
                  HAVING COUNT(*) > 1
              )
            """
        )

    # ── Phase 4: VERIFY ───────────────────────────────────────────────────────

    def orphaned_delete_hashes(self) -> list[str]:
        """
        Return sha256 values that appear in 'delete' rows but NOT in any 'keep' row.
        A non-empty list means verification has failed.
        """
        rows = self.conn.execute(
            """
            SELECT DISTINCT sha256 FROM files WHERE status = 'delete'
            EXCEPT
            SELECT DISTINCT sha256 FROM files WHERE status = 'keep'
            """
        ).fetchall()
        return [r[0] for r in rows]

    # ── Phase 5 / 6: SUMMARY + DELETE ─────────────────────────────────────────

    def summary(self) -> dict:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*)                                            AS total,
                COUNT(DISTINCT sha256)                              AS unique_hashes,
                SUM(CASE WHEN status = 'keep'   THEN 1 ELSE 0 END) AS keep_count,
                SUM(CASE WHEN status = 'delete' THEN 1 ELSE 0 END) AS delete_count,
                SUM(CASE WHEN status = 'error'  THEN 1 ELSE 0 END) AS error_count,
                SUM(CASE WHEN status = 'delete' THEN size_bytes ELSE 0 END) AS bytes_to_free
            FROM files
            """
        ).fetchone()
        return dict(row)

    def iter_delete_paths(self) -> Iterator[tuple[int, str]]:
        cur = self.conn.execute(
            "SELECT id, path FROM files WHERE status = 'delete' ORDER BY path"
        )
        yield from cur

    def mark_deleted(self, file_id: int) -> None:
        """After successful deletion, remove the row from the DB."""
        self.conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
