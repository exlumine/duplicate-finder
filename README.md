# duplicate_finder

A command-line tool that recursively scans a directory, catalogues every file
in a SQLite database, identifies duplicates by SHA-256 hash, and removes them
— keeping exactly one copy (the oldest by mtime) of each unique file.

## Requirements

- Python 3.10+ (uses `match`-free code, but walrus `:=` operator is used — 3.8+)
- `tqdm` for progress bars (`pip install tqdm`), or omit it — the tool degrades gracefully

```
pip install -r requirements.txt
```

## Usage

```
python main.py <path> [--db <database.db>] [--dry-run] [--phase <phase>]
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `path` | *(required)* | Root directory to scan |
| `--db` | `./duplicates.db` | Path to the SQLite database |
| `--dry-run` | off | Run all phases but skip actual deletion |
| `--phase` | `all` | Run a single phase: `scan`, `hash`, `analyse`, `verify`, `delete`, or `all` |

### Examples

**Full run with dry-run first (recommended workflow):**

```bash
# Step 1 — inspect without deleting anything
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --dry-run

# Step 2 — when you're happy, run for real
python main.py /mnt/ssd/media --db /tmp/media_dupes.db
```

**Resuming after an interrupted hash phase:**

```bash
# The scan phase writes metadata immediately; hashing can be resumed at any time.
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --phase hash
```

**Running phases individually:**

```bash
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --phase scan
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --phase hash
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --phase analyse
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --phase verify
python main.py /mnt/ssd/media --db /tmp/media_dupes.db --phase delete
```

## How it works

| Phase | What happens |
|---|---|
| **1. SCAN** | Walks the directory tree; records path, filename, size, mtime into SQLite. Resumable: existing rows are not reset. |
| **2. HASH** | Reads each file in 1 MiB chunks and computes SHA-256. Only processes rows with `sha256 IS NULL`, so it's safe to interrupt and restart. |
| **3. ANALYSE** | Groups files by hash. Marks the file with the lowest mtime `keep`; all others in the group `delete`. Files with a unique hash are automatically marked `keep`. |
| **4. VERIFY** | Runs a SQL `EXCEPT` query: every hash in the `delete` set must appear in the `keep` set. Hard-stops if this fails — no deletion is possible. |
| **5. CONFIRM** | Prints a full summary. In dry-run mode, lists all files that *would* be deleted. In normal mode, requires typing `DELETE` to proceed. |
| **6. DELETE** | Calls `os.remove()` per file, then removes the row from the database. Failures are logged and marked `error`. |

## Database schema

```sql
CREATE TABLE files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT    UNIQUE NOT NULL,
    filename    TEXT    NOT NULL,
    sha256      TEXT,
    size_bytes  INTEGER NOT NULL,
    mtime       REAL    NOT NULL,   -- Unix timestamp
    status      TEXT    NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','keep','delete','error'))
);
```

You can query the database directly at any point:

```bash
sqlite3 /tmp/media_dupes.db "SELECT status, COUNT(*), SUM(size_bytes) FROM files GROUP BY status;"
```

## Safety notes

- The database file must be **outside** the scanned directory (enforced at startup).
- Files that cannot be read (`error` status) are never deleted.
- Deletion is skipped entirely if verification fails.
- `--dry-run` prints the full deletion list with no side effects.
