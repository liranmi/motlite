# oro-db SQL Integration: `CREATE MOT TABLE`

This document describes the SQLite-based SQL frontend for the oro-db MOT engine.

## What it is

A patched SQLite 3.49.1 that recognizes `CREATE MOT TABLE` as a new DDL form.
Tables created this way are stored in the MOT in-memory engine (MassTree +
OCC + MVCC) instead of SQLite's native B-tree. Both engine types coexist in
the same database — you can JOIN a MOT table against a native B-tree table in
a single query.

Think of it as MySQL's InnoDB/MyISAM engine choice, but for SQLite.

## Quick start

```bash
# Build (fetches oro-db engine automatically via CMake)
make debug -sj

# Interactive shell
./build/debug/oro_shell                   # in-memory SQLite + MOT engine
./build/debug/oro_shell ./mydata.db       # persistent SQLite schema + in-memory MOT data

# PostgreSQL wire protocol server (connect with psql, pgAdmin, DBeaver)
./build/debug/oro_server --port 5433
psql -h localhost -p 5433

# Run the tests
make test
```

```sql
-- Native B-tree table (disk-backed, full SQLite semantics)
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);

-- MOT in-memory table (fast, ephemeral)
CREATE MOT TABLE orders (
    id       INTEGER PRIMARY KEY,
    user_id  INTEGER,
    total    REAL
);

-- Standard DML works on both:
INSERT INTO users VALUES(1, 'Alice'), (2, 'Bob');
INSERT INTO orders VALUES(100, 1, 99.99), (101, 2, 75.00);

-- Cross-engine JOIN:
SELECT u.name, SUM(o.total)
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.name;

-- Explicit transactions work:
BEGIN;
UPDATE orders SET total = 199.99 WHERE id = 100;
DELETE FROM orders WHERE total < 50;
COMMIT;  -- or ROLLBACK;
```

## What works

| Feature | MOT tables | Native tables |
|---------|-----------|---------------|
| INSERT / UPDATE / DELETE | Yes | Yes |
| SELECT with WHERE (point lookup) | Yes | Yes |
| SELECT with WHERE (range scan: `>, <, >=, <=`) | Yes | Yes |
| Aggregates (COUNT, SUM, AVG, MIN, MAX) | Yes | Yes |
| GROUP BY | Yes | Yes |
| Cross-engine JOINs | Yes | Yes |
| Explicit transactions (BEGIN/COMMIT/ROLLBACK) | Yes | Yes |
| Autocommit (implicit per-statement) | Yes | Yes |
| Data types: INTEGER, REAL, TEXT, BLOB | Yes | Yes |
| Schema persistence (survives DB reopen) | Yes (schema only) | Yes (with data) |
| WAL / journal | No | Yes |
| CREATE INDEX | Yes | Yes |
| Triggers | Yes | Yes |
| Foreign keys | Yes | Yes |

## How it works

```
    CREATE MOT TABLE orders (...)
             |
             v
    SQLite parser (modified tokenizer: MOT keyword between CREATE and TABLE)
             |
             v
    Table struct (tabFlags |= TF_Mot, eTabType = TABTYP_MOT)
             |
             v
    VDBE opcodes (OP_OpenRead, OP_Insert, etc.)
             |
             v  dispatches on eCurType
      +------+------+
      |             |
  CURTYPE_BTREE  CURTYPE_MOT
  (unchanged)    (adapter: sqlite/oro_mot_adapter.cpp)
      |             |
      v             v
  Pager/WAL     MassTree + OCC + MVCC
  (disk)        (memory)
```

### What was modified in SQLite

All changes are to the amalgamation `third_party/sqlite/sqlite3.c` — no files
deleted, only additive patches (~300 lines total). Key hooks:

- **Parser**: tokenizer intercepts `CREATE MOT TABLE` and sets a flag so the
  parser sees `CREATE TABLE` plus `TF_Mot`.
- **VDBE dispatch** on `eCurType`: OpenRead/Write, Close, Rewind, Next,
  Rowid, Column, SeekRowid, SeekGT/GE/LT/LE, Insert (with UPDATE support),
  Delete, Count, NewRowid all route MOT cursors to the adapter.
- **Transaction coordination**: `sqlite3VdbeHalt` auto-commits MOT txns in
  autocommit mode; `OP_AutoCommit` handles explicit BEGIN/COMMIT/ROLLBACK.
- **Schema reload**: `sqlite3EndTable` calls `oroMotTableCreate` for each MOT
  table, both on first CREATE and on DB reopen (where MOT data is gone but the
  schema is preserved in `sqlite_schema`).

### The adapter (~650 lines)

`sqlite/oro_mot_adapter.{h,cpp}` is the C-linkage bridge between SQLite's
VDBE and the MOT engine. It:

- Maintains a per-sqlite3-connection MOT session.
- Stores SQLite serialized records as opaque BLOBs in MOT (each MOT table has
  2 internal columns: `data BLOB` and `rowid LONG`).
- Translates cursor operations: `oroMotFirst`, `oroMotNext`, `oroMotSeekRowid`,
  `oroMotSeekCmp`, `oroMotInsert`, `oroMotDelete`.

This storage-as-BLOB approach avoids per-column type translation. SQLite's
OP_Column decodes the record bytes normally after our adapter hands them
back via `oroMotPayloadFetch`.

## Limitations

1. **In-memory only**: MOT data is lost on process exit. The SQLite schema
   is preserved (MOT tables come back empty on reopen). Activating MOT's
   own redo-log infrastructure is a future task.

2. **Read-your-own-writes during transaction**: Within a `BEGIN..COMMIT`,
   `SELECT` on a MOT table does not yet see the current transaction's
   uncommitted INSERTs. MOT inserts rows into the MassTree index
   immediately but the iterator sentinel lookup doesn't match the txn's
   access set entry. Autocommit (the default) is unaffected.

3. **CREATE INDEX on populated tables**: `CREATE INDEX` must be issued
   before data is inserted. Indexes created after rows exist will be
   empty until new rows are inserted (no backfill).

4. **Secondary index key encoding**: Index keys use a memcmp-comparable
   encoding (integers, text with BINARY collation, blobs). NOCASE and
   other custom collations may not sort correctly through the index.

5. **Concurrent connections**: Each `sqlite3*` handle gets its own MOT
   session. Cross-connection MOT transactions are serialized by MOT's OCC.

## Source layout

```
motlite/
|-- sqlite/                    # SQLite ↔ MOT bridge
|   |-- oro_mot_adapter.h/cpp  # C-linkage API (VDBE dispatch, key encoding)
|   |-- oro_shell.cpp          # Interactive CLI (wraps SQLite shell.c)
|   `-- oro_server.cpp         # PG wire protocol server
|-- third_party/
|   |-- sqlite/                # SQLite 3.49.1 amalgamation (patched)
|   `-- linenoise/             # Bundled line editing library
|-- test/
|   `-- test_mot_engine.cpp    # Integration tests (17)
|-- config/mot.conf            # MOT engine configuration
`-- CMakeLists.txt             # Fetches oro-db via FetchContent
```

The MOT engine ([oro-db](https://github.com/liranmi/oro-db)) is fetched
automatically by CMake at build time.

## Build targets

| Command | Output |
|---------|--------|
| `make debug -sj` | All targets (debug) |
| `make release -sj` | All targets (release) |
| `make test` | Build + run tests |

## Test status

```
motlite_test : 17 passed, 0 failed
```

## Next steps

Roadmap in rough priority order:

1. Read-your-own-writes during transaction (sentinel identity mismatch in MOT's access set).
2. Durability via MOT's redo log (implement `ILogger` file endpoint).
3. Secondary indexes used by SQLite's query planner for WHERE clauses.
4. `CREATE INDEX` backfill for populated tables.
