# motlite — Known Gaps and Future Work

Honest list of what doesn't work or isn't built yet. Sorted by severity.

---

## Critical (correctness, scalability, compatibility)

### 1. Read-your-own-writes in explicit transactions
**Status**: documented limitation, not fixed

Within a `BEGIN..COMMIT`, `SELECT` on a MOT table doesn't see the current
transaction's own uncommitted INSERTs. MOT's `RowLookup` via iterator
sentinel returns null for rows the current txn inserted — the sentinel
is in the MassTree and access set, but `AccessLookup` doesn't find a
match (sentinel identity mismatch).

**Impact**: any read-after-write pattern in a transaction returns stale data.
Autocommit (the default) works fine.

**Fix direction**: requires diving into MOT's `TxnAccess::AccessLookup`
and understanding why the access set lookup misses the current txn's
inserts. Possible workaround in the adapter: maintain a shadow "pending
inserts" map per connection and merge into `CursorAdvance`. Attempted
this once; the merge-iterator approach corrupted the regular iterator.

---

### 2. WAL has no checkpointing / truncation
**Status**: unbounded growth

`_mot_wal` accumulates every INSERT/DELETE forever. Every new connection
replays the full log during recovery.

**Impact**:
- DB file grows linearly with write count
- Recovery time grows linearly with log length
- After ~1M operations, startup becomes impractical

**Fix direction**: checkpointing:
```sql
BEGIN;
-- For each MOT table, write current state as INS records
INSERT INTO _mot_wal_new SELECT * FROM existing_rows_snapshot;
DROP TABLE _mot_wal;
ALTER TABLE _mot_wal_new RENAME TO _mot_wal;
COMMIT;
```
Could be automatic (on size threshold) or manual (`CHECKPOINT` command).

---

### 3. No extended query protocol
**Status**: only simple queries supported

The PG wire server implements only the simple query protocol (`Q`
message). Extended protocol (`Parse`/`Bind`/`Execute`) is not handled.

**Impact**: breaks most real clients —
- JDBC drivers
- node-postgres with `client.query(text, params)`
- Python psycopg3 prepared statements
- Any ORM (SQLAlchemy, Prisma, TypeORM, ActiveRecord, Django ORM)
- Any language using parameterized queries

**Fix direction**: implement `'P'` (Parse), `'B'` (Bind), `'D'` (Describe),
`'E'` (Execute), `'S'` (Sync), `'C'` (Close) messages. Keep a
per-connection map of prepared statements. Bind parameters via
`sqlite3_bind_*`. Text format is the common path; binary format can
start with integers and text only.

---

### 4. Concurrency untested
**Status**: probably works, unproven

The server spawns one thread per client connection. MOT's OCC should
allow concurrent writers, but we haven't stress-tested:
- Secondary-index creation from multiple threads
- `_mot_wal` writes from concurrent txns (SQLite WAL mode would help)
- Heavy read contention with RowLookup

**Fix direction**: write a concurrency test that fires N clients doing
mixed INSERT/SELECT and check for crashes, deadlocks, lost updates, or
index corruption.

---

### 5. No authentication, no TLS
**Status**: wide open on network

`oro_server` sends `AuthenticationOk` immediately — any user/password
combination is accepted. No SSL/TLS negotiation.

**Impact**: safe for localhost development, dangerous on any network.

**Fix direction**:
- Minimal: accept `user`+`password` matching a config file entry, MD5 or
  SCRAM-SHA-256 (PG default)
- TLS: respond to SSL request (`80877103`) with `'S'` instead of `'N'`,
  then wrap the socket in OpenSSL/mbedTLS

---

### 6. DELETE from oro_server sometimes segfaults
**Status**: open bug, pre-existing

Running `DELETE FROM ...` against `oro_server` via psql produces a
segfault with no stack trace. The same DELETE works fine in the test
harness (`motlite_test` passes 22/22 including `TestDelete`,
`TestWalDeletes`, etc.) and in a standalone thread-based reproducer.
The crash happens after `oroMotCursorOpen` returns but before any of
our subsequent cursor hooks (`oroMotSeekRowid`, `oroMotFirst`,
`oroMotDelete`) fire — i.e., inside SQLite's VDBE execution between
`OP_OpenWrite` and the next opcode.

**Reproduction**:
```
./oro_server --port 5433 --db /tmp/x.db &
psql -h localhost -p 5433 -U t -d t <<SQL
CREATE MOT TABLE t (id INT PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'a');
DELETE FROM t WHERE id=1;  -- server segfaults here
SQL
```

**Fix direction**: needs gdb/valgrind on the crashing binary to get a
proper backtrace. Most likely candidate: something in the VDBE sets
up state on the MOT cursor (e.g. via `pCur->uc.pCursor->...` assuming
BtCursor layout) after our OP_OpenWrite handler returns.

---

### 7. Crash recovery unverified
**Status**: clean shutdown works; hard crash untested

We've tested stop→restart with Ctrl-C. Never simulated `kill -9` in the
middle of a transaction batch.

**Fix direction**: test script that spawns the server, fires a stream of
INSERTs, then `kill -9`s the server and verifies the DB reopens cleanly
with the last-committed state. SQLite's journal/WAL should handle this
correctly, but we should prove it.

---

## Significant (workarounds exist, but rough edges)

### 8. Query planner doesn't use MOT secondary indexes
**Status**: indexes are maintained but never used for reads

SQLite's optimizer has no awareness of MOT's MassTree indexes. Every
`WHERE col = X` on a MOT table does a full scan + filter, even with a
secondary index.

**Impact**: indexes help UNIQUE constraint enforcement but not query
performance. Misleading to users.

**Fix direction**: either (a) teach the adapter to detect simple
equality predicates and serve them directly from the MOT index (this
requires VDBE cooperation), or (b) register MOT tables as SQLite
virtual tables with `xBestIndex` — but we moved away from vtables
deliberately.

---

### 9. ALTER TABLE unsupported on MOT tables
**Status**: MOT tables are immutable post-creation

Can't add/drop/rename columns on a MOT table. Native tables support ALTER.

**Fix direction**: MOT's engine has limited ALTER support. Could be
supported for adding nullable columns via MOT's `AddColumn`. Would need
coordination with SQLite's schema at ALTER time.

---

### 10. pg_catalog is per-connection snapshot
**Status**: stale within a session

`pg_catalog.pg_class` etc. are populated via `CREATE TABLE AS SELECT` at
connection time. Tables created in connection A don't appear in
connection B's `\dt` until B reconnects.

**Fix direction**: either (a) rebuild catalog tables on every query
(expensive), or (b) find a SQLite trick to have views in attached DBs
reference main (currently forbidden), or (c) invalidate cache on DDL.

---

### 11. No CREATE INDEX backfill
**Status**: must create indexes before data

If you `CREATE INDEX` on a MOT table that already has rows, the index
stays empty. Only new rows land in it.

**Fix direction**: after creating the MOT index, walk the primary table's
rows and insert into the new index. ~30 LOC.

---

### 12. BLOB size limit 4KB
**Status**: `MAX_RECORD_SIZE = 4096` in adapter

Rows larger than 4KB silently fail to insert. SQLite can store rows up
to page size (64KB) by default, even larger with overflow.

**Fix direction**: increase the constant or implement overflow pages
inside MOT. Simplest: bump to 16KB or 64KB.

---

### 13. No sequences / SERIAL on MOT tables
**Status**: only implicit rowid counter

Native tables have `AUTOINCREMENT`. MOT has a rowid counter (fixed
earlier) but no user-exposed sequence object.

**Fix direction**: minor — most use cases are `INTEGER PRIMARY KEY`
which uses the rowid counter. Sequences would need a native-table-
backed counter that MOT reads from.

---

## Minor (cosmetic or rarely-used)

### 14. `\d+` shows "Replica Identity: ???"
Our `pg_class` doesn't populate `relreplident` with a meaningful value.
Cosmetic.

### 15. VIEWs on MOT tables untested
Native SQLite views on MOT tables probably work (VIEW resolution happens
at query plan time above the cursor layer) but not verified.

### 16. No COPY protocol
`psql \copy` uses the PG COPY protocol (F/G messages) which we don't
implement. Users can still bulk-INSERT via SQL.

### 17. Client-server version mismatch warning
psql 14 against our reported server 15 produces a harmless warning.
Could report 14 instead to silence it.

### 18. No pg_dump / pg_restore support
These tools use COPY protocol + specific catalog queries. Won't work
against oro_server.

### 19. Limited data type coverage
- No `NUMERIC` with precision/scale (we store as REAL)
- No proper `TIMESTAMP WITH TIME ZONE`
- No `UUID`, `JSON`, `JSONB`, arrays
SQLite's dynamic typing hides this somewhat, but column types aren't
validated at insert.

---

## Non-goals (by design)

These are intentional limitations, not gaps:

- **Distributed / replication**: oro-db is a single-process engine. No
  streaming replication.
- **MOT durability via its own redo log**: we piggyback on SQLite's WAL
  instead. MOT's redo-log infrastructure is disabled in standalone mode.
- **Cross-engine transactions with 2PC**: MOT + native-table changes
  commit together via SQLite's single fsync. No prepared-transactions
  support.
- **SQL features SQLite lacks**: window functions work (SQLite has
  them), CTEs work, but stored procedures / triggers with languages
  other than the SQLite-native trigger body don't.
