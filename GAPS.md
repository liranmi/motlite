# motlite — Known Gaps and Future Work

Honest list of what doesn't work or isn't built yet. Sorted by severity.

---

## Critical (correctness, scalability, compatibility)

### 1. ~~Read-your-own-writes in explicit transactions~~ — FIXED (adapter overlay)
**Status**: fixed by per-connection pending-inserts overlay in
`sqlite/oro_mot_adapter.cpp`. `OroMotConn::pending_inserts` records each
staged INSERT under the active txn; `CursorAdvance` falls through to
`PendingNextAfter` after the regular MOT iterator exhausts, `oroMotSeekRowid`
consults `PendingFind` on miss, and `oroMotCount` adds `oroMotPendingCount`.
The map is cleared on commit, rollback, autocommit, and detach. WAL replay
bypasses it (`wal_replaying` skip in `oroMotInsert`) so recovery doesn't
accumulate a phantom pending set.

**Caveats**: Reverse iteration (`oroMotPrev`) is still EOF-only; UPDATE-then-
read of the same row in a txn relies on SQLite's DELETE-then-INSERT plan,
which removes the original from pending and re-stages the new row. Regression
covered by the strengthened `TestReadYourOwnWrites` in `test/test_mot_engine.cpp`.

---

### 2. ~~WAL has no checkpointing / truncation~~ — FIXED
**Status**: manual `CHECKPOINT` command landed earlier (1fff872); this
branch adds the automatic trigger.

- Manual: `CHECKPOINT` from `psql` rebuilds `_mot_wal` as a snapshot of live
  rows. Implementation in `oroMotWalCheckpoint`
  (`sqlite/oro_mot_adapter.cpp:1675`).
- Auto: `CHECKPOINT AUTO <N>` (or the C entrypoint
  `oroMotWalSetAutoCheckpoint`) arms a per-connection write counter. After
  every N WAL writes, `MaybeAutoCheckpoint` runs at the next statement
  boundary inside `oroMotAutoCommit`. The check only fires when no
  explicit user transaction is open and we're not replaying.

Regression coverage in `TestWalCheckpointTruncates`,
`TestRecoveryAfterCheckpoint`, and `TestWalAutoCheckpoint`.

---

### 3. ~~No extended query protocol~~ — FIXED
**Status**: implemented in `sqlite/oro_server.cpp` —
`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close`/`Flush` are all handled.
Per-connection `ServerConn` holds `ExtPrepared` (sqlite3_stmt pool) and
`ExtPortal` (bound statements + result format codes). Parameter binding
supports text format universally and binary INT2/INT4/INT8 (binary BLOB
fallback for other widths). `RowDescription` carries per-column PG type
OIDs inferred from SQLite declared types via `pgTypeOidForStmtCol`.
Verified via `harness_libpq_load --mode extended` (PQexecParams round-trip).
SCRAM-SHA-256 binary parameter formats beyond integers, and `COPY`, remain
future work.

---

### 4. ~~Concurrency untested~~ — FIXED
**Status**: closed by the `claude/concurrency-hardening` branch.

Verification suite, all green:
- `make stress-asan` — 16 threads × 60 s mixed INSERT/SELECT/UPDATE/DELETE
  under AddressSanitizer. ~78k ops, no use-after-free / heap-overflow.
- `make stress-tsan` — same workload under ThreadSanitizer. ~46k ops, no
  data races outside SQLite's known-safe lock-free WAL shared-memory
  protocol (suppressed via `test/harness/tsan.supp`).
- `make stress-ddl` — 16 DML threads + 1 DDL thread (CREATE/DROP INDEX +
  sibling-table churn) × 30 s. ~42k DML ops, ~190 DDL ops, zero errors.
- `make oracle` — single-threaded deterministic workload against MOT and
  native-SQLite tables in lockstep; row count and sum-based checksum on
  `id` and `h` columns must match. 25k ops, MATCH.

Real bugs surfaced and fixed in this branch:
- Shutdown-time data race on `OroMotConn::in_txn`: `oroMotShutdown` was
  iterating live connections (under `g.mu`) while owning client threads
  mutated `c->in_txn` (under no shared lock — thread ownership invariant).
  Fix in `sqlite/oro_server.cpp` (`handleClient` calls `oroMotConnDetach`
  on exit) + `sqlite/oro_mot_adapter.cpp` (`oroMotShutdown` no longer
  iterates live connections; per-thread detach handles cleanup).

Remaining (lower priority, not blocking #4 closure):
- TSan suppression for SQLite shared-memory WAL is correct but coarse —
  hides any real adapter race that bottoms out in the SQLite stack. Tighten
  once we hit a false negative.
- `wal_writes_since_ck` (`oro_mot_adapter.cpp`) is a non-atomic counter,
  safe today under thread-per-connection ownership but a latent risk if
  one connection is ever driven from multiple threads.

---

### 5. ~~No authentication~~ — MD5 LANDED; SCRAM + TLS remain
**Status**: MD5 password auth is implemented (`sqlite/oro_auth.{h,cpp}`),
opt-in via the `--users PATH` CLI flag. Users file format: one
`user password` line per entry; password may be plaintext or PG-style
`md5<hex>`. Without `--users`, the server stays in trust mode for backwards
compatibility. Verified: wrong password / unknown user → `28P01 FATAL`;
correct password → AuthenticationOk.

Remaining work for #5:
- **SCRAM-SHA-256**: PG default since v10. Requires the channel-binding
  state machine; `oro_auth.cpp` already pulls in OpenSSL (`-lcrypto`) so
  the primitives are available.
- **TLS**: the SSL request handshake still answers `'N'`. Adding
  OpenSSL-backed `TLS_server_method()` would mean wrapping all socket
  reads/writes through a `Conn` abstraction; deferred.

---

### 6. ~~DELETE from oro_server sometimes segfaults~~ — FIXED (fc568a2)
**Status**: fixed in commit fc568a2 ("Fix DELETE segfault via MOT dispatch in
sqlite3VdbeFinishMoveto")

Root cause was `OP_FinishSeek` (emitted by DELETE's VDBE when WHERE used a
deferred seek) calling `sqlite3VdbeFinishMoveto`, which asserted
`CURTYPE_BTREE` and then called `sqlite3BtreeTableMoveto` on the MOT cursor's
union slot (`uc.pMotCursor`). With `SQLITE_DEBUG` off, the assert is a no-op
and we segfaulted inside btree's `moveToRoot`. Fix added `CURTYPE_MOT`
dispatch at the top of `sqlite3VdbeFinishMoveto` that calls `oroMotSeekRowid`
instead and clears `deferredMoveto`. Regression covered by the
`TestDeleteOverWire` script in `test/harness/`.

---

### 7. ~~Crash recovery unverified~~ — VERIFIED
**Status**: kill-9 oracle is `test/harness/run_crash_loop.sh`. Each
iteration: spawn server → writer client commits IDs one by one, logging
each successful commit to an ack file → kill -9 at random 50–500 ms →
restart on the same DB file (no clean) → query for every ack'd ID. 5/5
iterations green; can scale up via the iteration argument. The
recovery path is `oroMotWalRecover` (`sqlite/oro_mot_adapter.cpp:1723`),
which is idempotent (UNIQUE_VIOLATION on replayed INSERT counts as success).

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
