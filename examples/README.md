# End-to-end persistence test

Verify that MOT data survives a server restart via the WAL.

## Prereqs

- Built: `make debug -sj` from the motlite root
- `psql` installed (`apt install postgresql-client`)

## Walkthrough

### 1. Start the server against a fresh file DB

```bash
rm -f /tmp/oro.db
./build/debug/oro_server --port 5433 --db /tmp/oro.db
```

You should see:
```
oro-db server listening on port 5433
Connect with: psql -h localhost -p 5433
```

Leave this running.

### 2. Populate in another terminal

```bash
psql -h localhost -p 5433 -U test -d test -f examples/populate.sql
```

Expected output:
```
CREATE TABLE
CREATE TABLE
CREATE TABLE
INSERT 0 1
...
      label         | value
--------------------+-------
 users row count:   | 3
 orders row count:  | 4
 audit_log row count: | 1

 name  | orders | total
-------+--------+-------
 Alice |    2   | 115.49
 Bob   |    1   | 250.00
 Carol |    1   |  42.00
```

### 3. Stop the server

Go back to the server terminal and press **Ctrl-C**. You should see:
```
Shutting down...
```

### 4. Restart the server on the same file

```bash
./build/debug/oro_server --port 5433 --db /tmp/oro.db
```

The server replays the MOT WAL on startup (silent — check test results below).

### 5. Verify and add more data

```bash
psql -h localhost -p 5433 -U test -d test -f examples/verify.sql
```

Expected: the counts still show **3 users, 4 orders, 1 audit_log**, the JOIN still works, and new rows (Dave, msg 2) get added.

### 6. One more round-trip

Stop the server (Ctrl-C), restart, and check the counts:

```bash
psql -h localhost -p 5433 -U test -d test -c "SELECT count(*) FROM users"
psql -h localhost -p 5433 -U test -d test -c "SELECT count(*) FROM audit_log"
```

Should return **4** and **2** respectively — the `verify.sql` additions also survived.

## What's happening under the hood

- `CREATE MOT TABLE` persists schema in SQLite's native storage → survives restart automatically
- `INSERT` into a MOT table mirrors into a hidden `_mot_wal` table → SQLite fsyncs it on commit
- On restart, `oro_server` auto-enables WAL and calls `oroMotWalRecover(db)` before serving — this reads `_mot_wal` and replays each INSERT/DELETE against the freshly recreated (empty) MOT tables
- Native tables (like `audit_log`) are untouched — SQLite persists them natively

## Bypassing pgpass

If psql complains about credentials, use:

```bash
PGPASSWORD=x psql -h localhost -p 5433 -U test -d test -f examples/populate.sql
```

`oro_server` accepts any user/password (no auth), so the values don't matter.
