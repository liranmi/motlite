-- bench/tpcb.sql — pgbench TPC-B-like script for motlite.
--
-- Scaled to the 1000-account / 10-teller / 1-branch init in tpcb_init.sql.
-- Each transaction debits a random account and credits a random teller +
-- the (single) branch, then appends to pgbench_history.

\set aid random(1, 1000)
\set bid 1
\set tid random(1, 10)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
