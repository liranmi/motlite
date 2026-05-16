-- bench/tpcb_init.sql — manual TPC-B init for motlite.
--
-- pgbench's own `-i` flow doesn't work yet against oro_server (it relies on
-- comma-list DROP, COPY protocol, and ALTER TABLE ADD PRIMARY KEY — see
-- GAPS.md #18, #16, #9). This script populates the same schema by hand so
-- pgbench can run its built-in TPC-B-like transaction against it.
--
-- Schema matches pgbench's defaults but uses CREATE MOT TABLE so the MOT
-- engine path is exercised. Account count is small (1000) for fast CI;
-- production benchmarking should bump it.

DROP TABLE IF EXISTS pgbench_history;
DROP TABLE IF EXISTS pgbench_accounts;
DROP TABLE IF EXISTS pgbench_tellers;
DROP TABLE IF EXISTS pgbench_branches;

CREATE MOT TABLE pgbench_branches (bid INT PRIMARY KEY, bbalance INT, filler TEXT);
CREATE MOT TABLE pgbench_tellers  (tid INT PRIMARY KEY, bid INT, tbalance INT, filler TEXT);
CREATE MOT TABLE pgbench_accounts (aid INT PRIMARY KEY, bid INT, abalance INT, filler TEXT);
CREATE MOT TABLE pgbench_history  (id INTEGER PRIMARY KEY, tid INT, bid INT, aid INT, delta INT, mtime TEXT);

INSERT INTO pgbench_branches VALUES (1, 0, '');
INSERT INTO pgbench_tellers VALUES
  (1,1,0,''),(2,1,0,''),(3,1,0,''),(4,1,0,''),(5,1,0,''),
  (6,1,0,''),(7,1,0,''),(8,1,0,''),(9,1,0,''),(10,1,0,'');

-- 1000 accounts. Inline batch to keep init under one second.
INSERT INTO pgbench_accounts VALUES
  (1,1,0,''),(2,1,0,''),(3,1,0,''),(4,1,0,''),(5,1,0,''),(6,1,0,''),(7,1,0,''),(8,1,0,''),(9,1,0,''),(10,1,0,'');
-- Then 99 more batches of 10 via a recursive CTE-driven INSERT generator:
-- SQLite supports WITH RECURSIVE which lets us populate the rest without
-- thousands of individual statements.
INSERT INTO pgbench_accounts (aid, bid, abalance, filler)
SELECT n + 10, 1, 0, '' FROM (
  WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 990)
  SELECT n FROM seq
);
