-- verify.sql: check that all data from populate.sql is still there
-- Run with: psql -h localhost -p 5433 -f examples/verify.sql

SELECT 'users row count:' AS label, count(*) AS value FROM users
UNION ALL SELECT 'orders row count:', count(*) FROM orders
UNION ALL SELECT 'audit_log row count:', count(*) FROM audit_log;

-- Point lookup (MOT)
SELECT * FROM users WHERE id = 2;

-- Cross-engine JOIN (MOT + native)
SELECT u.name, o.amount, o.status
FROM users u JOIN orders o ON u.id = o.user_id
ORDER BY u.id, o.id;

-- Aggregate on MOT
SELECT status, count(*), sum(amount) AS total
FROM orders GROUP BY status ORDER BY status;

-- Write + read after restart, to confirm WAL still works
INSERT INTO audit_log VALUES(2, 'verified after restart');
INSERT INTO users VALUES(4, 'Dave', 'dave@example.com');

SELECT * FROM users ORDER BY id;
SELECT * FROM audit_log ORDER BY id;
