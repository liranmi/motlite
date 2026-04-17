-- populate.sql: create MOT + native tables and insert data
-- Run with: psql -h localhost -p 5433 -f examples/populate.sql

-- In-memory MOT table (MassTree + OCC, fast)
CREATE MOT TABLE users (
    id    INTEGER PRIMARY KEY,
    name  TEXT,
    email TEXT
);

CREATE MOT TABLE orders (
    id      INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount  REAL,
    status  TEXT
);

-- Native SQLite table (B-tree, on disk) — coexists in the same DB
CREATE TABLE audit_log (
    id   INTEGER PRIMARY KEY,
    msg  TEXT
);

-- Populate users
INSERT INTO users VALUES(1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES(2, 'Bob',   'bob@example.com');
INSERT INTO users VALUES(3, 'Carol', 'carol@example.com');

-- Populate orders
INSERT INTO orders VALUES(100, 1, 99.99,  'shipped');
INSERT INTO orders VALUES(101, 1, 15.50,  'pending');
INSERT INTO orders VALUES(102, 2, 250.00, 'shipped');
INSERT INTO orders VALUES(103, 3, 42.00,  'shipped');

INSERT INTO audit_log VALUES(1, 'initial data loaded');

-- Show what we wrote
SELECT 'users row count:' AS label, count(*) AS value FROM users
UNION ALL SELECT 'orders row count:', count(*) FROM orders
UNION ALL SELECT 'audit_log row count:', count(*) FROM audit_log;

SELECT u.name, count(o.id) AS orders, sum(o.amount) AS total
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.name
ORDER BY u.name;
