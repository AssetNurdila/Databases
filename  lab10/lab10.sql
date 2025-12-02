DROP DATABASE IF EXISTS advanced_lab;

CREATE DATABASE advanced_lab;

\c advanced_lab

DO
$$
DECLARE
    r RECORD;
BEGIN
    -- Перебираем все таблицы в public-схеме
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END;
$$;
DROP DATABASE advanced_lab;
CREATE TABLE accounts (
id SERIAL PRIMARY KEY,
name VARCHAR(100) NOT NULL,
balance DECIMAL(10, 2) DEFAULT 0.00
);
CREATE TABLE products (
id SERIAL PRIMARY KEY,
shop VARCHAR(100) NOT NULL,
product VARCHAR(100) NOT NULL,
price DECIMAL(10, 2) NOT NULL
);
-- Insert test data
INSERT INTO accounts (name, balance) VALUES
('Alice', 1000.00),
('Bob', 500.00),
('Wally', 750.00);
INSERT INTO products (shop, product, price) VALUES
('Joe''s Shop', 'Coke', 2.50),
('Joe''s Shop', 'Pepsi', 3.00);
----------------------------------------
-- TASK 1
----------------------------------------
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE name='Alice';
UPDATE accounts SET balance = balance + 100 WHERE name='Bob';
COMMIT;

-- ANSWERS:
-- a) Alice = 900, Bob = 600
-- b) Both operations must be atomic; money transfer must not be partial
-- c) Alice would lose 100 while Bob gets nothing (inconsistent state)


----------------------------------------
-- TASK 2
----------------------------------------
BEGIN;
UPDATE accounts SET balance = balance - 500 WHERE name='Alice';
-- SELECT shows: 500
ROLLBACK;
-- SELECT shows: 1000

-- ANSWERS:
-- a) 500
-- b) 1000
-- c) ROLLBACK is used when an error/incorrect update occurs


----------------------------------------
-- TASK 3
----------------------------------------
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE name='Alice';
SAVEPOINT sp;
UPDATE accounts SET balance = balance + 100 WHERE name='Bob';
ROLLBACK TO sp;
UPDATE accounts SET balance = balance + 100 WHERE name='Wally';
COMMIT;

-- ANSWERS:
-- a) Alice = 900, Bob = 500, Wally = 850
-- b) Bob was credited temporarily but rolled back before commit
-- c) SAVEPOINT allows partial rollback instead of rolling back entire transaction


----------------------------------------
-- TASK 4 (READ COMMITTED / SERIALIZABLE)
----------------------------------------

-- SCENARIO A (READ COMMITTED)

-- Terminal 1:
-- BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- COMMIT;

-- Terminal 2:
-- BEGIN;
-- DELETE FROM products WHERE shop='Joe''s Shop';
-- INSERT INTO products VALUES ('Joe''s Shop','Fanta',3.50);
-- COMMIT;

-- SCENARIO B (SERIALIZABLE)
-- Terminal 1:
-- BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- COMMIT;

-- ANSWERS:
-- a) READ COMMITTED: first SELECT sees old data, second SELECT sees new data
-- b) SERIALIZABLE: Terminal 1 always sees a consistent snapshot or gets a serialization error
-- c) READ COMMITTED is weaker; SERIALIZABLE ensures full isolation


----------------------------------------
-- TASK 5 (Phantom Reads)
----------------------------------------

-- Terminal 1:
-- BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
-- SELECT MAX(price), MIN(price) FROM products WHERE shop='Joe''s Shop';
-- SELECT MAX(price), MIN(price) FROM products WHERE shop='Joe''s Shop';
-- COMMIT;

-- Terminal 2:
-- BEGIN;
-- INSERT INTO products VALUES ('Joe''s Shop','Sprite',4.00);
-- COMMIT;

-- ANSWERS:
-- a) Terminal 1 will NOT see the new row (snapshot fixed)
-- b) Phantom read = new rows appearing between two identical queries
-- c) SERIALIZABLE prevents phantom reads


----------------------------------------
-- TASK 6 (Dirty Reads)
----------------------------------------

-- Terminal 1:
-- BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- SELECT * FROM products WHERE shop='Joe''s Shop';
-- COMMIT;

-- Terminal 2:
-- BEGIN;
-- UPDATE products SET price=99.99 WHERE product='Fanta';
-- ROLLBACK;

-- ANSWERS:
-- a) Yes, Terminal 1 may see 99.99 (dirty read)
-- b) Dirty read = reading uncommitted changes
-- c) READ UNCOMMITTED is unsafe and should be avoided


----------------------------------------
-- EXERCISE 1
----------------------------------------
DO $$
DECLARE
    bob_balance numeric;
BEGIN
    SELECT balance INTO bob_balance FROM accounts WHERE name = 'Bob' FOR UPDATE;

    IF bob_balance >= 200 THEN
        UPDATE accounts SET balance = balance - 200 WHERE name='Bob';
        UPDATE accounts SET balance = balance + 200 WHERE name='Wally';
        RAISE NOTICE 'Transfer successful';
    ELSE
        RAISE NOTICE 'Insufficient funds — transfer cancelled';
        -- Changes are automatically rolled back if you raise an exception,
        -- but here we simply don't apply them.
    END IF;

END $$;
-- ANSWER:
-- Transfer succeeds only if Bob has >= 200


----------------------------------------
-- EXERCISE 2
----------------------------------------
BEGIN;

INSERT INTO products (shop, product, price)
VALUES ('NewShop', 'XProduct', 10.00);

SAVEPOINT sp1;

UPDATE products
SET price = 12.50
WHERE shop='NewShop' AND product='XProduct';

SAVEPOINT sp2;

DELETE FROM products
WHERE shop='NewShop' AND product='XProduct';

ROLLBACK TO sp1;

COMMIT;

-- ANSWER:
-- Final state: XProduct price = 10.00 (insert only)


----------------------------------------
-- EXERCISE 3 (template)
----------------------------------------
-- Terminal A/B:
-- BEGIN;
-- SELECT balance FROM accounts WHERE name='Alice' FOR UPDATE;
-- UPDATE accounts SET balance = balance - 800 WHERE name='Alice';
-- COMMIT;

-- ANSWERS:
-- read committed + FOR UPDATE: one transaction waits, second cannot withdraw 800
-- serializable: may cause serialization failure (must retry)


----------------------------------------
-- EXERCISE 4
----------------------------------------

-- Incorrect (no transaction):
-- SELECT MAX(price) FROM sells WHERE shop='S';
-- SELECT MIN(price) FROM sells WHERE shop='S';

-- Correct (isolated):
-- BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
-- SELECT MAX(price) FROM sells WHERE shop='S';
-- SELECT MIN(price) FROM sells WHERE shop='S';
-- COMMIT;

-- ANSWER:
-- Transactions prevent inconsistent reads such as MAX < MIN
----------------------------------------
-- SELF-ASSESSMENT QUESTIONS (ANSWERS)
----------------------------------------

-- 1. Explain each ACID property with a practical example.
-- A = Atomicity: all steps in a transaction succeed or none. Example: bank transfer.
-- C = Consistency: database moves from valid state to valid state. Example: no negative balances.
-- I = Isolation: concurrent transactions don't see each other's intermediate changes.
-- D = Durability: once committed, changes survive crashes.

-- 2. What is the difference between COMMIT and ROLLBACK?
-- COMMIT saves all changes permanently.
-- ROLLBACK cancels all changes in the current transaction.

-- 3. When would you use a SAVEPOINT instead of a full ROLLBACK?
-- When only part of a transaction should be undone, not the entire block.
-- Example: multi-step operation where only one step fails.

-- 4. Compare and contrast the four SQL isolation levels.
-- READ UNCOMMITTED: allows dirty reads (weakest).
-- READ COMMITTED: sees only committed data; non-repeatable reads allowed.
-- REPEATABLE READ: snapshot fixed; prevents non-repeatable reads.
-- SERIALIZABLE: strongest; transactions behave as if executed one-by-one.

-- 5. What is a dirty read and which isolation level allows it?
-- Dirty read: reading uncommitted changes from another transaction.
-- Only allowed in READ UNCOMMITTED.

-- 6. What is a non-repeatable read? Give an example scenario.
-- Same SELECT inside one transaction returns different values.
-- Example: T1 reads balance=100, T2 updates balance to 200 and commits, T1 reads again → 200.

-- 7. What is a phantom read? Which isolation levels prevent it?
-- Phantom read: new rows appear between identical queries in the same transaction.
-- Prevented by SERIALIZABLE.

-- 8. Why choose READ COMMITTED over SERIALIZABLE in high traffic?
-- READ COMMITTED has fewer locks, better performance, fewer serialization failures.

-- 9. How do transactions maintain consistency during concurrent access?
-- By grouping operations atomically and isolating snapshots so no partial/inconsistent states appear.

-- 10. What happens to uncommitted changes if the system crashes?
-- They are lost (rolled back automatically). Only committed data is recovered.
