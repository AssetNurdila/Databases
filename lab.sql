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


-- Lab.sql
-- Author: Nurdilla Aset
-- Student ID: 24B031945
-- Date: 2025-10-14

-- ==================================================================
-- Part 1: CHECK Constraints
-- Task 1.1: Basic CHECK Constraint: employees
-- ==================================================================
DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees (
    employee_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER CHECK (age BETWEEN 18 AND 65), -- ensure age in [18,65]
    salary NUMERIC CHECK (salary > 0)           -- salary must be positive
);

-- Insert valid rows (at least 2)
INSERT INTO employees (employee_id, first_name, last_name, age, salary) VALUES
(1, 'Aida', 'Bek', 30, 45000),
(2, 'Daniyar', 'Suleiman', 45, 72000);

-- Attempt to insert invalid rows (commented out):
-- INSERT INTO employees VALUES (3, 'Invalid', 'Young', 16, 30000); -- violates CHECK (age BETWEEN 18 AND 65)
-- INSERT INTO employees VALUES (4, 'Invalid', 'ZeroSalary', 25, 0);  -- violates CHECK (salary > 0)
-- Violations documented: first fails because age=16 < 18; second fails because salary must be > 0.


-- ==================================================================
-- Task 1.2: Named CHECK Constraint: products_catalog
-- ==================================================================
DROP TABLE IF EXISTS products_catalog CASCADE;
CREATE TABLE products_catalog (
    product_id INTEGER,
    product_name TEXT,
    regular_price NUMERIC,
    discount_price NUMERIC,
    CONSTRAINT valid_discount CHECK (
        regular_price > 0
        AND discount_price > 0
        AND discount_price < regular_price
    )
);

-- Valid inserts
INSERT INTO products_catalog (product_id, product_name, regular_price, discount_price) VALUES
(101, 'Wireless Mouse', 25.00, 20.00),
(102, 'Keyboard', 40.00, 35.00);

-- Invalid attempts (commented):
-- INSERT INTO products_catalog VALUES (103, 'Bad1', 0, 0);        -- violates regular_price > 0 and discount_price > 0
-- INSERT INTO products_catalog VALUES (104, 'Bad2', 50, 60);       -- violates discount_price < regular_price
-- Violations: product 103 fails because prices must be > 0; product 104 fails because discount >= regular.


-- ==================================================================
-- Task 1.3: Multiple Column CHECK: bookings
-- ==================================================================
DROP TABLE IF EXISTS bookings CASCADE;
CREATE TABLE bookings (
    booking_id INTEGER,
    check_in_date DATE,
    check_out_date DATE,
    num_guests INTEGER CHECK (num_guests BETWEEN 1 AND 10),
    CHECK (check_out_date > check_in_date) -- ensures checkout after checkin
);

-- Valid inserts
INSERT INTO bookings (booking_id, check_in_date, check_out_date, num_guests) VALUES
(201, '2025-11-01', '2025-11-05', 2),
(202, '2025-12-20', '2025-12-25', 4);

-- Invalid inserts (commented):
-- INSERT INTO bookings VALUES (203, '2025-11-10', '2025-11-09', 2); -- violates check_out_date > check_in_date
-- INSERT INTO bookings VALUES (204, '2025-11-10', '2025-11-12', 0); -- violates num_guests BETWEEN 1 AND 10
-- Violations documented above.


-- ==================================================================
-- Part 2: NOT NULL Constraints
-- Task 2.1: NOT NULL Implementation: customers
-- ==================================================================
DROP TABLE IF EXISTS customers CASCADE;
CREATE TABLE customers (
    customer_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    phone TEXT, -- nullable
    registration_date DATE NOT NULL
);

-- Valid inserts
INSERT INTO customers (customer_id, email, phone, registration_date) VALUES
(1, 'ali@example.com', '+7-701-000-0001', '2025-01-15'),
(2, 'nursultan@example.com', NULL, '2025-02-10'); -- phone can be NULL

-- Invalid attempts (commented):
-- INSERT INTO customers VALUES (3, NULL, '+7-701-000-0003', '2025-03-01'); -- violates NOT NULL on email
-- INSERT INTO customers VALUES (NULL, 'noid@example.com', NULL, '2025-03-02'); -- violates NOT NULL on customer_id
-- Notes: those would fail with NOT NULL constraint violation.


-- ==================================================================
-- Task 2.2: Combining Constraints: inventory
-- ==================================================================
DROP TABLE IF EXISTS inventory CASCADE;
CREATE TABLE inventory (
    item_id INTEGER NOT NULL,
    item_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    unit_price NUMERIC NOT NULL CHECK (unit_price > 0),
    last_updated TIMESTAMP NOT NULL
);

-- Valid inserts
INSERT INTO inventory (item_id, item_name, quantity, unit_price, last_updated) VALUES
(1, 'USB Cable', 100, 5.50, '2025-09-01 10:00:00'),
(2, 'Charger', 50, 12.90, '2025-09-02 11:00:00');

-- Invalid attempts (commented):
-- INSERT INTO inventory VALUES (3, 'BadQty', -5, 3.00, '2025-09-03 12:00:00'); -- violates quantity >= 0
-- INSERT INTO inventory VALUES (4, 'BadPrice', 10, 0, '2025-09-04 12:00:00'); -- violates unit_price > 0
-- INSERT INTO inventory VALUES (NULL, 'NoID', 10, 1.00, '2025-09-05 12:00:00'); -- violates NOT NULL item_id

-- Task 2.3: Testing NOT NULL
-- Insert records with NULLs in nullable columns (phone is nullable in customers):
INSERT INTO customers (customer_id, email, phone, registration_date) VALUES
(3, 'meri@example.com', NULL, '2025-05-05');


-- ==================================================================
-- Part 3: UNIQUE Constraints
-- Task 3.1: Single Column UNIQUE: users
-- ==================================================================
DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    user_id INTEGER,
    username TEXT UNIQUE,
    email TEXT UNIQUE,
    created_at TIMESTAMP
);

-- Valid inserts
INSERT INTO users (user_id, username, email, created_at) VALUES
(1, 'ali', 'ali@domain.com', '2025-01-01 09:00:00'),
(2, 'nura', 'nura@domain.com', '2025-02-01 10:00:00');

-- Invalid attempts (commented):
-- INSERT INTO users VALUES (3, 'ali', 'ali2@domain.com', now()); -- violates UNIQUE on username
-- INSERT INTO users VALUES (4, 'someone', 'ali@domain.com', now()); -- violates UNIQUE on email


-- ==================================================================
-- Task 3.2: Multi-Column UNIQUE: course_enrollments
-- ==================================================================
DROP TABLE IF EXISTS course_enrollments CASCADE;
CREATE TABLE course_enrollments (
    enrollment_id INTEGER,
    student_id INTEGER,
    course_code TEXT,
    semester TEXT,
    UNIQUE (student_id, course_code, semester)
);

-- Valid inserts
INSERT INTO course_enrollments (enrollment_id, student_id, course_code, semester) VALUES
(1, 1001, 'CS101', '2025-Fall'),
(2, 1002, 'CS101', '2025-Fall');

-- Invalid attempt (commented):
-- INSERT INTO course_enrollments VALUES (3, 1001, 'CS101', '2025-Fall'); -- violates UNIQUE (student_id, course_code, semester)
-- This fails because student 1001 already enrolled in CS101 in 2025-Fall.


-- ==================================================================
-- Task 3.3: Named UNIQUE Constraints: modify users
-- ==================================================================
DROP TABLE IF EXISTS users_named CASCADE;
CREATE TABLE users_named (
    user_id INTEGER,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP,
    CONSTRAINT unique_username UNIQUE (username),
    CONSTRAINT unique_email UNIQUE (email)
);

-- Valid inserts
INSERT INTO users_named (user_id, username, email, created_at) VALUES
(1, 'bek', 'bek@example.com', '2025-03-01 12:00:00'),
(2, 'marat', 'marat@example.com', '2025-03-02 13:00:00');

-- Invalid attempts (commented):
-- INSERT INTO users_named VALUES (3, 'bek', 'newbek@example.com', now()); -- violates constraint unique_username
-- INSERT INTO users_named VALUES (4, 'new', 'marat@example.com', now());   -- violates constraint unique_email


-- ==================================================================
-- Part 4: PRIMARY KEY Constraints
-- Task 4.1: Single Column Primary Key: departments
-- ==================================================================
DROP TABLE IF EXISTS departments CASCADE;
CREATE TABLE departments (
    dept_id INTEGER PRIMARY KEY,
    dept_name TEXT NOT NULL,
    location TEXT
);

-- Valid inserts
INSERT INTO departments (dept_id, dept_name, location) VALUES
(10, 'Computer Science', 'Building A'),
(20, 'Mathematics', 'Building B'),
(30, 'Physics', 'Building C');

-- Invalid attempts (commented):
-- INSERT INTO departments VALUES (10, 'Duplicate', 'Nowhere'); -- violates PK duplicate dept_id
-- INSERT INTO departments VALUES (NULL, 'NoID', 'Nowhere');     -- violates PRIMARY KEY NOT NULL requirement


-- ==================================================================
-- Task 4.2: Composite Primary Key: student_courses
-- ==================================================================
DROP TABLE IF EXISTS student_courses CASCADE;
CREATE TABLE student_courses (
    student_id INTEGER,
    course_id INTEGER,
    enrollment_date DATE,
    grade TEXT,
    PRIMARY KEY (student_id, course_id)
);

-- Valid inserts
INSERT INTO student_courses (student_id, course_id, enrollment_date, grade) VALUES
(1001, 501, '2025-09-01', 'A'),
(1002, 501, '2025-09-01', 'B');

-- Invalid attempt (commented):
-- INSERT INTO student_courses VALUES (1001, 501, '2025-09-01', 'A+'); -- violates composite PK duplicate

-- Task 4.3: Comparison Exercise (written explanation below as SQL comments)
/*
1. Difference between UNIQUE and PRIMARY KEY:
   - PRIMARY KEY enforces uniqueness and NOT NULL (one per table).
   - UNIQUE enforces uniqueness but allows NULLs (unless column is declared NOT NULL).
   - A table may have multiple UNIQUE constraints but only one PRIMARY KEY.

2. When to use single-column vs composite PRIMARY KEY:
   - Use single-column PK when a single attribute uniquely identifies a row (e.g., id).
   - Use composite PK when uniqueness depends on a combination of columns (e.g., student_id + course_id).

3. Why only one PRIMARY KEY but multiple UNIQUE constraints:
   - PRIMARY KEY defines the table's main identifier; relational model allows one canonical identifier.
   - UNIQUE constraints are additional uniqueness rules for other columns or column combinations.
*/


-- ==================================================================
-- Part 5: FOREIGN KEY Constraints
-- Task 5.1: Basic Foreign Key: employees_dept referencing departments
-- ==================================================================
DROP TABLE IF EXISTS employees_dept CASCADE;
CREATE TABLE employees_dept (
    emp_id INTEGER PRIMARY KEY,
    emp_name TEXT NOT NULL,
    dept_id INTEGER REFERENCES departments(dept_id), -- foreign key to departments
    hire_date DATE
);

-- Valid inserts (dept_id exists)
INSERT INTO employees_dept (emp_id, emp_name, dept_id, hire_date) VALUES
(100, 'Aisha', 10, '2025-02-01'),
(101, 'Ruslan', 20, '2025-03-01');

-- Invalid attempt (commented):
-- INSERT INTO employees_dept VALUES (102, 'Ghost', 999, '2025-04-01'); -- violates FK: dept_id 999 does not exist in departments


-- ==================================================================
-- Task 5.2: Multiple Foreign Keys: library schema
-- ==================================================================
DROP TABLE IF EXISTS books CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS publishers CASCADE;

CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY,
    author_name TEXT NOT NULL,
    country TEXT
);

CREATE TABLE publishers (
    publisher_id INTEGER PRIMARY KEY,
    publisher_name TEXT NOT NULL,
    city TEXT
);

CREATE TABLE books (
    book_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    author_id INTEGER REFERENCES authors(author_id),
    publisher_id INTEGER REFERENCES publishers(publisher_id),
    publication_year INTEGER,
    isbn TEXT UNIQUE
);

-- Sample inserts
INSERT INTO authors (author_id, author_name, country) VALUES
(1, 'Ernest Hemingway', 'USA'),
(2, 'Gabriel Garcia Marquez', 'Colombia'),
(3, 'Chinua Achebe', 'Nigeria');

INSERT INTO publishers (publisher_id, publisher_name, city) VALUES
(1, 'Penguin Books', 'London'),
(2, 'HarperCollins', 'New York'),
(3, 'Vintage', 'London');

INSERT INTO books (book_id, title, author_id, publisher_id, publication_year, isbn) VALUES
(1001, 'The Old Man and the Sea', 1, 1, 1952, 'ISBN-0001'),
(1002, 'One Hundred Years of Solitude', 2, 2, 1967, 'ISBN-0002'),
(1003, 'Things Fall Apart', 3, 3, 1958, 'ISBN-0003');

-- Attempt invalid insert (commented):
-- INSERT INTO books VALUES (1004, 'Unknown Author Book', 999, 1, 2020, 'ISBN-0004'); -- violates FK on author_id


-- ==================================================================
-- Task 5.3: ON DELETE Options
-- ==================================================================
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products_fk CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE products_fk (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id INTEGER REFERENCES categories(category_id) ON DELETE RESTRICT
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL
);

CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products_fk(product_id),
    quantity INTEGER CHECK (quantity > 0)
);

-- Sample data
INSERT INTO categories (category_id, category_name) VALUES
(1, 'Electronics'),
(2, 'Books');

INSERT INTO products_fk (product_id, product_name, category_id) VALUES
(1, 'Smartphone', 1),
(2, 'Laptop', 1),
(3, 'Novel', 2);

INSERT INTO orders (order_id, order_date) VALUES
(5001, '2025-10-01'),
(5002, '2025-10-02');

INSERT INTO order_items (item_id, order_id, product_id, quantity) VALUES
(9001, 5001, 1, 2),
(9002, 5001, 3, 1),
(9003, 5002, 2, 1);

-- Tests (explain expected behavior):
-- 1) Try to delete a category that has products (RESTRICT) -> should fail.
--    Example (do not execute if you want to keep data):
--    DELETE FROM categories WHERE category_id = 1; -- would fail because products_fk rows reference it (ON DELETE RESTRICT)
-- 2) Delete an order and observe order_items are deleted (CASCADE):
--    DELETE FROM orders WHERE order_id = 5001; -- this will remove order 5001 and corresponding order_items (9001,9002)
-- 3) If you delete a product referenced by order_items, behavior depends on products_fk foreign key (no ON DELETE specified -> default RESTRICT/NO ACTION in PostgreSQL) so it will fail if order_items reference it.


-- ==================================================================
-- Part 6: Practical Application (E-commerce Database Design)
-- ==================================================================
DROP TABLE IF EXISTS order_details CASCADE;
DROP TABLE IF EXISTS orders_ecom CASCADE;
DROP TABLE IF EXISTS products_ecom CASCADE;
DROP TABLE IF EXISTS customers_ecom CASCADE;

-- customers
CREATE TABLE customers_ecom (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    registration_date DATE NOT NULL
);

-- products
CREATE TABLE products_ecom (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price NUMERIC NOT NULL CHECK (price >= 0), -- price non-negative
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0)
);

-- orders: order status constraint
CREATE TABLE orders_ecom (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers_ecom(customer_id) ON DELETE SET NULL,
    order_date DATE NOT NULL,
    total_amount NUMERIC NOT NULL CHECK (total_amount >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending','processing','shipped','delivered','cancelled'))
);

-- order_details
CREATE TABLE order_details (
    order_detail_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders_ecom(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products_ecom(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC NOT NULL CHECK (unit_price >= 0)
);

-- Sample records (at least 5 per table)
-- customers_ecom
INSERT INTO customers_ecom (customer_id, name, email, phone, registration_date) VALUES
(1, 'Ali Nur', 'ali.nur@example.com', '+7-701-111-1111', '2025-01-10'),
(2, 'Gulzhan', 'gul@example.com', '+7-701-222-2222', '2025-02-12'),
(3, 'Dina', 'dina@example.com', NULL, '2025-03-05'),
(4, 'Marlen', 'marlen@example.com', '+7-701-333-3333', '2025-04-20'),
(5, 'Ermek', 'ermek@example.com', '+7-701-444-4444', '2025-05-15');

-- products_ecom
INSERT INTO products_ecom (product_id, name, description, price, stock_quantity) VALUES
(10, 'Wireless Earbuds', 'Noise-cancelling earbuds', 59.99, 150),
(11, 'Gaming Mouse', 'Ergonomic mouse', 39.99, 200),
(12, 'Mechanical Keyboard', 'RGB keyboard', 89.99, 80),
(13, 'USB-C Cable', '1m cable', 5.99, 1000),
(14, 'Portable Charger', '10000 mAh', 29.99, 300);

-- orders_ecom
INSERT INTO orders_ecom (order_id, customer_id, order_date, total_amount, status) VALUES
(2001, 1, '2025-10-01', 99.98, 'processing'),
(2002, 2, '2025-10-02', 39.99, 'pending'),
(2003, 3, '2025-10-03', 119.98, 'shipped'),
(2004, 4, '2025-10-04', 5.99, 'delivered'),
(2005, 5, '2025-10-05', 29.99, 'cancelled');

-- order_details
INSERT INTO order_details (order_detail_id, order_id, product_id, quantity, unit_price) VALUES
(3001, 2001, 10, 1, 59.99),
(3002, 2001, 13, 2, 5.99),
(3003, 2002, 11, 1, 39.99),
(3004, 2003, 12, 1, 89.99),
(3005, 2003, 13, 1, 5.99);

-- Constraint tests for e-commerce (comments explain expected results):
-- 1) Price and stock_quantity non-negative: Trying to insert product with negative price or stock fails.
--    -- INSERT INTO products_ecom VALUES (15, 'BadProduct', 'bad', -1.00, 10); -- fails price >= 0
--    -- INSERT INTO products_ecom VALUES (16, 'BadStock', 'bad', 1.00, -5); -- fails stock_quantity >= 0
-- 2) Order status constraint: invalid status fails.
--    -- INSERT INTO orders_ecom VALUES (2010, 1, '2025-10-06', 10.00, 'unknown'); -- fails CHECK status IN (...)
-- 3) Quantity in order_details positive: inserting quantity=0 fails.
--    -- INSERT INTO order_details VALUES (4000, 2001, 10, 0, 59.99); -- fails CHECK (quantity > 0)
-- 4) UNIQUE constraint on customer email: inserting duplicate email fails.
--    -- INSERT INTO customers_ecom VALUES (6, 'Copy', 'ali.nur@example.com', NULL, '2025-06-01'); -- fails UNIQUE on email
-- 5) NOT NULL: email and registration_date in customers_ecom cannot be NULL; attempts to insert NULL will fail.


-- ==================================================================
-- Test queries demonstrating constraints (examples)
-- ==================================================================
-- 1) Show employees
-- SELECT * FROM employees;
-- 2) Show products with discount
-- SELECT * FROM products_catalog;
-- 3) Attempt to delete a category that has products (expected to fail):
-- DELETE FROM categories WHERE category_id = 1; -- RESTRICT -> error
-- 4) Delete an order and verify order_items removed:
-- DELETE FROM orders WHERE order_id = 5002; -- this will also delete order_items referencing 5002
-- SELECT * FROM order_items WHERE order_id = 5002; -- should return 0 rows after cascade delete

-- ==================================================================
-- End of SQL file
-- Notes: All intentionally failing INSERT/DELETE statements are commented out so the file can be run from top to bottom
-- to create and populate the schema with valid data. Uncomment failing statements only when testing constraint enforcement.
-- ==================================================================
