-- =============================================================
-- Laboratory Work 7: SQL Views and Roles (PostgreSQL)
-- =============================================================
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
-- Create tables
-- === Part 1: Database Setup ===
-- Step 1.1: Create Sample Tables
CREATE TABLE employees (
  emp_id INT PRIMARY KEY,
  emp_name VARCHAR(50),
  dept_id INT,
  salary DECIMAL(10,2)
);

CREATE TABLE departments (
  dept_id INT PRIMARY KEY,
  dept_name VARCHAR(50),
  location VARCHAR(50)
);

CREATE TABLE projects (
  project_id INT PRIMARY KEY,
  project_name VARCHAR(50),
  dept_id INT,
  budget DECIMAL(10,2)
);

-- Step 1.2: Insert Sample Data
INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES
(1, 'John Smith', 101, 50000),
(2, 'Jane Doe', 102, 60000),
(3, 'Mike Johnson', 101, 55000),
(4, 'Sarah Williams', 103, 65000),
(5, 'Tom Brown', NULL, 45000);

INSERT INTO departments (dept_id, dept_name, location) VALUES
(101, 'IT', 'Building A'),
(102, 'HR', 'Building B'),
(103, 'Finance', 'Building C'),
(104, 'Marketing', 'Building D');

INSERT INTO projects (project_id, project_name, dept_id, budget) VALUES
(1, 'Website Redesign', 101, 100000),
(2, 'Employee Training', 102, 50000),
(3, 'Budget Analysis', 103, 75000),
(4, 'Cloud Migration', 101, 150000),
(5, 'AI Research', NULL, 200000);

-- === Part 2: Basic Views =====================================

-- 2.1 Simple View
CREATE OR REPLACE VIEW employee_details AS
SELECT
  e.emp_id,
  e.emp_name,
  e.salary,
  e.dept_id,
  d.dept_name,
  d.location
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;

-- 2.2 View with Aggregation
CREATE OR REPLACE VIEW dept_statistics AS
SELECT
  d.dept_id,
  d.dept_name,
  COUNT(e.emp_id) AS employee_count,
  ROUND(AVG(e.salary)::numeric, 2) AS avg_salary,
  MAX(e.salary) AS max_salary,
  MIN(e.salary) AS min_salary
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
GROUP BY d.dept_id, d.dept_name;

-- 2.3 View with Multiple Joins
CREATE OR REPLACE VIEW project_overview AS
SELECT
  p.project_id,
  p.project_name,
  p.budget,
  p.dept_id,
  d.dept_name,
  d.location,
  COALESCE(cnt.team_size, 0) AS team_size
FROM projects p
LEFT JOIN departments d ON p.dept_id = d.dept_id
LEFT JOIN (
  SELECT dept_id, COUNT(emp_id) AS team_size
  FROM employees
  GROUP BY dept_id
) cnt ON cnt.dept_id = d.dept_id;

-- 2.4 View with Filtering
CREATE OR REPLACE VIEW high_earners AS
SELECT emp_id, emp_name, salary, dept_id
FROM employees
WHERE salary > 55000;


-- === Part 3: Managing Views ==================================

-- 3.1 Replace a View (add salary grade)
CREATE OR REPLACE VIEW employee_details AS
SELECT
  e.emp_id,
  e.emp_name,
  e.salary,
  e.dept_id,
  d.dept_name,
  d.location,
  CASE
    WHEN e.salary > 60000 THEN 'High'
    WHEN e.salary > 50000 THEN 'Medium'
    ELSE 'Standard'
  END AS salary_grade
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id;

-- 3.2 Rename View
ALTER VIEW high_earners RENAME TO top_performers;

-- 3.3 Temporary View and Drop
CREATE TEMP VIEW temp_view AS
SELECT emp_id, emp_name, salary
FROM employees
WHERE salary < 50000;
DROP VIEW IF EXISTS temp_view;


-- === Part 4: Updatable Views =================================

-- 4.1 Updatable View
CREATE OR REPLACE VIEW employee_salaries AS
SELECT emp_id, emp_name, dept_id, salary
FROM employees;

-- 4.2 Update Through View
UPDATE employee_salaries
SET salary = 52000
WHERE emp_name = 'John Smith';

-- 4.3 Insert Through View
INSERT INTO employee_salaries (emp_id, emp_name, dept_id, salary)
VALUES (6, 'Alice Johnson', 102, 58000);

-- 4.4 View with CHECK OPTION
CREATE OR REPLACE VIEW it_employees AS
SELECT emp_id, emp_name, dept_id, salary
FROM employees
WHERE dept_id = 101
WITH LOCAL CHECK OPTION;

-- Should fail:
-- INSERT INTO it_employees (emp_id, emp_name, dept_id, salary)
-- VALUES (7, 'Bob Wilson', 103, 60000);


-- === Part 5: Materialized Views ==============================

-- 5.1 Create Materialized View
CREATE MATERIALIZED VIEW dept_summary_mv AS
SELECT
  d.dept_id,
  d.dept_name,
  COALESCE(COUNT(e.emp_id), 0) AS total_employees,
  COALESCE(SUM(e.salary), 0) AS total_salaries,
  COALESCE(p.project_count, 0) AS total_projects,
  COALESCE(p.total_budget, 0) AS total_project_budget
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
LEFT JOIN (
  SELECT dept_id, COUNT(*) AS project_count, SUM(budget) AS total_budget
  FROM projects
  GROUP BY dept_id
) p ON p.dept_id = d.dept_id
GROUP BY d.dept_id, d.dept_name, p.project_count, p.total_budget
WITH DATA;

-- 5.2 Refresh Materialized View
INSERT INTO employees (emp_id, emp_name, dept_id, salary)
VALUES (8, 'Charlie Brown', 101, 54000);
REFRESH MATERIALIZED VIEW dept_summary_mv;

-- 5.3 Concurrent Refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_dept_summary_mv_dept_id
ON dept_summary_mv(dept_id);
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dept_summary_mv;

-- 5.4 Materialized View WITH NO DATA
CREATE MATERIALIZED VIEW project_stats_mv AS
SELECT
  p.project_id,
  p.project_name,
  p.budget,
  d.dept_name,
  COUNT(e.emp_id) AS assigned_employees
FROM projects p
LEFT JOIN departments d ON p.dept_id = d.dept_id
LEFT JOIN employees e ON e.dept_id = p.dept_id
GROUP BY p.project_id, p.project_name, p.budget, d.dept_name
WITH NO DATA;
-- REFRESH MATERIALIZED VIEW project_stats_mv;


-- === Part 6: Database Roles ==================================

-- 6.1 Basic Roles
CREATE ROLE analyst NOLOGIN;
CREATE ROLE data_viewer LOGIN PASSWORD 'viewer123';
CREATE ROLE report_user LOGIN PASSWORD 'report456';

-- 6.2 Roles with Attributes
CREATE ROLE db_creator LOGIN PASSWORD 'creator789' CREATEDB;
CREATE ROLE user_manager LOGIN PASSWORD 'manager101' CREATEROLE;
CREATE ROLE admin_user LOGIN PASSWORD 'admin999' SUPERUSER;

-- 6.3 Grant Privileges
GRANT SELECT ON employees, departments, projects TO analyst;
GRANT ALL PRIVILEGES ON employee_details TO data_viewer;
GRANT SELECT, INSERT ON employees TO report_user;

-- 6.4 Group Roles
CREATE ROLE hr_team NOLOGIN;
CREATE ROLE finance_team NOLOGIN;
CREATE ROLE it_team NOLOGIN;

CREATE ROLE hr_user1 LOGIN PASSWORD 'hr001';
CREATE ROLE hr_user2 LOGIN PASSWORD 'hr002';
CREATE ROLE finance_user1 LOGIN PASSWORD 'fin001';

GRANT hr_team TO hr_user1, hr_user2;
GRANT finance_team TO finance_user1;

GRANT SELECT, UPDATE ON employees TO hr_team;
GRANT SELECT ON dept_statistics TO finance_team;

-- 6.5 Revoke Privileges
REVOKE UPDATE ON employees FROM hr_team;
REVOKE hr_team FROM hr_user2;
REVOKE ALL PRIVILEGES ON employee_details FROM data_viewer;

-- 6.6 Modify Role Attributes
ALTER ROLE analyst WITH LOGIN PASSWORD 'analyst123';
ALTER ROLE user_manager WITH SUPERUSER;
ALTER ROLE analyst WITH PASSWORD NULL;
ALTER ROLE data_viewer WITH CONNECTION LIMIT 5;


-- === Part 7: Advanced Role Management ========================

-- 7.1 Role Hierarchies
CREATE ROLE read_only NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only;

CREATE ROLE junior_analyst LOGIN PASSWORD 'junior123';
CREATE ROLE senior_analyst LOGIN PASSWORD 'senior123';
GRANT read_only TO junior_analyst, senior_analyst;
GRANT INSERT, UPDATE ON employees TO senior_analyst;

-- 7.2 Object Ownership
CREATE ROLE project_manager LOGIN PASSWORD 'pm123';
ALTER VIEW dept_statistics OWNER TO project_manager;
ALTER TABLE projects OWNER TO project_manager;

-- 7.3 Reassign and Drop Roles
CREATE ROLE temp_owner LOGIN PASSWORD 'temp001';
CREATE TABLE temp_table (id INT);
ALTER TABLE temp_table OWNER TO temp_owner;
REASSIGN OWNED BY temp_owner TO postgres;
DROP OWNED BY temp_owner;
DROP ROLE temp_owner;

-- 7.4 Row-Level Security via Views
CREATE OR REPLACE VIEW hr_employee_view AS
SELECT emp_id, emp_name, dept_id, salary
FROM employees
WHERE dept_id = 102;
GRANT SELECT ON hr_employee_view TO hr_team;

CREATE OR REPLACE VIEW finance_employee_view AS
SELECT emp_id, emp_name, salary
FROM employees;
GRANT SELECT ON finance_employee_view TO finance_team;


-- === Part 8: Practical Scenarios =============================

-- 8.1 Department Dashboard View
CREATE OR REPLACE VIEW dept_dashboard AS
SELECT
  d.dept_id,
  d.dept_name,
  d.location,
  COUNT(e.emp_id) AS employee_count,
  ROUND(COALESCE(AVG(e.salary),0)::numeric, 2) AS avg_salary,
  COALESCE(SUM(CASE WHEN p.project_id IS NOT NULL THEN 1 ELSE 0 END),0) AS active_projects,
  COALESCE(SUM(p.budget),0) AS total_project_budget,
  ROUND(
    CASE WHEN COUNT(e.emp_id) = 0 THEN 0
         ELSE (COALESCE(SUM(p.budget),0) / COUNT(e.emp_id))::numeric
    END
  ,2) AS budget_per_employee
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
LEFT JOIN projects p ON p.dept_id = d.dept_id
GROUP BY d.dept_id, d.dept_name, d.location;

-- 8.2 Audit View for High-Value Projects
ALTER TABLE projects
ADD COLUMN IF NOT EXISTS created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE OR REPLACE VIEW high_budget_projects AS
SELECT
  p.project_id,
  p.project_name,
  p.budget,
  d.dept_name,
  p.created_date,
  CASE
    WHEN p.budget > 150000 THEN 'Critical Review Required'
    WHEN p.budget > 100000 THEN 'Management Approval Needed'
    ELSE 'Standard Process'
  END AS approval_status
FROM projects p
LEFT JOIN departments d ON p.dept_id = d.dept_id
WHERE p.budget > 75000;

-- 8.3 Access Control System
CREATE ROLE viewer_role NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO viewer_role;

CREATE ROLE entry_role NOLOGIN;
GRANT viewer_role TO entry_role;
GRANT INSERT ON employees, projects TO entry_role;

CREATE ROLE analyst_role NOLOGIN;
GRANT entry_role TO analyst_role;
GRANT UPDATE ON employees, projects TO analyst_role;

CREATE ROLE manager_role NOLOGIN;
GRANT analyst_role TO manager_role;
GRANT DELETE ON employees, projects TO manager_role;

CREATE ROLE alice LOGIN PASSWORD 'alice123';
CREATE ROLE bob LOGIN PASSWORD 'bob123';
CREATE ROLE charlie LOGIN PASSWORD 'charlie123';

GRANT viewer_role TO alice;
GRANT analyst_role TO bob;
GRANT manager_role TO charlie;

