-- Database Creation

DROP DATABASE IF EXISTS university_main;
CREATE DATABASE university_main
    ENCODING  'UTF8'
    TEMPLATE  template0;

ALTER DATABASE university_main OWNER TO CURRENT_USER;

DROP DATABASE IF EXISTS university_archive;
CREATE DATABASE university_archive
    TEMPLATE        template0
    CONNECTION LIMIT 50;

DROP DATABASE IF EXISTS university_test;
CREATE DATABASE university_test
    CONNECTION LIMIT 10
    IS_TEMPLATE TRUE;

-- Tablespaces

DROP TABLESPACE IF EXISTS student_data;
CREATE TABLESPACE student_data
    LOCATION '/usr/local/pgsql/tablespaces/students';

DROP TABLESPACE IF EXISTS course_data;
CREATE TABLESPACE course_data
    LOCATION '/usr/local/pgsql/tablespaces/courses';

ALTER TABLESPACE course_data OWNER TO CURRENT_USER;

DROP DATABASE IF EXISTS university_distributed;
CREATE DATABASE university_distributed
    ENCODING   'LATIN9'
    LC_COLLATE 'C'
    LC_CTYPE   'C'
    TEMPLATE   template0
    TABLESPACE student_data;


-- Task 2.1 Base Tables

DROP TABLE IF EXISTS students;
CREATE TABLE students (
    student_id       SERIAL PRIMARY KEY,
    first_name       VARCHAR(50),
    middle_name      VARCHAR(30),
    last_name        VARCHAR(50),
    email            VARCHAR(100),
    phone            VARCHAR(20),
    date_of_birth    DATE,
    enrollment_date  DATE,
    gpa              NUMERIC(4,2) DEFAULT 0.00,
    student_status   VARCHAR(20) DEFAULT 'ACTIVE',
    is_active        BOOLEAN,
    graduation_year  SMALLINT,
    advisor_id       INTEGER
);

DROP TABLE IF EXISTS professors;
CREATE TABLE professors (
    professor_id        SERIAL PRIMARY KEY,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),
    office_number       VARCHAR(20),
    hire_date           DATE,
    salary              NUMERIC(14,2),
    is_tenured          BOOLEAN DEFAULT FALSE,
    years_experience    SMALLINT,
    department_code     CHAR(5),
    research_area       TEXT,
    last_promotion_date DATE,
    department_id       INTEGER
);

DROP TABLE IF EXISTS courses;
CREATE TABLE courses (
    course_id              SERIAL PRIMARY KEY,
    course_code            VARCHAR(10),
    course_title           VARCHAR(100),
    description            TEXT,
    credits                SMALLINT DEFAULT 3,
    max_enrollment         INTEGER,
    course_fee             NUMERIC(10,2),
    is_online              BOOLEAN,
    created_at             TIMESTAMP WITHOUT TIME ZONE,
    prerequisite_course_id INTEGER,
    difficulty_level       SMALLINT,
    lab_required           BOOLEAN DEFAULT FALSE,
    department_id          INTEGER
);

-- Task 2.2 Schedules & Records

DROP TABLE IF EXISTS class_schedule;
CREATE TABLE class_schedule (
    schedule_id      SERIAL PRIMARY KEY,
    course_id        INTEGER,
    professor_id     INTEGER,
    classroom        VARCHAR(30),
    class_date       DATE,
    start_time       TIME WITHOUT TIME ZONE,
    end_time         TIME WITHOUT TIME ZONE,
    session_type     VARCHAR(15),
    room_capacity    INTEGER,
    equipment_needed TEXT
);

DROP TABLE IF EXISTS student_records;
CREATE TABLE student_records (
    record_id             SERIAL PRIMARY KEY,
    student_id            INTEGER,
    course_id             INTEGER,
    semester              VARCHAR(20),
    year                  INTEGER,
    grade                 VARCHAR(5),
    attendance_percentage NUMERIC(4,1),
    submission_timestamp  TIMESTAMP WITH TIME ZONE,
    extra_credit_points   NUMERIC(4,1) DEFAULT 0.0,
    final_exam_date       DATE
);


-- Task 4.1 Additional Entities

DROP TABLE IF EXISTS departments;
CREATE TABLE departments (
    department_id    SERIAL PRIMARY KEY,
    department_name  VARCHAR(100),
    department_code  CHAR(5),
    building         VARCHAR(50),
    phone            VARCHAR(15),
    budget           NUMERIC(14,2),
    established_year INTEGER
);

-- пересоздаём grade_scale с описанием
DROP TABLE IF EXISTS grade_scale;
CREATE TABLE grade_scale (
    grade_id        SERIAL PRIMARY KEY,
    letter_grade    CHAR(2),
    min_percentage  NUMERIC(4,1),
    max_percentage  NUMERIC(4,1),
    gpa_points      NUMERIC(4,2),
    description     TEXT
);

DROP TABLE IF EXISTS semester_calendar CASCADE;
CREATE TABLE semester_calendar (
    semester_id            SERIAL PRIMARY KEY,
    semester_name          VARCHAR(20),
    academic_year          INTEGER,
    start_date             DATE,
    end_date               DATE,
    registration_deadline  TIMESTAMP WITH TIME ZONE,
    is_current             BOOLEAN
);

-- Task 5.2 Cleanup & Backup

UPDATE pg_database
   SET datistemplate = FALSE
 WHERE datname = 'university_test';

DROP DATABASE IF EXISTS university_test;
DROP DATABASE IF EXISTS university_distributed;

-- завершаем активные подключения перед удалением
SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
 WHERE datname = 'university_main'
   AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS university_backup;
CREATE DATABASE university_backup
    TEMPLATE university_main;