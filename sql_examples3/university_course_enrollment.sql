/*
 * Dremio "Messy Data" Challenge: University Course Enrollment
 * 
 * Scenario: 
 * Class registration.
 * 3 Tables: STUDENTS (Meta), COURSES (Catalog), ENROLLMENTS (Fact).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: ENROLLMENTS -> COURSES and STUDENTS.
 * 2. Prereq Check: Ensure Student Year >= Course Min_Year.
 * 3. Class Count: Count(Students) per Course vs Max_Capacity.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Ivy_League;
CREATE FOLDER IF NOT EXISTS Ivy_League.Registrar;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Ivy_League.Registrar.STUDENTS (
    STUDENT_ID VARCHAR,
    NAME VARCHAR,
    YEAR_LVL INT -- 1=Freshman, 4=Senior
);

CREATE TABLE IF NOT EXISTS Ivy_League.Registrar.COURSES (
    COURSE_ID VARCHAR,
    TITLE VARCHAR,
    MIN_YEAR_LVL INT,
    CAPACITY INT
);

CREATE TABLE IF NOT EXISTS Ivy_League.Registrar.ENROLLMENTS (
    ENROLL_ID VARCHAR,
    STUDENT_ID VARCHAR,
    COURSE_ID VARCHAR,
    TS TIMESTAMP,
    STATUS VARCHAR -- 'Enrolled', 'Waitlist'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- STUDENTS
INSERT INTO Ivy_League.Registrar.STUDENTS VALUES
('S-1', 'Freshman Frank', 1), ('S-2', 'Senior Sarah', 4), ('S-3', 'Sophomore Sam', 2);

-- COURSES
INSERT INTO Ivy_League.Registrar.COURSES VALUES
('CS-101', 'Intro to CS', 1, 100), ('CS-400', 'Advanced AI', 4, 10);

-- ENROLLMENTS (50 Rows)
INSERT INTO Ivy_League.Registrar.ENROLLMENTS VALUES
('E-1', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-2', 'S-2', 'CS-400', '2023-01-01 08:00:00', 'Enrolled'),
('E-3', 'S-1', 'CS-400', '2023-01-01 08:05:00', 'Enrolled'), -- Prereq Violation
('E-4', 'S-3', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-5', 'S-3', 'CS-400', '2023-01-01 08:00:00', 'Enrolled'), -- Prereq Violation
('E-6', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'), -- Dupe
('E-7', 'S-1', 'CS-101', '2023-01-01 09:00:00', 'Waitlist'),
('E-8', 'S-2', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-9', 'S-2', 'CS-101', '2023-01-01 08:00:00', 'Waitlist'),
('E-10', 'S-2', 'CS-101', '2023-01-01 08:00:00', 'Dropped'),
('E-11', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-12', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-13', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-14', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-15', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-16', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-17', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-18', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-19', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-20', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-21', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-22', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-23', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-24', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-25', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-26', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-27', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-28', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-29', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-30', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-31', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-32', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-33', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-34', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-35', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-36', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-37', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-38', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-39', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-40', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-41', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-42', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-43', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-44', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-45', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-46', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-47', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-48', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-49', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled'),
('E-50', 'S-1', 'CS-101', '2023-01-01 08:00:00', 'Enrolled');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit enrollment in Ivy_League.Registrar.
 *  
 *  1. Bronze: Raw View of ENROLLMENTS, COURSES, STUDENTS.
 *  2. Silver: 
 *     - Join: ENROLLMENTS -> COURSES and STUDENTS.
 *     - Flag Issues: 1. Student Year < Course Min_Year. 2. Duplicates.
 *  3. Gold: 
 *     - Course Roster: List valid Enrolled students per Course.
 *  
 *  Show the SQL."
 */
