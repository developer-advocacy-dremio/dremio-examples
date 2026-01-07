/*
 * Education Student Performance Demo
 * 
 * Scenario:
 * A university tracks student attendance and grades to identify at-risk students.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Early intervention for students falling behind.
 * 
 * Note: Assumes a catalog named 'EducationDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EducationDB.Bronze;
CREATE FOLDER IF NOT EXISTS EducationDB.Silver;
CREATE FOLDER IF NOT EXISTS EducationDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EducationDB.Bronze.Students (
    StudentID INT,
    Name VARCHAR,
    Major VARCHAR,
    YearLevel INT -- 1=Freshman, 4=Senior
);

CREATE TABLE IF NOT EXISTS EducationDB.Bronze.Courses (
    CourseID VARCHAR,
    Title VARCHAR,
    Credits INT
);

CREATE TABLE IF NOT EXISTS EducationDB.Bronze.Assessments (
    AssessmentID INT,
    CourseID VARCHAR,
    StudentID INT,
    Score INT, -- 0-100
    Type VARCHAR -- 'Midterm', 'Final', 'Quiz'
);

CREATE TABLE IF NOT EXISTS EducationDB.Bronze.Attendance (
    LogID INT,
    StudentID INT,
    CourseID VARCHAR,
    "Date" DATE,
    Status VARCHAR -- 'Present', 'Absent', 'Excused'
);

-- 1.2 Populate Bronze Tables
INSERT INTO EducationDB.Bronze.Students (StudentID, Name, Major, YearLevel) VALUES
(1, 'Alice', 'CS', 3),
(2, 'Bob', 'History', 2),
(3, 'Charlie', 'Math', 1),
(4, 'Diana', 'CS', 4),
(5, 'Evan', 'Physics', 2);

INSERT INTO EducationDB.Bronze.Courses (CourseID, Title, Credits) VALUES
('CS101', 'Intro to Programming', 3),
('HIST200', 'World History', 4),
('MATH101', 'Calculus I', 3);

INSERT INTO EducationDB.Bronze.Assessments (AssessmentID, CourseID, StudentID, Score, Type) VALUES
(1, 'CS101', 1, 95, 'Midterm'),
(2, 'CS101', 4, 88, 'Midterm'),
(3, 'HIST200', 2, 72, 'Midterm'),
(4, 'MATH101', 3, 45, 'Midterm'), -- At risk
(5, 'MATH101', 5, 80, 'Midterm');

INSERT INTO EducationDB.Bronze.Attendance (LogID, StudentID, CourseID, "Date", Status) VALUES
(1, 1, 'CS101', '2025-09-01', 'Present'),
(2, 3, 'MATH101', '2025-09-01', 'Absent'),
(3, 3, 'MATH101', '2025-09-03', 'Absent'), -- Trending absent
(4, 2, 'HIST200', '2025-09-02', 'Present'),
(5, 5, 'MATH101', '2025-09-01', 'Present');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Academic Progress
-------------------------------------------------------------------------------

-- 2.1 View: Student_Course_Summary
-- Combines grades and attendance ratios.
CREATE OR REPLACE VIEW EducationDB.Silver.Student_Course_Summary AS
SELECT
    s.StudentID,
    s.Name,
    c.CourseID,
    AVG(a.Score) AS AvgScore,
    COUNT(att.LogID) AS TotalClasses,
    SUM(CASE WHEN att.Status = 'Absent' THEN 1 ELSE 0 END) AS Absences
FROM EducationDB.Bronze.Students s
JOIN EducationDB.Bronze.Assessments a ON s.StudentID = a.StudentID
JOIN EducationDB.Bronze.Courses c ON a.CourseID = c.CourseID
LEFT JOIN EducationDB.Bronze.Attendance att ON s.StudentID = att.StudentID AND c.CourseID = att.CourseID
GROUP BY s.StudentID, s.Name, c.CourseID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Intervention List
-------------------------------------------------------------------------------

-- 3.1 View: At_Risk_Students
-- Flag students with AvgScore < 60 OR Absences > 2.
CREATE OR REPLACE VIEW EducationDB.Gold.At_Risk_Students AS
SELECT
    Name,
    CourseID,
    AvgScore,
    Absences,
    CASE
        WHEN AvgScore < 60 THEN 'Academic Warning'
        WHEN Absences > 2 THEN 'Attendance Warning'
        ELSE 'Monitor'
    END AS InterventionType
FROM EducationDB.Silver.Student_Course_Summary
WHERE AvgScore < 70 OR Absences > 1;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Intervention):
"List all students from EducationDB.Gold.At_Risk_Students who need 'Academic Warning'."

PROMPT 2 (Grade Distribution):
"What is the average score by CourseID from EducationDB.Silver.Student_Course_Summary?"
*/
