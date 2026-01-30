/*
 * Education: MOOC Student Retention
 * 
 * Scenario:
 * Analyzing drop-off rates in Massively Open Online Courses (MOOCs) to identify "at-risk" content.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EducationDB;
CREATE FOLDER IF NOT EXISTS EducationDB.OnlineLearning;
CREATE FOLDER IF NOT EXISTS EducationDB.OnlineLearning.Bronze;
CREATE FOLDER IF NOT EXISTS EducationDB.OnlineLearning.Silver;
CREATE FOLDER IF NOT EXISTS EducationDB.OnlineLearning.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Course Data
-------------------------------------------------------------------------------

-- CourseCatalog Table
CREATE TABLE IF NOT EXISTS EducationDB.OnlineLearning.Bronze.CourseCatalog (
    CourseID VARCHAR,
    Title VARCHAR,
    TotalModules INT,
    Instructor VARCHAR,
    Category VARCHAR
);

INSERT INTO EducationDB.OnlineLearning.Bronze.CourseCatalog VALUES
('CS101', 'Intro to Python', 10, 'Dr. Smith', 'Tech'),
('DS201', 'Data Science 101', 12, 'Prof. Lee', 'Data'),
('MKT301', 'Digital Marketing', 8, 'Sarah Jones', 'Business'),
('DES401', 'UX Design', 15, 'Mike Brown', 'Design'),
('BUS501', 'Project Mgmt', 10, 'Karen White', 'Business'),
('CS102', 'Web Dev Bootcamp', 20, 'John Doe', 'Tech'),
('DS202', 'Machine Learning', 14, 'Prof. Lee', 'Data'),
('ART101', 'Art History', 8, 'Dr. Green', 'Arts'),
('MUS101', 'Music Theory', 10, 'Alice Blue', 'Arts'),
('LIT101', 'Classic Lit', 12, 'Tom Gray', 'Humanities');

-- StudentProgress Table: Completion status per module per student
CREATE TABLE IF NOT EXISTS EducationDB.OnlineLearning.Bronze.StudentProgress (
    RecordID INT,
    StudentID INT,
    CourseID VARCHAR,
    ModulesCompleted INT,
    LastActiveDate DATE,
    Status VARCHAR -- Active, Dropped, Completed
);

INSERT INTO EducationDB.OnlineLearning.Bronze.StudentProgress VALUES
(1, 1001, 'CS101', 10, '2025-05-01', 'Completed'),
(2, 1002, 'CS101', 5, '2025-04-15', 'Dropped'),
(3, 1003, 'CS101', 2, '2025-04-01', 'Dropped'),
(4, 1004, 'CS101', 10, '2025-05-02', 'Completed'),
(5, 1005, 'CS101', 9, '2025-05-15', 'Active'),
(6, 1006, 'DS201', 12, '2025-05-10', 'Completed'),
(7, 1007, 'DS201', 3, '2025-04-20', 'Dropped'),
(8, 1008, 'DS201', 12, '2025-05-11', 'Completed'),
(9, 1009, 'DS201', 11, '2025-05-15', 'Active'),
(10, 1010, 'MKT301', 8, '2025-05-05', 'Completed'),
(11, 1011, 'MKT301', 4, '2025-04-25', 'Dropped'),
(12, 1012, 'MKT301', 8, '2025-05-06', 'Completed'),
(13, 1013, 'DES401', 15, '2025-05-20', 'Completed'),
(14, 1014, 'DES401', 2, '2025-04-10', 'Dropped'),
(15, 1015, 'DES401', 1, '2025-04-05', 'Dropped'),
(16, 1016, 'DES401', 14, '2025-05-15', 'Active'),
(17, 1017, 'BUS501', 10, '2025-05-01', 'Completed'),
(18, 1018, 'BUS501', 5, '2025-04-22', 'Dropped'),
(19, 1019, 'CS102', 20, '2025-05-25', 'Completed'),
(20, 1020, 'CS102', 5, '2025-04-12', 'Dropped'),
(21, 1021, 'CS102', 10, '2025-04-30', 'Dropped'),
(22, 1022, 'CS102', 20, '2025-05-26', 'Completed'),
(23, 1023, 'DS202', 14, '2025-05-15', 'Completed'),
(24, 1024, 'DS202', 2, '2025-04-05', 'Dropped'),
(25, 1025, 'DS202', 14, '2025-05-16', 'Completed'),
(26, 1026, 'ART101', 8, '2025-05-01', 'Completed'),
(27, 1027, 'ART101', 3, '2025-04-10', 'Dropped'),
(28, 1028, 'MUS101', 10, '2025-05-05', 'Completed'),
(29, 1029, 'MUS101', 5, '2025-04-20', 'Dropped'),
(30, 1030, 'LIT101', 12, '2025-05-10', 'Completed'),
(31, 1031, 'CS101', 10, '2025-05-03', 'Completed'),
(32, 1032, 'CS101', 8, '2025-05-15', 'Active'),
(33, 1033, 'CS101', 1, '2025-04-01', 'Dropped'),
(34, 1034, 'DS201', 12, '2025-05-12', 'Completed'),
(35, 1035, 'DS201', 6, '2025-04-25', 'Dropped'),
(36, 1036, 'MKT301', 8, '2025-05-07', 'Completed'),
(37, 1037, 'MKT301', 2, '2025-04-15', 'Dropped'),
(38, 1038, 'DES401', 15, '2025-05-21', 'Completed'),
(39, 1039, 'DES401', 8, '2025-05-10', 'Active'),
(40, 1040, 'BUS501', 10, '2025-05-02', 'Completed'),
(41, 1041, 'CS102', 20, '2025-05-27', 'Completed'),
(42, 1042, 'CS102', 2, '2025-04-05', 'Dropped'),
(43, 1043, 'DS202', 14, '2025-05-17', 'Completed'),
(44, 1044, 'DS202', 7, '2025-05-01', 'Active'),
(45, 1045, 'ART101', 8, '2025-05-02', 'Completed'),
(46, 1046, 'ART101', 1, '2025-04-01', 'Dropped'),
(47, 1047, 'MUS101', 10, '2025-05-06', 'Completed'),
(48, 1048, 'MUS101', 6, '2025-05-10', 'Active'),
(49, 1049, 'LIT101', 12, '2025-05-11', 'Completed'),
(50, 1050, 'LIT101', 4, '2025-04-20', 'Dropped');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Course Completion Calculation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EducationDB.OnlineLearning.Silver.EnrichStudentData AS
SELECT 
    s.StudentID,
    s.CourseID,
    c.Title,
    c.Instructor,
    s.ModulesCompleted,
    c.TotalModules,
    (CAST(s.ModulesCompleted AS DOUBLE) / c.TotalModules) * 100 AS ProgressPct,
    s.Status
FROM EducationDB.OnlineLearning.Bronze.StudentProgress s
JOIN EducationDB.OnlineLearning.Bronze.CourseCatalog c ON s.CourseID = c.CourseID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Retention Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EducationDB.OnlineLearning.Gold.CourseRetention AS
SELECT 
    CourseID,
    Title,
    Instructor,
    COUNT(*) AS TotalEnrolled,
    SUM(CASE WHEN Status = 'Dropped' THEN 1 ELSE 0 END) AS TotalDrops,
    SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) AS TotalCompletions,
    (CAST(SUM(CASE WHEN Status = 'Dropped' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100 AS DropRatePct
FROM EducationDB.OnlineLearning.Silver.EnrichStudentData
GROUP BY CourseID, Title, Instructor;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify courses with a drop rate higher than 40% using the EducationDB.OnlineLearning.Gold.CourseRetention view."

PROMPT 2:
"List all active students who have completed more than 80% of their course but haven't finished yet."

PROMPT 3:
"Show the breakdown of student status (Active, Dropped, Completed) for Dr. Smith's courses."
*/
