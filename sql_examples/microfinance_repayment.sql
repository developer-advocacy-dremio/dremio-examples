/*
 * Microfinance Repayment Demo
 * 
 * Scenario:
 * Tracking repayment rates for group-based lending in developing markets.
 * "Group Liability" means if one member defaults, the group is responsible.
 * 
 * Data Context:
 * - Groups: Lending circles (Village A Group).
 * - Loans: Individual loans within the group.
 * - Repayments: Weekly collections.
 * 
 * Analytical Goal:
 * Identify groups with failing attendance or partial payments.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MicroFinDB;
CREATE FOLDER IF NOT EXISTS MicroFinDB.Bronze;
CREATE FOLDER IF NOT EXISTS MicroFinDB.Silver;
CREATE FOLDER IF NOT EXISTS MicroFinDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MicroFinDB.Bronze.Groups (
    GroupID VARCHAR,
    Region VARCHAR,
    OfficerID VARCHAR,
    FormationDate DATE
);

CREATE TABLE IF NOT EXISTS MicroFinDB.Bronze.Loans (
    LoanID VARCHAR,
    GroupID VARCHAR,
    MemberName VARCHAR,
    Amount DOUBLE,
    WeeklyDue DOUBLE
);

CREATE TABLE IF NOT EXISTS MicroFinDB.Bronze.Collections (
    CollectionID INT,
    LoanID VARCHAR,
    CollectionDate DATE,
    AmountPaid DOUBLE,
    Attendance VARCHAR -- 'Present', 'Absent'
);

INSERT INTO MicroFinDB.Bronze.Groups VALUES
('G-100', 'Nairobi_North', 'OFF-01', '2024-01-01'),
('G-101', 'Nairobi_South', 'OFF-02', '2024-01-01');

INSERT INTO MicroFinDB.Bronze.Loans VALUES
('L-01', 'G-100', 'Mama A', 100.0, 10.0),
('L-02', 'G-100', 'Mama B', 100.0, 10.0),
('L-03', 'G-100', 'Mama C', 100.0, 10.0),
('L-04', 'G-100', 'Mama D', 100.0, 10.0),
('L-05', 'G-101', 'Member E', 200.0, 20.0),
('L-06', 'G-101', 'Member F', 200.0, 20.0);

INSERT INTO MicroFinDB.Bronze.Collections VALUES
-- Week 1 (Group 100 - Perfect)
(1, 'L-01', '2025-01-07', 10.0, 'Present'),
(2, 'L-02', '2025-01-07', 10.0, 'Present'),
(3, 'L-03', '2025-01-07', 10.0, 'Present'),
(4, 'L-04', '2025-01-07', 10.0, 'Present'),
-- Week 2 (Group 100 - One Absent)
(5, 'L-01', '2025-01-14', 10.0, 'Present'),
(6, 'L-02', '2025-01-14', 0.0, 'Absent'), -- Missed
(7, 'L-03', '2025-01-14', 10.0, 'Present'),
(8, 'L-04', '2025-01-14', 10.0, 'Present'),
-- Week 1 (Group 101)
(9, 'L-05', '2025-01-07', 20.0, 'Present'),
(10, 'L-06', '2025-01-07', 20.0, 'Present'),
(11, 'L-05', '2025-01-14', 20.0, 'Present'),
(12, 'L-06', '2025-01-14', 10.0, 'Present'); -- Partial

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MicroFinDB.Silver.WeeklyPerformance AS
SELECT 
    l.GroupID,
    c.CollectionDate,
    COUNT(l.LoanID) AS MembersCount,
    SUM(CASE WHEN c.Attendance = 'Present' THEN 1 ELSE 0 END) AS AttendanceCount,
    SUM(l.WeeklyDue) AS TotalExpected,
    SUM(c.AmountPaid) AS TotalCollected
FROM MicroFinDB.Bronze.Loans l
JOIN MicroFinDB.Bronze.Collections c ON l.LoanID = c.LoanID
GROUP BY l.GroupID, c.CollectionDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MicroFinDB.Gold.GroupRisk AS
SELECT 
    GroupID,
    CollectionDate,
    (AttendanceCount / MembersCount) * 100 AS AttendancePct,
    (TotalCollected / TotalExpected) * 100 AS RepaymentPct,
    CASE 
        WHEN (TotalCollected / TotalExpected) < 1.0 THEN 'Arrears'
        ELSE 'On Track'
    END AS Status
FROM MicroFinDB.Silver.WeeklyPerformance;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show the RepaymentPct trend for GroupID 'G-100' over time."
*/
