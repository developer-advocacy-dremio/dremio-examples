/*
 * HR Recruitment Pipeline Demo
 * 
 * Scenario:
 * Tracking job applications, interview stages, and time-to-hire.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Reduce time-to-fill and identify bottlenecks in the hiring process.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RecruitingDB;
CREATE FOLDER IF NOT EXISTS RecruitingDB.Bronze;
CREATE FOLDER IF NOT EXISTS RecruitingDB.Silver;
CREATE FOLDER IF NOT EXISTS RecruitingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RecruitingDB.Bronze.Positions (
    JobID INT,
    Title VARCHAR,
    Department VARCHAR,
    DateOpened DATE,
    DateFilled DATE
);

CREATE TABLE IF NOT EXISTS RecruitingDB.Bronze.Applications (
    AppID INT,
    JobID INT,
    CandidateID INT,
    ApplyDate DATE,
    Status VARCHAR -- 'New', 'Interview', 'Offer', 'Rejected', 'Hired'
);

INSERT INTO RecruitingDB.Bronze.Positions VALUES
(101, 'Data Engineer', 'Engineering', '2025-01-01', '2025-02-15'),
(102, 'Sales Rep', 'Sales', '2025-01-10', NULL); -- Still open

INSERT INTO RecruitingDB.Bronze.Applications VALUES
(1, 101, 500, '2025-01-05', 'Hired'),
(2, 101, 501, '2025-01-06', 'Rejected'),
(3, 102, 502, '2025-01-12', 'Interview');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RecruitingDB.Silver.PipelineVelocity AS
SELECT 
    p.Title,
    p.Department,
    a.Status,
    a.ApplyDate,
    p.DateOpened,
    TIMESTAMPDIFF(DAY, p.DateOpened, a.ApplyDate) AS DaysSinceOpen
FROM RecruitingDB.Bronze.Applications a
JOIN RecruitingDB.Bronze.Positions p ON a.JobID = p.JobID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RecruitingDB.Gold.TimeToFill AS
SELECT 
    Department,
    COUNT(JobID) AS PositionsFilled,
    AVG(TIMESTAMPDIFF(DAY, DateOpened, DateFilled)) AS AvgDaysToFill
FROM RecruitingDB.Bronze.Positions
WHERE DateFilled IS NOT NULL
GROUP BY Department;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the AvgDaysToFill for the 'Engineering' department in RecruitingDB.Gold.TimeToFill?"
*/
