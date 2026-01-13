/*
 * Government Judicial Case Management Demo
 * 
 * Scenario:
 * Analyzing court docket backlogs, case processing frequency, and case aging.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Courts;
CREATE FOLDER IF NOT EXISTS RetailDB.Courts.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Courts.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Courts.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Courts.Bronze.Cases (
    CaseID INT,
    Type VARCHAR, -- Civil, Criminal, Traffic
    FileDate DATE,
    CloseDate DATE,
    JudgeId INT
);

INSERT INTO RetailDB.Courts.Bronze.Cases VALUES
(1, 'Traffic', '2024-11-01', '2024-12-01', 101),
(2, 'Traffic', '2024-11-15', '2024-11-15', 102),
(3, 'Civil', '2024-06-01', '2025-01-10', 101), -- Long case
(4, 'Criminal', '2024-08-01', NULL, 103), -- Open
(5, 'Civil', '2024-09-01', NULL, 102),
(6, 'Traffic', '2025-01-01', '2025-01-15', 101),
(7, 'Criminal', '2025-01-05', NULL, 103),
(8, 'Traffic', '2025-01-10', '2025-01-10', 101),
(9, 'Civil', '2025-01-12', NULL, 104),
(10, 'Criminal', '2024-05-01', '2025-01-01', 103),
(11, 'Traffic', '2025-01-15', NULL, 102);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Courts.Silver.CaseAging AS
SELECT 
    CaseID,
    Type,
    JudgeId,
    FileDate,
    CloseDate,
    CASE 
        WHEN CloseDate IS NULL THEN 'Open' 
        ELSE 'Closed' 
    END AS Status,
    DATE_DIFF(
        CASE WHEN CloseDate IS NULL THEN CAST(CURRENT_DATE AS TIMESTAMP) ELSE CAST(CloseDate AS TIMESTAMP) END,
        CAST(FileDate AS TIMESTAMP)
    ) AS AgeDays
FROM RetailDB.Courts.Bronze.Cases;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Courts.Gold.DocketBacklog AS
SELECT 
    Type,
    COUNT(*) AS TotalCases,
    SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) AS OpenCases,
    AVG(AgeDays) AS AvgCaseDuration
FROM RetailDB.Courts.Silver.CaseAging
GROUP BY Type;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which case Type has the highest AvgCaseDuration in RetailDB.Courts.Gold.DocketBacklog?"
*/
