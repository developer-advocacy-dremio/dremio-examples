/*
 * Government Social Services & Benefits Demo
 * 
 * Scenario:
 * Tracking benefit application processing times and approval rates.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.SocialServices;
CREATE FOLDER IF NOT EXISTS RetailDB.SocialServices.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.SocialServices.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.SocialServices.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.SocialServices.Bronze.Applications (
    AppID INT,
    Program VARCHAR, -- SNAP, Unemployment, Housing
    ApplicantID INT,
    SubmitDate DATE,
    DecisionDate DATE,
    Status VARCHAR -- Approved, Denied, Pending
);

INSERT INTO RetailDB.SocialServices.Bronze.Applications VALUES
(1, 'SNAP', 101, '2025-01-01', '2025-01-05', 'Approved'),
(2, 'SNAP', 102, '2025-01-01', '2025-01-10', 'Approved'),
(3, 'Unemployment', 103, '2025-01-02', '2025-01-03', 'Denied'),
(4, 'Housing', 104, '2025-01-02', '2025-01-20', 'Approved'), -- Slow
(5, 'SNAP', 105, '2025-01-03', '2025-01-06', 'Denied'),
(6, 'Unemployment', 106, '2025-01-03', '2025-01-15', 'Approved'),
(7, 'Housing', 107, '2025-01-04', NULL, 'Pending'),
(8, 'SNAP', 108, '2025-01-05', '2025-01-08', 'Approved'),
(9, 'Unemployment', 109, '2025-01-06', '2025-01-09', 'Approved'),
(10, 'Housing', 110, '2025-01-07', NULL, 'Pending'),
(11, 'SNAP', 111, '2025-01-08', '2025-01-12', 'Denied');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.SocialServices.Silver.ProcessingTimes AS
SELECT 
    AppID,
    Program,
    Status,
    CASE 
        WHEN DecisionDate IS NULL THEN CAST(CURRENT_DATE AS TIMESTAMP)
        ELSE CAST(DecisionDate AS TIMESTAMP) 
    END AS CalcEndDate,
    DATE_DIFF(
        CASE 
            WHEN DecisionDate IS NULL THEN CAST(CURRENT_DATE AS TIMESTAMP)
            ELSE CAST(DecisionDate AS TIMESTAMP) 
        END, 
        CAST(SubmitDate AS TIMESTAMP)
    ) AS DaysInSystem
FROM RetailDB.SocialServices.Bronze.Applications;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.SocialServices.Gold.ProgramEfficiency AS
SELECT 
    Program,
    COUNT(*) AS TotalApps,
    AVG(DaysInSystem) AS AvgProcessingDays,
    SUM(CASE WHEN Status = 'Pending' THEN 1 ELSE 0 END) AS BacklogCount
FROM RetailDB.SocialServices.Silver.ProcessingTimes
GROUP BY Program;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Program has the highest BacklogCount in RetailDB.SocialServices.Gold.ProgramEfficiency?"
*/
