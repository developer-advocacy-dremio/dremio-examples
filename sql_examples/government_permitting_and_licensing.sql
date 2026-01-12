/*
 * Government Permitting & Licensing Demo
 * 
 * Scenario:
 * Tracking turnaround times for building permits and zoning requests.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Permits;
CREATE FOLDER IF NOT EXISTS RetailDB.Permits.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Permits.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Permits.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Permits.Bronze.Applications (
    AppID INT,
    PermitType VARCHAR, -- Residential, Commercial, Renovation
    SubmitDate DATE,
    ApprovalDate DATE,
    ZoneCode VARCHAR
);

INSERT INTO RetailDB.Permits.Bronze.Applications VALUES
(1, 'Residential', '2025-01-01', '2025-01-10', 'R1'),
(2, 'Residential', '2025-01-01', '2025-01-05', 'R1'),
(3, 'Commercial', '2025-01-02', '2025-01-25', 'C2'), -- Long
(4, 'Renovation', '2025-01-03', '2025-01-04', 'R1'),
(5, 'Commercial', '2025-01-05', NULL, 'C1'), -- Pending
(6, 'Residential', '2025-01-06', '2025-01-15', 'R2'),
(7, 'Renovation', '2025-01-07', '2025-01-09', 'R2'),
(8, 'Commercial', '2025-01-10', '2025-02-01', 'C2'),
(9, 'Residential', '2025-01-12', '2025-01-18', 'R1'),
(10, 'Renovation', '2025-01-15', NULL, 'R3'), -- Pending
(11, 'Commercial', '2025-01-15', NULL, 'C1');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Permits.Silver.TurnaroundMetrics AS
SELECT 
    AppID,
    PermitType,
    ZoneCode,
    CASE 
        WHEN ApprovalDate IS NULL THEN 'Pending'
        ELSE 'Approved'
    END AS Status,
    DATE_DIFF(CAST(ApprovalDate AS TIMESTAMP), CAST(SubmitDate AS TIMESTAMP)) AS DaysToApprove
FROM RetailDB.Permits.Bronze.Applications;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Permits.Gold.ZonePerformance AS
SELECT 
    ZoneCode,
    COUNT(*) AS TotalApps,
    AVG(DaysToApprove) AS AvgApprovalDays,
    SUM(CASE WHEN Status = 'Pending' THEN 1 ELSE 0 END) AS PendingCount
FROM RetailDB.Permits.Silver.TurnaroundMetrics
GROUP BY ZoneCode;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Rank zones by AvgApprovalDays ascending (fastest first) from RetailDB.Permits.Gold.ZonePerformance."
*/
