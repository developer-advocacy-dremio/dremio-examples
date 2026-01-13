/*
 * DevOps DORA Metrics Demo
 * 
 * Scenario:
 * Measuring software delivery performance: Deployment Frequency, Lead Time, Failure Rate, MTTR.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Benchmark high-performing engineering teams.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS DevOpsDB;
CREATE FOLDER IF NOT EXISTS DevOpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS DevOpsDB.Silver;
CREATE FOLDER IF NOT EXISTS DevOpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DevOpsDB.Bronze.Deployments (
    DeployID VARCHAR,
    ServiceName VARCHAR,
    Env VARCHAR, -- 'Prod', 'Staging'
    DeployTime TIMESTAMP,
    Status VARCHAR, -- 'Success', 'Failed'
    GitCommitHash VARCHAR
);

CREATE TABLE IF NOT EXISTS DevOpsDB.Bronze.Incidents (
    IncidentID VARCHAR,
    ServiceName VARCHAR,
    StartTime TIMESTAMP,
    ResolvedTime TIMESTAMP,
    Severity VARCHAR -- 'Sev1', 'Sev2'
);

INSERT INTO DevOpsDB.Bronze.Deployments VALUES
('D-101', 'AuthService', 'Prod', '2025-01-01 10:00:00', 'Success', 'abc1234'),
('D-102', 'AuthService', 'Prod', '2025-01-02 14:00:00', 'Failed', 'def5678'),
('D-103', 'PaymentService', 'Prod', '2025-01-03 09:00:00', 'Success', 'ghi9012');

INSERT INTO DevOpsDB.Bronze.Incidents VALUES
('INC-001', 'AuthService', '2025-01-02 14:05:00', '2025-01-02 14:35:00', 'Sev1'); -- Caused by D-102?

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW DevOpsDB.Silver.DailyMetrics AS
SELECT 
    CAST(DeployTime AS DATE) AS Day,
    ServiceName,
    COUNT(*) AS DeployCount,
    SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS FailureCount
FROM DevOpsDB.Bronze.Deployments
WHERE Env = 'Prod'
GROUP BY CAST(DeployTime AS DATE), ServiceName;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

-- DORA Metrics Aggregation
CREATE OR REPLACE VIEW DevOpsDB.Gold.TeamPerformance AS
SELECT 
    ServiceName,
    AVG(DeployCount) AS AvgDailyDeploys, -- Deployment Frequency
    (SUM(FailureCount) / SUM(DeployCount)) * 100 AS ChangeFailureRate
FROM DevOpsDB.Silver.DailyMetrics
GROUP BY ServiceName;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which ServiceName has the highest ChangeFailureRate in DevOpsDB.Gold.TeamPerformance?"
*/
