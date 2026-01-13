/*
 * Cloud Infrastructure Cost (FinOps) Demo
 * 
 * Scenario:
 * Analyzing cloud spend across compute, storage, and networking to identify waste.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Reduce monthly cloud bill by identifying idle resources and right-sizing.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS CloudCostDB;
CREATE FOLDER IF NOT EXISTS CloudCostDB.Bronze;
CREATE FOLDER IF NOT EXISTS CloudCostDB.Silver;
CREATE FOLDER IF NOT EXISTS CloudCostDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CloudCostDB.Bronze.BillingData (
    ResourceID VARCHAR,
    ServiceName VARCHAR, -- 'EC2', 'S3', 'RDS'
    Region VARCHAR,
    UsageDate DATE,
    UsageAmount DOUBLE, -- Hours or GB
    Cost DOUBLE,
    Tags VARCHAR -- JSON-like string '{"Team": "Eng", "Env": "Prod"}'
);

CREATE TABLE IF NOT EXISTS CloudCostDB.Bronze.Utilization (
    ResourceID VARCHAR,
    AvgCPU DOUBLE,
    MaxCPU DOUBLE,
    AvgMemory DOUBLE,
    LastActive DATE
);

INSERT INTO CloudCostDB.Bronze.BillingData VALUES
('i-12345', 'EC2', 'us-east-1', '2025-06-01', 24.0, 12.50, 'Team:Data,Env:Dev'),
('i-67890', 'EC2', 'us-west-2', '2025-06-01', 24.0, 48.00, 'Team:Web,Env:Prod'),
('vol-abcde', 'EBS', 'us-east-1', '2025-06-01', 100.0, 10.00, 'Team:Data,Env:Dev'),
('db-xyz', 'RDS', 'us-east-1', '2025-06-01', 24.0, 35.00, 'Team:Web,Env:Prod');

INSERT INTO CloudCostDB.Bronze.Utilization VALUES
('i-12345', 2.5, 10.0, 15.0, '2025-06-01'), -- Idle dev box?
('i-67890', 45.0, 80.0, 60.0, '2025-06-01'), -- Healthy
('db-xyz', 30.0, 50.0, 40.0, '2025-06-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CloudCostDB.Silver.EnrichedCosts AS
SELECT 
    b.ResourceID,
    b.ServiceName,
    b.Cost,
    b.Tags,
    u.AvgCPU,
    u.MaxCPU,
    CASE 
        WHEN b.ServiceName = 'EC2' AND u.MaxCPU < 20 THEN 'Idle/Oversized'
        WHEN u.LastActive < DATE_SUB(CURRENT_DATE, 30) THEN 'Zombie'
        ELSE 'Active'
    END AS EfficiencyStatus
FROM CloudCostDB.Bronze.BillingData b
LEFT JOIN CloudCostDB.Bronze.Utilization u ON b.ResourceID = u.ResourceID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CloudCostDB.Gold.OptimizationOpportunities AS
SELECT 
    ServiceName,
    EfficiencyStatus,
    COUNT(ResourceID) AS ResourceCount,
    SUM(Cost) AS PotentialSavings
FROM CloudCostDB.Silver.EnrichedCosts
WHERE EfficiencyStatus != 'Active'
GROUP BY ServiceName, EfficiencyStatus;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate the total PotentialSavings from 'Idle/Oversized' resources in CloudCostDB.Gold.OptimizationOpportunities."
*/
