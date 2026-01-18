/*
    Dremio High-Volume SQL Pattern: Healthcare ACO Shared Savings
    
    Business Scenario:
    Accountable Care Organizations (ACOs) are rewarded for keeping costs below a Benchmark
    while maintaining quality. Tracking PMPY (Per Member Per Year) spend is critical.
    
    Data Story:
    We track Claims Costs and the Member Roster.
    
    Medallion Architecture:
    - Bronze: ClaimsCost, MemberRoster.
      *Volume*: 50+ records.
    - Silver: PMPY_Calc (Spend per member).
    - Gold: SavingsProjection (Actual vs Benchmark).
    
    Key Dremio Features:
    - Aggregation
    - Financial Formatting
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareACODB;
CREATE FOLDER IF NOT EXISTS HealthcareACODB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareACODB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareACODB.Gold;
USE HealthcareACODB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareACODB.Bronze.MemberRoster (
    MemberID STRING,
    AttributionYear INT,
    RiskScore DOUBLE -- 1.0 is avg
);

-- Bulk Members
INSERT INTO HealthcareACODB.Bronze.MemberRoster
SELECT 
  'M' || CAST(rn + 100 AS STRING),
  2024,
  1.0 + ((rn % 10) * 0.1) -- Risk ranges 1.0 to 2.0
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareACODB.Bronze.ClaimsCost (
    ClaimID STRING,
    MemberID STRING,
    TotalCost DOUBLE,
    Category STRING -- Inpatient, Outpatient, Rx
);

-- Bulk Claims
INSERT INTO HealthcareACODB.Bronze.ClaimsCost
SELECT 
  'C' || CAST(rn + 1000 AS STRING),
  'M' || CAST((rn % 50) + 100 AS STRING),
  CAST((rn * 50) + 100 AS DOUBLE), -- Variable costs
  CASE WHEN rn % 5 = 0 THEN 'Inpatient' WHEN rn % 2 = 0 THEN 'Rx' ELSE 'Outpatient' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: PMPY Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareACODB.Silver.MemberSpend AS
SELECT
    m.MemberID,
    m.RiskScore,
    COALESCE(SUM(c.TotalCost), 0) AS TotalSpend,
    -- Benchmark: $10,000 * RiskScore
    (10000.0 * m.RiskScore) AS BenchmarkSpend
FROM HealthcareACODB.Bronze.MemberRoster m
LEFT JOIN HealthcareACODB.Bronze.ClaimsCost c ON m.MemberID = c.MemberID
GROUP BY m.MemberID, m.RiskScore;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Savings Projection
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareACODB.Gold.ACOPerformance AS
SELECT
    COUNT(MemberID) AS TotalMembers,
    SUM(TotalSpend) AS ActualTotalSpend,
    SUM(BenchmarkSpend) AS BenchmarkTotalSpend,
    (SUM(BenchmarkSpend) - SUM(TotalSpend)) AS SharedSavingsPool,
    CASE WHEN SUM(TotalSpend) < SUM(BenchmarkSpend) THEN 'Profitable' ELSE 'Loss' END AS Outcome
FROM HealthcareACODB.Silver.MemberSpend;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "What is the projected Shared Savings Pool?"
    2. "List members with spend exceeding their benchmark."
    3. "Compare Actual vs Benchmark spend by Risk Score range."
*/
