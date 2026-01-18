/*
    Dremio High-Volume SQL Pattern: Healthcare Claims Denial Appeals
    
    Business Scenario:
    Hospitals lose millions in denied claims. Tracking the "Overturn Rate" (Success of appeals)
    helps identify which denials are worth fighting vs writing off.
    
    Data Story:
    We track Initial Denials and Appeal Submissions.
    
    Medallion Architecture:
    - Bronze: Denials, Appeals.
      *Volume*: 50+ records.
    - Silver: AppealOutcome (Matched denials to appeal results).
    - Gold: StrategyReport (Success rate by Payer and Reason).
    
    Key Dremio Features:
    - Aggregation
    - Success Rate Calculation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB;
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB.Gold;
USE HealthcareAppealsDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareAppealsDB.Bronze.Denials (
    ClaimID STRING,
    Payer STRING,
    DenialReason STRING, -- Medical Necessity, Authorization, Coding
    DenialAmount DOUBLE,
    DenialDate DATE
);

-- Bulk Denials
INSERT INTO HealthcareAppealsDB.Bronze.Denials
SELECT 
  'C' || CAST(rn + 1000 AS STRING),
  CASE WHEN rn % 3 = 0 THEN 'UHC' WHEN rn % 3 = 1 THEN 'Aetna' ELSE 'BCBS' END,
  CASE WHEN rn % 3 = 0 THEN 'MedNecessity' WHEN rn % 3 = 1 THEN 'NoAuth' ELSE 'CodingError' END,
  CAST((rn * 100) + 500 AS DOUBLE),
  DATE_SUB(DATE '2025-01-01', CAST(rn AS INT))
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareAppealsDB.Bronze.Appeals (
    AppealID STRING,
    ClaimID STRING,
    AppealDate DATE,
    Outcome STRING -- Overturned (Win), Upheld (Loss), Pending
);

-- Bulk Appeals (Match about 80% of denials)
INSERT INTO HealthcareAppealsDB.Bronze.Appeals
SELECT 
  'A' || CAST(rn + 5000 AS STRING),
  'C' || CAST(rn + 1000 AS STRING),
  DATE_ADD(DATE_SUB(DATE '2025-01-01', CAST(rn AS INT)), 15), -- Appeal 15 days later
  CASE 
    WHEN (rn % 3 = 2) THEN 'Upheld' -- Coding errors hard to win?
    WHEN (rn % 3 = 0) THEN 'Overturned' -- Med Nec easy to win?
    ELSE 'Pending' 
  END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Appeal Outcomes
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareAppealsDB.Silver.AppealOutcomes AS
SELECT
    d.ClaimID,
    d.Payer,
    d.DenialReason,
    d.DenialAmount,
    a.Outcome
FROM HealthcareAppealsDB.Bronze.Denials d
LEFT JOIN HealthcareAppealsDB.Bronze.Appeals a ON d.ClaimID = a.ClaimID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Strategy Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareAppealsDB.Gold.StrategyReport AS
SELECT
    Payer,
    DenialReason,
    COUNT(ClaimID) AS TotalDenials,
    SUM(CASE WHEN Outcome = 'Overturned' THEN 1 ELSE 0 END) AS Wins,
    SUM(CASE WHEN Outcome = 'Overturned' THEN DenialAmount ELSE 0.0 END) AS RecoveredAmount,
    (CAST(SUM(CASE WHEN Outcome = 'Overturned' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(ClaimID)) * 100 AS WinRatePct
FROM HealthcareAppealsDB.Silver.AppealOutcomes
GROUP BY Payer, DenialReason;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Payer has the highest overturn rate?"
    2. "Calculate total revenue recovered by Denial Reason."
    3. "Show the win rate for 'Medical Necessity' denials."
*/
