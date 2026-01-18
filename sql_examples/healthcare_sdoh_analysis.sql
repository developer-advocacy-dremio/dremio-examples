/*
    Dremio High-Volume SQL Pattern: Healthcare SDOH (Social Determinants of Health)
    
    Business Scenario:
    Clinical outcomes are heavily influenced by non-clinical factors (Housing, Food Access, Income).
    Stratifying patients by SDOH risk helps target community health interventions.
    
    Data Story:
    We join Patient Addresses with Census Tract data (SDOH metrics).
    
    Medallion Architecture:
    - Bronze: Patients, CensusTractData.
      *Volume*: 50+ records.
    - Silver: SocialRiskScore (Composite index based on Food Desert, Income).
    - Gold: InterventionTargets (High risk patients).
    
    Key Dremio Features:
    - Weighted Scoring Logic
    - JOIN on Zip/Tract
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareSDOHDB;
CREATE FOLDER IF NOT EXISTS HealthcareSDOHDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareSDOHDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareSDOHDB.Gold;
USE HealthcareSDOHDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareSDOHDB.Bronze.CensusTractData (
    ZipCode STRING,
    MedianIncome INT,
    FoodDesertFlag INT, -- 1=Yes
    HousingInstabilityFlag INT -- 1=Yes
);

INSERT INTO HealthcareSDOHDB.Bronze.CensusTractData VALUES
('10001', 85000, 0, 0), -- Low Risk
('10002', 45000, 1, 0), -- Med Risk (Food)
('10003', 32000, 1, 1), -- High Risk
('10004', 120000, 0, 0), -- Very Low Risk
('10005', 40000, 0, 1); -- Med Risk (Housing)

CREATE OR REPLACE TABLE HealthcareSDOHDB.Bronze.Patients (
    PatientID STRING,
    ZipCode STRING,
    ChronicConditionCount INT
);

-- Bulk Patients
INSERT INTO HealthcareSDOHDB.Bronze.Patients
SELECT 
  'P' || CAST(rn + 100 AS STRING),
  CASE WHEN (rn % 5) = 0 THEN '10001' 
       WHEN (rn % 5) = 1 THEN '10002' 
       WHEN (rn % 5) = 2 THEN '10003' 
       WHEN (rn % 5) = 3 THEN '10004' 
       ELSE '10005' END,
  (rn % 4) -- 0 to 3 chronic conditions
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Risk Scoring
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareSDOHDB.Silver.SocialRiskScore AS
SELECT
    p.PatientID,
    p.ZipCode,
    p.ChronicConditionCount,
    c.MedianIncome,
    c.FoodDesertFlag,
    c.HousingInstabilityFlag,
    -- Simple Score: +2 for Housing, +1 for Food, +1 for Low Income (<40k)
    (c.HousingInstabilityFlag * 2) + 
    (c.FoodDesertFlag * 1) + 
    (CASE WHEN c.MedianIncome < 40000 THEN 1 ELSE 0 END) AS SDOH_Score
FROM HealthcareSDOHDB.Bronze.Patients p
JOIN HealthcareSDOHDB.Bronze.CensusTractData c ON p.ZipCode = c.ZipCode;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Intervention Targets
-------------------------------------------------------------------------------
-- Patients with High SDOH (>=2) AND Chronic Conditions (>=2)
CREATE OR REPLACE VIEW HealthcareSDOHDB.Gold.CommunityInterventionTargets AS
SELECT
    PatientID,
    ZipCode,
    SDOH_Score,
    ChronicConditionCount,
    'Social Worker Referral' AS RecommendedAction
FROM HealthcareSDOHDB.Silver.SocialRiskScore
WHERE SDOH_Score >= 2 AND ChronicConditionCount >= 2;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me the distribution of SDOH Scores by Zip Code."
    2. "List all patients recommended for Social Worker Referral."
    3. "Correlate Median Income with Chronic Condition Count."
*/
