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

INSERT INTO HealthcareSDOHDB.Bronze.Patients VALUES
('P001', '10001', 0),
('P002', '10002', 1),
('P003', '10003', 2),
('P004', '10004', 3),
('P005', '10005', 0),
('P006', '10001', 1),
('P007', '10002', 2),
('P008', '10003', 3),
('P009', '10004', 0),
('P010', '10005', 1),
('P011', '10001', 2),
('P012', '10002', 3),
('P013', '10003', 0),
('P014', '10004', 1),
('P015', '10005', 2),
('P016', '10001', 3),
('P017', '10002', 0),
('P018', '10003', 1),
('P019', '10004', 2),
('P020', '10005', 3),
('P021', '10001', 0),
('P022', '10002', 1),
('P023', '10003', 2),
('P024', '10004', 3),
('P025', '10005', 0),
('P026', '10001', 1),
('P027', '10002', 2),
('P028', '10003', 3),
('P029', '10004', 0),
('P030', '10005', 1),
('P031', '10001', 2),
('P032', '10002', 3),
('P033', '10003', 0),
('P034', '10004', 1),
('P035', '10005', 2),
('P036', '10001', 3),
('P037', '10002', 0),
('P038', '10003', 1),
('P039', '10004', 2),
('P040', '10005', 3),
('P041', '10001', 0),
('P042', '10002', 1),
('P043', '10003', 2),
('P044', '10004', 3),
('P045', '10005', 0),
('P046', '10001', 1),
('P047', '10002', 2),
('P048', '10003', 3),
('P049', '10004', 0),
('P050', '10005', 1);

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
