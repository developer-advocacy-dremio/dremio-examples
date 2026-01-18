/*
    Dremio High-Volume SQL Pattern: Healthcare SNF Referral Leakage
    
    Business Scenario:
    Hospitals form "Preferred Networks" with Skilled Nursing Facilities (SNFs) to ensure quality
    and reduce readmission penalties. Sending patients to out-of-network SNFs is "Leakage".
    
    Data Story:
    We track Discharge Orders and the SNF Network Master list.
    
    Medallion Architecture:
    - Bronze: Discharges, NetworkMaster.
      *Volume*: 50+ records.
    - Silver: LeakageFlags (In-Network vs Out-of-Network).
    - Gold: NetworkOptimization (Leakage % by Dept).
    
    Key Dremio Features:
    - LEFT JOIN validation
    - Percentage Calc
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareReferralDB;
CREATE FOLDER IF NOT EXISTS HealthcareReferralDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareReferralDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareReferralDB.Gold;
USE HealthcareReferralDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareReferralDB.Bronze.NetworkMaster (
    SNF_ID STRING,
    SNF_Name STRING,
    NetworkStatus STRING -- Preferred, Standard, OutOfNetwork
);

INSERT INTO HealthcareReferralDB.Bronze.NetworkMaster VALUES
('SNF01', 'Sunnyvale Care', 'Preferred'),
('SNF02', 'Riverview Rehab', 'Preferred'),
('SNF03', 'City Home', 'OutOfNetwork'),
('SNF04', 'County Facility', 'Standard');

CREATE OR REPLACE TABLE HealthcareReferralDB.Bronze.Discharges (
    DischargeID STRING,
    PatientID STRING,
    DestinationSNF_ID STRING,
    DischargeDept STRING -- Ortho, Neuro
);

-- Bulk Discharges
INSERT INTO HealthcareReferralDB.Bronze.Discharges
SELECT 
  'D' || CAST(rn + 100 AS STRING),
  'P' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 4 = 0 THEN 'SNF03' -- Leakage
       WHEN rn % 4 = 1 THEN 'SNF01' 
       ELSE 'SNF02' END,
  CASE WHEN rn % 2 = 0 THEN 'Ortho' ELSE 'Neuro' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Leakage Detection
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareReferralDB.Silver.ReferralStatus AS
SELECT
    d.DischargeID,
    d.DischargeDept,
    n.SNF_Name,
    n.NetworkStatus,
    CASE WHEN n.NetworkStatus = 'Preferred' THEN 0 ELSE 1 END AS IsLeakage
FROM HealthcareReferralDB.Bronze.Discharges d
LEFT JOIN HealthcareReferralDB.Bronze.NetworkMaster n ON d.DestinationSNF_ID = n.SNF_ID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Network Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareReferralDB.Gold.LeakageDashboard AS
SELECT
    DischargeDept,
    COUNT(*) AS TotalDischarges,
    SUM(IsLeakage) AS LeakedPatients,
    (CAST(SUM(IsLeakage) AS DOUBLE) / COUNT(*)) * 100 AS LeakagePct
FROM HealthcareReferralDB.Silver.ReferralStatus
GROUP BY DischargeDept;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Department has the highest referral leakage?"
    2. "List all patients sent to Out-of-Network SNFs."
    3. "Compare leakage percentages for Ortho vs Neuro."
*/
