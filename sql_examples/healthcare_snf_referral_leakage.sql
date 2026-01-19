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

INSERT INTO HealthcareReferralDB.Bronze.Discharges VALUES
('D101', 'P101', 'SNF03', 'Ortho'), -- Leakage
('D102', 'P102', 'SNF01', 'Neuro'),
('D103', 'P103', 'SNF02', 'Ortho'),
('D104', 'P104', 'SNF04', 'Neuro'),
('D105', 'P105', 'SNF03', 'Ortho'), -- Leakage
('D106', 'P106', 'SNF01', 'Neuro'),
('D107', 'P107', 'SNF02', 'Ortho'),
('D108', 'P108', 'SNF01', 'Neuro'),
('D109', 'P109', 'SNF03', 'Ortho'), -- Leakage
('D110', 'P110', 'SNF04', 'Neuro'),
('D111', 'P111', 'SNF01', 'Ortho'),
('D112', 'P112', 'SNF02', 'Neuro'),
('D113', 'P113', 'SNF03', 'Ortho'), -- Leakage
('D114', 'P114', 'SNF04', 'Neuro'),
('D115', 'P115', 'SNF01', 'Ortho'),
('D116', 'P116', 'SNF02', 'Neuro'),
('D117', 'P117', 'SNF01', 'Ortho'),
('D118', 'P118', 'SNF02', 'Neuro'),
('D119', 'P119', 'SNF03', 'Ortho'), -- Leakage
('D120', 'P120', 'SNF04', 'Neuro'),
('D121', 'P121', 'SNF01', 'Ortho'),
('D122', 'P122', 'SNF02', 'Neuro'),
('D123', 'P123', 'SNF03', 'Ortho'), -- Leakage
('D124', 'P124', 'SNF04', 'Neuro'),
('D125', 'P125', 'SNF01', 'Ortho'),
('D126', 'P126', 'SNF02', 'Neuro'),
('D127', 'P127', 'SNF03', 'Ortho'), -- Leakage
('D128', 'P128', 'SNF01', 'Neuro'),
('D129', 'P129', 'SNF02', 'Ortho'),
('D130', 'P130', 'SNF04', 'Neuro'),
('D131', 'P131', 'SNF01', 'Ortho'),
('D132', 'P132', 'SNF02', 'Neuro'),
('D133', 'P133', 'SNF03', 'Ortho'), -- Leakage
('D134', 'P134', 'SNF04', 'Neuro'),
('D135', 'P135', 'SNF01', 'Ortho'),
('D136', 'P136', 'SNF02', 'Neuro'),
('D137', 'P137', 'SNF03', 'Ortho'), -- Leakage
('D138', 'P138', 'SNF01', 'Neuro'),
('D139', 'P139', 'SNF02', 'Ortho'),
('D140', 'P140', 'SNF04', 'Neuro'),
('D141', 'P141', 'SNF01', 'Ortho'),
('D142', 'P142', 'SNF02', 'Neuro'),
('D143', 'P143', 'SNF03', 'Ortho'), -- Leakage
('D144', 'P144', 'SNF04', 'Neuro'),
('D145', 'P145', 'SNF01', 'Ortho'),
('D146', 'P146', 'SNF02', 'Neuro'),
('D147', 'P147', 'SNF03', 'Ortho'), -- Leakage
('D148', 'P148', 'SNF01', 'Neuro'),
('D149', 'P149', 'SNF02', 'Ortho'),
('D150', 'P150', 'SNF04', 'Neuro');

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
