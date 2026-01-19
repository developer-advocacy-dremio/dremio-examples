/*
    Dremio High-Volume SQL Pattern: Healthcare HAI (Hospital Acquired Infection) Tracking
    
    Business Scenario:
    Infection Control teams need to monitor rates of CLABSI (Central Line-Associated Bloodstream Infection)
    and CAUTI (Catheter-Associated Urinary Tract Infection).
    
    Data Story:
    We track device utilization (how many days a patient had a central line/catheter) and positive lab cultures.
    An infection is "Hospital Acquired" if the positive culture occurs > 48 hours after admission.
    
    Medallion Architecture:
    - Bronze: DeviceLogs (Insertion/Removal), LabCultures (Results), Admissions.
      *Volume*: 50+ records.
    - Silver: InfectionEvents (Flagged as HAI vs POA - Present on Admission).
    - Gold: UnitSafetyScorecard (Infection Rate per 1000 device days).
    
    Key Dremio Features:
    - TIMESTAMPDIFF for 48h rule
    - Aggregation for rate calc
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareHAIDB;
CREATE FOLDER IF NOT EXISTS HealthcareHAIDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareHAIDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareHAIDB.Gold;
USE HealthcareHAIDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareHAIDB.Bronze.Admissions (
    PatientID STRING,
    AdmitDate TIMESTAMP,
    Unit STRING
);

INSERT INTO HealthcareHAIDB.Bronze.Admissions VALUES
('P001', TIMESTAMP '2025-01-01 08:00:00', 'ICU'),
('P002', TIMESTAMP '2025-01-02 09:00:00', 'ICU'),
('P003', TIMESTAMP '2025-01-03 10:00:00', 'MED-SURG'),
('P004', TIMESTAMP '2025-01-01 11:00:00', 'ICU'),
('P005', TIMESTAMP '2025-01-05 12:00:00', 'CCU'),
('P006', TIMESTAMP '2025-01-06 08:00:00', 'MED-SURG'),
('P007', TIMESTAMP '2025-01-07 09:00:00', 'ICU'),
('P008', TIMESTAMP '2025-01-08 10:00:00', 'CCU'),
('P009', TIMESTAMP '2025-01-09 11:00:00', 'ICU'),
('P010', TIMESTAMP '2025-01-10 12:00:00', 'MED-SURG'),
('P011', TIMESTAMP '2025-01-11 08:00:00', 'ICU'),
('P012', TIMESTAMP '2025-01-12 09:00:00', 'CCU'),
('P013', TIMESTAMP '2025-01-13 10:00:00', 'MED-SURG'),
('P014', TIMESTAMP '2025-01-14 11:00:00', 'ICU'),
('P015', TIMESTAMP '2025-01-15 12:00:00', 'CCU'),
('P016', TIMESTAMP '2025-01-16 08:00:00', 'MED-SURG'),
('P017', TIMESTAMP '2025-01-17 09:00:00', 'ICU'),
('P018', TIMESTAMP '2025-01-18 10:00:00', 'CCU'),
('P019', TIMESTAMP '2025-01-19 11:00:00', 'ICU'),
('P020', TIMESTAMP '2025-01-20 12:00:00', 'MED-SURG');

CREATE OR REPLACE TABLE HealthcareHAIDB.Bronze.DeviceLogs (
    DeviceLogID STRING,
    PatientID STRING,
    DeviceType STRING, -- CentralLine, Catheter, Vent
    InsertDate DATE,
    RemoveDate DATE -- NULL if still in
);

INSERT INTO HealthcareHAIDB.Bronze.DeviceLogs VALUES
('D001', 'P001', 'CentralLine', DATE '2025-01-01', DATE '2025-01-10'),
('D002', 'P001', 'Catheter', DATE '2025-01-01', DATE '2025-01-05'),
('D003', 'P002', 'CentralLine', DATE '2025-01-02', DATE '2025-01-15'),
('D004', 'P004', 'Vent', DATE '2025-01-01', DATE '2025-01-08'),
('D005', 'P005', 'CentralLine', DATE '2025-01-05', NULL),
('D006', 'P006', 'Catheter', DATE '2025-01-06', DATE '2025-01-10'),
('D007', 'P007', 'CentralLine', DATE '2025-01-07', NULL),
('D008', 'P008', 'Vent', DATE '2025-01-08', DATE '2025-01-12'),
('D009', 'P009', 'Catheter', DATE '2025-01-09', DATE '2025-01-15'),
('D010', 'P010', 'CentralLine', DATE '2025-01-10', NULL),
('D011', 'P011', 'Catheter', DATE '2025-01-11', DATE '2025-01-14'),
('D012', 'P012', 'CentralLine', DATE '2025-01-12', NULL),
('D013', 'P013', 'Vent', DATE '2025-01-13', DATE '2025-01-20'),
('D014', 'P014', 'CentralLine', DATE '2025-01-14', NULL),
('D015', 'P015', 'Catheter', DATE '2025-01-15', NULL),
('D016', 'P016', 'CentralLine', DATE '2025-01-16', DATE '2025-01-20'),
('D017', 'P017', 'Vent', DATE '2025-01-17', NULL),
('D018', 'P018', 'Catheter', DATE '2025-01-18', NULL),
('D019', 'P019', 'CentralLine', DATE '2025-01-19', NULL),
('D020', 'P020', 'Catheter', DATE '2025-01-20', NULL),
('D021', 'P002', 'Catheter', DATE '2025-01-03', DATE '2025-01-06'),
('D022', 'P004', 'CentralLine', DATE '2025-01-02', DATE '2025-01-09'),
('D023', 'P007', 'Vent', DATE '2025-01-08', NULL),
('D024', 'P009', 'CentralLine', DATE '2025-01-10', NULL),
('D025', 'P012', 'Catheter', DATE '2025-01-13', NULL);


CREATE OR REPLACE TABLE HealthcareHAIDB.Bronze.LabCultures (
    LabID STRING,
    PatientID STRING,
    CollectionTime TIMESTAMP,
    Organism STRING, -- E.Coli, MRSA, None
    SpecimenType STRING -- Blood, Urine
);

INSERT INTO HealthcareHAIDB.Bronze.LabCultures VALUES
('L001', 'P001', TIMESTAMP '2025-01-08 10:00:00', 'MRSA', 'Blood'), -- > 48h (Admit 1/1), likely CLABSI
('L002', 'P002', TIMESTAMP '2025-01-02 10:00:00', 'E.Coli', 'Urine'), -- < 48h, likely POA
('L003', 'P003', TIMESTAMP '2025-01-04 10:00:00', 'None', 'Blood'),
('L004', 'P004', TIMESTAMP '2025-01-05 10:00:00', 'Staph', 'Blood'), -- > 48h, Possible CLABSI
('L005', 'P005', TIMESTAMP '2025-01-08 10:00:00', 'Candida', 'Urine'), -- Possible CAUTI if Cath present (P005 has CentralLine, no Cath in log?)
('L006', 'P009', TIMESTAMP '2025-01-13 10:00:00', 'E.Coli', 'Urine'), -- > 48h, Check if Cath present (Yes D009) -> CAUTI
('L007', 'P010', TIMESTAMP '2025-01-12 10:00:00', 'None', 'Blood'),
('L008', 'P011', TIMESTAMP '2025-01-13 10:00:00', 'None', 'Urine'),
('L009', 'P012', TIMESTAMP '2025-01-15 10:00:00', 'MRSA', 'Blood'), -- > 48h, CentralLine D012 present -> CLABSI
('L010', 'P014', TIMESTAMP '2025-01-18 10:00:00', 'None', 'Blood');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: HAI Determination
-------------------------------------------------------------------------------
-- Calculate Device Days and check timing of positive cultures
CREATE OR REPLACE VIEW HealthcareHAIDB.Silver.InfectionEvents AS
SELECT
    l.LabID,
    l.PatientID,
    a.Unit,
    l.Organism,
    l.SpecimenType,
    l.CollectionTime,
    a.AdmitDate,
    TIMESTAMPDIFF(HOUR, a.AdmitDate, l.CollectionTime) AS HoursSinceAdmit,
    CASE 
        WHEN TIMESTAMPDIFF(HOUR, a.AdmitDate, l.CollectionTime) > 48 AND l.Organism != 'None' THEN 'HAI'
        WHEN l.Organism != 'None' THEN 'POA'
        ELSE 'Negative'
    END AS InfectionStatus,
    -- Simple linkage: if Blood culture positive & Central Line present -> Possible CLABSI
    d.DeviceType
FROM HealthcareHAIDB.Bronze.LabCultures l
JOIN HealthcareHAIDB.Bronze.Admissions a ON l.PatientID = a.PatientID
LEFT JOIN HealthcareHAIDB.Bronze.DeviceLogs d 
    ON l.PatientID = d.PatientID 
    AND (d.RemoveDate IS NULL OR d.RemoveDate >= CAST(l.CollectionTime AS DATE))
    AND d.InsertDate <= CAST(l.CollectionTime AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Safety Scorecard
-------------------------------------------------------------------------------
-- Metric: Infections per Unit
CREATE OR REPLACE VIEW HealthcareHAIDB.Gold.UnitSafetyScorecard AS
SELECT
    Unit,
    COUNT(DISTINCT LabID) AS TotalTests,
    SUM(CASE WHEN InfectionStatus = 'HAI' THEN 1 ELSE 0 END) AS TotalHAIs,
    SUM(CASE WHEN InfectionStatus = 'POA' THEN 1 ELSE 0 END) AS TotalPOAs,
    SUM(CASE WHEN InfectionStatus = 'HAI' AND DeviceType = 'CentralLine' AND SpecimenType = 'Blood' THEN 1 ELSE 0 END) AS SuspectedCLABSI,
    SUM(CASE WHEN InfectionStatus = 'HAI' AND DeviceType = 'Catheter' AND SpecimenType = 'Urine' THEN 1 ELSE 0 END) AS SuspectedCAUTI
FROM HealthcareHAIDB.Silver.InfectionEvents
GROUP BY Unit;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Calculate the HAI rate per unit."
    2. "Show the breakdown of organisms found in HAI cases."
    3. "Identify patients with Suspected CLABSI."
    4. "Compare the number of infections in ICU vs MED-SURG."
*/
