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
('P005', TIMESTAMP '2025-01-05 12:00:00', 'CCU');
-- Bulk admissions
INSERT INTO HealthcareHAIDB.Bronze.Admissions
SELECT 
  'P' || CAST(rn + 100 AS STRING),
  TIMESTAMP '2025-01-01 00:00:00',
  CASE WHEN rn % 3 = 0 THEN 'ICU' WHEN rn % 3 = 1 THEN 'CCU' ELSE 'MED-SURG' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

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
('D005', 'P005', 'CentralLine', DATE '2025-01-05', NULL);
-- Bulk device logs
INSERT INTO HealthcareHAIDB.Bronze.DeviceLogs
SELECT 
  'D' || CAST(rn + 100 AS STRING),
  'P' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 3 = 0 THEN 'CentralLine' WHEN rn % 3 = 1 THEN 'Catheter' ELSE 'Vent' END,
  DATE '2025-01-01',
  DATE_ADD(DATE '2025-01-01', CAST((rn % 10) + 1 AS INT))
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareHAIDB.Bronze.LabCultures (
    LabID STRING,
    PatientID STRING,
    CollectionTime TIMESTAMP,
    Organism STRING, -- E.Coli, MRSA, None
    SpecimenType STRING -- Blood, Urine
);

INSERT INTO HealthcareHAIDB.Bronze.LabCultures VALUES
('L001', 'P001', TIMESTAMP '2025-01-08 10:00:00', 'MRSA', 'Blood'), -- > 48h, likely CLABSI
('L002', 'P002', TIMESTAMP '2025-01-02 10:00:00', 'E.Coli', 'Urine'), -- < 48h, likely POA
('L003', 'P003', TIMESTAMP '2025-01-04 10:00:00', 'None', 'Blood'),
('L004', 'P103', TIMESTAMP '2025-01-05 10:00:00', 'Staph', 'Blood'), -- Potential HAI
('L005', 'P105', TIMESTAMP '2025-01-04 10:00:00', 'Candida', 'Urine'); -- Potential CAUTI if Cath present
-- Bulk labs (mostly negative)
INSERT INTO HealthcareHAIDB.Bronze.LabCultures
SELECT 
  'L' || CAST(rn + 100 AS STRING),
  'P' || CAST(rn + 100 AS STRING),
  TIMESTAMP '2025-01-05 12:00:00',
  CASE WHEN rn % 20 = 0 THEN 'E.Coli' ELSE 'None' END,
  CASE WHEN rn % 2 = 0 THEN 'Blood' ELSE 'Urine' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

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
