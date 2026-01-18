/*
    Dremio High-Volume SQL Pattern: Healthcare Pharmacy Safety Loops
    
    Business Scenario:
    Ensuring medication safety involves reconciling what was Dispensed (from Pyxis/ADC)
    vs what was Administered (eMAR scan). Discrepancies can indicate diversion or errors.
    
    Data Story:
    We track Dispense Logs and Administration Logs.
    
    Medallion Architecture:
    - Bronze: DispensingLogs, AdminLogs.
      *Volume*: 50+ records.
    - Silver: AdminVariances (Matched transactions with delta checks).
    - Gold: SafetyIncidentReport (Unmatched or Dose Mismatches).
    
    Key Dremio Features:
    - JOIN checks
    - TIMESTAMPDIFF
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB;
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB.Gold;
USE HealthcarePharmaDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcarePharmaDB.Bronze.DispensingLogs (
    DispenseID STRING,
    PatientID STRING,
    MedicationID STRING,
    DispenseTime TIMESTAMP,
    User STRING, -- Nurse or Tech
    Quantity INT
);

INSERT INTO HealthcarePharmaDB.Bronze.DispensingLogs VALUES
('D100', 'P001', 'MED01', TIMESTAMP '2025-01-18 08:00:00', 'NURSE_A', 2),
('D101', 'P002', 'MED02', TIMESTAMP '2025-01-18 09:00:00', 'NURSE_B', 1),
('D102', 'P003', 'MED03', TIMESTAMP '2025-01-18 09:30:00', 'NURSE_A', 1),
('D103', 'P004', 'MED01', TIMESTAMP '2025-01-18 10:00:00', 'NURSE_C', 2),
('D104', 'P001', 'MED01', TIMESTAMP '2025-01-18 12:00:00', 'NURSE_A', 2);
-- Bulk Dispenses
INSERT INTO HealthcarePharmaDB.Bronze.DispensingLogs
SELECT 
  'D' || CAST(rn + 200 AS STRING),
  'P' || CAST((rn % 20) + 1 AS STRING),
  CASE WHEN rn % 3 = 0 THEN 'MED01' WHEN rn % 3 = 1 THEN 'MED02' ELSE 'MED03' END,
  DATE_ADD(TIMESTAMP '2025-01-18 08:00:00', CAST(rn * 10 AS INT) * 1000 * 60), -- Staggered times
  CASE WHEN rn % 2 = 0 THEN 'NURSE_A' ELSE 'NURSE_B' END,
  CASE WHEN rn % 5 = 0 THEN 2 ELSE 1 END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcarePharmaDB.Bronze.AdminLogs (
    AdminID STRING,
    PatientID STRING,
    MedicationID STRING,
    AdminTime TIMESTAMP,
    User STRING,
    Quantity INT,
    Status STRING -- Given, Held, Refused
);

INSERT INTO HealthcarePharmaDB.Bronze.AdminLogs VALUES
('A100', 'P001', 'MED01', TIMESTAMP '2025-01-18 08:15:00', 'NURSE_A', 2, 'Given'), -- Matches D100
('A101', 'P002', 'MED02', TIMESTAMP '2025-01-18 10:30:00', 'NURSE_B', 1, 'Given'), -- Late admin (1.5h later)
('A102', 'P003', 'MED03', TIMESTAMP '2025-01-18 09:35:00', 'NURSE_A', 1, 'Given'), -- Matches D102
-- D103 missing admin (Diversion risk?)
('A104', 'P001', 'MED01', TIMESTAMP '2025-01-18 12:10:00', 'NURSE_A', 1, 'Given'); -- D104 dispense=2, Admin=1 (Wastage documented?)

-- Bulk Admins (some missing to create variances)
INSERT INTO HealthcarePharmaDB.Bronze.AdminLogs
SELECT 
  'A' || CAST(rn + 200 AS STRING),
  'P' || CAST((rn % 20) + 1 AS STRING),
  CASE WHEN rn % 3 = 0 THEN 'MED01' WHEN rn % 3 = 1 THEN 'MED02' ELSE 'MED03' END,
  DATE_ADD(TIMESTAMP '2025-01-18 08:15:00', CAST(rn * 10 AS INT) * 1000 * 60), -- 15 mins after dispense usually
  CASE WHEN rn % 2 = 0 THEN 'NURSE_A' ELSE 'NURSE_B' END,
  CASE WHEN rn % 5 = 0 THEN 2 ELSE 1 END,
  'Given'
FROM (VALUES(1),(3),(4),(5),(7),(8),(9),(10),(11),(12),(13),(15),(16),(17),(19),(20), -- Skipping some to create gaps
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Variance Calculation
-------------------------------------------------------------------------------
-- Logic: Join Dispense to Admin on Patient, Med, and nearest time window (+/- 2 hours)
CREATE OR REPLACE VIEW HealthcarePharmaDB.Silver.AdminVariances AS
SELECT
    d.DispenseID,
    d.PatientID,
    d.MedicationID,
    d.DispenseTime,
    d.Quantity AS DispensedQty,
    a.AdminID,
    a.AdminTime,
    a.Quantity AS AdminQty,
    CASE 
        WHEN a.AdminID IS NULL THEN 'Unmatched Dispense'
        WHEN d.Quantity != a.Quantity THEN 'Qty Mismatch'
        WHEN TIMESTAMPDIFF(MINUTE, d.DispenseTime, a.AdminTime) > 60 THEN 'Delayed Admin'
        ELSE 'Matched'
    END AS VarianceType
FROM HealthcarePharmaDB.Bronze.DispensingLogs d
LEFT JOIN HealthcarePharmaDB.Bronze.AdminLogs a
    ON d.PatientID = a.PatientID 
    AND d.MedicationID = a.MedicationID
    AND a.AdminTime BETWEEN d.DispenseTime AND DATE_ADD(d.DispenseTime, 2, 'HOUR'); -- 2 hour window

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Safety Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcarePharmaDB.Gold.MedSafetyReport AS
SELECT
    VarianceType,
    COUNT(*) AS IncidentCount,
    -- List details for high risk
    ARRAY_AGG(DispenseID) AS IncidentIDs
FROM HealthcarePharmaDB.Silver.AdminVariances
WHERE VarianceType != 'Matched'
GROUP BY VarianceType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all Unmatched Dispense events (Potential Diversion)."
    2. "Show incidences of Quantity Mismatches."
    3. "Which User has the most Delayed Admin events?"
*/
