/*
    Dremio High-Volume SQL Pattern: Healthcare Nurse Staffing & Acuity
    
    Business Scenario:
    Simply counting patients isn't enough; staffing must match "Patient Acuity" (Severity).
    We calculate HPPD (Hours Per Patient Day) based on acuity scores.
    
    Data Story:
    We track Patient Acuity Daily Scores and Staff Schedules.
    
    Medallion Architecture:
    - Bronze: AcuityScores, NurseShifts.
      *Volume*: 50+ records.
    - Silver: StaffingRatios (Supply vs Demand).
    - Gold: SafetyVarianceReport (Shifts with dangerous ratios).
    
    Key Dremio Features:
    - Aggregation
    - Division checks
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareStaffingDB;
CREATE FOLDER IF NOT EXISTS HealthcareStaffingDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareStaffingDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareStaffingDB.Gold;
USE HealthcareStaffingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareStaffingDB.Bronze.AcuityScores (
    PatientID STRING,
    Date DATE,
    Unit STRING,
    AcuityScore INT -- 1 (Low) to 5 (Critical)
);

-- Bulk Acuity
INSERT INTO HealthcareStaffingDB.Bronze.AcuityScores
SELECT 
  'P' || CAST(rn + 100 AS STRING),
  DATE '2025-01-20',
  CASE WHEN rn % 2 = 0 THEN 'ICU' ELSE 'MED-SURG' END,
  CASE WHEN (rn % 10) = 0 THEN 5 ELSE (rn % 4) + 1 END -- Mixed acuity
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareStaffingDB.Bronze.NurseShifts (
    NurseID STRING,
    Date DATE,
    Unit STRING,
    HoursWorked DOUBLE
);

-- Bulk Shifts
INSERT INTO HealthcareStaffingDB.Bronze.NurseShifts
SELECT 
  'N' || CAST(rn + 500 AS STRING),
  DATE '2025-01-20',
  CASE WHEN rn % 3 = 0 THEN 'ICU' ELSE 'MED-SURG' END,
  12.0
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS t(rn); -- 10 Nurses

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Supply vs Demand
-------------------------------------------------------------------------------
-- Demand: Acuity * Multiplier (e.g., Score 5 = 20 hours care, Score 1 = 4 hours)
CREATE OR REPLACE VIEW HealthcareStaffingDB.Silver.UnitLoad AS
SELECT
    Unit,
    Date,
    COUNT(PatientID) AS Census,
    SUM(AcuityScore * 4.0) AS RequiredNursingHours, -- Simple model
    SUM(AcuityScore) / COUNT(PatientID) AS AvgAcuity
FROM HealthcareStaffingDB.Bronze.AcuityScores
GROUP BY Unit, Date;

CREATE OR REPLACE VIEW HealthcareStaffingDB.Silver.UnitSupply AS
SELECT
    Unit,
    Date,
    SUM(HoursWorked) AS AvailableNursingHours
FROM HealthcareStaffingDB.Bronze.NurseShifts
GROUP BY Unit, Date;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Variance Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareStaffingDB.Gold.StaffingVariance AS
SELECT
    d.Unit,
    d.Date,
    d.RequiredNursingHours,
    s.AvailableNursingHours,
    (s.AvailableNursingHours - d.RequiredNursingHours) AS VarianceHours,
    CASE WHEN (s.AvailableNursingHours - d.RequiredNursingHours) < -12 THEN 'Unsafe/Understaffed' ELSE 'Safe' END AS Status
FROM HealthcareStaffingDB.Silver.UnitLoad d
JOIN HealthcareStaffingDB.Silver.UnitSupply s ON d.Unit = s.Unit AND d.Date = s.Date;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which units were Understaffed on 2025-01-20?"
    2. "Show Average Acuity by Unit."
    3. "Calculate the gap between Required and Available hours."
*/
