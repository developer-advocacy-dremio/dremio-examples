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

INSERT INTO HealthcareStaffingDB.Bronze.AcuityScores VALUES
('P101', DATE '2025-01-20', 'ICU', 5),
('P102', DATE '2025-01-20', 'MED-SURG', 2),
('P103', DATE '2025-01-20', 'ICU', 4),
('P104', DATE '2025-01-20', 'MED-SURG', 3),
('P105', DATE '2025-01-20', 'ICU', 5),
('P106', DATE '2025-01-20', 'MED-SURG', 1),
('P107', DATE '2025-01-20', 'ICU', 5),
('P108', DATE '2025-01-20', 'MED-SURG', 2),
('P109', DATE '2025-01-20', 'ICU', 4),
('P110', DATE '2025-01-20', 'MED-SURG', 3),
('P111', DATE '2025-01-20', 'ICU', 5),
('P112', DATE '2025-01-20', 'MED-SURG', 1),
('P113', DATE '2025-01-20', 'ICU', 5),
('P114', DATE '2025-01-20', 'MED-SURG', 2),
('P115', DATE '2025-01-20', 'ICU', 4),
('P116', DATE '2025-01-20', 'MED-SURG', 3),
('P117', DATE '2025-01-20', 'ICU', 5),
('P118', DATE '2025-01-20', 'MED-SURG', 1),
('P119', DATE '2025-01-20', 'ICU', 5),
('P120', DATE '2025-01-20', 'MED-SURG', 2),
('P121', DATE '2025-01-20', 'ICU', 4),
('P122', DATE '2025-01-20', 'MED-SURG', 3),
('P123', DATE '2025-01-20', 'ICU', 5),
('P124', DATE '2025-01-20', 'MED-SURG', 1),
('P125', DATE '2025-01-20', 'ICU', 5),
('P126', DATE '2025-01-20', 'MED-SURG', 2),
('P127', DATE '2025-01-20', 'ICU', 4),
('P128', DATE '2025-01-20', 'MED-SURG', 3),
('P129', DATE '2025-01-20', 'ICU', 5),
('P130', DATE '2025-01-20', 'MED-SURG', 1),
('P131', DATE '2025-01-20', 'ICU', 5),
('P132', DATE '2025-01-20', 'MED-SURG', 2),
('P133', DATE '2025-01-20', 'ICU', 4),
('P134', DATE '2025-01-20', 'MED-SURG', 3),
('P135', DATE '2025-01-20', 'ICU', 5),
('P136', DATE '2025-01-20', 'MED-SURG', 1),
('P137', DATE '2025-01-20', 'ICU', 5),
('P138', DATE '2025-01-20', 'MED-SURG', 2),
('P139', DATE '2025-01-20', 'ICU', 4),
('P140', DATE '2025-01-20', 'MED-SURG', 3),
('P141', DATE '2025-01-20', 'ICU', 5),
('P142', DATE '2025-01-20', 'MED-SURG', 1),
('P143', DATE '2025-01-20', 'ICU', 5),
('P144', DATE '2025-01-20', 'MED-SURG', 2),
('P145', DATE '2025-01-20', 'ICU', 4),
('P146', DATE '2025-01-20', 'MED-SURG', 3),
('P147', DATE '2025-01-20', 'ICU', 5),
('P148', DATE '2025-01-20', 'MED-SURG', 1),
('P149', DATE '2025-01-20', 'ICU', 5),
('P150', DATE '2025-01-20', 'MED-SURG', 2);

CREATE OR REPLACE TABLE HealthcareStaffingDB.Bronze.NurseShifts (
    NurseID STRING,
    Date DATE,
    Unit STRING,
    HoursWorked DOUBLE
);

INSERT INTO HealthcareStaffingDB.Bronze.NurseShifts VALUES
('N001', DATE '2025-01-20', 'ICU', 12.0),
('N002', DATE '2025-01-20', 'MED-SURG', 12.0),
('N003', DATE '2025-01-20', 'ICU', 12.0),
('N004', DATE '2025-01-20', 'MED-SURG', 12.0),
('N005', DATE '2025-01-20', 'ICU', 12.0),
('N006', DATE '2025-01-20', 'MED-SURG', 12.0),
('N007', DATE '2025-01-20', 'ICU', 12.0),
('N008', DATE '2025-01-20', 'MED-SURG', 12.0),
('N009', DATE '2025-01-20', 'ICU', 12.0),
('N010', DATE '2025-01-20', 'MED-SURG', 12.0),
('N011', DATE '2025-01-20', 'ICU', 12.0),
('N012', DATE '2025-01-20', 'MED-SURG', 12.0),
('N013', DATE '2025-01-20', 'ICU', 12.0),
('N014', DATE '2025-01-20', 'MED-SURG', 12.0),
('N015', DATE '2025-01-20', 'ICU', 12.0),
('N016', DATE '2025-01-20', 'MED-SURG', 12.0),
('N017', DATE '2025-01-20', 'ICU', 12.0),
('N018', DATE '2025-01-20', 'ICU', 12.0),
('N019', DATE '2025-01-20', 'ICU', 12.0),
('N020', DATE '2025-01-20', 'MED-SURG', 12.0);

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
