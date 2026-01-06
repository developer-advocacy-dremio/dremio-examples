/*
 * Healthcare Readmission Analytics Demo
 * 
 * Scenario:
 * A hospital network wants to track patient readmissions and cost of care.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Improve patient outcomes and reduce penalties.
 * 
 * Note: Assumes a catalog named 'HealthcareDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HealthcareDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS HealthcareDB.Bronze.Patients (
    PatientID INT,
    Name VARCHAR,
    DOB DATE,
    Gender VARCHAR,
    City VARCHAR
);

CREATE TABLE IF NOT EXISTS HealthcareDB.Bronze.Encounters (
    EncounterID INT,
    PatientID INT,
    AdmitDate DATE,
    DischargeDate DATE,
    Department VARCHAR,
    Cost FLOAT
);

CREATE TABLE IF NOT EXISTS HealthcareDB.Bronze.Diagnoses (
    DiagnosisID INT,
    EncounterID INT,
    DiagnosisCode VARCHAR,
    Description VARCHAR,
    Severity VARCHAR
);

-- 1.2 Populate Bronze Tables

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Longitudinal Records
-------------------------------------------------------------------------------

-- 2.1 View: Patient_History
-- Joins Encounters with Patient and Diagnosis info.
CREATE OR REPLACE VIEW HealthcareDB.Silver.Patient_History AS
SELECT
    e.EncounterID,
    e.PatientID,
    p.Name AS PatientName,
    p.Gender,
    e.AdmitDate,
    e.DischargeDate,
    e.Department,
    d.Description AS Diagnosis,
    d.Severity,
    e.Cost,
    DATEDIFF(day, e.AdmitDate, e.DischargeDate) AS LengthOfStay
FROM HealthcareDB.Bronze.Encounters e
JOIN HealthcareDB.Bronze.Patients p ON e.PatientID = p.PatientID
JOIN HealthcareDB.Bronze.Diagnoses d ON e.EncounterID = d.EncounterID;

-- 2.2 View: Readmission_Events
-- Identifies if a patient had another visit within 30 days of a previous discharge.
CREATE OR REPLACE VIEW HealthcareDB.Silver.Readmission_Events AS
SELECT
    e1.PatientID,
    e1.EncounterID AS InitialVisit,
    e1.DischargeDate AS InitialDischarge,
    e2.EncounterID AS ReadmissionVisit,
    e2.AdmitDate AS ReadmissionDate,
    DATEDIFF(day, e1.DischargeDate, e2.AdmitDate) AS DaysBetweenVisits
FROM HealthcareDB.Bronze.Encounters e1
JOIN HealthcareDB.Bronze.Encounters e2 
    ON e1.PatientID = e2.PatientID 
    AND e2.AdmitDate > e1.DischargeDate
    AND DATEDIFF(day, e1.DischargeDate, e2.AdmitDate) <= 30;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Clinical Insights
-------------------------------------------------------------------------------

-- 3.1 View: Readmission_Rate_By_Dept
-- Aggregates readmissions by the initial department.
CREATE OR REPLACE VIEW HealthcareDB.Gold.Readmission_Stats AS
SELECT
    ph.Department,
    COUNT(DISTINCT ph.EncounterID) AS TotalDischarges,
    COUNT(DISTINCT re.InitialVisit) AS Readmissions,
    (COUNT(DISTINCT re.InitialVisit) * 100.0 / COUNT(DISTINCT ph.EncounterID)) AS ReadmissionRate
FROM HealthcareDB.Silver.Patient_History ph
LEFT JOIN HealthcareDB.Silver.Readmission_Events re ON ph.EncounterID = re.InitialVisit
GROUP BY ph.Department;

-- 3.2 View: High_Cost_Patients
-- Lists top 10% of patients by total cost.
CREATE OR REPLACE VIEW HealthcareDB.Gold.High_Cost_Patients AS
SELECT
    PatientName,
    Gender,
    COUNT(EncounterID) AS VisitCount,
    SUM(Cost) AS TotalCost_YTD
FROM HealthcareDB.Silver.Patient_History
GROUP BY PatientName, Gender
HAVING SUM(Cost) > 15000; -- Arbitrary threshold for 'High Cost'

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Readmission Dashboard):
"Using HealthcareDB.Gold.Readmission_Stats, create a bar chart of ReadmissionRate by Department."

PROMPT 2 (Cost Analysis):
"Who are my most expensive patients? Show me the top 10 from HealthcareDB.Gold.High_Cost_Patients sorted by TotalCost_YTD."

PROMPT 3 (Diagnosis Trends):
"From HealthcareDB.Silver.Patient_History, show me the average LengthOfStay for each Diagnosis severity level."
*/
