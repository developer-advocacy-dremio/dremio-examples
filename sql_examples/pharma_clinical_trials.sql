/*
 * Pharma Clinical Trials Demo
 * 
 * Scenario:
 * A pharmaceutical company tracks patient outcomes and safety signals.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Monitor drug safety and efficacy.
 * 
 * Note: Assumes a catalog named 'PharmaDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS PharmaDB;
CREATE FOLDER IF NOT EXISTS PharmaDB.Bronze;
CREATE FOLDER IF NOT EXISTS PharmaDB.Silver;
CREATE FOLDER IF NOT EXISTS PharmaDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Trial Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PharmaDB.Bronze.Studies (
    StudyID VARCHAR,
    DrugName VARCHAR,
    Phase VARCHAR, -- 'Phase I', 'Phase II', 'Phase III'
    StartDate DATE
);

CREATE TABLE IF NOT EXISTS PharmaDB.Bronze.Patients (
    PatientID INT,
    StudyID VARCHAR,
    EnrollmentDate DATE,
    Age INT,
    Gender VARCHAR,
    TreatmentGroup VARCHAR -- 'Placebo', 'Active'
);

CREATE TABLE IF NOT EXISTS PharmaDB.Bronze.AdverseEvents (
    EventID INT,
    PatientID INT,
    EventDate DATE,
    Description VARCHAR,
    Severity VARCHAR -- 'Mild', 'Moderate', 'Severe'
);

-- 1.2 Populate Bronze Tables
INSERT INTO PharmaDB.Bronze.Studies (StudyID, DrugName, Phase, StartDate) VALUES
('CT-001', 'CardioFix', 'Phase III', '2024-01-01'),
('CT-002', 'NeuroCalm', 'Phase II', '2024-06-15');

INSERT INTO PharmaDB.Bronze.Patients (PatientID, StudyID, EnrollmentDate, Age, Gender, TreatmentGroup) VALUES
(1, 'CT-001', '2024-02-01', 55, 'M', 'Active'),
(2, 'CT-001', '2024-02-05', 60, 'F', 'Placebo'),
(3, 'CT-001', '2024-03-10', 45, 'M', 'Active'),
(4, 'CT-002', '2024-07-01', 30, 'F', 'Active');

INSERT INTO PharmaDB.Bronze.AdverseEvents (EventID, PatientID, EventDate, Description, Severity) VALUES
(1, 1, '2024-04-10', 'Dizziness', 'Mild'),
(2, 3, '2024-05-15', 'Nausea', 'Moderate'),
(3, 1, '2024-06-01', 'Headache', 'Mild');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Safety Monitoring
-------------------------------------------------------------------------------

-- 2.1 View: Patient_Safety_Profile
-- Combines patient info with adverse events.
CREATE OR REPLACE VIEW PharmaDB.Silver.Patient_Safety_Profile AS
SELECT
    p.PatientID,
    p.StudyID,
    p.TreatmentGroup,
    ae.Description AS AdverseEvent,
    ae.Severity,
    ae.EventDate
FROM PharmaDB.Bronze.Patients p
LEFT JOIN PharmaDB.Bronze.AdverseEvents ae ON p.PatientID = ae.PatientID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Drug Efficacy & Safety
-------------------------------------------------------------------------------

-- 3.1 View: Study_Safety_Report
-- Aggregates adverse events by treatment group.
CREATE OR REPLACE VIEW PharmaDB.Gold.Study_Safety_Report AS
SELECT
    StudyID,
    TreatmentGroup,
    COUNT(DISTINCT PatientID) AS TotalPatients,
    COUNT(AdverseEvent) AS TotalEvents,
    SUM(CASE WHEN Severity = 'Severe' THEN 1 ELSE 0 END) AS SevereEvents
FROM PharmaDB.Silver.Patient_Safety_Profile
GROUP BY StudyID, TreatmentGroup;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Safety Signals):
"Compare TotalEvents between 'Placebo' and 'Active' groups in PharmaDB.Gold.Study_Safety_Report."

PROMPT 2 (Side Effects):
"List all patients who experienced 'Severe' adverse events."
*/
