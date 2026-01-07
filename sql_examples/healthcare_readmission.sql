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

CREATE FOLDER IF NOT EXISTS HealthcareDB;
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
    Cost DOUBLE
);

CREATE TABLE IF NOT EXISTS HealthcareDB.Bronze.Diagnoses (
    DiagnosisID INT,
    EncounterID INT,
    DiagnosisCode VARCHAR,
    Description VARCHAR,
    Severity VARCHAR
);

-- 1.2 Populate Bronze Tables

-- Insert 20 Patients
INSERT INTO HealthcareDB.Bronze.Patients (PatientID, Name, DOB, Gender, City) VALUES
(1, 'John Smith', '1980-05-15', 'M', 'New York'),
(2, 'Jane Doe', '1992-08-22', 'F', 'Brooklyn'),
(3, 'Michael Johnson', '1975-11-30', 'M', 'Queens'),
(4, 'Emily Davis', '1988-02-14', 'F', 'Manhattan'),
(5, 'William Brown', '1955-06-19', 'M', 'Bronx'),
(6, 'Olivia Wilson', '2001-09-25', 'F', 'Staten Island'),
(7, 'James Taylor', '1968-03-12', 'M', 'New York'),
(8, 'Sophia Anderson', '1995-07-08', 'F', 'Brooklyn'),
(9, 'Benjamin Thomas', '1982-12-05', 'M', 'Queens'),
(10, 'Isabella Martinez', '1990-10-30', 'F', 'Manhattan'),
(11, 'Lucas White', '1978-04-18', 'M', 'Bronx'),
(12, 'Mia Harris', '2005-01-22', 'F', 'Staten Island'),
(13, 'Ethan Clark', '1960-09-14', 'M', 'New York'),
(14, 'Charlotte Lewis', '1998-11-11', 'F', 'Brooklyn'),
(15, 'Alexander Robinson', '1985-06-28', 'M', 'Queens'),
(16, 'Amelia Walker', '1993-02-03', 'F', 'Manhattan'),
(17, 'Henry Young', '1972-08-17', 'M', 'Bronx'),
(18, 'Harper Hall', '2000-05-20', 'F', 'Staten Island'),
(19, 'Sebastian Allen', '1989-12-12', 'M', 'New York'),
(20, 'Evelyn King', '1996-03-09', 'F', 'Brooklyn');

-- Insert 30 Encounters (including some readmissions)
INSERT INTO HealthcareDB.Bronze.Encounters (EncounterID, PatientID, AdmitDate, DischargeDate, Department, Cost) VALUES
(1, 1, '2024-01-05', '2024-01-08', 'Cardiology', 5000.00),
(2, 2, '2024-01-10', '2024-01-12', 'Emergency', 1200.50),
(3, 3, '2024-01-15', '2024-01-20', 'Neurology', 8500.75),
(4, 1, '2024-01-25', '2024-01-28', 'Cardiology', 5500.00), -- Readmission for Patient 1 within 30 days
(5, 4, '2024-02-01', '2024-02-03', 'Pediatrics', 3000.25),
(6, 5, '2024-02-05', '2024-02-10', 'Orthopedics', 15000.00),
(7, 6, '2024-02-12', '2024-02-13', 'Emergency', 800.00),
(8, 7, '2024-02-15', '2024-02-20', 'Oncology', 12000.50),
(9, 2, '2024-02-20', '2024-02-22', 'Emergency', 1500.00), -- Readmission for Patient 2 > 30 days (check logic)
(10, 8, '2024-03-01', '2024-03-05', 'General Surgery', 20000.00),
(11, 9, '2024-03-08', '2024-03-10', 'Cardiology', 4000.00),
(12, 5, '2024-03-12', '2024-03-15', 'Orthopedics', 6000.00), -- Readmission for Patient 5 > 30 days
(13, 10, '2024-03-15', '2024-03-16', 'Radiology', 500.00),
(14, 11, '2024-03-20', '2024-03-25', 'Neurology', 9000.00),
(15, 12, '2024-04-01', '2024-04-02', 'Pediatrics', 1200.00),
(16, 13, '2024-04-05', '2024-04-10', 'Cardiology', 6500.00),
(17, 3, '2024-04-12', '2024-04-15', 'Neurology', 4000.00), -- Readmission for Patient 3 > 30 days
(18, 14, '2024-04-18', '2024-04-20', 'Emergency', 1800.00),
(19, 15, '2024-04-22', '2024-04-28', 'General Surgery', 25000.00),
(20, 16, '2024-05-01', '2024-05-02', 'Radiology', 600.00),
(21, 14, '2024-05-05', '2024-05-07', 'Emergency', 2000.00), -- Readmission for Patient 14 within 30 days
(22, 17, '2024-05-10', '2024-05-15', 'Oncology', 11000.00),
(23, 18, '2024-05-18', '2024-05-19', 'Pediatrics', 900.00),
(24, 19, '2024-05-22', '2024-05-25', 'Orthopedics', 7000.00),
(25, 20, '2024-06-01', '2024-06-05', 'Cardiology', 5200.00),
(26, 1, '2024-06-10', '2024-06-12', 'Cardiology', 3000.00),
(27, 4, '2024-06-15', '2024-06-16', 'Pediatrics', 800.00),
(28, 7, '2024-06-20', '2024-06-25', 'Oncology', 13000.00),
(29, 9, '2024-06-28', '2024-06-30', 'Cardiology', 3500.00),
(30, 20, '2024-06-15', '2024-06-18', 'Cardiology', 4000.00); -- Readmission for Patient 20 within 30 days

-- Insert 35 Diagnoses
INSERT INTO HealthcareDB.Bronze.Diagnoses (DiagnosisID, EncounterID, DiagnosisCode, Description, Severity) VALUES
(1, 1, 'I20.9', 'Angina pectoris, unspecified', 'Moderate'),
(2, 2, 'R07.9', 'Chest pain, unspecified', 'Low'),
(3, 3, 'G40.9', 'Epilepsy, unspecified', 'High'),
(4, 4, 'I21.9', 'Acute myocardial infarction, unspecified', 'Critical'),
(5, 5, 'J06.9', 'Acute upper respiratory infection, unspecified', 'Low'),
(6, 6, 'M16.9', 'Osteoarthritis of hip, unspecified', 'Moderate'),
(7, 7, 'S06.0', 'Concussion', 'Moderate'),
(8, 8, 'C34.9', 'Malignant neoplasm of lung, unspecified', 'Critical'),
(9, 9, 'R10.9', 'Abdominal pain, unspecified', 'Low'),
(10, 10, 'K35.8', 'Acute appendicitis, other and unspecified', 'High'),
(11, 11, 'I10', 'Essential (primary) hypertension', 'Moderate'),
(12, 12, 'M54.5', 'Low back pain', 'Moderate'),
(13, 13, 'R51', 'Headache', 'Low'),
(14, 14, 'I63.9', 'Cerebral infarction, unspecified', 'Critical'),
(15, 15, 'J45.9', 'Asthma, unspecified', 'Moderate'),
(16, 16, 'I50.9', 'Heart failure, unspecified', 'High'),
(17, 17, 'G43.9', 'Migraine, unspecified', 'Moderate'),
(18, 18, 'A09', 'Infectious gastroenteritis and colitis, unspecified', 'Low'),
(19, 19, 'K80.2', 'Calculus of gallbladder without cholecystitis', 'High'),
(20, 20, 'S93.4', 'Sprain and strain of ankle', 'Low'),
(21, 21, 'R11.2', 'Nausea with vomiting', 'Low'),
(22, 22, 'C50.9', 'Malignant neoplasm of breast, unspecified', 'Critical'),
(23, 23, 'H66.9', 'Otitis media, unspecified', 'Low'),
(24, 24, 'S82.4', 'Fracture of fibula alone', 'High'),
(25, 25, 'I48.9', 'Atrial fibrillation and flutter, unspecified', 'Moderate'),
(26, 26, 'I25.1', 'Atherosclerotic heart disease', 'Moderate'),
(27, 27, 'J20.9', 'Acute bronchitis, unspecified', 'Low'),
(28, 28, 'C18.9', 'Malignant neoplasm of colon, unspecified', 'Critical'),
(29, 29, 'I49.9', 'Cardiac arrhythmia, unspecified', 'Moderate'),
(30, 30, 'I50.1', 'Left ventricular failure, unspecified', 'High'),
(31, 1, 'E78.5', 'Hyperlipidemia, unspecified', 'Low'), -- Secondary diagnosis for Enc 1
(32, 6, 'E66.9', 'Obesity, unspecified', 'Moderate'), -- Secondary diagnosis for Enc 6
(33, 8, 'Z85.1', 'Personal history of malignant neoplasm of bronchus and lung', 'Low'), -- Secondary for Enc 8
(34, 16, 'E11.9', 'Type 2 diabetes mellitus without complications', 'Moderate'), -- Secondary for Enc 16
(35, 19, 'K81.0', 'Acute cholecystitis', 'High'); -- Secondary for Enc 19

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
