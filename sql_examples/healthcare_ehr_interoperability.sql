/*
 * EHR Interoperability Demo
 * 
 * Scenario:
 * Unifying patient records from disparate systems (Hospital A vs Hospital B) to create a single longitudinal record.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Healthcare;
CREATE FOLDER IF NOT EXISTS RetailDB.Healthcare.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Healthcare.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Healthcare.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

-- System A (Legacy ID format)
CREATE TABLE IF NOT EXISTS RetailDB.Healthcare.Bronze.HospitalA_Patients (
    Pat_ID VARCHAR,
    FullName VARCHAR,
    DOB DATE,
    BloodType VARCHAR
);

INSERT INTO RetailDB.Healthcare.Bronze.HospitalA_Patients VALUES
('A-101', 'John Doe', '1980-01-01', 'O+'),
('A-102', 'Jane Smith', '1990-05-15', 'A-'),
('A-103', 'Robert Brown', '1975-08-20', 'AB+'),
('A-104', 'Emily Davis', '1985-02-10', 'B+'),
('A-105', 'Michael Wilson', '1992-11-30', 'O-'),
('A-106', 'Sarah Johnson', '1988-07-25', 'A+'),
('A-107', 'David Lee', '1970-04-12', 'B-'),
('A-108', 'Jennifer White', '1995-09-05', 'O+'),
('A-109', 'James Harris', '1982-12-18', 'AB-'),
('A-110', 'Maria Martin', '1998-03-22', 'A+');

-- System B (Numeric ID, Split Name)
CREATE TABLE IF NOT EXISTS RetailDB.Healthcare.Bronze.HospitalB_Patients (
    PatientNum INT,
    FirstName VARCHAR,
    LastName VARCHAR,
    BirthDate VARCHAR, -- String format YYYYMMDD
    BloodGroup VARCHAR
);

INSERT INTO RetailDB.Healthcare.Bronze.HospitalB_Patients VALUES
(501, 'John', 'Doe', '19800101', 'O Pos'), -- Duplicate patient
(502, 'Alice', 'Wonder', '20000101', 'B Neg'),
(503, 'Tom', 'Hanks', '19600709', 'A Pos'),
(504, 'Bruce', 'Wayne', '19750510', 'AB Neg'),
(505, 'Clark', 'Kent', '19840229', 'O Pos'),
(506, 'Diana', 'Prince', '19900601', 'Unknown'),
(507, 'Peter', 'Parker', '19960810', 'A Pos'),
(508, 'Tony', 'Stark', '19700529', 'B Pos'),
(509, 'Steve', 'Rogers', '19180704', 'O Pos'),
(510, 'Natasha', 'Romanoff', '19841122', 'AB Pos');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Healthcare.Silver.UnifiedPatients AS
SELECT 
    'Hospital_A' AS SourceSystem,
    Pat_ID AS LocalID,
    FullName,
    DOB,
    BloodType
FROM RetailDB.Healthcare.Bronze.HospitalA_Patients
UNION ALL
SELECT 
    'Hospital_B' AS SourceSystem,
    CAST(PatientNum AS VARCHAR) AS LocalID,
    CONCAT(FirstName, ' ', LastName) AS FullName,
    TO_DATE(BirthDate, 'YYYYMMDD') AS DOB,
    -- Normalize BloodGroup codes
    CASE 
        WHEN BloodGroup = 'O Pos' THEN 'O+'
        WHEN BloodGroup = 'B Neg' THEN 'B-'
        WHEN BloodGroup = 'A Pos' THEN 'A+'
        WHEN BloodGroup = 'AB Neg' THEN 'AB-'
        WHEN BloodGroup = 'B Pos' THEN 'B+'
        WHEN BloodGroup = 'AB Pos' THEN 'AB+'
        ELSE 'Unknown'
    END AS BloodType
FROM RetailDB.Healthcare.Bronze.HospitalB_Patients;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Healthcare.Gold.PatientMaster AS
SELECT 
    -- Rudimentary Mastering: ID based on Name + DOB hash
    HASH(FullName, DOB) AS MasterPatientID,
    FullName,
    DOB,
    BloodType,
    LISTAGG(SourceSystem, ', ') AS FoundInSystems,
    COUNT(*) AS SystemCount
FROM RetailDB.Healthcare.Silver.UnifiedPatients
GROUP BY FullName, DOB, BloodType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Find patients in RetailDB.Healthcare.Gold.PatientMaster that exist in more than one source system."
*/
