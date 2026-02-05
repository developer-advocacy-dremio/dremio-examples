/*
 * Dremio "Messy Data" Challenge: Legacy Hospital Records
 * 
 * Scenario: 
 * Patient data merged from two hospitals.
 * 'Gender' encoded differently ('M/F', '1/2', 'Male/Female').
 * 'DOB' has mixed separators and some future dates (typos).
 * 
 * Objective for AI Agent:
 * 1. Normalize Gender to 'M', 'F', 'U' (Unknown).
 * 2. Sanitize DOB (standard DATE format, remove future dates).
 * 3. Calculate Age based on DOB.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Hospital_Merge;
CREATE FOLDER IF NOT EXISTS Hospital_Merge.Admissions;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Hospital_Merge.Admissions.PATIENTS_V1 (
    PAT_ID VARCHAR,
    GENDER_CODE VARCHAR, -- Mixed
    DOB_STR VARCHAR, -- Mixed
    DIAGNOSIS VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Encoding Drift)
-------------------------------------------------------------------------------

-- Standard
INSERT INTO Hospital_Merge.Admissions.PATIENTS_V1 VALUES
('P-100', 'M', '1980-01-01', 'Flu'),
('P-101', 'F', '1990-05-20', 'Broken Leg');

-- Numeric Codes
INSERT INTO Hospital_Merge.Admissions.PATIENTS_V1 VALUES
('P-102', '1', '1975/12/25', 'Cold'), -- 1 = Male?
('P-103', '2', '01-01-2000', 'Fever'); -- 2 = Female?

-- Full Text / Future Date
INSERT INTO Hospital_Merge.Admissions.PATIENTS_V1 VALUES
('P-104', 'Male', '2050-01-01', 'Time Travel Sickness'), -- Future
('P-105', 'Female', '1985.10.10', 'Checkup');

-- Unknowns
INSERT INTO Hospital_Merge.Admissions.PATIENTS_V1 VALUES
('P-106', '?', 'Unknown', 'Triage'),
('P-107', 'U', 'NULL', 'Triage');

-- Bulk Fill
INSERT INTO Hospital_Merge.Admissions.PATIENTS_V1 VALUES
('P-108', 'M', '1980-01-01', 'Flu'),
('P-109', '1', '1981-02-02', 'Flu'),
('P-110', 'Male', '1982-03-03', 'Flu'),
('P-111', 'm', '1983-04-04', 'Flu'), -- Lowercase
('P-112', 'F', '1990-01-01', 'XRay'),
('P-113', '2', '1991-02-02', 'XRay'),
('P-114', 'Female', '1992-03-03', 'XRay'),
('P-115', 'f', '1993-04-04', 'XRay'),
('P-116', '0', '2000-01-01', 'Test'), -- 0?
('P-117', '9', '2000-01-01', 'Test'),
('P-118', 'X', '2000-01-01', 'Test'),
('P-119', 'NB', '2000-01-01', 'Test'), -- Non-binary
('P-120', 'Non-Binary', '2000-01-01', 'Test'),
('P-121', 'M', '01/01/1980', 'Checkup'),
('P-122', 'M', '01-01-1980', 'Checkup'),
('P-123', 'M', '1980.01.01', 'Checkup'),
('P-124', 'M', '19800101', 'Checkup'), -- No separator
('P-125', 'M', 'Jan 1 1980', 'Checkup'),
('P-126', 'F', '2023-01-01', 'Newborn'),
('P-127', 'F', '2024-01-01', 'Future?'),
('P-128', 'M', '1800-01-01', 'Vampire?'), -- Very old
('P-129', 'M', '1950-01-01', 'Checkup'),
('P-130', 'F', '1960-01-01', 'Checkup'),
('P-131', 'M', '1970-01-01', 'Checkup'),
('P-132', 'F', '1980-01-01', 'Checkup'),
('P-133', 'M', '1990-01-01', 'Checkup'),
('P-134', 'F', '2000-01-01', 'Checkup'),
('P-135', 'M', '2010-01-01', 'Child'),
('P-136', 'F', '2015-01-01', 'Child'),
('P-137', 'M', '2020-01-01', 'Toddler'),
('P-138', 'F', '2022-01-01', 'Infant'),
('P-139', 'M', '', 'No DOB'),
('P-140', 'F', ' ', 'Empty DOB'),
('P-141', 'M', 'N/A', 'N/A DOB'),
('P-142', 'F', '1980-13-01', 'Bad Month'),
('P-143', 'M', '1980-01-32', 'Bad Day'),
('P-144', 'F', '1980-00-00', 'Zeros'),
('P-145', 'M', '2023-02-30', 'Feb 30'),
('P-146', 'M', '1999-99-99', 'Nines'),
('P-147', 'Male', '1990/01/01 10:00', 'With Time'),
('P-148', 'Female', '1990', 'Year only'),
('P-149', 'M', '10/10/90', '2-digit Year'),
('P-150', 'F', '12/31/23', '2-digit Year');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Standardize the patient feed in Hospital_Merge.Admissions.PATIENTS_V1.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Normalize Gender: Map '1','M','Male','m' -> 'M'. Map '2','F','Female','f' -> 'F'. Else 'U'.
 *     - Clean DOB: Cast valid strings to DATE. Set Future dates (> CURRENT_DATE) or Old dates (< 1900) to NULL.
 *  3. Gold: 
 *     - Calculate 'Age_Years' (Datediff).
 *     - Count Patients by Age Group (0-18, 19-65, 65+).
 *  
 *  Show the SQL."
 */
