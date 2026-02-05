/*
 * Dremio "Messy Data" Challenge: Pharmaceutical Clinical Trials
 * 
 * Scenario: 
 * Drug trial data.
 * 3 Tables: TRIALS (Meta), PATIENTS (Subjects), VISITS (Observations).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: VISITS -> PATIENTS -> TRIALS.
 * 2. Anonymize: Mask Patient_Name.
 * 3. Cohort Analysis: Avg 'Bio_Marker' per Trial Group (Placebo vs Active).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Pharma_R_and_D;
CREATE FOLDER IF NOT EXISTS Pharma_R_and_D.Clinical;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Pharma_R_and_D.Clinical.TRIALS (
    TRIAL_ID VARCHAR,
    DRUG_NAME VARCHAR,
    PHASE VARCHAR -- 'I', 'II', 'III'
);

CREATE TABLE IF NOT EXISTS Pharma_R_and_D.Clinical.PATIENTS (
    PATIENT_ID VARCHAR,
    TRIAL_ID VARCHAR,
    TREATMENT_GROUP VARCHAR, -- 'Control', 'Test'
    CONSENT_DATE DATE
);

CREATE TABLE IF NOT EXISTS Pharma_R_and_D.Clinical.VISITS (
    VISIT_ID VARCHAR,
    PATIENT_ID VARCHAR,
    VISIT_DATE DATE,
    BIO_MARKER_VAL DOUBLE,
    ADVERSE_EVENT VARCHAR -- 'None', 'Nausea', 'Headache'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- TRIALS
INSERT INTO Pharma_R_and_D.Clinical.TRIALS VALUES
('T-1', 'CureAll', 'III'), ('T-2', 'FixIt', 'II');

-- PATIENTS (20 Rows)
INSERT INTO Pharma_R_and_D.Clinical.PATIENTS VALUES
('P-1', 'T-1', 'Test', '2023-01-01'), ('P-2', 'T-1', 'Control', '2023-01-01'),
('P-3', 'T-1', 'Test', '2023-01-01'), ('P-4', 'T-1', 'Control', '2023-01-01'),
('P-5', 'T-1', 'Test', '2023-01-01'), ('P-6', 'T-1', 'Control', '2023-01-01'),
('P-7', 'T-1', 'Test', '2023-01-01'), ('P-8', 'T-1', 'Control', '2023-01-01'),
('P-9', 'T-1', 'Test', '2023-01-01'), ('P-10', 'T-1', 'Control', '2023-01-01'),
('P-11', 'T-2', 'Test', '2023-01-01'), ('P-12', 'T-2', 'Test', '2023-01-01'),
('P-13', 'T-2', 'Test', '2023-01-01'), ('P-14', 'T-2', 'Test', '2023-01-01'),
('P-15', 'T-2', 'Test', '2023-01-01'), ('P-16', 'T-2', 'Test', '2023-01-01'),
('P-17', 'T-2', 'Test', '2023-01-01'), ('P-18', 'T-2', 'Test', '2023-01-01'),
('P-19', 'T-2', 'Test', '2023-01-01'), ('P-20', 'T-2', 'Test', '2023-01-01');

-- VISITS (50 Rows)
INSERT INTO Pharma_R_and_D.Clinical.VISITS VALUES
('V-1', 'P-1', '2023-02-01', 100.0, 'None'), ('V-2', 'P-1', '2023-03-01', 90.0, 'None'),
('V-3', 'P-1', '2023-04-01', 80.0, 'None'), ('V-4', 'P-2', '2023-02-01', 100.0, 'None'),
('V-5', 'P-2', '2023-03-01', 100.0, 'None'), ('V-6', 'P-3', '2023-02-01', 100.0, 'Nausea'),
('V-7', 'P-3', '2023-03-01', 95.0, 'None'), ('V-8', 'P-4', '2023-02-01', 100.0, 'None'),
('V-9', 'P-5', '2023-02-01', 50.0, 'Headache'), ('V-10', 'P-6', '2023-02-01', 100.0, 'None'),
('V-11', 'P-1', '2023-05-01', NULL, 'None'), -- Missing Data
('V-12', 'P-1', '2023-06-01', -10.0, 'None'), -- Error
('V-13', 'P-7', '2023-02-01', 100.0, 'None'), ('V-14', 'P-8', '2023-02-01', 100.0, 'None'),
('V-15', 'P-9', '2023-02-01', 100.0, 'None'), ('V-16', 'P-10', '2023-02-01', 100.0, 'None'),
('V-17', 'P-11', '2023-02-01', 100.0, 'None'), ('V-18', 'P-12', '2023-02-01', 100.0, 'None'),
('V-19', 'P-13', '2023-02-01', 100.0, 'None'), ('V-20', 'P-14', '2023-02-01', 100.0, 'None'),
('V-21', 'P-15', '2023-02-01', 100.0, 'None'), ('V-22', 'P-16', '2023-02-01', 100.0, 'None'),
('V-23', 'P-17', '2023-02-01', 100.0, 'None'), ('V-24', 'P-18', '2023-02-01', 100.0, 'None'),
('V-25', 'P-19', '2023-02-01', 100.0, 'None'), ('V-26', 'P-20', '2023-02-01', 100.0, 'None'),
('V-27', 'P-1', '2023-07-01', 100.0, 'None'), ('V-28', 'P-1', '2023-08-01', 100.0, 'None'),
('V-29', 'P-1', '2023-09-01', 100.0, 'None'), ('V-30', 'P-1', '2023-10-01', 100.0, 'None'),
('V-31', 'P-2', '2023-04-01', 100.0, 'None'), ('V-32', 'P-2', '2023-05-01', 100.0, 'None'),
('V-33', 'P-2', '2023-06-01', 100.0, 'None'), ('V-34', 'P-2', '2023-07-01', 100.0, 'None'),
('V-35', 'P-2', '2023-08-01', 100.0, 'None'), ('V-36', 'P-2', '2023-09-01', 100.0, 'None'),
('V-37', 'P-3', '2023-04-01', 100.0, 'None'), ('V-38', 'P-3', '2023-05-01', 100.0, 'None'),
('V-39', 'P-3', '2023-06-01', 100.0, 'None'), ('V-40', 'P-3', '2023-07-01', 100.0, 'None'),
('V-41', 'P-4', '2023-03-01', 100.0, 'None'), ('V-42', 'P-4', '2023-04-01', 100.0, 'None'),
('V-43', 'P-5', '2023-03-01', 100.0, 'None'), ('V-44', 'P-5', '2023-04-01', 100.0, 'None'),
('V-45', 'P-6', '2023-03-01', 100.0, 'None'), ('V-46', 'P-6', '2023-04-01', 100.0, 'None'),
('V-47', 'P-7', '2023-03-01', 100.0, 'None'), ('V-48', 'P-7', '2023-04-01', 100.0, 'None'),
('V-49', 'P-8', '2023-03-01', 100.0, 'None'), ('V-50', 'P-8', '2023-04-01', 100.0, 'None');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze trial results in Pharma_R_and_D.Clinical.
 *  
 *  1. Bronze: Raw View of VISITS, PATIENTS, TRIALS.
 *  2. Silver: 
 *     - Join: VISITS -> PATIENTS -> TRIALS.
 *     - Clean: Remove negative Bio_Marker_Val.
 *  3. Gold: 
 *     - Efficacy Report: Avg Bio_Marker by Treatment_Group per Trial.
 *  
 *  Show the SQL."
 */
