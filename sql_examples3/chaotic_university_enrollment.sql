/*
 * Dremio "Messy Data" Challenge: Chaotic University Enrollment
 * 
 * Scenario: 
 * Student ID system migration. 
 * 'Old' IDs are 5 digits. 'New' IDs are 'S-' + 8 digits.
 * 'Grade' column mixes Letter grades (A, B) and GPA floats (4.0, 3.0).
 * 
 * Objective for AI Agent:
 * 1. Normalize Student IDs: Prefix old IDs with 'OLD-'.
 * 2. Normalize Grades: Map Letter grades to Numeric points (A=4.0, B=3.0, etc.).
 * 3. Calculate Average GPA per Course.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Uni_Registrar;
CREATE FOLDER IF NOT EXISTS Uni_Registrar.Transcript_Dump;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Uni_Registrar.Transcript_Dump.GRADES (
    STUDENT_ID VARCHAR,
    COURSE_CODE VARCHAR,
    GRADE_RAW VARCHAR, -- 'A', '3.5'
    SEMESTER VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (ID Migration, Grade Mixing)
-------------------------------------------------------------------------------

-- New IDs, GPA format
INSERT INTO Uni_Registrar.Transcript_Dump.GRADES VALUES
('S-20230001', 'CS101', '4.0', 'Fall 2023'),
('S-20230002', 'CS101', '3.5', 'Fall 2023');

-- Old IDs, Letter format
INSERT INTO Uni_Registrar.Transcript_Dump.GRADES VALUES
('12345', 'MATH101', 'A', 'Spring 2020'),
('67890', 'MATH101', 'B+', 'Spring 2020');

-- Mixed
INSERT INTO Uni_Registrar.Transcript_Dump.GRADES VALUES
('S-20230003', 'HIST101', 'A-', 'Fall 2023'),
('54321', 'HIST101', '3.7', 'Spring 2020');

-- Errors
INSERT INTO Uni_Registrar.Transcript_Dump.GRADES VALUES
('99999', 'CS101', 'Incomplete', 'Spring 2020'), -- 'I' grade
('S-0000', 'CS101', 'W', 'Summer 2023'); -- Withdraw

-- Bulk Fill
INSERT INTO Uni_Registrar.Transcript_Dump.GRADES VALUES
('10001', 'ENG101', 'A', 'Fall 2019'),
('10002', 'ENG101', 'B', 'Fall 2019'),
('10003', 'ENG101', 'C', 'Fall 2019'),
('10004', 'ENG101', 'D', 'Fall 2019'),
('10005', 'ENG101', 'F', 'Fall 2019'),
('S-20230100', 'ENG101', '4.0', 'Fall 2023'),
('S-20230101', 'ENG101', '3.0', 'Fall 2023'),
('S-20230102', 'ENG101', '2.0', 'Fall 2023'),
('S-20230103', 'ENG101', '1.0', 'Fall 2023'),
('S-20230104', 'ENG101', '0.0', 'Fall 2023'),
('20001', 'PHY101', 'A+', 'Spring 2020'), -- 4.3?
('20002', 'PHY101', 'A-', 'Spring 2020'), -- 3.7
('20003', 'PHY101', 'B+', 'Spring 2020'), -- 3.3
('S-20230200', 'PHY101', '4.3', 'Fall 2023'),
('S-20230201', 'PHY101', '3.7', 'Fall 2023'),
('S-20230202', 'PHY101', '3.3', 'Fall 2023'),
('30001', 'CHEM101', 'P', 'Spring 2021'), -- Pass (No GPA)
('30002', 'CHEM101', 'F', 'Spring 2021'),
('S-20230300', 'CHEM101', 'Pass', 'Fall 2023'),
('S-20230301', 'CHEM101', 'Fail', 'Fall 2023'),
('40001', 'ART101', 'A', 'Fall 2022'),
('40001', 'ART101', 'A', 'Fall 2022'), -- Dupe
('40002', 'ART101', NULL, 'Fall 2022'), -- Null Grade
('NULL', 'ART101', 'A', 'Fall 2022'), -- Null ID string
(NULL, 'ART101', 'B', 'Fall 2022'),
('50001', 'SOC101', '100', 'Fall 2021'), -- Percentage?
('50002', 'SOC101', '90', 'Fall 2021'),
('S-20230400', 'SOC101', '95.5', 'Fall 2023'),
('60001', 'MUS101', 'A ', 'Fall 2020'), -- Trailing space
('60002', 'MUS101', ' b', 'Fall 2020'), -- Leading space
('60003', 'MUS101', 'C+', 'Fall 2020'),
('S-20230500', 'MUS101', '3. 8', 'Fall 2023'), -- Space in float
('S-20230501', 'MUS101', '4,0', 'Fall 2023'), -- Comma decimal
('70001', 'BIO101', 'A', 'Spring 2022'),
('70002', 'BIO101', 'B', 'Spring 2022'),
('70003', 'BIO101', 'C', 'Spring 2022'),
('70004', 'BIO101', 'D', 'Spring 2022'),
('70005', 'BIO101', 'F', 'Spring 2022'),
('80001', 'GEO101', 'A', 'Spring 2022'),
('80002', 'GEO101', 'B', 'Spring 2022'),
('80003', 'GEO101', 'C', 'Spring 2022'),
('80004', 'GEO101', 'D', 'Spring 2022'),
('80005', 'GEO101', 'F', 'Spring 2022'),
('90001', 'ECO101', 'A', 'Spring 2022'),
('90002', 'ECO101', 'B', 'Spring 2022'),
('90003', 'ECO101', 'C', 'Spring 2022'),
('90004', 'ECO101', 'D', 'Spring 2022'),
('90005', 'ECO101', 'F', 'Spring 2022');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Standardize grades in Uni_Registrar.Transcript_Dump.GRADES.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Student IDs: If length is 5, prepend 'OLD-'.
 *     - Normalize Grades: 
 *       - Map 'A'->4.0, 'B'->3.0, 'C'->2.0, 'D'->1.0, 'F'->0.0.
 *       - Handle 'W' or 'I' as NULL (Non-numeric).
 *       - Cast numeric strings ('3.5') to DOUBLE.
 *  3. Gold: 
 *     - Calculate Avg GPA by Course Code.
 *  
 *  Show the SQL."
 */
