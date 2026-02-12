/*
 * Dremio "Messy Data" Challenge: Beekeeping Hive Logs
 * 
 * Scenario: 
 * Commercial apiary management system with hand-entered inspection logs.
 * 3 Tables: APIARIES, HIVES, INSPECTIONS.
 * 
 * Objective for AI Agent:
 * 1. Free-Text Boolean: Parse queen status ('present', 'QR', 'QUEENRIGHT', 'no Q seen') to BOOLEAN.
 * 2. Weight Parsing: Extract numeric weight from mixed formats ('45', '45.0', '~45 lbs').
 * 3. Disease Code Unification: Map codes and descriptions ('AFB', 'American Foulbrood') to canonical form.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Golden_Bee;
CREATE FOLDER IF NOT EXISTS Golden_Bee.Apiary_Mgmt;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Golden_Bee.Apiary_Mgmt.APIARIES (
    APIARY_ID VARCHAR,
    APIARY_NAME VARCHAR,
    COUNTY VARCHAR,
    GPS_LAT DOUBLE,
    GPS_LON DOUBLE
);

CREATE TABLE IF NOT EXISTS Golden_Bee.Apiary_Mgmt.HIVES (
    HIVE_ID VARCHAR,
    APIARY_ID VARCHAR,
    HIVE_TYPE VARCHAR, -- 'Langstroth', 'Top Bar', 'Warre'
    INSTALL_DT DATE
);

CREATE TABLE IF NOT EXISTS Golden_Bee.Apiary_Mgmt.INSPECTIONS (
    INSP_ID VARCHAR,
    HIVE_ID VARCHAR,
    INSP_DT DATE,
    QUEEN_STATUS_RAW VARCHAR, -- 'present', 'QR', 'QUEENRIGHT', 'no Q seen', 'queen cells', NULL
    FRAME_COUNT_RAW VARCHAR, -- '10', 'ten', '8-10', NULL
    WEIGHT_RAW VARCHAR, -- '45', '45.0 lbs', '~20 kg', 'heavy', NULL
    DISEASE_RAW VARCHAR, -- 'AFB', 'American Foulbrood', 'varroa', 'None', 'clean', NULL
    MOOD_TAG VARCHAR -- 'Calm', 'Aggressive', 'CALM', 'calm', 'agitated'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- APIARIES (5 rows)
INSERT INTO Golden_Bee.Apiary_Mgmt.APIARIES VALUES
('AP-001', 'Sunny Meadow', 'Travis', 30.2672, -97.7431),
('AP-002', 'Oak Hill Bees', 'Williamson', 30.5083, -97.6789),
('AP-003', 'River Bend Apiary', 'Hays', 29.8833, -97.9414),
('AP-004', 'Highland Ranch', 'Travis', 30.3500, -97.8000),
('AP-005', 'Lost Pines', 'Bastrop', 30.1100, -97.3300);

-- HIVES (12 rows)
INSERT INTO Golden_Bee.Apiary_Mgmt.HIVES VALUES
('HV-001', 'AP-001', 'Langstroth', '2021-03-15'),
('HV-002', 'AP-001', 'Langstroth', '2021-03-15'),
('HV-003', 'AP-001', 'Top Bar', '2022-04-01'),
('HV-004', 'AP-002', 'Langstroth', '2020-05-10'),
('HV-005', 'AP-002', 'Langstroth', '2020-05-10'),
('HV-006', 'AP-002', 'Warre', '2023-01-20'),
('HV-007', 'AP-003', 'Top Bar', '2022-06-15'),
('HV-008', 'AP-003', 'Langstroth', '2022-06-15'),
('HV-009', 'AP-004', 'Langstroth', '2021-07-01'),
('HV-010', 'AP-004', 'Langstroth', '2021-07-01'),
('HV-011', 'AP-005', 'Langstroth', '2023-03-01'),
('HV-012', 'AP-005', 'Top Bar', '2023-03-01');

-- INSPECTIONS (50+ rows with messy fields)
INSERT INTO Golden_Bee.Apiary_Mgmt.INSPECTIONS VALUES
-- Normal entries
('INS-001', 'HV-001', '2023-04-01', 'present', '10', '45 lbs', 'None', 'Calm'),
('INS-002', 'HV-002', '2023-04-01', 'QR', '10', '42.5 lbs', 'clean', 'Calm'),
('INS-003', 'HV-003', '2023-04-01', 'QUEENRIGHT', '8', '30 lbs', 'None', 'calm'),
('INS-004', 'HV-004', '2023-04-05', 'present', '10', '50 lbs', 'None', 'Calm'),
('INS-005', 'HV-005', '2023-04-05', 'yes', '10', '48 lbs', 'varroa', 'Aggressive'),
-- Queen absent/problem
('INS-006', 'HV-006', '2023-04-05', 'no Q seen', '6', '25 lbs', 'None', 'agitated'),
('INS-007', 'HV-007', '2023-04-10', 'absent', '7', '28 lbs', 'clean', 'Aggressive'),
('INS-008', 'HV-008', '2023-04-10', 'queen cells', '9', '40 lbs', 'None', 'CALM'),
('INS-009', 'HV-009', '2023-04-15', 'NOT SEEN', '10', '55 lbs', 'varroa mites', 'calm'),
('INS-010', 'HV-010', '2023-04-15', 'QL', '10', '52 lbs', 'Varroa', 'Calm'),   -- Queenless?
-- Weight mess
('INS-011', 'HV-001', '2023-05-01', 'present', '10', '~50 lbs', 'None', 'Calm'),     -- Approximate
('INS-012', 'HV-002', '2023-05-01', 'QR', '10', '22 kg', 'None', 'Calm'),            -- Metric
('INS-013', 'HV-003', '2023-05-01', 'present', '8', 'heavy', 'None', 'calm'),         -- Text
('INS-014', 'HV-004', '2023-05-05', 'present', '10', '55', 'None', 'Calm'),            -- No units
('INS-015', 'HV-005', '2023-05-05', 'present', '10', NULL, 'None', 'Calm'),            -- NULL
('INS-016', 'HV-011', '2023-05-10', 'present', '10', '35.5lbs', 'None', 'Calm'),      -- No space
('INS-017', 'HV-012', '2023-05-10', 'QR', '8', '~15 kg', 'clean', 'calm'),
-- Frame count mess
('INS-018', 'HV-006', '2023-05-05', 'no Q seen', 'six', '20 lbs', 'EFB', 'Aggressive'),  -- Text number
('INS-019', 'HV-007', '2023-05-10', 'absent', '8-10', '30 lbs', 'clean', 'agitated'),    -- Range
('INS-020', 'HV-008', '2023-05-10', 'queen cells', NULL, '38 lbs', 'None', 'CALM'),
('INS-021', 'HV-009', '2023-05-15', 'present', '10+', '58 lbs', 'varroa', 'calm'),       -- Plus sign
-- Disease code mess
('INS-022', 'HV-001', '2023-06-01', 'present', '10', '48 lbs', 'AFB', 'Calm'),
('INS-023', 'HV-002', '2023-06-01', 'QR', '10', '45 lbs', 'American Foulbrood', 'Calm'), -- Same as AFB
('INS-024', 'HV-003', '2023-06-01', 'present', '8', '32 lbs', 'EFB', 'calm'),
('INS-025', 'HV-004', '2023-06-05', 'present', '10', '52 lbs', 'European Foulbrood', 'Calm'),
('INS-026', 'HV-005', '2023-06-05', 'present', '10', '50 lbs', 'Varroa destructor', 'Calm'),
('INS-027', 'HV-006', '2023-06-05', 'no Q seen', '6', '22 lbs', 'nosema', 'agitated'),
('INS-028', 'HV-007', '2023-06-10', 'absent', '7', '29 lbs', 'Nosema ceranae', 'Aggressive'),
('INS-029', 'HV-008', '2023-06-10', 'present', '9', '41 lbs', 'SHB', 'Calm'),
('INS-030', 'HV-009', '2023-06-15', 'present', '10', '56 lbs', 'Small Hive Beetle', 'calm'),
('INS-031', 'HV-010', '2023-06-15', 'QR', '10', '53 lbs', 'None', 'Calm'),
-- Duplicates
('INS-032', 'HV-001', '2023-04-01', 'present', '10', '45 lbs', 'None', 'Calm'),   -- Dupe of INS-001
-- Bulk fill
('INS-033', 'HV-011', '2023-06-10', 'present', '10', '38 lbs', 'None', 'Calm'),
('INS-034', 'HV-012', '2023-06-10', 'QR', '8', '28 lbs', 'clean', 'calm'),
('INS-035', 'HV-001', '2023-07-01', 'present', '10', '52 lbs', 'None', 'Calm'),
('INS-036', 'HV-002', '2023-07-01', 'QR', '10', '49 lbs', 'None', 'Calm'),
('INS-037', 'HV-003', '2023-07-01', 'present', '8', '34 lbs', 'None', 'calm'),
('INS-038', 'HV-004', '2023-07-05', 'present', '10', '54 lbs', 'None', 'Calm'),
('INS-039', 'HV-005', '2023-07-05', 'yes', '10', '51 lbs', 'varroa', 'Calm'),
('INS-040', 'HV-006', '2023-07-05', 'no Q seen', '5', '18 lbs', 'EFB', 'Aggressive'),
('INS-041', 'HV-007', '2023-07-10', 'absent', '4', '15 lbs', 'AFB', 'agitated'),
('INS-042', 'HV-008', '2023-07-10', 'present', '9', '42 lbs', 'None', 'CALM'),
('INS-043', 'HV-009', '2023-07-15', 'present', '10', '57 lbs', 'varroa', 'calm'),
('INS-044', 'HV-010', '2023-07-15', 'QR', '10', '54 lbs', 'None', 'Calm'),
('INS-045', 'HV-011', '2023-07-10', 'present', '10', '40 lbs', 'None', 'Calm'),
('INS-046', 'HV-012', '2023-07-10', 'QR', '8', '30 lbs', 'clean', 'calm'),
('INS-047', 'HV-001', '2023-08-01', 'present', '10', '55 lbs', 'None', 'Calm'),
('INS-048', 'HV-002', '2023-08-01', 'QR', '10', '50 lbs', 'None', 'Calm'),
('INS-049', 'HV-003', '2023-08-01', 'present', '8', '36 lbs', 'None', 'calm'),
('INS-050', 'HV-004', '2023-08-05', 'present', '10', '56 lbs', 'None', 'Calm');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze hive health in Golden_Bee.Apiary_Mgmt.
 *  
 *  1. Bronze: Raw Views of APIARIES, HIVES, INSPECTIONS.
 *  2. Silver: 
 *     - Queen Status: Map 'present'/'QR'/'QUEENRIGHT'/'yes' -> TRUE, 'absent'/'no Q seen'/'NOT SEEN' -> FALSE.
 *     - Parse Weight: Strip '~', 'lbs', 'kg'. IF 'kg' THEN value * 2.205. Cast to DOUBLE.
 *     - Normalize Disease: Map 'AFB'/'American Foulbrood' -> 'AFB', 'varroa'/'Varroa destructor' -> 'VARROA'.
 *     - Standardize Mood: UPPER(MOOD_TAG). Map 'agitated' -> 'AGGRESSIVE'.
 *  3. Gold: 
 *     - Queenless Hive Alert: Hives with 2+ consecutive queen-absent inspections.
 *     - Disease Prevalence by Apiary.
 *     - Weight Trend per Hive over time.
 *  
 *  Show the SQL."
 */
