/*
 * Dremio "Messy Data" Challenge: Aquarium Water Quality
 * 
 * Scenario: 
 * Public aquarium sensor network with drift and calibration issues.
 * 3 Tables: TANKS, SENSORS, READINGS.
 * 
 * Objective for AI Agent:
 * 1. Sensor Mapping: Join READINGS -> SENSORS -> TANKS.
 * 2. Unit Normalization: Convert mixed temp units (F vs C) to Celsius.
 * 3. Outlier Filtering: Remove impossible pH (<0 or >14) and negative dissolved O2.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Coral_Reef_Aquarium;
CREATE FOLDER IF NOT EXISTS Coral_Reef_Aquarium.Monitoring;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Coral_Reef_Aquarium.Monitoring.TANKS (
    TANK_ID VARCHAR,
    TANK_NAME VARCHAR,
    SPECIES_TYPE VARCHAR,
    VOLUME_GAL DOUBLE
);

CREATE TABLE IF NOT EXISTS Coral_Reef_Aquarium.Monitoring.SENSORS (
    SENSOR_ID VARCHAR, -- Old format 'S-001', new format 'SNS-00001'
    TANK_ID VARCHAR,
    SENSOR_TYPE VARCHAR, -- 'pH', 'TEMP', 'DO' (dissolved oxygen)
    INSTALL_DT DATE
);

CREATE TABLE IF NOT EXISTS Coral_Reef_Aquarium.Monitoring.READINGS (
    READING_ID VARCHAR,
    SENSOR_ID VARCHAR,
    TS TIMESTAMP,
    RAW_VALUE DOUBLE,
    UNIT_TAG VARCHAR -- 'C', 'F', 'pH', 'mg/L', or NULL
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- TANKS (8 rows)
INSERT INTO Coral_Reef_Aquarium.Monitoring.TANKS VALUES
('TK-001', 'Tropical Reef', 'Coral', 20000),
('TK-002', 'Kelp Forest', 'Temperate', 50000),
('TK-003', 'Jellyfish Gallery', 'Invertebrate', 5000),
('TK-004', 'Shark Cove', 'Elasmobranch', 100000),
('TK-005', 'Freshwater Falls', 'Freshwater', 15000),
('TK-006', 'Penguin Pool', 'Avian', 30000),
('TK-007', 'Touch Pool', 'Interactive', 2000),
('TK-008', 'Quarantine', 'Mixed', 500);

-- SENSORS (12 rows - note old vs new ID format)
INSERT INTO Coral_Reef_Aquarium.Monitoring.SENSORS VALUES
('S-001', 'TK-001', 'pH', '2020-01-15'),
('S-002', 'TK-001', 'TEMP', '2020-01-15'),
('S-003', 'TK-001', 'DO', '2020-01-15'),
('S-004', 'TK-002', 'pH', '2020-06-01'),
('S-005', 'TK-002', 'TEMP', '2020-06-01'),
('SNS-00006', 'TK-003', 'pH', '2023-03-01'),     -- New format
('SNS-00007', 'TK-003', 'TEMP', '2023-03-01'),
('SNS-00008', 'TK-004', 'pH', '2023-05-10'),
('SNS-00009', 'TK-004', 'TEMP', '2023-05-10'),
('SNS-00010', 'TK-005', 'pH', '2023-07-20'),
('SNS-00011', 'TK-005', 'TEMP', '2023-07-20'),
('SNS-00012', 'TK-005', 'DO', '2023-07-20');

-- READINGS (50+ rows with mess)
INSERT INTO Coral_Reef_Aquarium.Monitoring.READINGS VALUES
-- Normal pH readings
('R-001', 'S-001', '2023-06-01 08:00:00', 8.2, 'pH'),
('R-002', 'S-001', '2023-06-01 12:00:00', 8.3, 'pH'),
('R-003', 'S-001', '2023-06-01 16:00:00', 8.1, 'pH'),
('R-004', 'S-001', '2023-06-01 20:00:00', 8.25, 'pH'),
('R-005', 'S-004', '2023-06-01 08:00:00', 7.9, 'pH'),
('R-006', 'S-004', '2023-06-01 12:00:00', 7.8, 'pH'),
-- Impossible pH values (calibration failure)
('R-007', 'S-001', '2023-06-02 08:00:00', 0.5, 'pH'),      -- Way too low
('R-008', 'SNS-00006', '2023-06-02 08:00:00', 14.2, 'pH'),  -- Way too high
('R-009', 'SNS-00008', '2023-06-02 12:00:00', -1.0, 'pH'),  -- Negative pH
-- Temperature in Celsius
('R-010', 'S-002', '2023-06-01 08:00:00', 25.5, 'C'),
('R-011', 'S-002', '2023-06-01 12:00:00', 26.0, 'C'),
('R-012', 'S-002', '2023-06-01 16:00:00', 25.8, 'C'),
('R-013', 'S-005', '2023-06-01 08:00:00', 15.0, 'C'),
('R-014', 'S-005', '2023-06-01 12:00:00', 15.2, 'C'),
-- Temperature in Fahrenheit (needs conversion!)
('R-015', 'SNS-00007', '2023-06-01 08:00:00', 75.0, 'F'),
('R-016', 'SNS-00007', '2023-06-01 12:00:00', 76.5, 'F'),
('R-017', 'SNS-00009', '2023-06-01 08:00:00', 78.0, 'F'),
('R-018', 'SNS-00009', '2023-06-01 12:00:00', 79.2, 'F'),
-- Temperature with NULL unit (ambiguous!)
('R-019', 'SNS-00011', '2023-06-01 08:00:00', 68.0, NULL),  -- F or C?
('R-020', 'SNS-00011', '2023-06-01 12:00:00', 22.0, NULL),  -- Likely C
-- Dissolved Oxygen normal
('R-021', 'S-003', '2023-06-01 08:00:00', 6.5, 'mg/L'),
('R-022', 'S-003', '2023-06-01 12:00:00', 6.8, 'mg/L'),
('R-023', 'S-003', '2023-06-01 16:00:00', 7.0, 'mg/L'),
('R-024', 'SNS-00012', '2023-06-01 08:00:00', 8.0, 'mg/L'),
('R-025', 'SNS-00012', '2023-06-01 12:00:00', 7.5, 'mg/L'),
-- Dissolved Oxygen impossible
('R-026', 'S-003', '2023-06-02 08:00:00', -2.0, 'mg/L'),   -- Negative DO
('R-027', 'SNS-00012', '2023-06-02 08:00:00', 999.0, 'mg/L'), -- Sensor stuck
-- Orphan sensor ID (no match in SENSORS table)
('R-028', 'S-999', '2023-06-01 08:00:00', 7.0, 'pH'),
('R-029', 'UNKNOWN', '2023-06-01 08:00:00', 25.0, 'C'),
-- Duplicates
('R-030', 'S-001', '2023-06-03 08:00:00', 8.1, 'pH'),
('R-030', 'S-001', '2023-06-03 08:00:00', 8.1, 'pH'),       -- Exact dupe
-- Bulk fill normal readings
('R-031', 'S-001', '2023-06-04 08:00:00', 8.15, 'pH'),
('R-032', 'S-001', '2023-06-04 12:00:00', 8.18, 'pH'),
('R-033', 'S-001', '2023-06-04 16:00:00', 8.20, 'pH'),
('R-034', 'S-002', '2023-06-04 08:00:00', 25.3, 'C'),
('R-035', 'S-002', '2023-06-04 12:00:00', 25.6, 'C'),
('R-036', 'S-002', '2023-06-04 16:00:00', 25.9, 'C'),
('R-037', 'S-003', '2023-06-04 08:00:00', 6.9, 'mg/L'),
('R-038', 'S-003', '2023-06-04 12:00:00', 7.1, 'mg/L'),
('R-039', 'S-004', '2023-06-04 08:00:00', 8.0, 'pH'),
('R-040', 'S-004', '2023-06-04 12:00:00', 7.95, 'pH'),
('R-041', 'S-005', '2023-06-04 08:00:00', 14.8, 'C'),
('R-042', 'S-005', '2023-06-04 12:00:00', 15.1, 'C'),
('R-043', 'SNS-00006', '2023-06-04 08:00:00', 7.5, 'pH'),
('R-044', 'SNS-00006', '2023-06-04 12:00:00', 7.6, 'pH'),
('R-045', 'SNS-00007', '2023-06-04 08:00:00', 74.0, 'F'),
('R-046', 'SNS-00007', '2023-06-04 12:00:00', 75.5, 'F'),
('R-047', 'SNS-00008', '2023-06-04 08:00:00', 8.1, 'pH'),
('R-048', 'SNS-00009', '2023-06-04 08:00:00', 77.0, 'F'),
('R-049', 'SNS-00010', '2023-06-04 08:00:00', 7.2, 'pH'),
('R-050', 'SNS-00011', '2023-06-04 08:00:00', 21.5, NULL),  -- Ambiguous unit
('R-051', 'SNS-00012', '2023-06-04 08:00:00', 8.2, 'mg/L'),
('R-052', 'S-002', '2023-06-05 08:00:00', 0.0, 'C'),        -- Suspicious zero
('R-053', 'S-001', '2023-06-05 08:00:00', NULL, 'pH'),       -- NULL value
('R-054', 'SNS-00009', '2023-06-05 08:00:00', 212.0, 'F');   -- Boiling water?

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze water quality data in Coral_Reef_Aquarium.Monitoring.
 *  
 *  1. Bronze: Raw Views of TANKS, SENSORS, READINGS.
 *  2. Silver: 
 *     - Join: READINGS -> SENSORS -> TANKS.
 *     - Normalize Temp: IF UNIT_TAG = 'F' THEN (RAW_VALUE - 32) * 5/9 ELSE RAW_VALUE.
 *     - Filter Outliers: Remove pH < 0 or pH > 14, DO < 0, Temp > 50C.
 *     - Deduplicate by READING_ID.
 *  3. Gold: 
 *     - Daily Avg pH, Temp, DO per Tank.
 *     - Alert List: Readings outside safe ranges per species.
 *  
 *  Show the SQL."
 */
