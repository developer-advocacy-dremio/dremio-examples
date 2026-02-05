/*
 * Dremio "Messy Data" Challenge: Messy IoT Sensors
 * 
 * Scenario: 
 * A network of industrial sensors creates a data dump.
 * The data has "Null" sensor IDs, uncalibrated values (e.g., -9999), 
 * and exact duplicates due to network re-transmissions.
 * 
 * Objective for AI Agent:
 * 1. Filter out records where Sensor_ID is NULL.
 * 2. Identify and filter out "Error Codes" (Values < -100).
 * 3. Deduplicate records based on Timestamp and Sensor_ID.
 * 4. Create a Silver view with clean data.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS IoT_Dump_Zone;
CREATE FOLDER IF NOT EXISTS IoT_Dump_Zone.Raw_Stream;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS IoT_Dump_Zone.Raw_Stream.SENSORS_V1 (
    S_ID VARCHAR,
    TS TIMESTAMP,
    VAL_1 DOUBLE, -- Temptature?
    VAL_2 DOUBLE, -- Pressure?
    STATUS VARCHAR -- 'OK', 'ERR', NULL
);

CREATE TABLE IF NOT EXISTS IoT_Dump_Zone.Raw_Stream.SENSORS_REF (
    S_ID VARCHAR,
    LOC VARCHAR, -- Location
    MODEL VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Duplicates, Nulls, Outliers)
-------------------------------------------------------------------------------

-- Reference Table
INSERT INTO IoT_Dump_Zone.Raw_Stream.SENSORS_REF VALUES
('S-001', 'Factory Floor', 'Type A'),
('S-002', 'Boiler Room', 'Type B'),
('S-003', 'Roof', 'Type A');

-- Stream Data
-- Normal Data
INSERT INTO IoT_Dump_Zone.Raw_Stream.SENSORS_V1 VALUES
('S-001', '2025-01-01 10:00:00', 45.5, 101.3, 'OK'),
('S-002', '2025-01-01 10:00:00', 60.2, 200.5, 'OK'),
('S-003', '2025-01-01 10:00:00', 20.1, 99.8, 'OK');

-- Duplicates (Network Retry)
INSERT INTO IoT_Dump_Zone.Raw_Stream.SENSORS_V1 VALUES
('S-001', '2025-01-01 10:00:00', 45.5, 101.3, 'OK'), -- Exact Dupe
('S-001', '2025-01-01 10:00:00', 45.5, 101.3, 'OK'); -- Triplicate

-- Null IDs (Corrupt packet)
INSERT INTO IoT_Dump_Zone.Raw_Stream.SENSORS_V1 VALUES
(NULL, '2025-01-01 10:01:00', 0.0, 0.0, 'ERR'),
(NULL, '2025-01-01 10:01:05', 0.0, 0.0, NULL);

-- Uncalibrated / Error Values
INSERT INTO IoT_Dump_Zone.Raw_Stream.SENSORS_V1 VALUES
('S-001', '2025-01-01 10:05:00', -9999.0, -9999.0, 'OK'), -- Sensor glitch
('S-002', '2025-01-01 10:05:00', 60.5, -500.0, 'OK'); -- One bad val

-- Bulk Fill
INSERT INTO IoT_Dump_Zone.Raw_Stream.SENSORS_V1 VALUES
('S-001', '2025-01-01 10:10:00', 46.0, 101.0, 'OK'),
('S-001', '2025-01-01 10:15:00', 46.5, 101.2, 'OK'),
('S-001', '2025-01-01 10:20:00', 47.0, 101.4, 'OK'),
('S-001', '2025-01-01 10:25:00', 47.5, 101.6, 'OK'),
('S-001', '2025-01-01 10:30:00', 48.0, 101.8, 'OK'),
('S-002', '2025-01-01 10:10:00', 61.0, 201.0, 'OK'),
('S-002', '2025-01-01 10:15:00', 61.5, 201.5, 'OK'),
('S-002', '2025-01-01 10:20:00', 62.0, 202.0, 'OK'),
('S-002', '2025-01-01 10:25:00', 62.5, 202.5, 'OK'),
('S-002', '2025-01-01 10:30:00', 63.0, 203.0, 'OK'),
('S-003', '2025-01-01 10:10:00', 21.0, 100.0, 'OK'),
('S-003', '2025-01-01 10:15:00', 21.5, 100.2, 'OK'),
('S-003', '2025-01-01 10:20:00', 22.0, 100.4, 'OK'),
('S-003', '2025-01-01 10:25:00', 22.5, 100.6, 'OK'),
('S-003', '2025-01-01 10:30:00', 23.0, 100.8, 'OK'),
-- More Duplicates
('S-001', '2025-01-01 10:10:00', 46.0, 101.0, 'OK'),
('S-002', '2025-01-01 10:10:00', 61.0, 201.0, 'OK'),
-- More Errors
('S-001', '2025-01-01 10:35:00', NULL, NULL, 'MISSING'),
('S-002', '2025-01-01 10:35:00', 64.0, 204.0, 'OK'),
('S-003', '2025-01-01 10:35:00', -1.0, -1.0, 'CALIB'),
('S-001', '2025-01-01 10:40:00', 49.0, 102.0, 'OK'),
('S-001', '2025-01-01 10:45:00', 49.5, 102.2, 'OK'),
('S-001', '2025-01-01 10:50:00', 50.0, 102.4, 'OK'),
('S-001', '2025-01-01 10:55:00', 50.5, 102.6, 'OK'),
('S-001', '2025-01-01 11:00:00', 51.0, 102.8, 'OK'),
('S-002', '2025-01-01 10:40:00', 65.0, 205.0, 'OK'),
('S-002', '2025-01-01 10:45:00', 65.5, 205.5, 'OK'),
('S-002', '2025-01-01 10:50:00', 66.0, 206.0, 'OK'),
('S-002', '2025-01-01 10:55:00', 66.5, 206.5, 'OK'),
('S-002', '2025-01-01 11:00:00', 67.0, 207.0, 'OK'),
('S-003', '2025-01-01 10:40:00', 25.0, 101.0, 'OK'),
('S-003', '2025-01-01 10:45:00', 25.5, 101.2, 'OK'),
('S-003', '2025-01-01 10:50:00', 26.0, 101.4, 'OK'),
('S-003', '2025-01-01 10:55:00', 26.5, 101.6, 'OK'),
('S-003', '2025-01-01 11:00:00', 27.0, 101.8, 'OK'),
('S-002', '2025-01-01 11:05:00', 68.0, 208.0, 'OK'),
('S-002', '2025-01-01 11:10:00', 69.0, 209.0, 'OK'),
('S-002', '2025-01-01 11:15:00', 70.0, 210.0, 'OK'),
('S-002', '2025-01-01 11:20:00', 71.0, 211.0, 'OK'),
('S-002', '2025-01-01 11:25:00', 72.0, 212.0, 'OK'),
('S-004', '2025-01-01 11:00:00', 0.0, 0.0, 'NEW'); -- Unknown ID

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "I have raw sensor data in IoT_Dump_Zone.Raw_Stream.SENSORS_V1.
 *  Please generate a clean Medallion Architecture.
 *  
 *  1. Bronze: Create a view for the raw stream.
 *  2. Silver: 
 *     - Filter out records where S_ID is NULL.
 *     - Remove records where measurements are < -100 (Sensor Errors).
 *     - Deduplicate records based on Timestamp and S_ID.
 *  3. Gold: 
 *     - Calculate the Average Temperature (VAL_1) per Sensor ID per hour.
 *  
 *  Generate the SQL to build these views."
 */
