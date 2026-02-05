/*
 * Dremio "Messy Data" Challenge: Factory Predictive Maintenance
 * 
 * Scenario: 
 * Manufacturing plant sensors.
 * 3 Tables: LINES (Meta), MACHINES (Asset), SENSOR_DATA (Telemetry).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: SENSOR_DATA -> MACHINES -> LINES.
 * 2. Anomaly Detect: Temp > 100 AND Vibration > 5.0.
 * 3. Uptime: Count(Hours) where Status='Running'.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Mfg_Plant;
CREATE FOLDER IF NOT EXISTS Mfg_Plant.Shop_Floor;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Mfg_Plant.Shop_Floor.LINES (
    LINE_ID VARCHAR,
    NAME VARCHAR,
    MANAGER VARCHAR
);

CREATE TABLE IF NOT EXISTS Mfg_Plant.Shop_Floor.MACHINES (
    MACHINE_ID VARCHAR,
    LINE_ID VARCHAR,
    TYPE VARCHAR -- 'CNC', 'Lathe', 'Press'
);

CREATE TABLE IF NOT EXISTS Mfg_Plant.Shop_Floor.SENSOR_DATA (
    READING_ID VARCHAR,
    MACHINE_ID VARCHAR,
    TS TIMESTAMP,
    TEMP_C DOUBLE,
    VIBRATION_G DOUBLE,
    STATUS VARCHAR -- 'Running', 'Idle', 'Down'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- LINES
INSERT INTO Mfg_Plant.Shop_Floor.LINES VALUES
('L-1', 'Assembly A', 'Alice'), ('L-2', 'Assembly B', 'Bob');

-- MACHINES
INSERT INTO Mfg_Plant.Shop_Floor.MACHINES VALUES
('M-1', 'L-1', 'CNC'), ('M-2', 'L-1', 'Press'), ('M-3', 'L-2', 'Lathe');

-- SENSOR_DATA (50 Rows)
INSERT INTO Mfg_Plant.Shop_Floor.SENSOR_DATA VALUES
('R-1', 'M-1', '2023-01-01 10:00:00', 80.0, 1.0, 'Running'),
('R-2', 'M-1', '2023-01-01 10:05:00', 82.0, 1.1, 'Running'),
('R-3', 'M-1', '2023-01-01 10:10:00', 85.0, 1.5, 'Running'),
('R-4', 'M-1', '2023-01-01 10:15:00', 90.0, 2.0, 'Running'),
('R-5', 'M-1', '2023-01-01 10:20:00', 105.0, 5.5, 'Running'), -- Critical
('R-6', 'M-1', '2023-01-01 10:25:00', 120.0, 6.0, 'Down'), -- Halted
('R-7', 'M-2', '2023-01-01 10:00:00', 60.0, 0.5, 'Running'),
('R-8', 'M-2', '2023-01-01 10:05:00', 60.0, 0.5, 'Running'),
('R-9', 'M-2', '2023-01-01 10:10:00', 60.0, 0.5, 'Running'),
('R-10', 'M-2', '2023-01-01 10:15:00', 60.0, 0.5, 'Running'),
('R-11', 'M-3', '2023-01-01 10:00:00', 70.0, 0.8, 'Running'),
('R-12', 'M-3', '2023-01-01 10:05:00', 70.0, 0.8, 'Running'),
('R-13', 'M-3', '2023-01-01 10:10:00', 70.0, 0.8, 'Running'),
('R-14', 'M-3', '2023-01-01 10:15:00', 70.0, 0.8, 'Running'),
('R-15', 'M-3', '2023-01-01 10:20:00', 70.0, 0.8, 'Running'),
('R-16', 'M-1', '2023-01-01 11:00:00', 80.0, 1.0, 'Running'),
('R-17', 'M-1', '2023-01-01 11:05:00', 80.0, 1.0, 'Running'),
('R-18', 'M-1', '2023-01-01 11:10:00', 80.0, 1.0, 'Running'),
('R-19', 'M-1', '2023-01-01 11:15:00', 80.0, 1.0, 'Running'),
('R-20', 'M-1', '2023-01-01 11:20:00', 80.0, 1.0, 'Running'),
('R-21', 'M-1', '2023-01-01 11:25:00', 80.0, 1.0, 'Running'),
('R-22', 'M-1', '2023-01-01 11:30:00', 80.0, 1.0, 'Running'),
('R-23', 'M-1', '2023-01-01 11:35:00', 80.0, 1.0, 'Running'),
('R-24', 'M-1', '2023-01-01 11:40:00', 80.0, 1.0, 'Running'),
('R-25', 'M-1', '2023-01-01 11:45:00', 80.0, 1.0, 'Running'),
('R-26', 'M-2', '2023-01-01 11:00:00', 60.0, 0.5, 'Running'),
('R-27', 'M-2', '2023-01-01 11:05:00', 60.0, 0.5, 'Running'),
('R-28', 'M-2', '2023-01-01 11:10:00', 60.0, 0.5, 'Running'),
('R-29', 'M-2', '2023-01-01 11:15:00', 60.0, 0.5, 'Running'),
('R-30', 'M-2', '2023-01-01 11:20:00', 60.0, 0.5, 'Running'),
('R-31', 'M-2', '2023-01-01 11:25:00', 60.0, 0.5, 'Running'),
('R-32', 'M-2', '2023-01-01 11:30:00', 60.0, 0.5, 'Running'),
('R-33', 'M-2', '2023-01-01 11:35:00', 60.0, 0.5, 'Running'),
('R-34', 'M-2', '2023-01-01 11:40:00', 60.0, 0.5, 'Running'),
('R-35', 'M-2', '2023-01-01 11:45:00', 60.0, 0.5, 'Running'),
('R-36', 'M-3', '2023-01-01 11:00:00', 70.0, 0.8, 'Running'),
('R-37', 'M-3', '2023-01-01 11:05:00', 70.0, 0.8, 'Running'),
('R-38', 'M-3', '2023-01-01 11:10:00', 70.0, 0.8, 'Running'),
('R-39', 'M-3', '2023-01-01 11:15:00', 70.0, 0.8, 'Running'),
('R-40', 'M-3', '2023-01-01 11:20:00', 70.0, 0.8, 'Running'),
('R-41', 'M-3', '2023-01-01 11:25:00', 70.0, 0.8, 'Running'),
('R-42', 'M-3', '2023-01-01 11:30:00', 70.0, 0.8, 'Running'),
('R-43', 'M-3', '2023-01-01 11:35:00', 70.0, 0.8, 'Running'),
('R-44', 'M-3', '2023-01-01 11:40:00', 70.0, 0.8, 'Running'),
('R-45', 'M-3', '2023-01-01 11:45:00', 70.0, 0.8, 'Running'),
('R-46', 'M-1', '2023-01-01 12:00:00', NULL, 1.0, 'Running'),
('R-47', 'M-1', '2023-01-01 12:05:00', 80.0, NULL, 'Running'),
('R-48', 'M-1', '2023-01-01 12:10:00', 999.0, 1.0, 'Error'),
('R-49', 'M-2', '2023-01-01 12:00:00', 60.0, 0.5, 'Idle'),
('R-50', 'M-3', '2023-01-01 12:00:00', 70.0, 0.8, 'Idle');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Monitor factory health in Mfg_Plant.Shop_Floor.
 *  
 *  1. Bronze: Raw View of SENSOR_DATA, MACHINES, LINES.
 *  2. Silver: 
 *     - Join: SENSOR_DATA -> MACHINES -> LINES.
 *     - Flags: 'Overheat' (Temp > 100), 'Shake' (Vibration > 5).
 *  3. Gold: 
 *     - Maintenance List: Machines with > 3 'Overheat' events in last hour.
 *  
 *  Show the SQL."
 */
