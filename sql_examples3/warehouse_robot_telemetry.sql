/*
 * Dremio "Messy Data" Challenge: Warehouse Robot Telemetry
 * 
 * Scenario: 
 * Fleet of autonomous robots (Bots) in a warehouse.
 * 'Bot_ID' collisions happen (two bots claim ID 101).
 * 'Battery_Pct' drops to NULL during reboots.
 * 'Status' includes 'Idle', 'Picking', 'Charging', 'Error 505'.
 * 
 * Objective for AI Agent:
 * 1. Identify 'Duplicate' Bots: Where same Bot_ID reports from different 'Zone_ID' at same time.
 * 2. Impute Null Battery: Use LAST_VALUE() to fill forward.
 * 3. Analyze Fleet Efficiency: % of time spent 'Error'.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Amazonia_Warehousing;
CREATE FOLDER IF NOT EXISTS Amazonia_Warehousing.Fleet_Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS (
    BOT_ID INT,
    ZONE_ID VARCHAR,
    BATTERY_PCT INT, -- 0-100
    STATUS_CODE VARCHAR,
    TS TIMESTAMP
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Collisions, Gaps)
-------------------------------------------------------------------------------

-- Normal
INSERT INTO Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS VALUES
(101, 'Z-1', 90, 'Picking', '2023-01-01 10:00:00'),
(101, 'Z-1', 89, 'Picking', '2023-01-01 10:01:00');

-- Collision (Same ID, diff Zone)
INSERT INTO Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS VALUES
(101, 'Z-99', 50, 'Charging', '2023-01-01 10:00:00'); -- Imposter Bot 101

-- Battery Gaps
INSERT INTO Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS VALUES
(102, 'Z-2', 50, 'Idle', '2023-01-01 10:00:00'),
(102, 'Z-2', NULL, 'Rebooting', '2023-01-01 10:05:00'), -- Loss of telemetry
(102, 'Z-2', 45, 'Idle', '2023-01-01 10:10:00');

-- Error Codes
INSERT INTO Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS VALUES
(103, 'Z-3', 10, 'Error 404', '2023-01-01 11:00:00'), -- Lost
(103, 'Z-3', 9, 'Error 500', '2023-01-01 11:05:00'); -- Crash

-- Bulk Fill
INSERT INTO Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS VALUES
(104, 'Z-1', 100, 'Picking', '2023-01-01 09:00:00'),
(104, 'Z-1', 99, 'Picking', '2023-01-01 09:05:00'),
(104, 'Z-1', 98, 'Picking', '2023-01-01 09:10:00'),
(104, 'Z-1', 97, 'Picking', '2023-01-01 09:15:00'),
(104, 'Z-1', 96, 'Picking', '2023-01-01 09:20:00'),
(105, 'Z-2', 20, 'Charging', '2023-01-01 09:00:00'),
(105, 'Z-2', 25, 'Charging', '2023-01-01 09:05:00'),
(105, 'Z-2', 30, 'Charging', '2023-01-01 09:10:00'),
(105, 'Z-2', 35, 'Charging', '2023-01-01 09:15:00'),
(105, 'Z-2', 40, 'Charging', '2023-01-01 09:20:00'),
(106, 'Z-3', 0, 'Dead', '2023-01-01 09:00:00'),
(106, 'Z-3', 0, 'Dead', '2023-01-01 09:05:00'),
(106, 'Z-3', 0, 'Dead', '2023-01-01 09:10:00'),
(107, 'Z-4', 50, 'Idle', '2023-01-01 09:00:00'),
(107, 'Z-4', 50, 'Idle', '2023-01-01 09:05:00'),
(107, 'Z-4', 50, 'Idle', '2023-01-01 09:10:00'),
(108, 'Z-5', 110, 'Overcharge', '2023-01-01 09:00:00'), -- Physics error
(108, 'Z-5', -10, 'Undercharge', '2023-01-01 09:05:00'), -- Physics error
(109, 'Z-1', NULL, 'Unknown', '2023-01-01 09:00:00'),
(109, 'Z-1', NULL, 'Unknown', '2023-01-01 09:05:00'),
(110, 'Z-1', 80, 'Picking', '2023-01-01 12:00:00'),
(110, 'Z-1', 80, 'Picking', '2023-01-01 12:00:00'), -- Dupe
(110, 'Z-1', 80, 'Picking', '2023-01-01 12:00:00'), -- Tripe
(111, 'Z-2', 50, 'Error 1', '2023-01-01 13:00:00'),
(111, 'Z-2', 50, 'Error 2', '2023-01-01 13:05:00'),
(111, 'Z-2', 50, 'Error 3', '2023-01-01 13:10:00'),
(112, 'Z-3', 100, 'Picking', '2023-01-01 14:00:00'),
(112, 'Z-3', 90, 'Picking', '2023-01-01 15:00:00'),
(112, 'Z-3', 80, 'Picking', '2023-01-01 16:00:00'),
(112, 'Z-3', 70, 'Picking', '2023-01-01 17:00:00'),
(112, 'Z-3', 60, 'Picking', '2023-01-01 18:00:00'),
(113, 'Z-4', 100, 'Charging', '2023-01-01 14:00:00'), -- Already full?
(114, 'Z-5', 0, 'Charging', '2023-01-01 14:00:00'), -- Dead battery charge start
(115, 'Z-6', 75, 'Idle', '2023-01-01 01:00:00'),
(115, 'Z-6', 74, 'Idle', '2023-01-01 02:00:00'),
(115, 'Z-6', 73, 'Idle', '2023-01-01 03:00:00'),
(115, 'Z-6', 72, 'Idle', '2023-01-01 04:00:00'),
(115, 'Z-6', 71, 'Idle', '2023-01-01 05:00:00');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Monitor the warehouse fleet in Amazonia_Warehousing.Fleet_Logs.ROBOT_STATUS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Battery: Set < 0 to 0, > 100 to 100. Impute NULLs with 50.
 *     - Flag 'Errors': If STATUS_CODE starts with 'Error'.
 *  3. Gold: 
 *     - Uptime Report: Pct of records where Status != Error per Bot_ID.
 *     - Zone Activity: Count distinct Bots per Zone_ID.
 *  
 *  Show the SQL."
 */
