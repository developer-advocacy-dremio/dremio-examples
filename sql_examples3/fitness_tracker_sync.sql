/*
 * Dremio "Messy Data" Challenge: Fitness Tracker Sync
 * 
 * Scenario: 
 * Bio-telemetry data syncs from watch to phone to cloud.
 * Timezone jumps happen when user travels (e.g. UTC-5 to UTC+1).
 * 'Heart_Rate' sometimes records 0 (loose sensor) or > 220 (artifact).
 * 
 * Objective for AI Agent:
 * 1. Detect Timezone Jumps: Sudden large gaps or negative deltas in Local Timestamp.
 * 2. Clean HR: Filter valid range (30-220 bpm) and interpolate gaps.
 * 3. Aggregates: Daily Steps and Avg HR.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Health_Cloud;
CREATE FOLDER IF NOT EXISTS Health_Cloud.User_Sync;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Health_Cloud.User_Sync.ACTIVITY_LOG (
    USER_ID VARCHAR,
    TS_LOCAL TIMESTAMP,
    TZ_OFFSET INT, -- -5, +1
    STEPS INT,
    HEART_RATE INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Gaps, Artifacts)
-------------------------------------------------------------------------------

-- Normal Day
INSERT INTO Health_Cloud.User_Sync.ACTIVITY_LOG VALUES
('U-1', '2023-01-01 08:00:00', -5, 100, 70),
('U-1', '2023-01-01 09:00:00', -5, 500, 80);

-- Sensor Artifacts
INSERT INTO Health_Cloud.User_Sync.ACTIVITY_LOG VALUES
('U-1', '2023-01-01 09:15:00', -5, 0, 0), -- Watch off?
('U-1', '2023-01-01 09:30:00', -5, 50, 300); -- Tachycardia artifact

-- Travel (Time Jump)
INSERT INTO Health_Cloud.User_Sync.ACTIVITY_LOG VALUES
('U-1', '2023-01-02 08:00:00', -5, 0, 70), -- Takeoff NYC
('U-1', '2023-01-02 20:00:00', 1, 0, 75); -- Land London (Jump in Local Time + TZ)

-- Bulk Fill
INSERT INTO Health_Cloud.User_Sync.ACTIVITY_LOG VALUES
('U-2', '2023-01-01 10:00:00', -8, 1000, 90), -- LA
('U-2', '2023-01-01 10:05:00', -8, 100, 95),
('U-2', '2023-01-01 10:10:00', -8, 100, 100),
('U-2', '2023-01-01 10:15:00', -8, 100, 105),
('U-2', '2023-01-01 10:20:00', -8, 0, NULL), -- Null HR
('U-2', '2023-01-01 10:25:00', -8, 0, NULL),
('U-3', '2023-01-01 12:00:00', 9, 50, 60), -- Tokyo
('U-3', '2023-01-01 12:01:00', 9, 50, 60),
('U-3', '2023-01-01 12:02:00', 9, 50, 60),
('U-3', '2023-01-01 12:03:00', 9, 50, 60),
('U-3', '2023-01-01 12:04:00', 9, 50, 60),
('U-4', '2023-01-01 09:00:00', -5, 100, 70),
('U-4', '2023-01-01 09:01:00', -5, 100, 70), -- Exact Dupe
('U-4', '2023-01-01 09:02:00', -5, 100, 250), -- Artifact
('U-4', '2023-01-01 09:03:00', -5, 100, -10), -- Negative HR
('U-5', '2023-01-01 10:00:00', 0, 0, 60),
('U-5', '2023-01-01 11:00:00', 0, 0, 60),
('U-5', '2023-01-01 12:00:00', 0, 0, 60),
('U-5', '2023-01-01 13:00:00', 0, 0, 60),
('U-5', '2023-01-01 14:00:00', 0, 0, 60),
('U-5', '2023-01-01 15:00:00', 0, 0, 60), -- Sedentary?
('U-6', '2023-01-01 08:00:00', -5, 10000, 150), -- Marathon?
('U-6', '2023-01-01 09:00:00', -5, 10000, 160),
('U-6', '2023-01-01 10:00:00', -5, 10000, 170),
('U-7', '2023-01-01 12:00:00', -5, NULL, 80), -- Null Steps
('U-7', '2023-01-01 13:00:00', -5, NULL, 80),
('U-8', '2023-01-01 12:00:00', -5, -100, 80), -- Negative Steps
('U-8', '2023-01-01 13:00:00', -5, -50, 80),
('U-9', '2023-01-01 00:00:00', 0, 0, 50), -- Asleep
('U-9', '2023-01-01 01:00:00', 0, 0, 50),
('U-9', '2023-01-01 02:00:00', 0, 0, 50),
('U-9', '2023-01-01 03:00:00', 0, 0, 50),
('U-9', '2023-01-01 04:00:00', 0, 0, 50),
('U-10', '2023-01-01 12:00:00', 0, 500, 80),
('U-10', '2023-01-01 11:00:00', 0, 500, 80); -- Backwards timestamp

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Process the fitness logs in Health_Cloud.User_Sync.ACTIVITY_LOG.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Convert TS_LOCAL to TS_UTC using the TZ_OFFSET (TS - Offset Hours).
 *     - Clean HR: Set to NULL if < 30 or > 220.
 *     - Clean Steps: Set to 0 if < 0.
 *  3. Gold: 
 *     - Daily Summary: Total Steps, Avg HR, Min HR, Max HR per User per Day (UTC).
 *  
 *  Show the SQL."
 */
