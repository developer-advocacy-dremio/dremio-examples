/*
 * Dremio "Messy Data" Challenge: Streaming Service Buffering
 * 
 * Scenario: 
 * Video stream logs with concurrent sessions.
 * 'Device_ID' is consistent, but 'Session_ID' resets on buffering.
 * Events are 'Play', 'Buffer', 'Stop'.
 * A user can watch two streams at once (Concurrency).
 * 
 * Objective for AI Agent:
 * 1. Identify Concurrent Sessions: Same User, overlapping Play-Stop intervals.
 * 2. Calculate Total Buffering Time per User.
 * 3. Flag 'Churn Risk' users (> 10% of time spent buffering).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Stream_Flix;
CREATE FOLDER IF NOT EXISTS Stream_Flix.Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Stream_Flix.Logs.PLAYBACK_EVENTS (
    USER_ID VARCHAR,
    DEVICE_ID VARCHAR,
    SESSION_ID VARCHAR, -- Resets often
    EVENT_TYPE VARCHAR, -- 'PLAY', 'BUFFER', 'STOP'
    TS TIMESTAMP
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Concurrency, Buffering)
-------------------------------------------------------------------------------

-- Normal Session
INSERT INTO Stream_Flix.Logs.PLAYBACK_EVENTS VALUES
('U-100', 'D-1', 'S-001', 'PLAY', '2023-01-01 10:00:00'),
('U-100', 'D-1', 'S-001', 'STOP', '2023-01-01 10:30:00');

-- Buffering Nightmare
INSERT INTO Stream_Flix.Logs.PLAYBACK_EVENTS VALUES
('U-101', 'D-2', 'S-002', 'PLAY', '2023-01-01 11:00:00'),
('U-101', 'D-2', 'S-002', 'BUFFER', '2023-01-01 11:05:00'),
('U-101', 'D-2', 'S-003', 'PLAY', '2023-01-01 11:10:00'), -- New Session ID after buffer
('U-101', 'D-2', 'S-003', 'BUFFER', '2023-01-01 11:15:00'),
('U-101', 'D-2', 'S-004', 'PLAY', '2023-01-01 11:20:00'),
('U-101', 'D-2', 'S-004', 'STOP', '2023-01-01 12:00:00');

-- Concurrency (Account Sharing?)
INSERT INTO Stream_Flix.Logs.PLAYBACK_EVENTS VALUES
('U-102', 'D-3', 'S-005', 'PLAY', '2023-01-01 13:00:00'), -- TV
('U-102', 'D-4', 'S-006', 'PLAY', '2023-01-01 13:10:00'), -- Phone overlapping
('U-102', 'D-3', 'S-005', 'STOP', '2023-01-01 14:00:00'),
('U-102', 'D-4', 'S-006', 'STOP', '2023-01-01 14:00:00');

-- Errors
INSERT INTO Stream_Flix.Logs.PLAYBACK_EVENTS VALUES
('U-103', 'D-5', 'S-007', 'BUFFER', '2023-01-01 15:00:00'), -- Starts with Buffer?
('U-103', 'D-5', 'S-007', 'STOP', '2023-01-01 15:01:00');

-- Bulk Fill
INSERT INTO Stream_Flix.Logs.PLAYBACK_EVENTS VALUES
('U-200', 'D-10', 'S-100', 'PLAY', '2023-01-02 10:00:00'),
('U-200', 'D-10', 'S-100', 'STOP', '2023-01-02 11:00:00'),
('U-201', 'D-11', 'S-101', 'PLAY', '2023-01-02 10:00:00'),
('U-201', 'D-11', 'S-101', 'BUFFER', '2023-01-02 10:10:00'),
('U-201', 'D-11', 'S-102', 'PLAY', '2023-01-02 10:20:00'),
('U-201', 'D-11', 'S-102', 'STOP', '2023-01-02 11:00:00'),
('U-202', 'D-12', 'S-103', 'PLAY', '2023-01-02 12:00:00'),
('U-202', 'D-12', 'S-103', 'BUFFER', '2023-01-02 12:01:00'),
('U-202', 'D-12', 'S-104', 'BUFFER', '2023-01-02 12:02:00'),
('U-202', 'D-12', 'S-105', 'BUFFER', '2023-01-02 12:03:00'), -- Infinite buffer loop
('U-202', 'D-12', 'S-106', 'STOP', '2023-01-02 12:05:00'),
('U-203', 'D-13', 'S-107', 'PLAY', '2023-01-02 13:00:00'),
('U-203', 'D-14', 'S-108', 'PLAY', '2023-01-02 13:00:00'),
('U-203', 'D-15', 'S-109', 'PLAY', '2023-01-02 13:00:00'), -- 3 devices!
('U-203', 'D-13', 'S-107', 'STOP', '2023-01-02 13:30:00'),
('U-203', 'D-14', 'S-108', 'STOP', '2023-01-02 13:30:00'),
('U-203', 'D-15', 'S-109', 'STOP', '2023-01-02 13:30:00'),
('U-204', 'D-16', 'S-110', 'STOP', '2023-01-02 14:00:00'), -- Orphan Stop
('U-205', 'D-17', 'S-111', 'PLAY', '2023-01-02 15:00:00'), -- Orphan Play (never stops)
('U-206', 'D-18', 'S-112', 'PLAY', '2023-01-02 16:00:00'),
('U-206', 'D-18', 'S-112', 'PLAY', '2023-01-02 16:01:00'), -- Double Play event
('U-206', 'D-18', 'S-112', 'STOP', '2023-01-02 17:00:00'),
('U-207', 'D-19', 'S-113', 'BUFFER', '2023-01-02 18:00:00'),
('U-207', 'D-19', 'S-113', 'BUFFER', '2023-01-02 18:05:00'),
('U-207', 'D-19', 'S-113', 'BUFFER', '2023-01-02 18:10:00'),
('U-207', 'D-19', 'S-113', 'STOP', '2023-01-02 18:15:00'), -- 15 mins of buffer, 0 play
('U-208', 'D-20', 'S-114', 'PLAY', '2023-01-02 19:00:00'),
('U-208', 'D-20', 'S-114', 'buffer', '2023-01-02 19:30:00'), -- lowercase
('U-208', 'D-20', 'S-114', 'Play', '2023-01-02 19:35:00'), -- Mixed case
('U-208', 'D-20', 'S-114', 'Stop', '2023-01-02 20:00:00'),
('U-209', 'D-21', 'S-115', 'PLAY', '2023-01-02 21:00:00'),
('U-209', 'D-21', 'S-115', 'PAUSE', '2023-01-02 21:30:00'), -- Unknown event type?
('U-209', 'D-21', 'S-115', 'RESUME', '2023-01-02 21:40:00'), -- Unknown
('U-209', 'D-21', 'S-115', 'STOP', '2023-01-02 22:00:00'),
('U-210', 'D-22', 'S-116', 'PLAY', '2023-01-02 23:00:00'),
('U-210', 'D-22', 'S-116', 'STOP', '2023-01-02 23:00:00'); -- 0 duration

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze streaming quality in Stream_Flix.Logs.PLAYBACK_EVENTS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Event Types (Uppercase).
 *     - Chain events: Determine 'Start Time' and 'End Time' for each SESSION_ID.
 *     - Calculate 'Buffering_Duration' (Time between BUFFER and next PLAY/STOP).
 *  3. Gold: 
 *     - Flag 'High Buffering' users (> 5 mins total).
 *     - Detect 'Concurrent Users' (Overlapping timestamps for same USER_ID).
 *  
 *  Show the SQL."
 */
