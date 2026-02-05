/*
 * Dremio "Messy Data" Challenge: Obfuscated Ad Clicks
 * 
 * Scenario: 
 * Ad streams from two different providers have different schema.
 * 'Provider A' uses `u_id` (INT) while 'Provider B' uses `user_hash` (STRING).
 * Timestamps are in different formats and timezones.
 * 
 * Objective for AI Agent:
 * 1. Normalize timestamps to UTC.
 * 2. Join the streams on a common User Map table (which bridges INT and HASH).
 * 3. Handle records where the join fails (Unmatched Users).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AdTech_Dump;
CREATE FOLDER IF NOT EXISTS AdTech_Dump.Raw_Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AdTech_Dump.Raw_Logs.STREAM_A (
    u_id INT,
    clk_ts VARCHAR, -- '2023-01-01T10:00:00Z'
    camp_id INT
);

CREATE TABLE IF NOT EXISTS AdTech_Dump.Raw_Logs.STREAM_B (
    user_hash VARCHAR,
    epoch_ts BIGINT, -- 1672569600
    c_ref VARCHAR
);

CREATE TABLE IF NOT EXISTS AdTech_Dump.Raw_Logs.USER_MAP (
    u_id INT,
    u_hash VARCHAR,
    region VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Mismatched Keys, Formats)
-------------------------------------------------------------------------------

-- Stream A (Clean-ish)
INSERT INTO AdTech_Dump.Raw_Logs.STREAM_A VALUES
(101, '2023-01-01T10:00:00Z', 5001),
(102, '2023-01-01T10:05:00Z', 5001),
(103, '2023-01-01 10:10:00', 5002), -- Missing T/Z
(104, NULL, 5003), -- Null TS
(999, '2023-01-01T12:00:00Z', 5001); -- Unknown User

-- Stream B (Epochs, Hashes)
INSERT INTO AdTech_Dump.Raw_Logs.STREAM_B VALUES
('a1b2', 1672569600, 'CAMP-X'),
('c3d4', 1672570000, 'CAMP-Y'),
('e5f6', 1672570000, 'CAMP-X'),
('x9z8', 1672580000, 'CAMP-Z'); -- Unknown Hash

-- User Map
INSERT INTO AdTech_Dump.Raw_Logs.USER_MAP VALUES
(101, 'a1b2', 'US'),
(102, 'c3d4', 'UK'),
(103, 'e5f6', 'CA'),
(104, 'g7h8', 'US');

-- Fill Bulk
INSERT INTO AdTech_Dump.Raw_Logs.STREAM_A VALUES
(101, '2023-01-01T11:00:00Z', 5001),
(101, '2023-01-01T11:05:00Z', 5001),
(101, '2023-01-01T11:10:00Z', 5001),
(102, '2023-01-01T11:00:00Z', 5002),
(102, '2023-01-01T11:05:00Z', 5002),
(102, '2023-01-01T11:10:00Z', 5002),
(103, '2023-01-01T11:00:00Z', 5003),
(103, '2023-01-01T11:05:00Z', 5003),
(103, '2023-01-01T11:10:00Z', 5003),
(101, '2023-01-02T10:00:00Z', 5001),
(101, '2023-01-02T10:00:00Z', 5001), -- Dupe
(101, '2023-01-02T10:00:00Z', 5001), -- Dupe
(102, '2023-01-02T10:00:00Z', 5002),
(102, '2023-01-02T10:10:00Z', 5002),
(102, '2023-01-02T10:20:00Z', 5002),
(103, '2023-01-02T10:00:00Z', 5003),
(103, '2023-01-02T10:10:00Z', 5003),
(103, '2023-01-02T10:20:00Z', 5003),
(104, '2023-01-02T10:00:00Z', 5001),
(104, '2023-01-02T10:10:00Z', 5001),
(104, '2023-01-02T10:20:00Z', 5001),
(NULL, '2023-01-02T12:00:00Z', 5001), -- Null User
(101, 'INVALID_DATE', 5001); -- Bad Date

INSERT INTO AdTech_Dump.Raw_Logs.STREAM_B VALUES
('a1b2', 1672600000, 'CAMP-X'),
('a1b2', 1672600100, 'CAMP-X'),
('a1b2', 1672600200, 'CAMP-X'),
('c3d4', 1672600000, 'CAMP-Y'),
('c3d4', 1672600100, 'CAMP-Y'),
('c3d4', 1672600200, 'CAMP-Y'),
('e5f6', 1672600000, 'CAMP-Z'),
('e5f6', 1672600100, 'CAMP-Z'),
('e5f6', 1672600200, 'CAMP-Z'),
('a1b2', 1672700000, 'CAMP-X'),
('a1b2', 1672700100, 'CAMP-X'),
('a1b2', 1672700200, 'CAMP-X'),
('c3d4', 1672700000, 'CAMP-Y'),
('c3d4', 1672700100, 'CAMP-Y'),
('c3d4', 1672700200, 'CAMP-Y'),
('e5f6', 1672700000, 'CAMP-Z'),
('e5f6', 1672700100, 'CAMP-Z'),
('e5f6', 1672700200, 'CAMP-Z'),
('unknown', 1672800000, 'CAMP-X'),
('unknown', 1672800100, 'CAMP-X');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Data is split between AdTech_Dump.Raw_Logs.STREAM_A and STREAM_B.
 *  One uses integer User IDs, the other uses Hash strings. 
 *  Please generate a unified Silver layer.
 *  
 *  1. Bronze: Views for both streams and the MAP table.
 *  2. Silver: 
 *     - Join Stream A and B using the USER_MAP table to resolve User IDs.
 *     - Normalize timestamps (Stream B uses Epoch) to a common TIMESTAMP format.
 *     - Union the data into a single 'Unified_Clicks' view.
 *  3. Gold: 
 *     - Count total clicks by Region (from the User Map).
 *  
 *  Provide the Dremio SQL."
 */
