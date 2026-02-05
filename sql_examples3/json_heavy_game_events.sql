/*
 * Dremio "Messy Data" Challenge: JSON Heavy Game Events
 * 
 * Scenario: 
 * A game server logs all events into a single JSON blob.
 * The schema inside the JSON varies by 'Event_Type'.
 * 'Login' has {ip, device}, 'Purchase' has {item, cost}, 'Death' has {killer, map}.
 * 
 * Objective for AI Agent:
 * 1. Extract JSON fields using JSON extraction functions.
 * 2. Pivot the data: Create distinct views for Logins, Purchases, and Deaths.
 * 3. Handle malformed JSON strings.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS MMO_Logs;
CREATE FOLDER IF NOT EXISTS MMO_Logs.Event_Stream;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MMO_Logs.Event_Stream.RAW_JSON (
    ID VARCHAR,
    TS TIMESTAMP,
    EVENT_TYPE VARCHAR, -- 'Login', 'Purchase', 'Death'
    PAYLOAD_JSON VARCHAR -- The messy part
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Nested JSON, Varying Schema)
-------------------------------------------------------------------------------

-- Logins
INSERT INTO MMO_Logs.Event_Stream.RAW_JSON VALUES
('E-001', '2023-01-01 10:00:00', 'Login', '{"user_id": 101, "ip": "192.168.1.1", "device": "PC"}'),
('E-002', '2023-01-01 10:05:00', 'Login', '{"user_id": 102, "ip": "10.0.0.1", "device": "Console"}');

-- Purchases
INSERT INTO MMO_Logs.Event_Stream.RAW_JSON VALUES
('E-003', '2023-01-01 10:10:00', 'Purchase', '{"user_id": 101, "item": "Sword", "cost": 500, "currency": "Gold"}');

-- Deaths
INSERT INTO MMO_Logs.Event_Stream.RAW_JSON VALUES
('E-004', '2023-01-01 10:15:00', 'Death', '{"victim": 102, "killer": "Dragon", "map": "Dungeon_01"}');

-- Malformed / Empty
INSERT INTO MMO_Logs.Event_Stream.RAW_JSON VALUES
('E-005', '2023-01-01 10:20:00', 'Login', '{"user_id": 103, "ip": "Malformed...'), -- Truncated
('E-006', '2023-01-01 10:25:00', 'Unknown', '{}');

-- Bulk Fill
INSERT INTO MMO_Logs.Event_Stream.RAW_JSON VALUES
('E-010', '2023-01-01 11:00:00', 'Login', '{"user_id": 104, "ip": "192.168.1.2", "device": "Mobile"}'),
('E-011', '2023-01-01 11:01:00', 'Login', '{"user_id": 105, "ip": "192.168.1.3", "device": "PC"}'),
('E-012', '2023-01-01 11:02:00', 'Purchase', '{"user_id": 104, "item": "Potion", "cost": 10, "currency": "Gold"}'),
('E-013', '2023-01-01 11:03:00', 'Purchase', '{"user_id": 105, "item": "Shield", "cost": 200, "currency": "Gold"}'),
('E-014', '2023-01-01 11:04:00', 'Death', '{"victim": 104, "killer": "Goblin", "map": "Forest"}'),
('E-015', '2023-01-01 11:05:00', 'Death', '{"victim": 105, "killer": "Trap", "map": "Dungeon_02"}'),
('E-016', '2023-01-01 11:06:00', 'Login', '{"user_id": 106, "ip": "192.168.1.4"}'), -- Missing device key
('E-017', '2023-01-01 11:07:00', 'Login', '{"user_id": 107, "device": "Console"}'), -- Missing IP
('E-018', '2023-01-01 11:08:00', 'Purchase', '{"item": "Gem", "cost": 5}'), -- Missing user_id!
('E-019', '2023-01-01 11:09:00', 'Purchase', '{"user_id": 106, "item": "Skin", "curr": "USD"}'), -- Schema Drift: curr instead of currency
('E-020', '2023-01-01 11:10:00', 'Death', '{"vic": 106, "kil": "Boss"}'), -- Schema Drift: keys shortened
('E-021', '2023-01-01 12:00:00', 'Purchase', '{"user_id": 101, "item": "Axe", "cost": 1000, "currency": "Gold"}'),
('E-022', '2023-01-01 12:00:00', 'Purchase', '{"user_id": 101, "item": "Axe", "cost": 1000, "currency": "Gold"}'), -- JSON Exact Dupe
('E-023', '2023-01-01 12:05:00', 'Login', '{"user_id": 101, "ip": "192.168.1.1", "device": "PC", "version": "1.0"}'), -- Extra field
('E-024', '2023-01-01 12:10:00', 'Login', '{"user_id": 101, "ip": "192.168.1.1", "device": "PC", "version": "1.1"}'),
('E-025', '2023-01-01 12:15:00', 'Death', '{"victim": 101, "killer": "Player_99", "map": "Arena", "x": 100, "y": 200}'), -- Extra coordinates
('E-026', '2023-01-01 12:20:00', 'Purchase', '{"user_id": 101, "bundle_id": "B-001", "items": ["Sword", "Shield"]}'), -- Array inside JSON
('E-027', '2023-01-01 12:25:00', 'Login', '{"user_id": 108, "meta": {"ip": "1.1.1.1", "geo": "US"}}'), -- Nested Object
('E-028', '2023-01-01 12:30:00', 'Login', '{"user_id": 109, "meta": {"ip": "2.2.2.2", "geo": "EU"}}'),
('E-029', '2023-01-01 12:35:00', 'Purchase', '{"user_id": 108, "cart": [{"item": "X", "cost": 1}, {"item": "Y", "cost": 2}]}'), -- Nested Array of Objects
('E-030', '2023-01-01 12:40:00', 'Purchase', '{"user_id": 109, "cart": []}'), -- Empty Array
('E-031', '2023-01-01 13:00:00', 'Login', '{"user_id": 110, "ip": "192.168.1.5"}'),
('E-032', '2023-01-01 13:00:00', 'Login', '{"user_id": 111, "ip": "192.168.1.6"}'),
('E-033', '2023-01-01 13:00:00', 'Login', '{"user_id": 112, "ip": "192.168.1.7"}'),
('E-034', '2023-01-01 13:00:00', 'Login', '{"user_id": 113, "ip": "192.168.1.8"}'),
('E-035', '2023-01-01 13:00:00', 'Login', '{"user_id": 114, "ip": "192.168.1.9"}'),
('E-036', '2023-01-01 13:00:00', 'Death', '{"victim": 110, "killer": "Fall"}'),
('E-037', '2023-01-01 13:00:00', 'Death', '{"victim": 111, "killer": "Lava"}'),
('E-038', '2023-01-01 13:00:00', 'Death', '{"victim": 112, "killer": "Zombie"}'),
('E-039', '2023-01-01 13:00:00', 'Death', '{"victim": 113, "killer": "Skeleton"}'),
('E-040', '2023-01-01 13:00:00', 'Death', '{"victim": 114, "killer": "Vampire"}'),
('E-041', '2023-01-01 14:00:00', 'Purchase', '{"user_id": 110, "item": "Life"}'),
('E-042', '2023-01-01 14:00:00', 'Purchase', '{"user_id": 110, "item": "Mana"}'),
('E-043', '2023-01-01 14:00:00', 'Purchase', '{"user_id": 110, "item": "Stamina"}'),
('E-044', '2023-01-01 14:00:00', 'Purchase', '{"user_id": 110, "item": "Str"}'),
('E-045', '2023-01-01 14:00:00', 'Purchase', '{"user_id": 110, "item": "Dex"}'),
('E-046', '2023-01-01 14:00:00', 'Login', '{"user_id": 115, "ip": null}'), -- Explicit null value
('E-047', '2023-01-01 14:00:00', 'Login', '{"user_id": 116, "ip": "  1.2.3.4  "}'), -- Whitespace
('E-048', '2023-01-01 14:00:00', 'Login', '{"user_id": 117, "ip": "1.2.3.4\\n"}'), -- Newline char
('E-049', '2023-01-01 14:00:00', 'Login', '{"user_id": 118, "ip": "1.2.3.4\\t"}'), -- Tab
('E-050', '2023-01-01 14:00:00', 'Login', '{"user_id": 119, "ip": "1.2.3.4", "debug": true}'); -- Boolean format

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Extract insights from the Game Events key-value store in MMO_Logs.Event_Stream.RAW_JSON.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Parse valid JSON from the string column.
 *     - Flatten: Create separate views for 'Logins', 'Purchases', and 'Deaths' by filtering Event_Type.
 *     - Extract relevant keys (e.g. 'cost', 'killer') into first-class columns.
 *  3. Gold: 
 *     - Create a 'Leaderboard' showing Total Purchase Value and K/D Ratio per User ID.
 *  
 *  Generate the SQL logic."
 */
