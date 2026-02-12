/*
 * Dremio "Messy Data" Challenge: Youth Soccer League
 * 
 * Scenario: 
 * City rec league tracking teams, players, and match results across age groups.
 * 3 Tables: TEAMS, PLAYERS, MATCH_RESULTS.
 * 
 * Objective for AI Agent:
 * 1. Age Group Validation: Detect players registered in wrong age bracket.
 * 2. Score Parsing: '3-1' / '3 - 1' / '3:1' / 'W 3-1' / 'Forfeit' to home/away goals.
 * 3. Jersey Number Conflicts: Same number on same team in same season.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Rec;
CREATE FOLDER IF NOT EXISTS City_Rec.Soccer_League;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Rec.Soccer_League.TEAMS (
    TEAM_ID VARCHAR,
    TEAM_NAME VARCHAR,
    AGE_GROUP VARCHAR, -- 'U10', 'U-10', 'Under 10', 'u10'
    SEASON_YEAR INT,
    COACH_NAME VARCHAR
);

CREATE TABLE IF NOT EXISTS City_Rec.Soccer_League.PLAYERS (
    PLAYER_ID VARCHAR,
    TEAM_ID VARCHAR,
    PLAYER_NAME VARCHAR,
    DOB DATE,
    JERSEY_NUM INT,
    GUARDIAN_PHONE VARCHAR -- '(512) 555-1234', '512-555-1234', '5125551234'
);

CREATE TABLE IF NOT EXISTS City_Rec.Soccer_League.MATCH_RESULTS (
    MATCH_ID VARCHAR,
    HOME_TEAM_ID VARCHAR,
    AWAY_TEAM_ID VARCHAR,
    MATCH_DT DATE,
    SCORE_RAW VARCHAR, -- '3-1', '3 - 1', '3:1', 'W 3-1', 'Forfeit', NULL
    REFEREE_NAME VARCHAR,
    FIELD_NUM INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- TEAMS (10 rows with age group mess)
INSERT INTO City_Rec.Soccer_League.TEAMS VALUES
('TM-001', 'Red Dragons', 'U10', 2023, 'Coach Smith'),
('TM-002', 'Blue Sharks', 'U-10', 2023, 'Coach Lee'),
('TM-003', 'Green Gators', 'Under 10', 2023, 'Coach Patel'),
('TM-004', 'Yellow Jackets', 'u10', 2023, 'Coach Garcia'),
('TM-005', 'Red Dragons', 'U12', 2023, 'Coach Adams'),
('TM-006', 'Blue Sharks', 'U-12', 2023, 'Coach Kim'),
('TM-007', 'Purple Panthers', 'U10', 2023, 'Coach Brown'),
('TM-008', 'Orange Tigers', 'U12', 2023, 'Coach Wilson'),
('TM-009', 'Silver Stars', 'U14', 2023, 'Coach Davis'),
('TM-010', 'Gold Eagles', 'U-14', 2023, 'Coach Martinez');

-- PLAYERS (25 rows with DOB/jersey issues)
INSERT INTO City_Rec.Soccer_League.PLAYERS VALUES
('PL-001', 'TM-001', 'Jake Smith', '2014-03-15', 10, '(512) 555-1001'),
('PL-002', 'TM-001', 'Emma Johnson', '2014-06-01', 7, '512-555-1002'),
('PL-003', 'TM-001', 'Liam Chen', '2014-09-20', 10, '5125551003'),      -- Dupe jersey #10!
('PL-004', 'TM-001', 'Sophia Garcia', '2012-01-15', 4, '(512) 555-1004'), -- Too old for U10!
('PL-005', 'TM-002', 'Noah Kim', '2014-04-10', 9, '(512) 555-2001'),
('PL-006', 'TM-002', 'Olivia Lee', '2013-12-01', 11, '512.555.2002'),
('PL-007', 'TM-002', 'Aiden Patel', '2014-07-22', 3, '5125552003'),
('PL-008', 'TM-003', 'Isabella Brown', '2014-02-28', 8, '(512)555-3001'),
('PL-009', 'TM-003', 'Mason Davis', '2014-05-15', 6, '512 555 3002'),
('PL-010', 'TM-004', 'Ava Martinez', '2014-08-10', 5, '(512) 555-4001'),
('PL-011', 'TM-004', 'Ethan Wilson', '2015-01-05', 2, '+1-512-555-4002'),
('PL-012', 'TM-005', 'Charlotte Adams', '2012-03-20', 10, '(512) 555-5001'),
('PL-013', 'TM-005', 'Lucas Anderson', '2012-07-04', 7, '512-555-5002'),
('PL-014', 'TM-006', 'Mia Thompson', '2012-11-30', 9, '(512) 555-6001'),
('PL-015', 'TM-006', 'Jack White', '2012-04-18', 11, '512-555-6002'),
('PL-016', 'TM-007', 'Harper Moore', '2014-01-25', 1, '(512) 555-7001'),
('PL-017', 'TM-007', 'Leo Taylor', '2014-06-30', 14, '512-555-7002'),
('PL-018', 'TM-008', 'Ella Hernandez', '2012-02-14', 8, '(512) 555-8001'),
('PL-019', 'TM-008', 'Alexander Clark', '2012-08-22', 6, '512-555-8002'),
('PL-020', 'TM-009', 'Grace Robinson', '2010-05-10', 10, '(512) 555-9001'),
('PL-021', 'TM-009', 'Daniel Lewis', '2010-09-15', 7, '512-555-9002'),
('PL-022', 'TM-010', 'Chloe Walker', '2010-03-08', 9, '(512) 555-0001'),
('PL-023', 'TM-010', 'James Hall', '2010-12-25', 11, '512-555-0002'),
('PL-024', 'TM-001', 'Mystery Kid', NULL, NULL, NULL),                    -- NULL everything
('PL-025', 'TM-999', 'Ghost Player', '2014-01-01', 1, '000-000-0000');   -- Orphan team

-- MATCH_RESULTS (25+ rows with score mess)
INSERT INTO City_Rec.Soccer_League.MATCH_RESULTS VALUES
('M-001', 'TM-001', 'TM-002', '2023-09-09', '3-1', 'Ref. Johnson', 1),
('M-002', 'TM-003', 'TM-004', '2023-09-09', '2 - 2', 'Ref. Brown', 2),
('M-003', 'TM-001', 'TM-003', '2023-09-16', '4:0', 'Ref. Davis', 1),
('M-004', 'TM-002', 'TM-004', '2023-09-16', 'W 3-1', 'Ref. Wilson', 2),    -- W prefix
('M-005', 'TM-007', 'TM-001', '2023-09-23', '1-1', 'Ref. Johnson', 1),
('M-006', 'TM-004', 'TM-003', '2023-09-23', '0-3', 'Ref. Brown', 3),
('M-007', 'TM-002', 'TM-007', '2023-09-30', 'Forfeit', 'Ref. Davis', 1),   -- Forfeit
('M-008', 'TM-005', 'TM-006', '2023-09-09', '2-1', 'Ref. Martinez', 4),
('M-009', 'TM-008', 'TM-005', '2023-09-16', '1 to 3', 'Ref. Garcia', 4),   -- "to" format
('M-010', 'TM-006', 'TM-008', '2023-09-23', '0-0', 'Ref. Martinez', 5),
('M-011', 'TM-009', 'TM-010', '2023-09-09', '5-2', 'Ref. Lee', 6),
('M-012', 'TM-010', 'TM-009', '2023-09-16', '3-3', 'Ref. Lee', 6),
('M-013', 'TM-001', 'TM-004', '2023-10-07', '2-0', 'Ref. Johnson', 1),
('M-014', 'TM-002', 'TM-003', '2023-10-07', '1-2', 'Ref. Brown', 2),
('M-015', 'TM-007', 'TM-002', '2023-10-14', '3-2', 'Ref. Davis', 1),
('M-016', 'TM-003', 'TM-001', '2023-10-14', '1-4', 'Ref. Wilson', 3),
('M-017', 'TM-004', 'TM-007', '2023-10-21', NULL, 'Ref. Johnson', 2),      -- NULL score
('M-018', 'TM-005', 'TM-008', '2023-10-07', '4-1', 'Ref. Martinez', 4),
('M-019', 'TM-006', 'TM-005', '2023-10-14', '2-2', 'Ref. Garcia', 5),
('M-020', 'TM-008', 'TM-006', '2023-10-21', '1-0', 'Ref. Martinez', 4),
('M-021', 'TM-009', 'TM-010', '2023-10-07', '4-1', 'Ref. Lee', 6),
('M-022', 'TM-010', 'TM-009', '2023-10-14', '2-3', 'Ref. Lee', 6),
-- Duplicate
('M-023', 'TM-001', 'TM-002', '2023-09-09', '3-1', 'Ref. Johnson', 1),    -- Dupe of M-001
-- Self-play error
('M-024', 'TM-001', 'TM-001', '2023-10-28', '3-3', 'Ref. Brown', 1),      -- Team vs itself!
-- Orphan teams
('M-025', 'TM-999', 'TM-001', '2023-11-04', '0-5', 'Ref. Davis', 2);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Manage soccer league in City_Rec.Soccer_League.
 *  
 *  1. Bronze: Raw Views of TEAMS, PLAYERS, MATCH_RESULTS.
 *  2. Silver: 
 *     - Age Group: Normalize 'U-10'/'Under 10'/'u10' -> 'U10'. Validate DOB vs age group cutoff.
 *     - Score: Parse '3-1'/'3 - 1'/'3:1' into HOME_GOALS and AWAY_GOALS. Map 'Forfeit' -> 0-3.
 *     - Phone: Strip '(', ')', '-', '.', spaces, '+1'. Format as '5125551234'.
 *     - Jersey: Flag duplicate numbers within same team.
 *  3. Gold: 
 *     - League Standings: W/D/L/Points/GD per team per age group.
 *     - Ineligible Players: Players whose DOB falls outside age group range.
 *  
 *  Show the SQL."
 */
