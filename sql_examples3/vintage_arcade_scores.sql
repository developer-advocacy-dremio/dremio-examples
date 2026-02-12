/*
 * Dremio "Messy Data" Challenge: Vintage Arcade Scores
 * 
 * Scenario: 
 * Retro arcade chain consolidating high-score boards from 50 locations.
 * 3 Tables: CABINETS, PLAYERS, HIGH_SCORES.
 * 
 * Objective for AI Agent:
 * 1. Score Parsing: Normalize '001,500' / '1500' / '1.5K' to INT.
 * 2. Initial Validation: Clean player initials ('A.B.' -> 'AB', '---' -> NULL).
 * 3. Timezone Alignment: Machines record in local time but timezone tag varies.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Retro_Arcade;
CREATE FOLDER IF NOT EXISTS Retro_Arcade.Leaderboards;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Retro_Arcade.Leaderboards.CABINETS (
    CABINET_ID VARCHAR,
    GAME_TITLE VARCHAR,
    ARCADE_LOCATION VARCHAR,
    TZ_TAG VARCHAR -- 'EST', 'PST', 'CST', 'UTC', NULL
);

CREATE TABLE IF NOT EXISTS Retro_Arcade.Leaderboards.PLAYERS (
    PLAYER_ID VARCHAR,
    INITIALS_RAW VARCHAR, -- 'AAA', 'A.B.', '---', 'ASS', '   '
    MEMBER_SINCE DATE
);

CREATE TABLE IF NOT EXISTS Retro_Arcade.Leaderboards.HIGH_SCORES (
    SCORE_ID VARCHAR,
    CABINET_ID VARCHAR,
    PLAYER_ID VARCHAR,
    SCORE_RAW VARCHAR, -- '001,500', '1500', '1.5K', '2M'
    PLAYED_TS TIMESTAMP, -- In local time of cabinet's TZ
    LIVES_USED INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- CABINETS (10 rows)
INSERT INTO Retro_Arcade.Leaderboards.CABINETS VALUES
('CAB-001', 'Pac-Man', 'NYC Main', 'EST'),
('CAB-002', 'Donkey Kong', 'NYC Main', 'EST'),
('CAB-003', 'Galaga', 'LA Sunset', 'PST'),
('CAB-004', 'Space Invaders', 'LA Sunset', 'PST'),
('CAB-005', 'Street Fighter II', 'Chicago Loop', 'CST'),
('CAB-006', 'Asteroids', 'Chicago Loop', 'CST'),
('CAB-007', 'Centipede', 'Online Emulator', 'UTC'),
('CAB-008', 'Frogger', 'Austin HQ', 'CST'),
('CAB-009', 'Tetris', 'Seattle Pop-Up', NULL),      -- Unknown timezone
('CAB-010', 'Ms. Pac-Man', 'NYC Main', 'EST');

-- PLAYERS (12 rows)
INSERT INTO Retro_Arcade.Leaderboards.PLAYERS VALUES
('PLR-001', 'AAA', '2020-01-01'),
('PLR-002', 'JKL', '2020-03-15'),
('PLR-003', 'A.B.', '2021-06-01'),            -- Dots in initials
('PLR-004', '---', '2022-01-01'),              -- Placeholder
('PLR-005', 'Z Z', '2022-06-01'),              -- Space
('PLR-006', '   ', '2023-01-01'),              -- All spaces
('PLR-007', 'MAX', '2020-01-15'),
('PLR-008', 'ACE', '2020-02-20'),
('PLR-009', 'PRO', '2021-01-10'),
('PLR-010', 'ab', '2023-03-01'),               -- Lowercase
('PLR-011', 'X', '2023-04-01'),                -- Single char
('PLR-012', 'ABCD', '2023-05-01');             -- Too many chars

-- HIGH_SCORES (50+ rows with messy scores)
INSERT INTO Retro_Arcade.Leaderboards.HIGH_SCORES VALUES
-- Normal scores
('HS-001', 'CAB-001', 'PLR-001', '3333360', '2023-01-15 14:30:00', 3),
('HS-002', 'CAB-001', 'PLR-002', '256000', '2023-01-15 15:00:00', 3),
('HS-003', 'CAB-002', 'PLR-007', '1050000', '2023-01-20 10:00:00', 3),
('HS-004', 'CAB-002', 'PLR-008', '874300', '2023-01-20 10:30:00', 3),
-- Scores with commas
('HS-005', 'CAB-003', 'PLR-001', '001,500', '2023-02-01 18:00:00', 3),    -- Leading zeros + comma
('HS-006', 'CAB-003', 'PLR-002', '2,500', '2023-02-01 18:30:00', 3),
('HS-007', 'CAB-004', 'PLR-003', '12,345', '2023-02-05 19:00:00', 3),
-- K/M notation
('HS-008', 'CAB-005', 'PLR-007', '1.5K', '2023-02-10 12:00:00', 2),      -- 1500
('HS-009', 'CAB-005', 'PLR-008', '2.1K', '2023-02-10 12:30:00', 1),      -- 2100
('HS-010', 'CAB-006', 'PLR-009', '1M', '2023-02-15 16:00:00', 3),         -- 1000000
('HS-011', 'CAB-006', 'PLR-001', '2.5M', '2023-02-15 16:30:00', 3),       -- 2500000
-- Edge cases
('HS-012', 'CAB-007', 'PLR-004', '0', '2023-03-01 00:00:00', 0),          -- Zero score
('HS-013', 'CAB-007', 'PLR-005', '-100', '2023-03-01 00:30:00', 3),       -- Negative (glitch?)
('HS-014', 'CAB-008', 'PLR-006', '', '2023-03-05 10:00:00', 3),           -- Empty
('HS-015', 'CAB-008', 'PLR-010', NULL, '2023-03-05 10:30:00', 3),         -- NULL
('HS-016', 'CAB-009', 'PLR-011', '999999', '2023-03-10 20:00:00', 1),
('HS-017', 'CAB-009', 'PLR-012', '999,999,999', '2023-03-10 20:30:00', 1), -- Suspicious max
-- Duplicates
('HS-018', 'CAB-001', 'PLR-001', '3333360', '2023-01-15 14:30:00', 3),    -- Exact dupe of HS-001
-- Bulk fill
('HS-019', 'CAB-001', 'PLR-007', '150000', '2023-04-01 14:00:00', 3),
('HS-020', 'CAB-001', 'PLR-008', '120000', '2023-04-01 14:30:00', 3),
('HS-021', 'CAB-001', 'PLR-009', '95000', '2023-04-01 15:00:00', 2),
('HS-022', 'CAB-002', 'PLR-001', '900000', '2023-04-05 10:00:00', 3),
('HS-023', 'CAB-002', 'PLR-002', '750000', '2023-04-05 10:30:00', 3),
('HS-024', 'CAB-003', 'PLR-007', '3500', '2023-04-10 18:00:00', 3),
('HS-025', 'CAB-003', 'PLR-008', '4200', '2023-04-10 18:30:00', 3),
('HS-026', 'CAB-004', 'PLR-009', '18000', '2023-04-15 19:00:00', 3),
('HS-027', 'CAB-004', 'PLR-001', '22000', '2023-04-15 19:30:00', 3),
('HS-028', 'CAB-005', 'PLR-002', '1800', '2023-04-20 12:00:00', 2),
('HS-029', 'CAB-005', 'PLR-003', '2200', '2023-04-20 12:30:00', 1),
('HS-030', 'CAB-006', 'PLR-007', '850000', '2023-04-25 16:00:00', 3),
('HS-031', 'CAB-006', 'PLR-008', '920000', '2023-04-25 16:30:00', 3),
('HS-032', 'CAB-007', 'PLR-009', '500', '2023-05-01 00:00:00', 3),
('HS-033', 'CAB-007', 'PLR-001', '750', '2023-05-01 00:30:00', 3),
('HS-034', 'CAB-008', 'PLR-002', '10000', '2023-05-05 10:00:00', 3),
('HS-035', 'CAB-008', 'PLR-003', '8500', '2023-05-05 10:30:00', 3),
('HS-036', 'CAB-009', 'PLR-007', '450000', '2023-05-10 20:00:00', 1),
('HS-037', 'CAB-009', 'PLR-008', '380000', '2023-05-10 20:30:00', 2),
('HS-038', 'CAB-010', 'PLR-009', '200000', '2023-05-15 14:00:00', 3),
('HS-039', 'CAB-010', 'PLR-001', '250000', '2023-05-15 14:30:00', 3),
('HS-040', 'CAB-010', 'PLR-002', '180000', '2023-05-15 15:00:00', 3),
('HS-041', 'CAB-001', 'PLR-003', '50K', '2023-06-01 14:00:00', 3),       -- K notation
('HS-042', 'CAB-002', 'PLR-003', '1,200,000', '2023-06-05 10:00:00', 3),
('HS-043', 'CAB-003', 'PLR-009', '006,000', '2023-06-10 18:00:00', 3),   -- Leading zeros
('HS-044', 'CAB-004', 'PLR-007', '25K', '2023-06-15 19:00:00', 3),
('HS-045', 'CAB-005', 'PLR-008', '3.2K', '2023-06-20 12:00:00', 2),
('HS-046', 'CAB-999', 'PLR-001', '100000', '2023-07-01 10:00:00', 3),    -- Orphan cabinet
('HS-047', 'CAB-001', 'PLR-999', '85000', '2023-07-01 14:00:00', 3),     -- Orphan player
('HS-048', 'CAB-006', 'PLR-002', '1.1M', '2023-07-05 16:00:00', 3),
('HS-049', 'CAB-010', 'PLR-007', '300000', '2023-07-10 14:00:00', 3),
('HS-050', 'CAB-010', 'PLR-008', '275000', '2023-07-10 14:30:00', 3);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Build a unified leaderboard from Retro_Arcade.Leaderboards.
 *  
 *  1. Bronze: Raw Views of CABINETS, PLAYERS, HIGH_SCORES.
 *  2. Silver: 
 *     - Parse Scores: Remove commas, leading zeros. Convert 'K' to *1000, 'M' to *1000000. Cast to BIGINT.
 *     - Clean Initials: TRIM dots and spaces. Set '---' and blanks to NULL. UPPER all.
 *     - Filter: Remove negative, NULL, and empty scores.
 *  3. Gold: 
 *     - All-Time High Score per Game.
 *     - Player Ranking: Total score across all games.
 *     - Suspicious Scores: Flag scores > 99th percentile per game.
 *  
 *  Show the SQL."
 */
