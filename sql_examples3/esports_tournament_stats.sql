/*
 * Dremio "Messy Data" Challenge: Esports Tournament Stats
 * 
 * Scenario: 
 * Match stats from different servers.
 * 'Gamertags' change (Player1 -> Player1_Clan).
 * 'Match_Duration' overlaps (Restarted matches).
 * KDA lines are raw strings '10/2/5'.
 * 
 * Objective for AI Agent:
 * 1. Parse KDA: Split 'K/D/A' into Kills, Deaths, Assists integers.
 * 2. Entity Resolution: Map Tags to Canonical Player ID (Simple string contains).
 * 3. Flag Restarts: Matches with same Players starting < 5 mins apart.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Pro_Gaming;
CREATE FOLDER IF NOT EXISTS Pro_Gaming.Match_Stats;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Pro_Gaming.Match_Stats.LOGS (
    MATCH_ID VARCHAR,
    START_TS TIMESTAMP,
    PLAYER_TAG VARCHAR,
    KDA_STR VARCHAR, -- '10/0/5'
    RESULT VARCHAR -- 'Win', 'Loss'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Parsing, Tag Drift)
-------------------------------------------------------------------------------

-- Tag Change
INSERT INTO Pro_Gaming.Match_Stats.LOGS VALUES
('M-1', '2023-01-01 10:00:00', 'Faker', '5/1/10', 'Win'),
('M-2', '2023-01-02 10:00:00', 'T1_Faker', '3/0/5', 'Win');

-- KDA Parsing
INSERT INTO Pro_Gaming.Match_Stats.LOGS VALUES
('M-1', '2023-01-01 10:00:00', 'Zeus', '2/5/1', 'Win'),
('M-3', '2023-01-01 11:00:00', 'Zeus', '10/2/15', 'Loss');

-- Restarted Match
INSERT INTO Pro_Gaming.Match_Stats.LOGS VALUES
('M-4', '2023-01-01 12:00:00', 'PlayerA', '0/0/0', 'Loss'), -- Crash?
('M-5', '2023-01-01 12:02:00', 'PlayerA', '5/5/5', 'Win'); -- Restart

-- Bulk Fill
INSERT INTO Pro_Gaming.Match_Stats.LOGS VALUES
('M-10', '2023-01-01 13:00:00', 'NoobMaster', '0/10/0', 'Loss'),
('M-10', '2023-01-01 13:00:00', 'ProGamer', '10/0/0', 'Win'),
('M-11', '2023-01-01 13:00:00', 'NoobMaster69', '1/9/1', 'Loss'), -- Num suffix
('M-12', '2023-01-01 14:00:00', 'Healer', '0/0/20', 'Win'),
('M-13', '2023-01-01 14:00:00', 'Tank', '0/5/15', 'Win'),
('M-14', '2023-01-01 14:00:00', 'DPS', '15/2/3', 'Win'),
('M-15', '2023-01-01 15:00:00', 'AFK_Player', '0/0/0', 'Loss'),
('M-16', '2023-01-01 16:00:00', 'NullKDA', NULL, 'Loss'),
('M-17', '2023-01-01 16:00:00', 'BadKDA', '10-2-5', 'Loss'), -- Wrong delim
('M-18', '2023-01-01 16:00:00', 'BadKDA2', '10/2', 'Loss'), -- Missing Assist
('M-19', '2023-01-01 17:00:00', 'PlayerX', '5/5/5', 'Draw'),
('M-20', '2023-01-01 17:00:00', 'PlayerY', '5/5/5', 'Draw'),
('M-21', '2023-01-01 18:00:00', 'Smurf', '30/0/0', 'Win'),
('M-22', '2023-01-01 19:00:00', 'Smurf', '25/1/5', 'Win'),
('M-23', '2023-01-01 20:00:00', 'Smurf', '28/0/2', 'Win'),
('M-24', '2023-01-01 21:00:00', 'Bot1', '0/20/0', 'Loss'),
('M-25', '2023-01-01 21:00:00', 'Bot2', '0/20/0', 'Loss'),
('M-26', '2023-01-01 21:00:00', 'Bot3', '0/20/0', 'Loss'),
('M-27', '2023-01-01 21:00:00', 'Bot4', '0/20/0', 'Loss'),
('M-28', '2023-01-01 21:00:00', 'Bot5', '0/20/0', 'Loss'),
('M-29', '2023-01-01 22:00:00', 'Winner', '1/0/0', 'Win'),
('M-30', '2023-01-01 22:00:00', 'Loser', '0/1/0', 'Loss'),
('M-31', '2023-01-01 23:00:00', 'Name Changed', '1/1/1', 'Win'),
('M-32', '2023-01-02 23:00:00', 'Name_Changed', '2/2/2', 'Win'),
('M-33', '2023-01-03 23:00:00', '[Clan]Name', '3/3/3', 'Win'),
('M-34', '2023-01-01 00:00:00', 'Mid', '5/5/5', 'Win'),
('M-35', '2023-01-01 00:00:00', 'Top', '5/5/5', 'Win'),
('M-36', '2023-01-01 00:00:00', 'Jungle', '5/5/5', 'Win'),
('M-37', '2023-01-01 00:00:00', 'ADC', '5/5/5', 'Win'),
('M-38', '2023-01-01 00:00:00', 'Support', '5/5/5', 'Win');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze player performance in Pro_Gaming.Match_Stats.LOGS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Parse KDA: Extract K, D, A columns. Treat '0' deaths as 1 to avoid Div/0 in ratios.
 *     - Calc KDA Ratio: (K + A) / Max(D, 1).
 *  3. Gold: 
 *     - Ranking: Max KDA per Player Tag (normalized to ignore '_Tag' or '[Clan]').
 *  
 *  Show the SQL."
 */
