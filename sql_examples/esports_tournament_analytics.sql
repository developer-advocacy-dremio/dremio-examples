/*
 * Esports Tournament & Match Analytics Demo
 * 
 * Scenario:
 * An esports team organization wants to analyze match performance to optimize player strategy.
 * They track player stats, match events, and opponent tendencies.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Players: Roster information (Handle, Role, Team).
 * - Matches: Match metadata (Map, Duration, Result).
 * - Match_Events: Granular kill/death/objective log.
 * 
 * Silver Layer:
 * - Player_Match_Stats: Aggregated kills, deaths, assists per match.
 * - Map_Win_Rates: Win/Loss performance by map.
 * 
 * Gold Layer:
 * - Team_Performance_Summary: Rolling averages and diverse metrics (K/D Ratio).
 * - Role_Impact_Analysis: Comparison of performance by in-game role (e.g., Carry vs Support).
 * 
 * Note: Assumes a catalog named 'EsportsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EsportsDB;
CREATE FOLDER IF NOT EXISTS EsportsDB.Bronze;
CREATE FOLDER IF NOT EXISTS EsportsDB.Silver;
CREATE FOLDER IF NOT EXISTS EsportsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS EsportsDB.Bronze.Players (
    PlayerID INT,
    Handle VARCHAR,
    TeamName VARCHAR,
    Role VARCHAR, -- e.g., 'Tank', 'Support', 'DPS'
    Region VARCHAR
);

CREATE TABLE IF NOT EXISTS EsportsDB.Bronze.Matches (
    MatchID INT,
    Tournament VARCHAR,
    MapName VARCHAR,
    DurationSeconds INT,
    WinnerTeam VARCHAR,
    MatchDate DATE
);

CREATE TABLE IF NOT EXISTS EsportsDB.Bronze.Match_Events (
    EventID INT,
    MatchID INT,
    PlayerID INT,
    EventType VARCHAR, -- 'Kill', 'Death', 'Assist', 'Objective'
    TimestampSeconds INT,
    LocationX DOUBLE,
    LocationY DOUBLE
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Players
INSERT INTO EsportsDB.Bronze.Players (PlayerID, Handle, TeamName, Role, Region) VALUES
(1, 'ShadowSlayer', 'Team Alpha', 'DPS', 'NA'),
(2, 'HealBot9000', 'Team Alpha', 'Support', 'NA'),
(3, 'IronWall', 'Team Alpha', 'Tank', 'NA'),
(4, 'SwiftStrike', 'Team Alpha', 'DPS', 'NA'),
(5, 'Visionary', 'Team Alpha', 'Support', 'NA'),
(6, 'Viper', 'Team Omega', 'DPS', 'EU'),
(7, 'Guardian', 'Team Omega', 'Support', 'EU'),
(8, 'Titan', 'Team Omega', 'Tank', 'EU'),
(9, 'Ghost', 'Team Omega', 'DPS', 'EU'),
(10, 'Oracle', 'Team Omega', 'Support', 'EU');

-- Insert 10 Matches
INSERT INTO EsportsDB.Bronze.Matches (MatchID, Tournament, MapName, DurationSeconds, WinnerTeam, MatchDate) VALUES
(101, 'Winter Championship', 'Dusty Ruins', 1800, 'Team Alpha', '2025-02-01'),
(102, 'Winter Championship', 'Crystal Spire', 2100, 'Team Omega', '2025-02-01'),
(103, 'Winter Championship', 'Iron Forge', 1500, 'Team Alpha', '2025-02-02'),
(104, 'Spring League', 'Dusty Ruins', 1900, 'Team Alpha', '2025-03-10'),
(105, 'Spring League', 'Neon City', 2400, 'Team Omega', '2025-03-12'),
(106, 'Spring League', 'Iron Forge', 1600, 'Team Alpha', '2025-03-15'),
(107, 'Global Finals', 'Crystal Spire', 3000, 'Team Omega', '2025-07-20'),
(108, 'Global Finals', 'Neon City', 2800, 'Team Omega', '2025-07-21'),
(109, 'Global Finals', 'Dusty Ruins', 1750, 'Team Alpha', '2025-07-22'),
(110, 'Scrimmage', 'Iron Forge', 1200, 'Team Alpha', '2025-01-10');

-- Insert 50 records into EsportsDB.Bronze.Match_Events
-- Events for Match 101 (Alpha vs Omega)
INSERT INTO EsportsDB.Bronze.Match_Events (EventID, MatchID, PlayerID, EventType, TimestampSeconds, LocationX, LocationY) VALUES
(1, 101, 1, 'Kill', 45, 10.5, 20.1),
(2, 101, 6, 'Death', 45, 10.5, 20.1),
(3, 101, 3, 'Assist', 45, 12.0, 18.0),
(4, 101, 8, 'Objective', 120, 50.0, 50.0), -- Omega takes point
(5, 101, 4, 'Kill', 150, 60.5, 40.2),
(6, 101, 7, 'Death', 150, 60.5, 40.2),
(7, 101, 1, 'Kill', 200, 30.0, 30.0),
(8, 101, 9, 'Death', 200, 30.0, 30.0),
(9, 101, 2, 'Assist', 200, 25.0, 35.0),
(10, 101, 1, 'Objective', 300, 50.0, 50.0), -- Alpha takes point
(11, 101, 6, 'Kill', 400, 80.0, 20.0),
(12, 101, 2, 'Death', 400, 80.0, 20.0),
(13, 101, 3, 'Kill', 450, 50.0, 50.0),
(14, 101, 8, 'Death', 450, 50.0, 50.0),
(15, 101, 1, 'Kill', 600, 10.0, 10.0),
(16, 101, 6, 'Death', 600, 10.0, 10.0),
(17, 102, 6, 'Kill', 50, 15.0, 15.0),
(18, 102, 1, 'Death', 50, 15.0, 15.0),
(19, 102, 7, 'Assist', 50, 18.0, 12.0),
(20, 102, 8, 'Objective', 200, 40.0, 60.0),
(21, 103, 1, 'Kill', 30, 20.0, 20.0),
(22, 103, 9, 'Death', 30, 20.0, 20.0),
(23, 103, 3, 'Assist', 30, 22.0, 18.0),
(24, 104, 4, 'Kill', 100, 55.0, 55.0),
(25, 104, 8, 'Death', 100, 55.0, 55.0),
(26, 104, 2, 'Objective', 300, 50.0, 50.0),
(27, 105, 9, 'Kill', 60, 30.0, 40.0),
(28, 105, 5, 'Death', 60, 30.0, 40.0),
(29, 105, 10, 'Assist', 60, 32.0, 38.0),
(30, 106, 3, 'Kill', 120, 45.0, 45.0),
(31, 106, 6, 'Death', 120, 45.0, 45.0),
(32, 107, 6, 'Kill', 200, 60.0, 60.0),
(33, 107, 1, 'Death', 200, 60.0, 60.0),
(34, 107, 8, 'Objective', 500, 50.0, 50.0),
(35, 108, 9, 'Kill', 150, 25.0, 25.0),
(36, 108, 4, 'Death', 150, 25.0, 25.0),
(37, 109, 1, 'Kill', 50, 10.0, 10.0),
(38, 109, 7, 'Death', 50, 10.0, 10.0),
(39, 110, 3, 'Kill', 10, 50.0, 50.0),
(40, 110, 8, 'Death', 10, 50.0, 50.0),
(41, 101, 1, 'Kill', 800, 20.0, 20.0), -- ShadowSlayer dominant
(42, 101, 10, 'Death', 800, 20.0, 20.0),
(43, 101, 4, 'Kill', 850, 25.0, 25.0),
(44, 101, 9, 'Death', 850, 25.0, 25.0),
(45, 102, 6, 'Kill', 500, 40.0, 40.0),
(46, 102, 2, 'Death', 500, 40.0, 40.0),
(47, 102, 8, 'Kill', 600, 50.0, 50.0),
(48, 102, 3, 'Death', 600, 50.0, 50.0),
(49, 103, 1, 'Objective', 600, 50.0, 50.0),
(50, 109, 4, 'Kill', 200, 30.0, 30.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Aggregations & Stats
-------------------------------------------------------------------------------

-- 2.1 Player Match Stats
CREATE OR REPLACE VIEW EsportsDB.Silver.Player_Match_Stats AS
SELECT
    m.MatchID,
    m.Tournament,
    p.PlayerID,
    p.Handle,
    p.Role,
    p.TeamName,
    SUM(CASE WHEN e.EventType = 'Kill' THEN 1 ELSE 0 END) AS Kills,
    SUM(CASE WHEN e.EventType = 'Death' THEN 1 ELSE 0 END) AS Deaths,
    SUM(CASE WHEN e.EventType = 'Assist' THEN 1 ELSE 0 END) AS Assists,
    SUM(CASE WHEN e.EventType = 'Objective' THEN 1 ELSE 0 END) AS Objectives
FROM EsportsDB.Bronze.Matches m
JOIN EsportsDB.Bronze.Match_Events e ON m.MatchID = e.MatchID
JOIN EsportsDB.Bronze.Players p ON e.PlayerID = p.PlayerID
GROUP BY m.MatchID, m.Tournament, p.PlayerID, p.Handle, p.Role, p.TeamName;

-- 2.2 Map Win Rates
CREATE OR REPLACE VIEW EsportsDB.Silver.Map_Stats AS
SELECT
    MapName,
    WinnerTeam,
    COUNT(MatchID) AS Wins,
    AVG(DurationSeconds) AS AvgDuration
FROM EsportsDB.Bronze.Matches
GROUP BY MapName, WinnerTeam;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Team Intelligence
-------------------------------------------------------------------------------

-- 3.1 Team Performance Summary (KDA)
CREATE OR REPLACE VIEW EsportsDB.Gold.Player_Performance_Summary AS
SELECT
    Handle,
    TeamName,
    Role,
    SUM(Kills) AS TotalKills,
    SUM(Deaths) AS TotalDeaths,
    SUM(Assists) AS TotalAssists,
    CASE 
        WHEN SUM(Deaths) = 0 THEN SUM(Kills) + SUM(Assists) 
        ELSE (SUM(Kills) + SUM(Assists)) / SUM(Deaths) 
    END AS KDA_Ratio,
    AVG(Kills) AS AvgKillsPerMatch
FROM EsportsDB.Silver.Player_Match_Stats
GROUP BY Handle, TeamName, Role;

-- 3.2 Role Impact
CREATE OR REPLACE VIEW EsportsDB.Gold.Role_Impact_Analysis AS
SELECT
    Role,
    AVG(Kills) AS AvgKills,
    AVG(Deaths) AS AvgDeaths,
    AVG(Objectives) AS AvgObjectives
FROM EsportsDB.Silver.Player_Match_Stats
GROUP BY Role;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (MVP Search):
"Find the player with the highest KDA_Ratio in EsportsDB.Gold.Player_Performance_Summary."

PROMPT 2 (Map Strategy):
"Which team has the most wins on 'Dusty Ruins' according to EsportsDB.Silver.Map_Stats?"

PROMPT 3 (Role Analysis):
"Compare the average Objective captures for 'Tank' vs 'DPS' roles using EsportsDB.Gold.Role_Impact_Analysis."
*/
