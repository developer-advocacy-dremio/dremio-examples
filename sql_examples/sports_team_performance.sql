/*
 * Sports Team Performance Demo
 * 
 * Scenario:
 * A pro sports team analyzes player efficiency and game results.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize roster rotation and strategy.
 * 
 * Note: Assumes a catalog named 'SportsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SportsDB;
CREATE FOLDER IF NOT EXISTS SportsDB.Bronze;
CREATE FOLDER IF NOT EXISTS SportsDB.Silver;
CREATE FOLDER IF NOT EXISTS SportsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Game Logs
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SportsDB.Bronze.Teams (
    TeamID INT,
    City VARCHAR,
    Name VARCHAR,
    Conference VARCHAR
);

CREATE TABLE IF NOT EXISTS SportsDB.Bronze.Players (
    PlayerID INT,
    TeamID INT,
    Name VARCHAR,
    Position VARCHAR,
    Salary DOUBLE
);

CREATE TABLE IF NOT EXISTS SportsDB.Bronze.GameStats (
    GameID INT,
    PlayerID INT,
    "Date" DATE,
    Points INT,
    Assists INT,
    Rebounds INT,
    MinutesPlayed INT
);

-- 1.2 Populate Bronze Tables
INSERT INTO SportsDB.Bronze.Teams (TeamID, City, Name, Conference) VALUES
(1, 'Chicago', 'Bulls', 'East'),
(2, 'Los Angeles', 'Lakers', 'West');

INSERT INTO SportsDB.Bronze.Players (PlayerID, TeamID, Name, Position, Salary) VALUES
(101, 1, 'Jordan', 'SG', 30000000.0),
(102, 1, 'Pippen', 'SF', 15000000.0),
(103, 2, 'LeBron', 'SF', 40000000.0);

INSERT INTO SportsDB.Bronze.GameStats (GameID, PlayerID, "Date", Points, Assists, Rebounds, MinutesPlayed) VALUES
(1, 101, '2025-11-01', 35, 5, 6, 38),
(1, 102, '2025-11-01', 18, 8, 7, 35),
(2, 103, '2025-11-02', 28, 9, 8, 40),
(3, 101, '2025-11-03', 42, 4, 5, 41);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Player Efficiency
-------------------------------------------------------------------------------

-- 2.1 View: Player_Efficiency
-- Calculates Points Per Minute (PPM).
CREATE OR REPLACE VIEW SportsDB.Silver.Player_Efficiency AS
SELECT
    g.GameID,
    p.Name AS PlayerName,
    t.Name AS TeamName,
    g.Points,
    g.MinutesPlayed,
    (CAST(g.Points AS DOUBLE) / NULLIF(g.MinutesPlayed, 0)) AS PPM
FROM SportsDB.Bronze.GameStats g
JOIN SportsDB.Bronze.Players p ON g.PlayerID = p.PlayerID
JOIN SportsDB.Bronze.Teams t ON p.TeamID = t.TeamID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Season Averages
-------------------------------------------------------------------------------

-- 3.1 View: Season_Stats
-- Aggregates averages per player.
CREATE OR REPLACE VIEW SportsDB.Gold.Season_Stats AS
SELECT
    PlayerName,
    TeamName,
    COUNT(GameID) AS GamesPlayed,
    AVG(Points) AS AvgPoints,
    AVG(PPM) AS EfficiencyRating
FROM SportsDB.Silver.Player_Efficiency
GROUP BY PlayerName, TeamName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (MVP Watch):
"Who has the highest AvgPoints in SportsDB.Gold.Season_Stats?"

PROMPT 2 (Rotation):
"List all players with EfficiencyRating > 0.8."
*/
