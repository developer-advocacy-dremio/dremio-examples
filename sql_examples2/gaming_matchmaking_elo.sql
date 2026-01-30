/*
 * Gaming: Skill-Based Matchmaking (SBMM) Analysis
 * 
 * Scenario:
 * Analyzing player latency (Ping) and ELO scores to ensure fair and responsive competitive matches.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS GamingDB;
CREATE FOLDER IF NOT EXISTS GamingDB.Matchmaking;
CREATE FOLDER IF NOT EXISTS GamingDB.Matchmaking.Bronze;
CREATE FOLDER IF NOT EXISTS GamingDB.Matchmaking.Silver;
CREATE FOLDER IF NOT EXISTS GamingDB.Matchmaking.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Player Telemetry
-------------------------------------------------------------------------------

-- PlayerStats Table
CREATE TABLE IF NOT EXISTS GamingDB.Matchmaking.Bronze.PlayerStats (
    PlayerID VARCHAR,
    Region VARCHAR, -- NA-East, EU-West, APAC
    CurrentELO INT,
    GamesPlayed INT,
    LastLogin TIMESTAMP
);

INSERT INTO GamingDB.Matchmaking.Bronze.PlayerStats VALUES
('P-001', 'NA-East', 1200, 50, '2025-08-01 10:00:00'),
('P-002', 'NA-East', 1250, 60, '2025-08-01 10:05:00'),
('P-003', 'NA-East', 2400, 500, '2025-08-01 10:10:00'), -- Pro Level
('P-004', 'EU-West', 1100, 40, '2025-08-01 10:15:00'),
('P-005', 'APAC', 1500, 200, '2025-08-01 10:20:00'),
('P-006', 'NA-East', 1180, 45, '2025-08-01 10:25:00'),
('P-007', 'NA-West', 1300, 80, '2025-08-01 10:30:00'),
('P-008', 'EU-West', 2350, 480, '2025-08-01 10:35:00'), -- Pro Level
('P-009', 'APAC', 1600, 250, '2025-08-01 10:40:00'),
('P-010', 'NA-East', 900, 10, '2025-08-01 10:45:00'), -- Newbie
('P-011', 'NA-East', 1210, 52, '2025-08-01 10:50:00'),
('P-012', 'NA-East', 1240, 58, '2025-08-01 10:55:00'),
('P-013', 'NA-West', 1290, 75, '2025-08-01 11:00:00'),
('P-014', 'EU-West', 1150, 42, '2025-08-01 11:05:00'),
('P-015', 'APAC', 1550, 210, '2025-08-01 11:10:00'),
('P-016', 'NA-East', 2500, 600, '2025-08-01 11:15:00'), -- Top Tier
('P-017', 'NA-West', 800, 5, '2025-08-01 11:20:00'), -- New
('P-018', 'EU-West', 1800, 300, '2025-08-01 11:25:00'),
('P-019', 'APAC', 2000, 350, '2025-08-01 11:30:00'),
('P-020', 'NA-East', 1220, 55, '2025-08-01 11:35:00'),
('P-021', 'NA-East', 1230, 56, '2025-08-01 11:40:00'),
('P-022', 'NA-West', 1310, 82, '2025-08-01 11:45:00'),
('P-023', 'EU-West', 2420, 510, '2025-08-01 11:50:00'),
('P-024', 'APAC', 1450, 180, '2025-08-01 11:55:00'),
('P-025', 'NA-East', 850, 8, '2025-08-01 12:00:00'),
('P-026', 'NA-East', 1190, 48, '2025-08-01 12:05:00'),
('P-027', 'NA-West', 1280, 70, '2025-08-01 12:10:00'),
('P-028', 'EU-West', 1120, 35, '2025-08-01 12:15:00'),
('P-029', 'APAC', 1520, 205, '2025-08-01 12:20:00'),
('P-030', 'NA-East', 2450, 550, '2025-08-01 12:25:00'),
('P-031', 'NA-West', 2380, 490, '2025-08-01 12:30:00'),
('P-032', 'EU-West', 1750, 280, '2025-08-01 12:35:00'),
('P-033', 'APAC', 1900, 320, '2025-08-01 12:40:00'),
('P-034', 'NA-East', 1260, 62, '2025-08-01 12:45:00'),
('P-035', 'NA-East', 1270, 65, '2025-08-01 12:50:00'),
('P-036', 'NA-West', 1350, 90, '2025-08-01 12:55:00'),
('P-037', 'EU-West', 2300, 450, '2025-08-01 13:00:00'),
('P-038', 'APAC', 1400, 150, '2025-08-01 13:05:00'),
('P-039', 'NA-East', 950, 15, '2025-08-01 13:10:00'),
('P-040', 'NA-East', 1170, 44, '2025-08-01 13:15:00'),
('P-041', 'NA-West', 1265, 63, '2025-08-01 13:20:00'),
('P-042', 'EU-West', 1140, 38, '2025-08-01 13:25:00'),
('P-043', 'APAC', 1580, 220, '2025-08-01 13:30:00'),
('P-044', 'NA-East', 2480, 580, '2025-08-01 13:35:00'),
('P-045', 'NA-West', 820, 6, '2025-08-01 13:40:00'),
('P-046', 'EU-West', 1850, 310, '2025-08-01 13:45:00'),
('P-047', 'APAC', 2100, 380, '2025-08-01 13:50:00'),
('P-048', 'NA-East', 1205, 51, '2025-08-01 13:55:00'),
('P-049', 'NA-East', 1225, 54, '2025-08-01 14:00:00'),
('P-050', 'NA-West', 1320, 85, '2025-08-01 14:05:00');

-- MatchLatency Table
CREATE TABLE IF NOT EXISTS GamingDB.Matchmaking.Bronze.MatchLatency (
    MatchID VARCHAR,
    PlayerID VARCHAR,
    ServerRegion VARCHAR,
    PingMs INT,
    PacketLossPct DOUBLE,
    MatchTimestamp TIMESTAMP
);

INSERT INTO GamingDB.Matchmaking.Bronze.MatchLatency VALUES
('M-001', 'P-001', 'NA-East', 25, 0.0, '2025-08-01 10:05:00'),
('M-001', 'P-002', 'NA-East', 28, 0.0, '2025-08-01 10:05:00'),
('M-002', 'P-003', 'NA-East', 30, 0.0, '2025-08-01 10:15:00'),
('M-002', 'P-016', 'NA-East', 22, 0.0, '2025-08-01 10:15:00'), -- Pro Match
('M-003', 'P-004', 'EU-West', 15, 0.0, '2025-08-01 10:20:00'),
('M-004', 'P-005', 'APAC', 40, 0.5, '2025-08-01 10:25:00'),
('M-005', 'P-006', 'NA-East', 120, 2.0, '2025-08-01 10:30:00'), -- Laggy
('M-005', 'P-010', 'NA-East', 35, 0.0, '2025-08-01 10:30:00'),
('M-006', 'P-008', 'EU-West', 18, 0.0, '2025-08-01 10:40:00'), 
('M-006', 'P-023', 'EU-West', 20, 0.0, '2025-08-01 10:40:00'),
('M-007', 'P-007', 'NA-East', 90, 1.5, '2025-08-01 10:45:00'), -- Cross-region lag
('M-008', 'P-009', 'APAC', 45, 0.0, '2025-08-01 10:50:00'),
('M-009', 'P-044', 'NA-East', 25, 0.0, '2025-08-01 13:40:00'),
('M-009', 'P-030', 'NA-East', 27, 0.0, '2025-08-01 13:40:00'),
('M-010', 'P-015', 'APAC', 200, 5.0, '2025-08-01 11:15:00'), -- Unplayable
('M-011', 'P-011', 'NA-East', 30, 0.0, '2025-08-01 10:55:00'),
('M-012', 'P-012', 'NA-East', 32, 0.0, '2025-08-01 11:00:00'),
('M-013', 'P-013', 'NA-West', 20, 0.0, '2025-08-01 11:05:00'),
('M-014', 'P-014', 'EU-West', 18, 0.0, '2025-08-01 11:10:00'),
('M-015', 'P-018', 'EU-West', 22, 0.0, '2025-08-01 11:30:00'),
('M-016', 'P-019', 'APAC', 35, 0.0, '2025-08-01 11:35:00'),
('M-017', 'P-020', 'NA-East', 29, 0.0, '2025-08-01 11:40:00'),
('M-018', 'P-021', 'NA-East', 31, 0.0, '2025-08-01 11:45:00'),
('M-019', 'P-022', 'NA-West', 25, 0.0, '2025-08-01 11:50:00'),
('M-020', 'P-024', 'APAC', 42, 0.0, '2025-08-01 12:00:00'),
('M-021', 'P-025', 'NA-East', 33, 0.0, '2025-08-01 12:05:00'),
('M-022', 'P-026', 'NA-East', 28, 0.0, '2025-08-01 12:10:00'),
('M-023', 'P-027', 'NA-West', 85, 1.0, '2025-08-01 12:15:00'), -- Cross region
('M-024', 'P-028', 'EU-West', 19, 0.0, '2025-08-01 12:20:00'),
('M-025', 'P-029', 'APAC', 38, 0.0, '2025-08-01 12:25:00'),
('M-026', 'P-031', 'NA-West', 24, 0.0, '2025-08-01 12:35:00'),
('M-027', 'P-032', 'EU-West', 21, 0.0, '2025-08-01 12:40:00'),
('M-028', 'P-033', 'APAC', 40, 0.0, '2025-08-01 12:45:00'),
('M-029', 'P-034', 'NA-East', 30, 0.0, '2025-08-01 12:50:00'),
('M-030', 'P-035', 'NA-East', 31, 0.0, '2025-08-01 12:55:00'),
('M-031', 'P-036', 'NA-West', 26, 0.0, '2025-08-01 13:00:00'),
('M-032', 'P-037', 'EU-West', 17, 0.0, '2025-08-01 13:05:00'),
('M-033', 'P-038', 'APAC', 44, 0.0, '2025-08-01 13:10:00'),
('M-034', 'P-039', 'NA-East', 34, 0.0, '2025-08-01 13:15:00'),
('M-035', 'P-040', 'NA-East', 29, 0.0, '2025-08-01 13:20:00'),
('M-036', 'P-041', 'NA-West', 23, 0.0, '2025-08-01 13:25:00'),
('M-037', 'P-042', 'EU-West', 20, 0.0, '2025-08-01 13:30:00'),
('M-038', 'P-043', 'APAC', 39, 0.0, '2025-08-01 13:35:00'),
('M-039', 'P-045', 'NA-West', 28, 0.0, '2025-08-01 13:45:00'),
('M-040', 'P-046', 'EU-West', 22, 0.0, '2025-08-01 13:50:00'),
('M-041', 'P-047', 'APAC', 36, 0.0, '2025-08-01 13:55:00'),
('M-042', 'P-048', 'NA-East', 32, 0.0, '2025-08-01 14:00:00'),
('M-043', 'P-049', 'NA-East', 30, 0.0, '2025-08-01 14:05:00'),
('M-044', 'P-050', 'NA-West', 25, 0.0, '2025-08-01 14:10:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Fair Match Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW GamingDB.Matchmaking.Silver.GameQuality AS
SELECT 
    m.MatchID,
    p.PlayerID,
    p.CurrentELO,
    p.Region AS PlayerRegion,
    m.ServerRegion,
    m.PingMs,
    m.PacketLossPct,
    -- Determine match quality
    CASE 
        WHEN m.PingMs > 100 OR m.PacketLossPct > 1.0 THEN 'Poor Connectivity'
        WHEN p.Region != m.ServerRegion THEN 'Cross-Region Imbalance'
        ELSE 'Optimal'
    END AS ConnectionQuality,
    -- Skill Bracket
    CASE 
        WHEN p.CurrentELO >= 2400 THEN 'Grandmaster'
        WHEN p.CurrentELO >= 2000 THEN 'Diamond'
        WHEN p.CurrentELO >= 1500 THEN 'Platinum'
        WHEN p.CurrentELO >= 1200 THEN 'Gold'
        ELSE 'Silver/Bronze'
    END AS SkillTier
FROM GamingDB.Matchmaking.Bronze.PlayerStats p
JOIN GamingDB.Matchmaking.Bronze.MatchLatency m ON p.PlayerID = m.PlayerID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Matchmaking Health Dashboard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW GamingDB.Matchmaking.Gold.RegionalHealth AS
SELECT 
    ServerRegion,
    SkillTier,
    COUNT(MatchID) AS TotalMatches,
    AVG(PingMs) AS AvgPing,
    SUM(CASE WHEN ConnectionQuality != 'Optimal' THEN 1 ELSE 0 END) AS PoorQualityMatches,
    (CAST(SUM(CASE WHEN ConnectionQuality != 'Optimal' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(MatchID)) * 100 AS BadMatchPct
FROM GamingDB.Matchmaking.Silver.GameQuality
GROUP BY ServerRegion, SkillTier;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify which SkillTier in 'NA-East' has the highest percentage of bad matches from the GamingDB.Matchmaking.Gold.RegionalHealth view."

PROMPT 2:
"List all players with 'Poor Connectivity' status in the Silver layer, showing their region and ping."

PROMPT 3:
"Comparing 'Grandmaster' vs 'Gold' tiers, which group experiences higher average ping?"
*/
