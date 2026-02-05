/*
 * Dremio K-Pop Fandom Analytics Example
 * 
 * Domain: Social Media & Entertainment Intelligence
 * Scenario: 
 * Management agencies track "Fandom" engagement for Idol groups.
 * Metrics include "Streaming Velocity" (Spotify/Melon), "Hashtag Trending" (Twitter/X),
 * and "Merch Pre-orders".
 * The goal is to maximize "Comeback" impact and identify "Bias" (favorite member) trends.
 * 
 * Complexity: Medium (Social sentiment scaling, velocity calculation)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Idol_Metrics;
CREATE FOLDER IF NOT EXISTS Idol_Metrics.Sources;
CREATE FOLDER IF NOT EXISTS Idol_Metrics.Bronze;
CREATE FOLDER IF NOT EXISTS Idol_Metrics.Silver;
CREATE FOLDER IF NOT EXISTS Idol_Metrics.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Idol_Metrics.Sources.Group_Roster (
    GroupID VARCHAR,
    Group_Name VARCHAR, -- 'Starlight Girls', 'Neon Boys'
    Member_Name VARCHAR,
    Position VARCHAR -- 'Visual', 'Main Vocal', 'Rapper'
);

CREATE TABLE IF NOT EXISTS Idol_Metrics.Sources.Engagement_Log (
    LogID VARCHAR,
    GroupID VARCHAR,
    Platform VARCHAR, -- 'Spotify', 'Twitter', 'Weverse', 'YouTube'
    Activity_Timestamp TIMESTAMP,
    Metric_Type VARCHAR, -- 'Stream', 'Mention', 'Vote', 'PreOrder'
    Count_Value INT,
    Reference_Member VARCHAR -- NULL if group activity, or Member Name if solo
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Groups
INSERT INTO Idol_Metrics.Sources.Group_Roster VALUES
('GRP-01', 'Starlight Girls', 'Luna', 'Main Vocal'),
('GRP-01', 'Starlight Girls', 'Sola', 'Visual'),
('GRP-01', 'Starlight Girls', 'Nova', 'Rapper'),
('GRP-02', 'Neon Boys', 'Kai', 'Dancer'),
('GRP-02', 'Neon Boys', 'Jin', 'Leader');

-- Seed Engagement (Comeback Week)
INSERT INTO Idol_Metrics.Sources.Engagement_Log VALUES
('L-001', 'GRP-01', '2023-11-01 08:00:00', 'Stream', 50000, NULL),
('L-002', 'GRP-01', '2023-11-01 09:00:00', 'Stream', 55000, NULL), -- Rising
('L-003', 'GRP-01', '2023-11-01 10:00:00', 'Stream', 60000, NULL),
('L-004', 'GRP-01', '2023-11-01 08:00:00', 'Mention', 1000, 'Luna'), -- Solo buzz
('L-005', 'GRP-01', '2023-11-01 08:00:00', 'Mention', 500, 'Nova'),
('L-006', 'GRP-02', '2023-11-01 08:00:00', 'Stream', 10000, NULL), -- Off-season
('L-007', 'GRP-02', '2023-11-01 08:00:00', 'Vote', 200, NULL),
('L-008', 'GRP-01', '2023-11-02 08:00:00', 'PreOrder', 5000, NULL), -- Album drop
('L-009', 'GRP-01', '2023-11-02 09:00:00', 'PreOrder', 12000, NULL), -- Huge spike
('L-010', 'GRP-01', '2023-11-02 10:00:00', 'PreOrder', 8000, NULL),
-- Filler
('L-011', 'GRP-01', '2023-11-03 08:00:00', 'Stream', 45000, NULL),
('L-012', 'GRP-01', '2023-11-04 08:00:00', 'Stream', 40000, NULL),
('L-013', 'GRP-01', '2023-11-05 08:00:00', 'Stream', 38000, NULL),
('L-014', 'GRP-01', '2023-11-06 08:00:00', 'Stream', 37000, NULL),
('L-015', 'GRP-01', '2023-11-03 08:00:00', 'Mention', 2000, 'Sola'), -- Viral look
('L-016', 'GRP-01', '2023-11-04 08:00:00', 'Mention', 500, 'Sola'),
('L-017', 'GRP-01', '2023-11-03 08:00:00', 'Vote', 10000, NULL), -- Music show win
('L-018', 'GRP-02', '2023-11-02 08:00:00', 'Stream', 9000, NULL),
('L-019', 'GRP-02', '2023-11-03 08:00:00', 'Stream', 9000, NULL),
('L-020', 'GRP-02', '2023-11-04 08:00:00', 'Stream', 8500, NULL),
('L-021', 'GRP-01', '2023-11-07 08:00:00', 'YouTube', 1000000, NULL), -- MV View
('L-022', 'GRP-01', '2023-11-07 09:00:00', 'YouTube', 1500000, NULL),
('L-023', 'GRP-01', '2023-11-07 10:00:00', 'YouTube', 2000000, NULL),
('L-024', 'GRP-01', '2023-11-08 08:00:00', 'YouTube', 500000, NULL),
('L-025', 'GRP-01', '2023-11-09 08:00:00', 'YouTube', 400000, NULL),
('L-026', 'GRP-01', '2023-11-10 08:00:00', 'YouTube', 300000, NULL),
('L-027', 'GRP-02', '2023-11-05 08:00:00', 'Mention', 100, 'Kai'),
('L-028', 'GRP-02', '2023-11-06 08:00:00', 'Mention', 150, 'Kai'),
('L-029', 'GRP-02', '2023-11-07 08:00:00', 'Mention', 200, 'Jin'),
('L-030', 'GRP-02', '2023-11-08 08:00:00', 'Vote', 300, NULL),
('L-031', 'GRP-01', '2023-11-01 12:00:00', 'Stream', 62000, NULL),
('L-032', 'GRP-01', '2023-11-01 13:00:00', 'Stream', 61000, NULL),
('L-033', 'GRP-01', '2023-11-01 14:00:00', 'Stream', 59000, NULL),
('L-034', 'GRP-01', '2023-11-01 15:00:00', 'Stream', 58000, NULL),
('L-035', 'GRP-01', '2023-11-01 16:00:00', 'Stream', 65000, NULL), -- After school spike
('L-036', 'GRP-01', '2023-11-05 12:00:00', 'PreOrder', 1000, NULL),
('L-037', 'GRP-01', '2023-11-06 12:00:00', 'PreOrder', 500, NULL),
('L-038', 'GRP-01', '2023-11-07 12:00:00', 'PreOrder', 200, NULL),
('L-039', 'GRP-01', '2023-11-05 10:00:00', 'Mention', 2500, 'Sola'),
('L-040', 'GRP-01', '2023-11-05 11:00:00', 'Mention', 1500, 'Luna'),
('L-041', 'GRP-01', '2023-11-05 12:00:00', 'Mention', 1000, 'Nova'),
('L-042', 'GRP-02', '2023-11-05 12:00:00', 'Stream', 9500, NULL),
('L-043', 'GRP-02', '2023-11-06 12:00:00', 'Stream', 9600, NULL),
('L-044', 'GRP-02', '2023-11-07 12:00:00', 'Stream', 9700, NULL),
('L-045', 'GRP-02', '2023-11-08 12:00:00', 'Stream', 9800, NULL),
('L-046', 'GRP-01', '2023-11-10 12:00:00', 'Vote', 20000, NULL), -- Inkigayo
('L-047', 'GRP-01', '2023-11-10 13:00:00', 'Vote', 25000, NULL),
('L-048', 'GRP-01', '2023-11-10 14:00:00', 'Vote', 30000, NULL),
('L-049', 'GRP-01', '2023-11-10 15:00:00', 'Vote', 28000, NULL),
('L-050', 'GRP-01', '2023-11-10 16:00:00', 'Vote', 26000, NULL);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Idol_Metrics.Bronze.Bronze_Roster AS SELECT * FROM Idol_Metrics.Sources.Group_Roster;
CREATE OR REPLACE VIEW Idol_Metrics.Bronze.Bronze_Engagement AS SELECT * FROM Idol_Metrics.Sources.Engagement_Log;

-- 4b. SILVER LAYER (Standardized Impact)
CREATE OR REPLACE VIEW Idol_Metrics.Silver.Silver_Engagement_Score AS
SELECT
    l.LogID,
    l.GroupID,
    g.Group_Name,
    l.Activity_Timestamp,
    l.Metric_Type,
    l.Count_Value,
    l.Reference_Member,
    -- Weighted Score (Sales > Votes > Streams > Mentions)
    CASE 
        WHEN l.Metric_Type = 'PreOrder' THEN l.Count_Value * 50
        WHEN l.Metric_Type = 'Vote' THEN l.Count_Value * 5
        WHEN l.Metric_Type = 'YouTube' THEN l.Count_Value * 0.01
        WHEN l.Metric_Type = 'Stream' THEN l.Count_Value * 0.1
        WHEN l.Metric_Type = 'Mention' THEN l.Count_Value * 0.5
        ELSE 0
    END as Impact_Score
FROM Idol_Metrics.Bronze.Bronze_Engagement l
JOIN Idol_Metrics.Bronze.Bronze_Roster g ON l.GroupID = g.GroupID;

-- 4c. GOLD LAYER (Trend Dashboard)
CREATE OR REPLACE VIEW Idol_Metrics.Gold.Gold_Fandom_Trends AS
SELECT
    Group_Name,
    Metric_Type,
    Reference_Member,
    SUM(Count_Value) as Total_Volume,
    SUM(Impact_Score) as Total_Impact,
    -- Velocity (Last 24h vs Prev?) - approximated by total here
    COUNT(*) as Data_Points_Count
FROM Idol_Metrics.Silver.Silver_Engagement_Score
GROUP BY Group_Name, Metric_Type, Reference_Member;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Gold_Fandom_Trends'. Identify the member of 'Starlight Girls' with the highest citation of 'Mention' metrics. 
 * This identifies the current 'Bias' trend."
 */
