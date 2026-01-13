/*
 * Podcast Listener Analytics Demo
 * 
 * Scenario:
 * Analyzing episode downloads, listen-through rates, and geographic reach.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Improve content strategy and ad targeting.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS PodcastDB;
CREATE FOLDER IF NOT EXISTS PodcastDB.Bronze;
CREATE FOLDER IF NOT EXISTS PodcastDB.Silver;
CREATE FOLDER IF NOT EXISTS PodcastDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PodcastDB.Bronze.Episodes (
    EpID INT,
    Title VARCHAR,
    PublishDate DATE,
    DurationSeconds INT,
    Topic VARCHAR
);

CREATE TABLE IF NOT EXISTS PodcastDB.Bronze.Downloads (
    DownloadID INT,
    EpID INT,
    UserID INT,
    "Timestamp" TIMESTAMP,
    SecondsListened INT,
    UserCountry VARCHAR
);

INSERT INTO PodcastDB.Bronze.Episodes VALUES
(1, 'The Future of AI', '2025-01-01', 3600, 'Tech'),
(2, 'History of Pizza', '2025-01-08', 1800, 'Food');

INSERT INTO PodcastDB.Bronze.Downloads VALUES
(1, 1, 100, '2025-01-02 10:00:00', 3500, 'US'), -- Near full listen
(2, 1, 101, '2025-01-02 11:00:00', 300, 'UK'), -- Drop off
(3, 2, 100, '2025-01-09 09:00:00', 1800, 'US'); -- Full listen

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PodcastDB.Silver.Engagement AS
SELECT 
    d.DownloadID,
    e.Title,
    d.UserCountry,
    d.SecondsListened,
    e.DurationSeconds,
    (CAST(d.SecondsListened AS DOUBLE) / e.DurationSeconds) * 100 AS CompletionPct
FROM PodcastDB.Bronze.Downloads d
JOIN PodcastDB.Bronze.Episodes e ON d.EpID = e.EpID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PodcastDB.Gold.EpisodePerformance AS
SELECT 
    Title,
    COUNT(DownloadID) AS TotalDownloads,
    AVG(CompletionPct) AS AvgCompletionRate,
    SUM(CASE WHEN CompletionPct >= 90 THEN 1 ELSE 0 END) AS CompletedListens
FROM PodcastDB.Silver.Engagement
GROUP BY Title;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which podcast episode has the highest AvgCompletionRate in PodcastDB.Gold.EpisodePerformance?"
*/
