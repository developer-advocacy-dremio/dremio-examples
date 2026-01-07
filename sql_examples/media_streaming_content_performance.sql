/*
 * Media Streaming Content Performance Demo
 * 
 * Scenario:
 * A streaming platform analyzes viewership to recommend content.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Increase viewer engagement and retention.
 * 
 * Note: Assumes a catalog named 'MediaDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS MediaDB;
CREATE FOLDER IF NOT EXISTS MediaDB.Bronze;
CREATE FOLDER IF NOT EXISTS MediaDB.Silver;
CREATE FOLDER IF NOT EXISTS MediaDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Usage Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MediaDB.Bronze.ContentLibrary (
    ContentID INT,
    Title VARCHAR,
    Genre VARCHAR,
    DurationMinutes INT,
    ReleaseYear INT
);

CREATE TABLE IF NOT EXISTS MediaDB.Bronze.UserStreams (
    StreamID INT,
    UserID INT,
    ContentID INT,
    StartTime TIMESTAMP,
    MinutesWatched INT
);

CREATE TABLE IF NOT EXISTS MediaDB.Bronze.Ratings (
    RatingID INT,
    UserID INT,
    ContentID INT,
    Stars INT -- 1-5
);

-- 1.2 Populate Bronze Tables
INSERT INTO MediaDB.Bronze.ContentLibrary (ContentID, Title, Genre, DurationMinutes, ReleaseYear) VALUES
(1, 'Space Wars', 'Sci-Fi', 120, 2020),
(2, 'The Office Life', 'Comedy', 22, 2015),
(3, 'Planet Earth III', 'Documentary', 60, 2023),
(4, 'Dark Forest', 'Thriller', 100, 2021),
(5, 'Romance in Paris', 'Romance', 90, 2019);

INSERT INTO MediaDB.Bronze.UserStreams (StreamID, UserID, ContentID, StartTime, MinutesWatched) VALUES
(1, 101, 1, '2025-08-01 20:00:00', 120), -- Finished
(2, 101, 2, '2025-08-02 18:00:00', 22), -- Finished
(3, 102, 1, '2025-08-01 21:00:00', 10), -- Abandoned
(4, 103, 3, '2025-08-03 10:00:00', 60), -- Finished
(5, 104, 4, '2025-08-04 22:00:00', 95), -- Almost finished
(6, 102, 3, '2025-08-05 19:00:00', 30); -- Halfway

INSERT INTO MediaDB.Bronze.Ratings (RatingID, UserID, ContentID, Stars) VALUES
(1, 101, 1, 5),
(2, 101, 2, 4),
(3, 103, 3, 5),
(4, 102, 1, 2);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Engagement Metrics
-------------------------------------------------------------------------------

-- 2.1 View: Stream_Engagement
-- Calculates Completion Rate (MinutesWatched / Duration).
CREATE OR REPLACE VIEW MediaDB.Silver.Stream_Engagement AS
SELECT
    s.StreamID,
    c.Title,
    c.Genre,
    s.MinutesWatched,
    c.DurationMinutes,
    CAST(s.MinutesWatched AS DOUBLE) / c.DurationMinutes AS CompletionPct,
    CASE WHEN CAST(s.MinutesWatched AS DOUBLE) / c.DurationMinutes >= 0.9 THEN 1 ELSE 0 END AS IsCompleted
FROM MediaDB.Bronze.UserStreams s
JOIN MediaDB.Bronze.ContentLibrary c ON s.ContentID = c.ContentID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Content Strategy
-------------------------------------------------------------------------------

-- 3.1 View: Genre_Performance
-- Aggregates views and ratings by Genre.
CREATE OR REPLACE VIEW MediaDB.Gold.Genre_Performance AS
SELECT
    g.Genre,
    COUNT(g.StreamID) AS TotalStreams,
    AVG(g.CompletionPct) * 100 AS AvgCompletionRate,
    AVG(r.Stars) AS AvgRating
FROM MediaDB.Silver.Stream_Engagement g
LEFT JOIN MediaDB.Bronze.Ratings r ON 1=1 -- Note: Simplified join (would need User/Content mapping in real scenario)
GROUP BY g.Genre;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Top Content):
"Show me the top 3 Genres by AvgCompletionRate from MediaDB.Gold.Genre_Performance."

PROMPT 2 (Viewer Retention):
"List all streams in MediaDB.Silver.Stream_Engagement where CompletionPct < 0.5."
*/
