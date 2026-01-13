/*
 * Music Streaming Royalties Demo
 * 
 * Scenario:
 * Calculating artist payouts per stream based on user subscription type.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Ensure accurate billing and licensing compliance.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MusicStreamDB;
CREATE FOLDER IF NOT EXISTS MusicStreamDB.Bronze;
CREATE FOLDER IF NOT EXISTS MusicStreamDB.Silver;
CREATE FOLDER IF NOT EXISTS MusicStreamDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MusicStreamDB.Bronze.Tracks (
    TrackID INT,
    Title VARCHAR,
    ArtistID INT,
    DurationSeconds INT,
    Genre VARCHAR
);

CREATE TABLE IF NOT EXISTS MusicStreamDB.Bronze.Streams (
    StreamID INT,
    TrackID INT,
    UserID INT,
    StreamTime TIMESTAMP,
    DurationPlayed INT,
    UserType VARCHAR -- 'Free', 'Premium'
);

INSERT INTO MusicStreamDB.Bronze.Tracks VALUES
(1, 'Hit Song A', 101, 210, 'Pop'),
(2, 'Indie Song B', 102, 180, 'Rock');

INSERT INTO MusicStreamDB.Bronze.Streams VALUES
(1, 1, 1001, '2025-01-01 10:00:01', 210, 'Premium'), -- Full play
(2, 2, 1002, '2025-01-01 10:05:00', 30, 'Free'), -- Skipped?
(3, 1, 1003, '2025-01-01 10:10:00', 210, 'Free');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MusicStreamDB.Silver.ValidStreams AS
SELECT 
    s.StreamID,
    t.Title,
    t.ArtistID,
    s.UserType,
    CASE 
        WHEN s.DurationPlayed >= 30 THEN 1 
        ELSE 0 
    END AS IsPayable
FROM MusicStreamDB.Bronze.Streams s
JOIN MusicStreamDB.Bronze.Tracks t ON s.TrackID = t.TrackID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MusicStreamDB.Gold.ArtistPayouts AS
SELECT 
    ArtistID,
    COUNT(StreamID) AS TotalStreams,
    SUM(IsPayable) AS BillableStreams,
    SUM(CASE 
        WHEN IsPayable = 1 AND UserType = 'Premium' THEN 0.005 
        WHEN IsPayable = 1 AND UserType = 'Free' THEN 0.001 
        ELSE 0 
    END) AS EstRoyalties
FROM MusicStreamDB.Silver.ValidStreams
GROUP BY ArtistID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate total EstRoyalties for ArtistID 101 in MusicStreamDB.Gold.ArtistPayouts."
*/
