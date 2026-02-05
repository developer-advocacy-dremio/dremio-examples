/*
 * Dremio "Messy Data" Challenge: Music Streaming Rights
 * 
 * Scenario: 
 * Royalty calculations.
 * 3 Tables: ARTISTS (Meta), SONGS (Catalog), STREAMS (Usage).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: STREAMS -> SONGS -> ARTISTS.
 * 2. Deduplicate: Identify 'Ghosts' (Streams < 30s).
 * 3. Calc Royalties: Sum(Streams) * Per_Stream_Rate.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Spotify_Clone;
CREATE FOLDER IF NOT EXISTS Spotify_Clone.Rights;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Spotify_Clone.Rights.ARTISTS (
    ARTIST_ID VARCHAR,
    NAME VARCHAR,
    LABEL VARCHAR
);

CREATE TABLE IF NOT EXISTS Spotify_Clone.Rights.SONGS (
    SONG_ID VARCHAR,
    ARTIST_ID VARCHAR,
    TITLE VARCHAR,
    DURATION_SEC INT
);

CREATE TABLE IF NOT EXISTS Spotify_Clone.Rights.STREAMS (
    STREAM_ID VARCHAR,
    SONG_ID VARCHAR,
    USER_ID VARCHAR,
    TS TIMESTAMP,
    PLAY_DURATION_SEC INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- ARTISTS
INSERT INTO Spotify_Clone.Rights.ARTISTS VALUES
('A-1', 'The Beagles', 'Warner'), ('A-2', 'Tayla Swiff', 'Universal');

-- SONGS
INSERT INTO Spotify_Clone.Rights.SONGS VALUES
('S-1', 'A-1', 'Barking', 180), ('S-2', 'A-2', 'Breakup', 210);

-- STREAMS (50 Rows)
INSERT INTO Spotify_Clone.Rights.STREAMS VALUES
('St-1', 'S-1', 'U-1', '2023-01-01 10:00:00', 180), -- Full
('St-2', 'S-1', 'U-2', '2023-01-01 10:01:00', 10), -- Skip (<30s = No Royalty)
('St-3', 'S-1', 'U-3', '2023-01-01 10:02:00', 190), -- Over duration
('St-4', 'S-1', 'U-4', '2023-01-01 10:03:00', 29), -- Skip
('St-5', 'S-2', 'U-1', '2023-01-01 10:04:00', 210),
('St-6', 'S-2', 'U-1', '2023-01-01 10:04:00', 210), -- Dupe
('St-7', 'S-1', 'U-1', '2023-01-01 12:00:00', 100),
('St-8', 'S-1', 'U-1', '2023-01-01 12:01:00', 100),
('St-9', 'S-1', 'U-1', '2023-01-01 12:02:00', 100),
('St-10', 'S-1', 'U-1', '2023-01-01 12:03:00', 100),
('St-11', 'S-1', 'U-1', '2023-01-01 12:04:00', 100),
('St-12', 'S-1', 'U-1', '2023-01-01 12:05:00', 100),
('St-13', 'S-1', 'U-1', '2023-01-01 12:06:00', 100),
('St-14', 'S-1', 'U-1', '2023-01-01 12:07:00', 100),
('St-15', 'S-1', 'U-1', '2023-01-01 12:08:00', 100),
('St-16', 'S-1', 'U-1', '2023-01-01 12:09:00', 100),
('St-17', 'S-1', 'U-1', '2023-01-01 12:10:00', 100),
('St-18', 'S-1', 'U-1', '2023-01-01 12:11:00', 100),
('St-19', 'S-1', 'U-1', '2023-01-01 12:12:00', 100),
('St-20', 'S-1', 'U-1', '2023-01-01 12:13:00', 100),
('St-21', 'S-1', 'U-1', '2023-01-01 12:14:00', 100),
('St-22', 'S-1', 'U-1', '2023-01-01 12:15:00', 100),
('St-23', 'S-1', 'U-1', '2023-01-01 12:16:00', 100),
('St-24', 'S-1', 'U-1', '2023-01-01 12:17:00', 100),
('St-25', 'S-1', 'U-1', '2023-01-01 12:18:00', 100),
('St-26', 'S-1', 'U-1', '2023-01-01 12:19:00', 100),
('St-27', 'S-1', 'U-1', '2023-01-01 12:20:00', 100),
('St-28', 'S-1', 'U-1', '2023-01-01 12:21:00', 100),
('St-29', 'S-1', 'U-1', '2023-01-01 12:22:00', 100),
('St-30', 'S-1', 'U-1', '2023-01-01 12:23:00', 100),
('St-31', 'S-1', 'U-1', '2023-01-01 12:24:00', 100),
('St-32', 'S-1', 'U-1', '2023-01-01 12:25:00', 100),
('St-33', 'S-1', 'U-1', '2023-01-01 12:26:00', 100),
('St-34', 'S-1', 'U-1', '2023-01-01 12:27:00', 100),
('St-35', 'S-1', 'U-1', '2023-01-01 12:28:00', 100),
('St-36', 'S-1', 'U-1', '2023-01-01 12:29:00', 100),
('St-37', 'S-1', 'U-1', '2023-01-01 12:30:00', 100),
('St-38', 'S-1', 'U-1', '2023-01-01 12:31:00', 100),
('St-39', 'S-1', 'U-1', '2023-01-01 12:32:00', 100),
('St-40', 'S-1', 'U-1', '2023-01-01 12:33:00', 100),
('St-41', 'S-1', 'U-1', '2023-01-01 12:34:00', 100),
('St-42', 'S-1', 'U-1', '2023-01-01 12:35:00', 100),
('St-43', 'S-1', 'U-1', '2023-01-01 12:36:00', 100),
('St-44', 'S-1', 'U-1', '2023-01-01 12:37:00', 100),
('St-45', 'S-1', 'U-1', '2023-01-01 12:38:00', 100),
('St-46', 'S-1', 'U-1', '2023-01-01 12:39:00', 100),
('St-47', 'S-1', 'U-1', '2023-01-01 12:40:00', 100),
('St-48', 'S-1', 'U-1', '2023-01-01 12:41:00', 100),
('St-49', 'S-1', 'U-1', '2023-01-01 12:42:00', 100),
('St-50', 'S-1', 'U-1', '2023-01-01 12:43:00', 100);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Calc royalties in Spotify_Clone.Rights.
 *  
 *  1. Bronze: Raw View of STREAMS, SONGS, ARTISTS.
 *  2. Silver: 
 *     - Join: STREAMS -> SONGS -> ARTISTS.
 *     - Royalty Filter: Play_Duration >= 30s.
 *  3. Gold: 
 *     - Payroll: Sum(Royalty-Eligible Streams) * $0.004 per Artist.
 *  
 *  Show the SQL."
 */
