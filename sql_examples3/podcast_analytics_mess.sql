/*
 * Dremio "Messy Data" Challenge: Podcast Analytics Mess
 * 
 * Scenario: 
 * Podcast hosting platform aggregating data from multiple RSS feeds.
 * 3 Tables: SHOWS, EPISODES, DOWNLOADS.
 * 
 * Objective for AI Agent:
 * 1. Duration Parsing: Normalize mixed formats ("1:23:45" vs "5040s" vs "84 min").
 * 2. Bot Filtering: Detect bot user-agents (Googlebot, Bingbot, crawler).
 * 3. Geo Normalization: Unify country codes ("US" vs "USA" vs "United States").
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS PodNet;
CREATE FOLDER IF NOT EXISTS PodNet.Analytics;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PodNet.Analytics.SHOWS (
    SHOW_ID VARCHAR,
    TITLE VARCHAR,
    HOST_NAME VARCHAR,
    CATEGORY VARCHAR
);

CREATE TABLE IF NOT EXISTS PodNet.Analytics.EPISODES (
    EP_ID VARCHAR,
    SHOW_ID VARCHAR,
    EP_TITLE VARCHAR,
    DURATION_RAW VARCHAR, -- '1:23:45', '5040s', '84 min', '01:30:00'
    PUBLISH_DT DATE
);

CREATE TABLE IF NOT EXISTS PodNet.Analytics.DOWNLOADS (
    DL_ID VARCHAR,
    EP_ID VARCHAR,
    DL_TS TIMESTAMP,
    USER_AGENT VARCHAR,
    COUNTRY_RAW VARCHAR, -- 'US', 'USA', 'United States', 'us'
    IP_HASH VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- SHOWS (5 rows)
INSERT INTO PodNet.Analytics.SHOWS VALUES
('SH-001', 'Data Lakehouse Weekly', 'Alex M', 'Technology'),
('SH-002', 'True Crime Files', 'Jane D', 'True Crime'),
('SH-003', 'Cooking Corner', 'Chef B', 'Food'),
('SH-004', 'Morning News Wrap', 'Team News', 'News'),
('SH-005', 'Mindful Moments', 'Dr. Calm', 'Health');

-- EPISODES (15 rows - varied duration formats)
INSERT INTO PodNet.Analytics.EPISODES VALUES
('EP-001', 'SH-001', 'What is Apache Iceberg?', '1:02:30', '2023-01-05'),    -- HH:MM:SS
('EP-002', 'SH-001', 'Medallion Architecture Deep Dive', '3750s', '2023-01-12'), -- Seconds
('EP-003', 'SH-001', 'Dremio vs Spark', '45 min', '2023-01-19'),             -- Minutes text
('EP-004', 'SH-002', 'The Missing Heiress', '01:45:00', '2023-01-03'),
('EP-005', 'SH-002', 'Cold Case Reopened', '5400s', '2023-01-10'),
('EP-006', 'SH-002', 'Forensic Evidence', '90 min', '2023-01-17'),
('EP-007', 'SH-003', 'Perfect Pasta', '30:00', '2023-01-02'),                -- MM:SS (ambiguous!)
('EP-008', 'SH-003', 'Sourdough Secrets', '1800', '2023-01-09'),             -- Just a number (seconds?)
('EP-009', 'SH-004', 'Jan 2 Headlines', '15 min', '2023-01-02'),
('EP-010', 'SH-004', 'Jan 9 Headlines', '900s', '2023-01-09'),
('EP-011', 'SH-005', 'Breathing Techniques', '0:20:00', '2023-01-06'),       -- Leading zero hour
('EP-012', 'SH-005', 'Sleep Hygiene', '25m', '2023-01-13'),                  -- Abbrev
('EP-013', 'SH-001', 'Bonus: Q&A', '', '2023-01-26'),                        -- Empty string
('EP-014', 'SH-002', 'Listener Mailbag', NULL, '2023-01-24'),                -- NULL
('EP-015', 'SH-003', 'Holiday Special', '-1:00:00', '2023-01-31');           -- Negative?

-- DOWNLOADS (50+ rows)
INSERT INTO PodNet.Analytics.DOWNLOADS VALUES
-- Real users
('DL-001', 'EP-001', '2023-01-05 09:00:00', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0)', 'US', 'h1a2b3'),
('DL-002', 'EP-001', '2023-01-05 09:15:00', 'Mozilla/5.0 (Macintosh; Intel Mac OS X)', 'USA', 'h4d5e6'),
('DL-003', 'EP-001', '2023-01-05 10:00:00', 'Spotify/8.7', 'United States', 'h7g8h9'),
('DL-004', 'EP-001', '2023-01-05 12:00:00', 'AppleCoreMedia/1.0', 'us', 'hj1k2l'),
('DL-005', 'EP-002', '2023-01-12 08:30:00', 'Mozilla/5.0 (Windows NT 10.0)', 'GB', 'hm3n4o'),
('DL-006', 'EP-002', '2023-01-12 09:00:00', 'Overcast/3.0', 'GBR', 'hp5q6r'),
('DL-007', 'EP-002', '2023-01-12 09:30:00', 'PocketCasts/7.0', 'United Kingdom', 'hs7t8u'),
('DL-008', 'EP-003', '2023-01-19 07:00:00', 'Mozilla/5.0 (Linux; Android 13)', 'DE', 'hv9w0x'),
('DL-009', 'EP-003', '2023-01-19 07:30:00', 'CastBox/8.0', 'DEU', 'hy1z2a'),
('DL-010', 'EP-003', '2023-01-19 08:00:00', 'Google-Podcast/1.0', 'Germany', 'hb3c4d'),
-- Bots (should be filtered)
('DL-011', 'EP-001', '2023-01-05 03:00:00', 'Googlebot/2.1 (+http://www.google.com/bot.html)', 'US', 'BOT01'),
('DL-012', 'EP-001', '2023-01-05 03:01:00', 'Bingbot/2.0', 'US', 'BOT02'),
('DL-013', 'EP-002', '2023-01-12 02:00:00', 'PodcastCrawler/1.0', 'US', 'BOT03'),
('DL-014', 'EP-003', '2023-01-19 01:00:00', 'AhrefsBot/7.0', 'US', 'BOT04'),
('DL-015', 'EP-004', '2023-01-03 04:00:00', 'curl/7.68.0', '', 'BOT05'),       -- Empty country
('DL-016', 'EP-005', '2023-01-10 04:00:00', 'python-requests/2.28', NULL, 'BOT06'), -- NULL country
-- More real users
('DL-017', 'EP-004', '2023-01-03 10:00:00', 'Mozilla/5.0 (iPad)', 'CA', 'he5f6g'),
('DL-018', 'EP-004', '2023-01-03 11:00:00', 'Spotify/8.7', 'CAN', 'hh7i8j'),
('DL-019', 'EP-004', '2023-01-03 12:00:00', 'AppleCoreMedia/1.0', 'Canada', 'hk9l0m'),
('DL-020', 'EP-005', '2023-01-10 10:00:00', 'Mozilla/5.0', 'FR', 'hn1o2p'),
('DL-021', 'EP-005', '2023-01-10 10:30:00', 'Overcast/3.0', 'FRA', 'hq3r4s'),
('DL-022', 'EP-005', '2023-01-10 11:00:00', 'PocketCasts/7.0', 'France', 'ht5u6v'),
('DL-023', 'EP-006', '2023-01-17 09:00:00', 'Spotify/8.7', 'AU', 'hw7x8y'),
('DL-024', 'EP-006', '2023-01-17 09:30:00', 'CastBox/8.0', 'AUS', 'hz9a0b'),
('DL-025', 'EP-006', '2023-01-17 10:00:00', 'Mozilla/5.0', 'Australia', 'hc1d2e'),
('DL-026', 'EP-007', '2023-01-02 12:00:00', 'Spotify/8.7', 'JP', 'hf3g4h'),
('DL-027', 'EP-007', '2023-01-02 12:30:00', 'AppleCoreMedia/1.0', 'JPN', 'hi5j6k'),
('DL-028', 'EP-007', '2023-01-02 13:00:00', 'Mozilla/5.0', 'Japan', 'hl7m8n'),
('DL-029', 'EP-008', '2023-01-09 09:00:00', 'Spotify/8.7', 'BR', 'ho9p0q'),
('DL-030', 'EP-008', '2023-01-09 09:30:00', 'PocketCasts/7.0', 'BRA', 'hr1s2t'),
('DL-031', 'EP-009', '2023-01-02 06:00:00', 'Mozilla/5.0', 'us', 'hu3v4w'),
('DL-032', 'EP-009', '2023-01-02 06:30:00', 'Overcast/3.0', 'Us', 'hx5y6z'),
('DL-033', 'EP-010', '2023-01-09 06:15:00', 'Spotify/8.7', 'IN', 'ha7b8c'),
('DL-034', 'EP-010', '2023-01-09 06:45:00', 'Mozilla/5.0', 'IND', 'hd9e0f'),
('DL-035', 'EP-010', '2023-01-09 07:00:00', 'CastBox/8.0', 'India', 'hg1h2i'),
('DL-036', 'EP-011', '2023-01-06 08:00:00', 'AppleCoreMedia/1.0', 'MX', 'hj3k4l'),
('DL-037', 'EP-011', '2023-01-06 08:30:00', 'Spotify/8.7', 'MEX', 'hm5n6o'),
('DL-038', 'EP-012', '2023-01-13 08:00:00', 'Mozilla/5.0', 'US', 'hp7q8r'),
('DL-039', 'EP-012', '2023-01-13 08:30:00', 'Spotify/8.7', 'US', 'hs9t0u'),
-- Orphan episode
('DL-040', 'EP-999', '2023-01-15 10:00:00', 'Mozilla/5.0', 'US', 'hv1w2x'),
-- Duplicates
('DL-041', 'EP-001', '2023-01-05 09:00:00', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0)', 'US', 'h1a2b3'),
('DL-041', 'EP-001', '2023-01-05 09:00:00', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0)', 'US', 'h1a2b3'),
-- Weird country values
('DL-042', 'EP-013', '2023-01-26 10:00:00', 'Spotify/8.7', 'XX', 'hy3z4a'),    -- Invalid
('DL-043', 'EP-013', '2023-01-26 10:30:00', 'Mozilla/5.0', '??', 'hb5c6d'),    -- Unknown
('DL-044', 'EP-014', '2023-01-24 09:00:00', 'Overcast/3.0', 'N/A', 'he7f8g'),
('DL-045', 'EP-014', '2023-01-24 09:30:00', 'PocketCasts/7.0', 'ZZ', 'hi9j0k'),
('DL-046', 'EP-001', '2023-01-06 08:00:00', 'Spotify/8.7', 'US', 'hl1m2n'),
('DL-047', 'EP-002', '2023-01-13 08:00:00', 'Mozilla/5.0', 'GB', 'ho3p4q'),
('DL-048', 'EP-003', '2023-01-20 08:00:00', 'CastBox/8.0', 'DE', 'hr5s6t'),
('DL-049', 'EP-004', '2023-01-04 08:00:00', 'AppleCoreMedia/1.0', 'CA', 'hu7v8w'),
('DL-050', 'EP-005', '2023-01-11 08:00:00', 'Spotify/8.7', 'FR', 'hx9y0z');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze podcast metrics in PodNet.Analytics.
 *  
 *  1. Bronze: Raw Views of SHOWS, EPISODES, DOWNLOADS.
 *  2. Silver: 
 *     - Parse Duration: Convert DURATION_RAW to total seconds (handle HH:MM:SS, Xs, X min).
 *     - Filter Bots: Exclude DOWNLOADS where USER_AGENT contains 'bot', 'crawler', 'curl', 'python'.
 *     - Normalize Country: Map ISO-3 and full names to ISO-2 codes (e.g., 'USA' -> 'US').
 *  3. Gold: 
 *     - Downloads per Episode (excluding bots).
 *     - Top Countries by total downloads.
 *     - Average Episode Duration per Show.
 *  
 *  Show the SQL."
 */
