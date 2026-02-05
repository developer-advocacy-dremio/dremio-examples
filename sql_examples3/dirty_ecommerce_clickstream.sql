/*
 * Dremio "Messy Data" Challenge: Dirty E-commerce Clickstream
 * 
 * Scenario: 
 * Web logs containing User User-Agents (UA).
 * Contains "Bot" traffic (Googlebot, Bingbot) mixed with real users.
 * Session IDs are sometimes broken (restarts mid-session).
 * URL Parameters are embedded in the 'PAGE_URL' string.
 * 
 * Objective for AI Agent:
 * 1. Parse User Agent to classify 'Bot' vs 'Human'.
 * 2. Extract UTM Parameters (source, medium) from PAGE_URL.
 * 3. Filter out Bots from the Gold Layer metrics.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Clickstream_Chaos;
CREATE FOLDER IF NOT EXISTS Clickstream_Chaos.Web_Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Clickstream_Chaos.Web_Logs.HITS (
    HIT_ID VARCHAR,
    SESS_ID VARCHAR,
    TS TIMESTAMP,
    PAGE_URL VARCHAR,
    USER_AGENT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Bots, URL Params)
-------------------------------------------------------------------------------

-- Humans
INSERT INTO Clickstream_Chaos.Web_Logs.HITS VALUES
('H-001', 'S-101', '2023-01-01 10:00:00', 'https://shop.com/home', 'Mozilla/5.0 (Macintosh; Intel...)'),
('H-002', 'S-101', '2023-01-01 10:01:00', 'https://shop.com/prod?id=123&utm_source=google', 'Mozilla/5.0 (Macintosh; Intel...)');

-- Bots
INSERT INTO Clickstream_Chaos.Web_Logs.HITS VALUES
('H-003', 'S-999', '2023-01-01 10:02:00', 'https://shop.com/home', 'Googlebot/2.1 (+http://www.google.com/bot.html)'),
('H-004', 'S-998', '2023-01-01 10:03:00', 'https://shop.com/robots.txt', 'Bingbot/2.0'),
('H-005', 'S-997', '2023-01-01 10:04:00', 'https://shop.com/home', 'curl/7.64.1'); -- Scraper?

-- Messy URLs
INSERT INTO Clickstream_Chaos.Web_Logs.HITS VALUES
('H-006', 'S-102', '2023-01-01 11:00:00', '/prod/shoes?size=10&utm_medium=email', 'Mozilla/5.0 (iPhone...)'),
('H-007', 'S-102', '2023-01-01 11:05:00', '/cart', 'Mozilla/5.0 (iPhone...)');

-- Bulk Fill
INSERT INTO Clickstream_Chaos.Web_Logs.HITS VALUES
('H-010', 'S-103', '2023-01-01 12:00:00', '/home', 'Mozilla/5.0 (Windows NT 10.0...)'),
('H-011', 'S-103', '2023-01-01 12:01:00', '/search?q=laptop', 'Mozilla/5.0 (Windows NT 10.0...)'),
('H-012', 'S-103', '2023-01-01 12:02:00', '/prod?id=555', 'Mozilla/5.0 (Windows NT 10.0...)'),
('H-013', 'S-103', '2023-01-01 12:10:00', '/cart', 'Mozilla/5.0 (Windows NT 10.0...)'),
('H-014', 'S-104', '2023-01-01 12:15:00', '/home', 'Mozilla/5.0 (Linux; Android...)'),
('H-015', 'S-104', '2023-01-01 12:16:00', '/prod?id=999&utm_source=facebook', 'Mozilla/5.0 (Linux; Android...)'),
('H-016', 'S-888', '2023-01-01 13:00:00', '/admin', 'Python-urllib/3.8'), -- Script
('H-017', 'S-888', '2023-01-01 13:00:01', '/admin/login', 'Python-urllib/3.8'),
('H-018', 'S-105', '2023-01-01 13:05:00', '/home', 'Mozilla/5.0 (iPad...)'),
('H-019', 'S-105', '2023-01-01 13:06:00', '/prod?id=12', 'Mozilla/5.0 (iPad...)'),
('H-020', 'S-105', '2023-01-01 13:07:00', '/prod?id=13', 'Mozilla/5.0 (iPad...)'),
('H-021', 'S-105', '2023-01-01 13:08:00', '/prod?id=14', 'Mozilla/5.0 (iPad...)'),
('H-022', 'S-105', '2023-01-01 13:09:00', '/prod?id=15', 'Mozilla/5.0 (iPad...)'),
('H-023', 'S-777', '2023-01-01 14:00:00', '/sitemap.xml', 'Baiduspider'),
('H-024', 'S-777', '2023-01-01 14:00:01', '/home', 'Baiduspider'),
('H-025', 'S-777', '2023-01-01 14:00:02', '/prod/1', 'Baiduspider'),
('H-026', 'S-106', '2023-01-01 14:10:00', '/home', 'Mozilla/5.0 (X11; Ubuntu...)'),
('H-027', 'S-106', '2023-01-01 14:11:00', '/blog?utm_medium=social', 'Mozilla/5.0 (X11; Ubuntu...)'),
('H-028', 'S-106', '2023-01-01 14:12:00', '/blog/post-1', 'Mozilla/5.0 (X11; Ubuntu...)'),
('H-029', 'S-106', '2023-01-01 14:13:00', '/blog/post-2', 'Mozilla/5.0 (X11; Ubuntu...)'),
('H-030', 'S-106', '2023-01-01 14:14:00', '/prod?id=88', 'Mozilla/5.0 (X11; Ubuntu...)'),
('H-031', 'S-107', '2023-01-01 15:00:00', '/home', 'Opera/9.80'),
('H-032', 'S-107', '2023-01-01 15:05:00', '/cart', 'Opera/9.80'),
('H-033', 'S-107', '2023-01-01 15:10:00', '/checkout', 'Opera/9.80'), -- Abandoned?
('H-034', 'S-666', '2023-01-01 16:00:00', '/login', 'sqlmap/1.4'), -- Malicious scanner
('H-035', 'S-666', '2023-01-01 16:00:01', '/login?user=admin', 'sqlmap/1.4'),
('H-036', 'S-108', '2023-01-01 16:10:00', '/home', 'Mozilla/5.0 (compatible; MSIE 10.0...)'),
('H-037', 'S-108', '2023-01-01 16:11:00', '/prod?id=1&utm_campaign=winter_sale', 'Mozilla/5.0 (compatible; MSIE 10.0...)'),
('H-038', 'S-109', '2023-01-01 16:20:00', '/home', 'Mozilla/5.0 (Mobile; rv:40.0)'),
('H-039', 'S-109', '2023-01-01 16:21:00', '/prod?id=3', 'Mozilla/5.0 (Mobile; rv:40.0)'),
('H-040', 'S-109', '2023-01-01 16:22:00', '/cart', 'Mozilla/5.0 (Mobile; rv:40.0)'),
('H-041', 'S-555', '2023-01-01 17:00:00', '/rss', 'FeedFetcher-Google'),
('H-042', 'S-110', '2023-01-01 17:10:00', '/home', 'Mozilla/5.0 (Windows NT 6.1)'),
('H-043', 'S-110', '2023-01-01 17:11:00', '/search', 'Mozilla/5.0 (Windows NT 6.1)'),
('H-044', 'S-110', '2023-01-01 17:12:00', '/search?q=gift', 'Mozilla/5.0 (Windows NT 6.1)'),
('H-045', 'S-110', '2023-01-01 17:13:00', '/prod?id=77', 'Mozilla/5.0 (Windows NT 6.1)'),
('H-046', 'S-110', '2023-01-01 17:14:00', '/cart', 'Mozilla/5.0 (Windows NT 6.1)'),
('H-047', 'S-110', '2023-01-01 17:15:00', '/checkout', 'Mozilla/5.0 (Windows NT 6.1)'),
('H-048', 'S-110', '2023-01-01 17:16:00', '/confirm', 'Mozilla/5.0 (Windows NT 6.1)'), -- Conversion
('H-049', 'S-111', '2023-01-01 18:00:00', '/home', 'Safari/605.1.15'),
('H-050', 'S-111', '2023-01-01 18:01:00', '/about', 'Safari/605.1.15');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze the web logs in Clickstream_Chaos.Web_Logs.HITS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Flag 'Bots': If USER_AGENT contains 'bot', 'spider', 'curl', 'sqlmap'.
 *     - Extract UTM: Parse 'PAGE_URL' to extract 'utm_source' and 'utm_medium'.
 *  3. Gold: 
 *     - Calculate 'Conversion Rate' (Hits to '/confirm') for Human traffic only.
 *     - List Top 5 UTM Sources excluding bots.
 *  
 *  Show me the SQL."
 */
