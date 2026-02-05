/*
 * Dremio "Messy Data" Challenge: Noisy Social Media Comments
 * 
 * Scenario: 
 * User comments from a social platform.
 * Contains Emojis (non-ASCII), Spam bots (Repeated text), and URL spam.
 * Timestamp format varies (Relative '2h ago' vs Absolute ISO).
 * 
 * Objective for AI Agent:
 * 1. Filter out comments containing 'http' (potential spam).
 * 2. Detect and flag comments with > 50% non-ASCII characters (Emoji overload).
 * 3. Convert relative timestamps ('2 hours ago') to estimated absolute timestamps.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Social_Firehose;
CREATE FOLDER IF NOT EXISTS Social_Firehose.Comments;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Social_Firehose.Comments.STREAM_V1 (
    MSG_ID VARCHAR,
    USER_NAME VARCHAR,
    BODY_TEXT VARCHAR,
    POSTED_AT VARCHAR -- Mixed formats
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Spam, Emojis)
-------------------------------------------------------------------------------

-- Real Comments
INSERT INTO Social_Firehose.Comments.STREAM_V1 VALUES
('M-001', 'Alice', 'This product is great!', '2023-01-01 10:00:00'),
('M-002', 'Bob', 'I disagree with the review.', '2023-01-01 10:05:00');

-- Spam
INSERT INTO Social_Firehose.Comments.STREAM_V1 VALUES
('M-003', 'SpamBot99', 'CLICK HERE for FREE IPHONE: http://bit.ly/spam', 'Just now'),
('M-004', 'CryptoKing', 'Earn 1000% returns http://scam.com', '5 mins ago');

-- Emoji Overload
INSERT INTO Social_Firehose.Comments.STREAM_V1 VALUES
('M-005', 'Teenager123', '🔥🔥🔥🔥💯💯💯😂😂', '1 hr ago'), -- Pure Emoji
('M-006', 'FanGirl', 'Love this! ❤️❤️❤️', '2 hrs ago');

-- Bulk Fill
INSERT INTO Social_Firehose.Comments.STREAM_V1 VALUES
('M-010', 'User1', 'Good', '2023-01-01 12:00:00'),
('M-011', 'User2', 'Bad', '2023-01-01 12:01:00'),
('M-012', 'User3', 'Okay', '2023-01-01 12:02:00'),
('M-013', 'Bot1', 'http://malware.site', '10s ago'),
('M-014', 'Bot2', 'http://phishing.site', '20s ago'),
('M-015', 'Bot3', 'Visit http://mysite.com', '30s ago'),
('M-016', 'Emoji1', '😀', '1 day ago'),
('M-017', 'Emoji2', '😃', '2 days ago'),
('M-018', 'Emoji3', '😄', '3 days ago'),
('M-019', 'Mixed1', 'Hello World', '2023-01-02 10:00'),
('M-020', 'Mixed2', 'Testing', '2023-01-02T10:00:00Z'),
('M-021', 'Relative1', '1 min ago', 'Now'), -- 'Now' needs mapped to Current Time
('M-022', 'Relative2', '5 mins ago', 'Now'),
('M-023', 'Relative3', '10 mins ago', 'Now'),
('M-024', 'User4', 'Did you see this?', '2023-01-03 10:00:00'),
('M-025', 'User5', 'Yes I did', '2023-01-03 10:05:00'),
('M-026', 'Spammer1', 'Buy meds http://rx.com', 'Yesterday'),
('M-027', 'Spammer2', 'Cheap Rolex http://watch.com', 'Yesterday'),
('M-028', 'Spammer3', 'Dating site http://love.com', 'Yesterday'),
('M-029', 'Angry1', 'I HATE THIS 😡😡😡', '2023-01-04 09:00:00'),
('M-030', 'Happy1', 'BEST DAY EVER 🥳🥳🥳', '2023-01-04 10:00:00'),
('M-031', 'Confused1', 'What? 🤯', '2023-01-04 11:00:00'),
('M-032', 'Sad1', 'Oh no 😢', '2023-01-04 12:00:00'),
('M-033', 'User6', 'Check this out', '2023-01-05 10:00:00'),
('M-034', 'User6', 'Check this out', '2023-01-05 10:00:00'), -- Duplicate
('M-035', 'User6', 'Check this out', '2023-01-05 10:00:00'), -- Triplicate
('M-036', 'NullUser', NULL, '2023-01-05 11:00:00'), -- Null body
('M-037', 'NullTime', 'Message', NULL), -- Null time
('M-038', 'Bot4', 'http://spam.org', '1 week ago'),
('M-039', 'Bot5', 'http://spam.net', '1 week ago'),
('M-040', 'Bot6', 'http://spam.co', '1 week ago'),
('M-041', 'User7', 'Can anyone help?', '2023-01-06 10:00:00'),
('M-042', 'User8', 'Sure', '2023-01-06 10:01:00'),
('M-043', 'User9', 'Thanks', '2023-01-06 10:02:00'),
('M-044', 'User10', 'No problem', '2023-01-06 10:03:00'),
('M-045', 'User11', 'Great', '2023-01-06 10:04:00'),
('M-046', 'EmojiUser', '⚽️🏀🏈⚾️🥎', '2023-01-07 10:00:00'),
('M-047', 'EmojiUser', '🏐🏉🥏🎱🪀', '2023-01-07 10:05:00'),
('M-048', 'EmojiUser', '🏓🏸🏒🏑🥍', '2023-01-07 10:10:00'),
('M-049', 'EmojiUser', '🏏🥅⛳️🪁🏹', '2023-01-07 10:15:00'),
('M-050', 'EmojiUser', '🎣🤿🥊🥋🎽', '2023-01-07 10:20:00');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Moderate the Social_Firehose.Comments.STREAM_V1 table.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Flag 'Is_Spam' if BODY_TEXT contains 'http' or 'www'.
 *     - Standardize 'POSTED_AT':
 *       - If it contains 'ago' (relative), subtract duration from CURRENT_TIMESTAMP().
 *       - Otherwise cast to TIMESTAMP.
 *  3. Gold: 
 *     - Count Non-Spam comments per hour.
 *  
 *  Generate the SQL."
 */
