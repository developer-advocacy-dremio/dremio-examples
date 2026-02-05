/*
 * Dremio "Messy Data" Challenge: Multilingual Content Moderation
 * 
 * Scenario: 
 * User comments in multiple languages.
 * Character encoding issues (Mojibake) e.g., 'Ã©' instead of 'é'.
 * Mixed character sets (Latin, Cyrillic, CJK) in same column.
 * 3 Tables: USERS, COMMENTS, FLAGGED_TERMS
 * 
 * Objective for AI Agent:
 * 1. Clean Text: Fix encoding issues in COMMENTS.TXT_BODY.
 * 2. Join Context: Link COMMENTS to USERS to find 'Trusted' vs 'New' users.
 * 3. Flag Violations: Join FLAGGED_TERMS to detect banned words (fuzzy match).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Global_Social;
CREATE FOLDER IF NOT EXISTS Global_Social.Mod_Queue;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Global_Social.Mod_Queue.USERS (
    USER_ID VARCHAR,
    USERNAME VARCHAR,
    ACCOUNT_AGE_DAYS INT,
    TRUST_SCORE DOUBLE
);

CREATE TABLE IF NOT EXISTS Global_Social.Mod_Queue.COMMENTS (
    COMMENT_ID VARCHAR,
    USER_ID VARCHAR,
    TEXT_BODY VARCHAR,
    LANG_TAG VARCHAR, -- Often wrong or null
    POST_TS TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Global_Social.Mod_Queue.FLAGGED_TERMS (
    TERM_ID VARCHAR,
    TERM_TEXT VARCHAR,
    SEVERITY VARCHAR -- 'Low', 'High'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- USERS (20 rows)
INSERT INTO Global_Social.Mod_Queue.USERS VALUES
('U-1', 'Alice', 365, 0.9), ('U-2', 'Bob', 10, 0.5), ('U-3', 'Charlie', 1, 0.1), ('U-4', 'Dave', 100, 0.8), ('U-5', 'Eve', 50, 0.4),
('U-6', 'Frank', 200, 0.9), ('U-7', 'Grace', 30, 0.6), ('U-8', 'Heidi', 5, 0.2), ('U-9', 'Ivan', 400, 0.95), ('U-10', 'Judy', 150, 0.7),
('U-11', 'Kevin', 20, 0.5), ('U-12', 'Leo', 80, 0.7), ('U-13', 'Mallory', 2, 0.0), ('U-14', 'Niaj', 300, 0.8), ('U-15', 'Olivia', 60, 0.6),
('U-16', 'Peggy', 90, 0.7), ('U-17', 'Quentin', 10, 0.4), ('U-18', 'Rupert', 40, 0.5), ('U-19', 'Sybil', 3, 0.1), ('U-20', 'Trent', 500, 0.99);

-- FLAGGED TERMS (10 rows)
INSERT INTO Global_Social.Mod_Queue.FLAGGED_TERMS VALUES
('T-1', 'Spam', 'Low'), ('T-2', 'Scam', 'High'), ('T-3', 'Buy Now', 'Low'), ('T-4', 'Click Here', 'Low'), ('T-5', 'Hate', 'High'),
('T-6', 'Violence', 'High'), ('T-7', 'Casino', 'Low'), ('T-8', 'Lottery', 'Low'), ('T-9', 'Free Money', 'High'), ('T-10', 'Viagra', 'Low');

-- COMMENTS (50+ rows with Mojibake)
INSERT INTO Global_Social.Mod_Queue.COMMENTS VALUES
-- Latin-1 Mojibake
('C-100', 'U-1', 'This is fiancÃ©e', 'en', '2023-01-01 10:00:00'), -- fiancée
('C-101', 'U-2', 'SchÃ¶n', 'de', '2023-01-01 10:05:00'), -- Schön
('C-102', 'U-3', 'CafÃ©', 'fr', '2023-01-01 10:10:00'),
('C-103', 'U-4', 'NnaÃ¯ve', 'en', '2023-01-01 10:15:00'),
('C-104', 'U-5', 'CoÃ¶perate', 'en', '2023-01-01 10:20:00'),
('C-105', 'U-6', 'FaÃ§ade', 'fr', '2023-01-01 10:25:00'),
('C-106', 'U-7', 'JalapeÃ±o', 'es', '2023-01-01 10:30:00'),
('C-107', 'U-8', 'Ãœber', 'de', '2023-01-01 10:35:00'),
('C-108', 'U-9', 'EspaÃ±ol', 'es', '2023-01-01 10:40:00'),
('C-109', 'U-10', 'VÃ©ge', 'hu', '2023-01-01 10:45:00'),
('C-110', 'U-11', 'TÃ©lÃ©phone', 'fr', '2023-01-01 10:50:00'),
('C-111', 'U-12', 'DÃ©jÃ  vu', 'fr', '2023-01-01 10:55:00'),
('C-112', 'U-13', 'CrÃ¨me brÃ»lÃ©e', 'fr', '2023-01-01 11:00:00'),
('C-113', 'U-14', 'PiÃ±ata', 'es', '2023-01-01 11:05:00'),
('C-114', 'U-15', 'CaÃ±on', 'es', '2023-01-01 11:10:00'),
('C-115', 'U-16', 'MaÃ±ana', 'es', '2023-01-01 11:15:00'),
('C-116', 'U-17', 'SeÃ±or', 'es', '2023-01-01 11:20:00'),
('C-117', 'U-18', 'FÃ¼hrer', 'de', '2023-01-01 11:25:00'),
('C-118', 'U-19', 'StraÃŸe', 'de', '2023-01-01 11:30:00'),
('C-119', 'U-20', 'Curriculum Vitae', 'en', '2023-01-01 11:35:00'),
-- Mixed Scripts & Spam
('C-120', 'U-1', 'Hello 世界', 'en', '2023-01-01 12:00:00'),
('C-121', 'U-2', 'Привет world', 'ru', '2023-01-01 12:05:00'),
('C-122', 'U-3', 'Click Here for Free Money', 'en', '2023-01-01 12:10:00'), -- Flagged
('C-123', 'U-13', 'Buy Now Viagra', 'en', '2023-01-01 12:15:00'), -- Flagged
('C-124', 'U-4', 'Hidden\x00Null', 'en', '2023-01-01 12:20:00'),
('C-125', 'U-5', '   ', 'space', '2023-01-01 12:25:00'),
('C-126', 'U-6', '????', 'ja', '2023-01-01 12:30:00'),
('C-127', 'U-7', 'Zàijiàn', 'zh', '2023-01-01 12:35:00'),
('C-128', 'U-8', 'Goodbye', NULL, '2023-01-01 12:40:00'),
('C-129', 'U-9', 'Au Revoir', NULL, '2023-01-01 12:45:00'),
('C-130', 'U-10', 'Auf Wiedersehen', NULL, '2023-01-01 12:50:00'),
('C-131', 'U-11', 'Sayonara', 'en', '2023-01-01 12:55:00'),
('C-132', 'U-12', 'Adios', 'fr', '2023-01-01 13:00:00'),
('C-133', 'U-13', 'Ciao', 'de', '2023-01-01 13:05:00'),
('C-134', 'U-14', 'Aloha', '??', '2023-01-01 13:10:00'),
('C-135', 'U-15', '12345', 'num', '2023-01-01 13:15:00'),
('C-136', 'U-16', '!@#$%', 'sym', '2023-01-01 13:20:00'),
('C-137', 'U-17', '', 'empty', '2023-01-01 13:25:00'),
('C-138', 'U-18', 'Start', 'en', '2023-01-01 13:30:00'),
('C-139', 'U-18', 'Start', 'en', '2023-01-01 13:30:00'), -- Dupe
('C-140', 'U-19', 'The End', 'en', '2023-01-01 13:35:00'),
('C-141', 'U-1', 'Fin', 'fr', '2023-01-01 13:40:00'),
('C-142', 'U-2', 'Slut', 'sv', '2023-01-01 13:45:00'),
('C-143', 'U-3', 'Ende', 'de', '2023-01-01 13:50:00'),
('C-144', 'U-4', 'Fine', 'it', '2023-01-01 13:55:00'),
('C-145', 'U-5', 'Конец', 'ru', '2023-01-01 14:00:00'),
('C-146', 'U-6', 'Konec', 'cs', '2023-01-01 14:05:00'),
('C-147', 'U-7', 'Loppu', 'fi', '2023-01-01 14:10:00'),
('C-148', 'U-8', 'Vége', 'hu', '2023-01-01 14:15:00'),
('C-149', 'U-9', 'Scam update', 'en', '2023-01-01 14:20:00'),
('C-150', 'U-10', 'Lottery winner', 'en', '2023-01-01 14:25:00');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Clean the content moderation pipeline in Global_Social.Mod_Queue.
 *  
 *  1. Bronze: Raw View of COMMENTS, USERS, and FLAGGED_TERMS.
 *  2. Silver: 
 *     - Clean Comments: Fix Mojibake ('Ã©' -> 'é') in TEXT_BODY.
 *     - Join Users: Add TRUST_SCORE from USERS table.
 *     - Flag Terms: Left Join FLAGGED_TERMS where TEXT_BODY contains TERM_TEXT.
 *  3. Gold: 
 *     - High Risk Queue: Filter where TRUST_SCORE < 0.2 OR Severity = 'High'.
 *  
 *  Show the SQL."
 */
