/*
 * Dremio "Messy Data" Challenge: Mismatched HR Records
 * 
 * Scenario: 
 * HR data from a legacy acquisition (SmallCorp) needs merged with BigCorp.
 * Employee Names vary: 'Bob Smith' vs 'Robert J. Smith'.
 * Emails are the best bet, but some are missing or formatted differently.
 * 
 * Objective for AI Agent:
 * 1. Normalize Names (Capitalization, trim whitespace).
 * 2. Join tables using Email (Lowercase, trim) as primary key.
 * 3. Use Fuzzy Logic (Soundex/Levenshtein similarity) to match remaining records by Name.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HR_Merger;
CREATE FOLDER IF NOT EXISTS HR_Merger.Legacy_Payload;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS HR_Merger.Legacy_Payload.BIG_CORP_EMP (
    EMP_ID VARCHAR,
    FULL_NAME VARCHAR,
    EMAIL_ADDR VARCHAR,
    DEPT VARCHAR
);

CREATE TABLE IF NOT EXISTS HR_Merger.Legacy_Payload.ACQUIRED_STAFF (
    OLD_ID VARCHAR,
    NAME_RAW VARCHAR,
    WORK_EMAIL VARCHAR,
    ROLE VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Fuzzy Matches)
-------------------------------------------------------------------------------

-- BigCorp (Clean)
INSERT INTO HR_Merger.Legacy_Payload.BIG_CORP_EMP VALUES
('E-001', 'Robert Smith', 'robert.smith@bigcorp.com', 'Sales'),
('E-002', 'Jennifer Doe', 'jennifer.doe@bigcorp.com', 'Engineering'),
('E-003', 'Michael Brown', 'michael.brown@bigcorp.com', 'HR');

-- SmallCorp (Messy)
INSERT INTO HR_Merger.Legacy_Payload.ACQUIRED_STAFF VALUES
('SC-101', 'Bob Smith', 'bob.smith@smallcorp.com', 'Sales Rep'), -- Name Variant
('SC-102', 'Jen Doe', 'jennifer.doe@bigcorp.com', 'Eng Lead'), -- Email Match
('SC-103', 'Mike Brown', 'mike@smallcorp.com', 'Recruiter'); -- Nickname

-- Typos / Case Separation
INSERT INTO HR_Merger.Legacy_Payload.ACQUIRED_STAFF VALUES
('SC-104', 'SARAH CONNOR', 'sarah.connor@smallcorp.com', 'Director'), -- Uppercase
('SC-105', '  John   Connor ', 'john.connor@smallcorp.com', 'Intern'); -- Whitespace

-- Matching BigCorp counterparts for fuzzy testing
INSERT INTO HR_Merger.Legacy_Payload.BIG_CORP_EMP VALUES
('E-004', 'Sarah Connor', 'sarah.connor@bigcorp.com', 'Ops'),
('E-005', 'John Connor', 'john.connor@bigcorp.com', 'Ops');

-- Bulk Fill
INSERT INTO HR_Merger.Legacy_Payload.ACQUIRED_STAFF VALUES
('SC-106', 'Alice Wonderland', 'alice@smallcorp.com', 'Manager'),
('SC-107', 'Bob Buildr', 'bob.buildr@smallcorp.com', 'Construction'), -- Typo
('SC-108', 'Cat N. Hat', 'cat@smallcorp.com', 'Entertainment'),
('SC-109', 'Darth Vader', 'vader@empire.com', 'Security'),
('SC-110', 'Luke Skywalker', 'luke@rebellion.org', 'Pilot'),
('SC-111', 'Han Solo', 'han@falcon.ship', 'Transport'),
('SC-112', 'Chewbacca', 'chewie@falcon.ship', 'Mechanic'),
('SC-113', 'Leia Organa', 'leia@alderaan.gov', 'Diplomat'),
('SC-114', 'Yoda', 'yoda@dagobah.swamp', 'Mentor'),
('SC-115', 'Obi-Wan Kenobi', 'ben@tatooine.desert', 'Consultant'),
('SC-116', 'R2-D2', 'r2@droid.net', 'IT'),
('SC-117', 'C-3PO', '3po@droid.net', 'Translator'),
('SC-118', 'Lando Calrissian', 'lando@cloudcity.gov', 'Admin'),
('SC-119', 'Boba Fett', 'boba@hunters.guild', 'Freelance'),
('SC-120', 'Jabba Hutt', 'jabba@nalhutta.org', 'Finance'),
('SC-121', 'Emperor Palpatine', 'sheev@senate.gov', 'CEO'),
('SC-122', 'Mace Windu', 'mace@jedi.order', 'Legal'),
('SC-123', 'Qui-Gon Jinn', 'quigon@jedi.order', 'Field Ops'),
('SC-124', 'Padme Amidala', 'padme@naboo.gov', 'PR'),
('SC-125', 'Jar Jar Binks', 'jarjar@gungan.city', 'Representative');

-- BigCorp counterparts for matching
INSERT INTO HR_Merger.Legacy_Payload.BIG_CORP_EMP VALUES
('E-006', 'Alice In Wonderland', 'alice.wonderland@bigcorp.com', 'Management'),
('E-007', 'Robert Builder', 'bob.builder@bigcorp.com', 'Facilities'),
('E-008', 'C. Hat', 'cat.hat@bigcorp.com', 'Events'),
('E-009', 'Anakin Skywalker', 'vader@bigcorp.com', 'Security'), -- Alias
('E-010', 'Luke Skywalker', 'luke.skywalker@bigcorp.com', 'Logistics'),
('E-011', 'Han Solo', 'han.solo@bigcorp.com', 'Logistics'),
('E-012', 'Chewbacca', 'chewbacca@bigcorp.com', 'Maintenance'),
('E-013', 'Leia Organa', 'leia.organa@bigcorp.com', 'External Affairs'),
('E-014', 'Minch Yoda', 'yoda@bigcorp.com', 'L&D'),
('E-015', 'Obi-Wan', 'obiwan@bigcorp.com', 'Strategy'),
('E-016', 'R2 D2', 'r2d2@bigcorp.com', 'Tech Support'),
('E-017', 'C3PO', 'c3po@bigcorp.com', 'Translation'),
('E-018', 'Lando', 'lando@bigcorp.com', 'Cloud Ops'),
('E-019', 'Bob A. Fett', 'boba.fett@bigcorp.com', 'Contractor'),
('E-020', 'J. Hutt', 'jabba.hutt@bigcorp.com', 'Accounting'),
('E-021', 'Sheev Palpatine', 'palpatine@bigcorp.com', 'Executive'),
('E-022', 'Mace Windu', 'mace.windu@bigcorp.com', 'Compliance'),
('E-023', 'Qui-Gon', 'quigon.jinn@bigcorp.com', 'Operations'),
('E-024', 'Padme Naberrie', 'padme@bigcorp.com', 'Comms'),
('E-025', 'Jar-Jar', 'jarjar.binks@bigcorp.com', 'Gov Rel');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "I need to merge HR records from HR_Merger.Legacy_Payload.BIG_CORP_EMP and ACQUIRED_STAFF.
 *  
 *  1. Bronze: Raw Views.
 *  2. Silver: 
 *     - Standardize Names (Capitalize, Trim) and Emails (Lower).
 *     - Perform a FULL OUTER JOIN on Email Address.
 *     - For non-matches, attempt to Fuzzy Match on Name using SOUNDEX or Levenshtein distance.
 *  3. Gold: 
 *     - Create a consolidated 'Headcount_Report' listing Name, Email, Dept, and Source System.
 *  
 *  Generate the SQL."
 */
