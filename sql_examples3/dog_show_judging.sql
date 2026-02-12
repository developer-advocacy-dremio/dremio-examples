/*
 * Dremio "Messy Data" Challenge: Dog Show Judging
 * 
 * Scenario: 
 * Regional kennel club consolidating show results from paper score sheets.
 * 3 Tables: DOGS, JUDGES, SCORES.
 * 
 * Objective for AI Agent:
 * 1. Breed Group Mapping: Assign AKC group from breed name variants ('GSD' -> 'Herding').
 * 2. Score Reconciliation: Mixed scales (1-10, 1-100, Excellent/Good/Fair), partial scores.
 * 3. Registration ID Cleaning: Old format 'AKC-123456' vs new 'DN12345678', typos.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Lone_Star_Kennel;
CREATE FOLDER IF NOT EXISTS Lone_Star_Kennel.Show_Results;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Lone_Star_Kennel.Show_Results.DOGS (
    DOG_ID VARCHAR,
    REG_NUM_RAW VARCHAR, -- 'AKC-123456', 'DN12345678', 'CKC-98765', typos
    DOG_NAME VARCHAR,
    BREED_RAW VARCHAR, -- Full, abbreviated, or misspelled
    OWNER_NAME VARCHAR,
    DOB DATE
);

CREATE TABLE IF NOT EXISTS Lone_Star_Kennel.Show_Results.JUDGES (
    JUDGE_ID VARCHAR,
    JUDGE_NAME VARCHAR,
    SPECIALTY VARCHAR, -- Breed group specialty
    CERT_NUM VARCHAR
);

CREATE TABLE IF NOT EXISTS Lone_Star_Kennel.Show_Results.SCORES (
    SCORE_ID VARCHAR,
    DOG_ID VARCHAR,
    JUDGE_ID VARCHAR,
    SHOW_DT DATE,
    CATEGORY VARCHAR, -- 'Conformation', 'Obedience', 'Agility'
    SCORE_RAW VARCHAR, -- '9.5', '95', 'Excellent', 'Ex', '8/10', NULL
    PLACEMENT VARCHAR, -- '1st', '1', 'BOB', 'Best of Breed', 'WD', NULL
    NOTES_TXT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- JUDGES (6 rows)
INSERT INTO Lone_Star_Kennel.Show_Results.JUDGES VALUES
('J-001', 'Dr. Margaret Hall', 'Sporting', 'AKC-J-001'),
('J-002', 'Mr. Robert Kim', 'Working', 'AKC-J-002'),
('J-003', 'Mrs. Patricia Lopez', 'Herding', 'AKC-J-003'),
('J-004', 'Dr. James Wright', 'Terrier', 'AKC-J-004'),
('J-005', 'Ms. Chen Wei', 'Toy', 'AKC-J-005'),
('J-006', 'Prof. A. Blackwood', 'All-Breed', 'AKC-J-006');

-- DOGS (15 rows with messy registrations and breeds)
INSERT INTO Lone_Star_Kennel.Show_Results.DOGS VALUES
('D-001', 'AKC-123456', 'Champion Goldenrod', 'Golden Retriever', 'Sarah Mitchell', '2019-03-15'),
('D-002', 'DN12345678', 'Lucky Star', 'Golden', 'Sarah Mitchell', '2020-06-01'),       -- Shortened breed
('D-003', 'AKC-234567', 'Baron von Thunder', 'German Shepherd Dog', 'James Wilson', '2018-11-20'),
('D-004', 'DN23456789', 'Storm', 'GSD', 'James Wilson', '2021-01-10'),                  -- Abbreviation
('D-005', 'AKC-345678', 'Princess Bella', 'Yorkshire Terrier', 'Emily Chen', '2020-04-05'),
('D-006', 'DN3456789O', 'Tiny Dancer', 'Yorkie', 'Emily Chen', '2021-08-15'),          -- O for 0, abbrev
('D-007', 'CKC-98765', 'Maple Leaf', 'Labrador Retreiver', 'Bob Thompson', '2019-07-22'), -- Misspelled
('D-008', 'AKC-456789', 'Sir Winston', 'English Bulldog', 'Diana Ross', '2017-12-01'),
('D-009', 'DN456789I2', 'Lady Churchill', 'Eng. Bulldog', 'Diana Ross', '2020-05-10'),  -- I for 1, abbrev
('D-010', 'AKC-567890', 'Whispering Wind', 'Siberian Husky', 'Anna Petrov', '2018-09-30'),
('D-011', 'DN56789012', 'Arctic Frost', 'Husky', 'Anna Petrov', '2021-02-14'),         -- Shortened
('D-012', 'AKC-678901', 'Duchess of Dallas', 'Standard Poodle', 'Catherine Moore', '2019-05-25'),
('D-013', 'DN67890l23', 'Royal Flush', 'Std. Poodle', 'Catherine Moore', '2022-01-01'),  -- l for 1
('D-014', '', 'Mystery Mutt', 'Mixed', 'Unknown', '2020-01-01'),                        -- Empty reg
('D-015', NULL, 'No Papers', 'Unknown breed', 'Walk-in', NULL);                         -- NULL reg + DOB

-- SCORES (50+ rows with mixed scoring)
INSERT INTO Lone_Star_Kennel.Show_Results.SCORES VALUES
-- 10-point scale
('S-001', 'D-001', 'J-001', '2023-03-15', 'Conformation', '9.5', '1st', 'Outstanding movement'),
('S-002', 'D-001', 'J-006', '2023-03-15', 'Conformation', '9.0', 'BOB', 'Best of Breed'),
('S-003', 'D-002', 'J-001', '2023-03-15', 'Conformation', '8.5', '2nd', 'Good structure'),
('S-004', 'D-003', 'J-003', '2023-03-16', 'Conformation', '9.8', '1st', 'Excellent gait'),
('S-005', 'D-004', 'J-003', '2023-03-16', 'Conformation', '8.0', '3rd', 'Needs maturity'),
('S-006', 'D-005', 'J-004', '2023-03-16', 'Conformation', '9.2', '1st', 'Lovely coat'),
('S-007', 'D-006', 'J-005', '2023-03-16', 'Conformation', '7.5', '4th', 'Tail carriage off'),
-- 100-point scale (different show)
('S-008', 'D-001', 'J-006', '2023-06-10', 'Conformation', '95', '1', 'Top dog'),
('S-009', 'D-003', 'J-006', '2023-06-10', 'Conformation', '98', '1', 'Group winner'),
('S-010', 'D-005', 'J-006', '2023-06-10', 'Conformation', '92', '1', 'Breed winner'),
('S-011', 'D-007', 'J-001', '2023-06-10', 'Conformation', '88', '2', 'Nice head'),
('S-012', 'D-008', 'J-002', '2023-06-10', 'Conformation', '85', '3', 'Moderate build'),
('S-013', 'D-010', 'J-002', '2023-06-10', 'Conformation', '90', '2', 'Good coat'),
('S-014', 'D-012', 'J-006', '2023-06-10', 'Conformation', '93', '1', 'Elegant'),
-- Text scale
('S-015', 'D-001', 'J-001', '2023-09-20', 'Obedience', 'Excellent', '1st', 'Perfect recall'),
('S-016', 'D-002', 'J-001', '2023-09-20', 'Obedience', 'Good', '2nd', 'Minor hesitation'),
('S-017', 'D-003', 'J-003', '2023-09-20', 'Obedience', 'Excellent', '1st', 'Flawless heeling'),
('S-018', 'D-004', 'J-003', '2023-09-20', 'Obedience', 'Fair', '3rd', 'Distracted'),
('S-019', 'D-007', 'J-001', '2023-09-20', 'Obedience', 'Ex', '1st', 'Abbreviated score'),
('S-020', 'D-010', 'J-002', '2023-09-20', 'Obedience', 'VG', NULL, 'Very Good abbreviated'),
-- Fraction format
('S-021', 'D-008', 'J-002', '2023-09-21', 'Conformation', '8/10', 'WD', 'Winners Dog'),
('S-022', 'D-009', 'J-002', '2023-09-21', 'Conformation', '7/10', 'WB', 'Winners Bitch'),
-- Edge cases
('S-023', 'D-014', 'J-006', '2023-09-21', 'Conformation', NULL, NULL, 'Disqualified - mixed breed'),
('S-024', 'D-015', 'J-006', '2023-09-21', 'Conformation', '', '', 'No papers, exhibition only'),
('S-025', 'D-001', 'J-001', '2023-03-15', 'Conformation', '9.5', '1st', 'Outstanding movement'),  -- Dupe of S-001
-- Agility scores (time-based, lower is better)
('S-026', 'D-001', 'J-006', '2023-09-22', 'Agility', '32.5s', '1st', 'Clean run'),
('S-027', 'D-003', 'J-006', '2023-09-22', 'Agility', '28.1s', '1st', 'Fastest'),
('S-028', 'D-004', 'J-006', '2023-09-22', 'Agility', '35.0', '2nd', 'One fault'),
('S-029', 'D-007', 'J-006', '2023-09-22', 'Agility', '30.2 sec', '1st', 'Clean run'),
('S-030', 'D-010', 'J-006', '2023-09-22', 'Agility', 'DNF', NULL, 'Did not finish'),
-- Orphans
('S-031', 'D-999', 'J-001', '2023-10-01', 'Conformation', '8.0', '2nd', 'Unknown dog'),
('S-032', 'D-001', 'J-999', '2023-10-01', 'Conformation', '9.0', '1st', 'Unknown judge'),
-- Bulk fill
('S-033', 'D-002', 'J-006', '2023-06-11', 'Conformation', '87', '3', 'Good temperament'),
('S-034', 'D-006', 'J-005', '2023-06-11', 'Conformation', '82', '4', 'Needs grooming'),
('S-035', 'D-009', 'J-002', '2023-06-11', 'Conformation', '84', '3', 'Moderate quality'),
('S-036', 'D-011', 'J-002', '2023-06-11', 'Conformation', '89', '2', 'Nice movement'),
('S-037', 'D-013', 'J-006', '2023-06-11', 'Conformation', '91', '2', 'Good structure'),
('S-038', 'D-001', 'J-001', '2023-12-01', 'Conformation', '9.7', 'Best of Breed', 'Annual finale'),
('S-039', 'D-003', 'J-003', '2023-12-01', 'Conformation', '9.9', 'Best in Show', 'Perfect score'),
('S-040', 'D-005', 'J-004', '2023-12-01', 'Conformation', '9.3', 'BOB', 'Group finalist'),
('S-041', 'D-008', 'J-002', '2023-12-01', 'Conformation', '8.8', '2nd', 'Consistent'),
('S-042', 'D-010', 'J-002', '2023-12-01', 'Conformation', '9.1', 'BOB', 'Breed winner'),
('S-043', 'D-012', 'J-006', '2023-12-01', 'Conformation', '9.4', 'BOB', 'Elegant as always'),
('S-044', 'D-001', 'J-006', '2023-12-02', 'Obedience', 'Excellent', '1st', 'Finals champion'),
('S-045', 'D-003', 'J-003', '2023-12-02', 'Obedience', 'Excellent', '1st', 'Flawless'),
('S-046', 'D-001', 'J-006', '2023-12-03', 'Agility', '29.8s', '1st', 'Personal best'),
('S-047', 'D-003', 'J-006', '2023-12-03', 'Agility', '27.5s', '1st', 'Course record'),
('S-048', 'D-007', 'J-001', '2023-12-03', 'Agility', '31.0 sec', '2nd', 'Good run'),
('S-049', 'D-010', 'J-002', '2023-12-03', 'Agility', '33.5s', '3rd', 'Clean but slow'),
('S-050', 'D-011', 'J-002', '2023-12-03', 'Agility', '30.0', '2nd', 'Promising');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze dog show results in Lone_Star_Kennel.Show_Results.
 *  
 *  1. Bronze: Raw Views of DOGS, JUDGES, SCORES.
 *  2. Silver: 
 *     - Breed: Map 'GSD' -> 'German Shepherd Dog', 'Yorkie' -> 'Yorkshire Terrier', 'Golden' -> 'Golden Retriever'.
 *     - Registration: Clean reg numbers (replace 'O'->'0', 'l'->'1', 'I'->'1'). Identify format (AKC/CKC/DN).
 *     - Score: Normalize all to 0-100 scale. 'Excellent'->95, 'Good'->80, 'Fair'->65. Parse '8/10'->80.
 *     - Placement: Normalize 'BOB'/'Best of Breed' -> 'BOB', '1st'/'1' -> '1ST'.
 *  3. Gold: 
 *     - Career Points per Dog (sum of normalized scores).
 *     - Top Dogs per Breed Group across all shows.
 *  
 *  Show the SQL."
 */
