/*
 * Genealogy Record Linkage & Ancestry Demo
 * 
 * Scenario:
 * A genealogy platform links disparate historical records (Census, Birth certificates)
 * to build family trees and identify potential ancestors using fuzzy matching.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Persons: User-contributed tree nodes.
 * - Census_Records: Historical census data 1900-1950.
 * - Vital_Records: Birth/Death certificates.
 * 
 * Silver Layer:
 * - Candidate_Matches: Potential links between Persons and Records based on Name/DOB.
 * - Standardized_Names: Soundex/Phonetic encoding for fuzzy search.
 * 
 * Gold Layer:
 * - Ancestry_Tree_Enriched: Known persons enriched with census facts.
 * - Match_Confidence_Score: Scoring matches (High/Medium/Low).
 * 
 * Note: Assumes a catalog named 'GenealogyDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS GenealogyDB;
CREATE FOLDER IF NOT EXISTS GenealogyDB.Bronze;
CREATE FOLDER IF NOT EXISTS GenealogyDB.Silver;
CREATE FOLDER IF NOT EXISTS GenealogyDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS GenealogyDB.Bronze.Persons (
    PersonID INT,
    FirstName VARCHAR,
    LastName VARCHAR,
    BirthYear INT,
    BirthPlace VARCHAR,
    FatherID INT,
    MotherID INT
);

CREATE TABLE IF NOT EXISTS GenealogyDB.Bronze.Census_Records (
    RecordID INT,
    CensusYear INT,
    HeadOfHousehold VARCHAR,
    MemberName VARCHAR,
    Age INT,
    RelationToHead VARCHAR,
    State VARCHAR
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Persons (User Tree)
INSERT INTO GenealogyDB.Bronze.Persons (PersonID, FirstName, LastName, BirthYear, BirthPlace, FatherID, MotherID) VALUES
(1, 'John', 'Smith', 1980, 'NY', 3, 4),
(2, 'Mary', 'Smith', 1982, 'NY', 3, 4),
(3, 'Robert', 'Smith', 1950, 'OH', 5, 6),
(4, 'Linda', 'Jones', 1952, 'OH', 7, 8),
(5, 'William', 'Smith', 1920, 'PA', NULL, NULL),
(6, 'Barbara', 'Davis', 1922, 'PA', NULL, NULL),
(7, 'James', 'Jones', 1925, 'VA', NULL, NULL),
(8, 'Elizabeth', 'Miller', 1928, 'VA', NULL, NULL),
(9, 'Arthur', 'Morgan', 1899, 'West', NULL, NULL),
(10, 'Sadie', 'Adler', 1895, 'West', NULL, NULL);

-- Insert 50 records into GenealogyDB.Bronze.Census_Records
-- Simulating variations in spelling and ages
INSERT INTO GenealogyDB.Bronze.Census_Records (RecordID, CensusYear, HeadOfHousehold, MemberName, Age, RelationToHead, State) VALUES
(1, 1950, 'Robert Smith', 'Robert Smith', 0, 'Son', 'OH'), -- Matches Person 3 (born 1950)
(2, 1950, 'William Smith', 'William Smith', 30, 'Head', 'OH'), -- Matches Father Person 5
(3, 1950, 'William Smith', 'Barbara Smith', 28, 'Wife', 'OH'), -- Matches Mother Person 6 (Maiden Davis)
(4, 1930, 'Will Smith', 'Will Smith', 10, 'Son', 'PA'), -- William match
(5, 1930, 'John Smith', 'John Smith', 40, 'Head', 'PA'), -- William's father?
(6, 1930, 'Sarah Smith', 'Sarah Smith', 38, 'Wife', 'PA'),
(7, 1940, 'James Jones', 'James Jones', 15, 'Son', 'VA'), -- Match Person 7
(8, 1940, 'Henry Jones', 'Henry Jones', 45, 'Head', 'VA'),
(9, 1930, 'Henry Jones', 'Henry Jones', 35, 'Head', 'VA'),
(10, 1930, 'James Jones', 'Jimmy Jones', 5, 'Son', 'VA'), -- Nickname match
(11, 1900, 'Arthur Morgan', 'Arthur Morgan', 1, 'Son', 'TX'), -- Match Person 9
(12, 1910, 'Arthur Morgan', 'Artie Morgan', 11, 'Son', 'TX'),
(13, 1940, 'William Smith', 'Bill Smith', 20, 'Head', 'PA'),
(14, 1940, 'William Smith', 'Barb Smith', 18, 'Wife', 'PA'),
(15, 1950, 'James Jones', 'Jim Jones', 25, 'Head', 'VA'),
(16, 1950, 'James Jones', 'Liz Jones', 22, 'Wife', 'VA'), -- Match Person 8 (Miller)
(17, 1920, 'Unknown', 'Billy Smith', 0, 'Son', 'PA'),
(18, 1910, 'John Doe', 'John Doe', 30, 'Head', 'NY'),
(19, 1920, 'Jane Doe', 'Jane Doe', 40, 'Head', 'NY'),
(20, 1930, 'Sam Spade', 'Sam Spade', 35, 'Head', 'CA'),
(21, 1900, 'Dutch Linde', 'Dutch Linde', 40, 'Head', 'West'),
(22, 1900, 'Dutch Linde', 'Hosea Matthews', 50, 'Partner', 'West'),
(23, 1910, 'John Marston', 'John Marston', 38, 'Head', 'TX'),
(24, 1910, 'John Marston', 'Abigail Marston', 30, 'Wife', 'TX'),
(25, 1910, 'John Marston', 'Jack Marston', 14, 'Son', 'TX'),
(26, 1950, 'Linda Jones', 'Linda Jones', 0, 'Dau', 'OH'), -- Future person 4? no born 1952.
(27, 1940, 'Elizabeth Miller', 'Beth Miller', 12, 'Dau', 'VA'), -- Match Person 8 maiden
(28, 1930, 'Elizabeth Miller', 'Lizzy Miller', 2, 'Dau', 'VA'),
(29, 1920, 'Thomas Miller', 'Thomas Miller', 30, 'Head', 'VA'),
(30, 1920, 'Thomas Miller', 'Mary Miller', 28, 'Wife', 'VA'),
(31, 1950, 'John Smith', 'Johnny Smith', 5, 'Son', 'NY'), -- Wrong John (born 1980)
(32, 1900, 'Sadie Adler', 'Sadie Adler', 5, 'Dau', 'West'), -- Match Person 10
(33, 1910, 'Jake Adler', 'Jake Adler', 25, 'Head', 'West'),
(34, 1910, 'Jake Adler', 'Sadie Adler', 20, 'Wife', 'West'), -- Married
(35, 1920, 'Sadie Adler', 'Widow Adler', 30, 'Head', 'South'), -- Moved?
(36, 1930, 'Robert Smith', 'Bobby Smith', 10, 'Son', 'OH'), -- Too old for person 3 (1950)
(37, 1940, 'Robert Smith', 'Rob Smith', 20, 'Head', 'OH'),
(38, 1900, 'Clark Kent', 'Clark Kent', 10, 'Son', 'KS'),
(39, 1920, 'Bruce Wayne', 'Bruce Wayne', 8, 'Son', 'NJ'),
(40, 1930, 'Bruce Wayne', 'Bruce Wayne', 18, 'Head', 'NJ'),
(41, 1940, 'Peter Parker', 'Peter Parker', 0, 'Son', 'NY'),
(42, 1950, 'Tony Stark', 'Tony Stark', 5, 'Son', 'NY'),
(43, 1910, 'Steve Rogers', 'Steve Rogers', 10, 'Son', 'NY'),
(44, 1930, 'Steve Rogers', 'Steven Rogers', 30, 'Head', 'NY'),
(45, 1900, 'Logan', 'James Howlett', 50, 'Head', 'Canada'),
(46, 1920, 'Charles Xavier', 'Charles Xavier', 10, 'Son', 'NY'),
(47, 1930, 'Erik Lehnsherr', 'Erik Lehnsherr', 10, 'Son', 'Poland'),
(48, 1940, 'Natasha Romanoff', 'Natalia', 10, 'Dau', 'Russia'),
(49, 1950, 'Nick Fury', 'Nicholas Fury', 30, 'Head', 'DC'),
(50, 1950, 'Phil Coulson', 'Phil Coulson', 0, 'Son', 'WI');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Standardization & Fuzzy Logic
-------------------------------------------------------------------------------

-- 2.1 Candidate Matches
-- Using basic string matching, in real scenarios use Soundex/Levenshtein
CREATE OR REPLACE VIEW GenealogyDB.Silver.Candidate_Matches AS
SELECT
    p.PersonID,
    p.FirstName AS TreeName,
    p.LastName AS TreeSurname,
    p.BirthYear,
    c.RecordID,
    c.MemberName AS CensusName,
    c.CensusYear,
    c.Age,
    (c.CensusYear - c.Age) AS EstimatedBirthYear,
    ABS((c.CensusYear - c.Age) - p.BirthYear) AS YearDiff
FROM GenealogyDB.Bronze.Persons p
JOIN GenealogyDB.Bronze.Census_Records c 
  ON p.LastName = SUBSTR(c.MemberName, INSTR(c.MemberName, ' ') + 1) -- Match Surname
WHERE ABS((c.CensusYear - c.Age) - p.BirthYear) <= 2; -- Birth year within 2 years

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Ancestry Insights
-------------------------------------------------------------------------------

-- 3.1 Match Confidence Score
CREATE OR REPLACE VIEW GenealogyDB.Gold.Match_Confidence AS
SELECT
    PersonID,
    TreeName,
    CensusName,
    YearDiff,
    CASE 
        WHEN TreeName = CensusName AND YearDiff = 0 THEN 'High'
        WHEN SUBSTR(TreeName, 1, 3) = SUBSTR(CensusName, 1, 3) AND YearDiff <= 1 THEN 'Medium'
        ELSE 'Low'
    END AS Confidence
FROM GenealogyDB.Silver.Candidate_Matches;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Find Records):
"Find all 'High' confidence census matches for 'William Smith' in GenealogyDB.Gold.Match_Confidence."

PROMPT 2 (Validation):
"List potential matches from GenealogyDB.Silver.Candidate_Matches where the YearDiff is 0."
*/
