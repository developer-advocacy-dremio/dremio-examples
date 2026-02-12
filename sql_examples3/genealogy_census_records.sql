/*
 * Dremio "Messy Data" Challenge: Genealogy Census Records
 * 
 * Scenario: 
 * Historical society digitizing census records from 1850-1950.
 * 3 Tables: ENUMERATORS, HOUSEHOLDS, PERSONS.
 * 
 * Objective for AI Agent:
 * 1. Occupation Standardization: Map archaic terms ('Hostler', 'Wheelwright') to modern equivalents.
 * 2. Relationship Parsing: Normalize 'Wife', 'W', 'Spouse', 'Married to head' to canonical codes.
 * 3. Age vs Birth Year Reconciliation: Cross-check stated age against census year and birth year.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Heritage_Society;
CREATE FOLDER IF NOT EXISTS Heritage_Society.Census_Archive;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Heritage_Society.Census_Archive.ENUMERATORS (
    ENUM_ID VARCHAR,
    ENUM_NAME VARCHAR,
    COUNTY VARCHAR,
    CENSUS_YEAR INT
);

CREATE TABLE IF NOT EXISTS Heritage_Society.Census_Archive.HOUSEHOLDS (
    HOUSEHOLD_ID VARCHAR,
    ENUM_ID VARCHAR,
    DWELLING_NUM INT,
    FAMILY_NUM INT,
    STREET_RAW VARCHAR, -- '123 Main St', 'Main Street', 'RFD 3', NULL
    WARD VARCHAR
);

CREATE TABLE IF NOT EXISTS Heritage_Society.Census_Archive.PERSONS (
    PERSON_ID VARCHAR,
    HOUSEHOLD_ID VARCHAR,
    PERSON_NAME VARCHAR, -- 'SMITH, John W.' vs 'John Smith' vs 'Jno. Smith'
    RELATION_RAW VARCHAR, -- 'Head', 'Wife', 'W', 'Son', 'S', 'Dau', 'Boarder', 'Servant'
    AGE_RAW VARCHAR, -- '35', '3/12' (3 months), '35y', 'abt 40', NULL
    SEX_RAW VARCHAR, -- 'M', 'Male', 'm', 'F', 'Female', 'f'
    BIRTHPLACE_RAW VARCHAR, -- 'Texas', 'TX', 'Tex.', 'Ireland', 'Ire.', 'Deutschland'
    OCCUPATION_RAW VARCHAR, -- 'Farmer', 'Hostler', 'Wheelwright', 'At Home', 'None', NULL
    BIRTH_YEAR_RAW VARCHAR -- '1865', 'abt 1865', '186?', NULL
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- ENUMERATORS (5 rows)
INSERT INTO Heritage_Society.Census_Archive.ENUMERATORS VALUES
('EN-001', 'J.R. Walker', 'Travis', 1870),
('EN-002', 'S. Martinez', 'Travis', 1900),
('EN-003', 'A.B. Johnson', 'Williamson', 1910),
('EN-004', 'M.F. Thompson', 'Travis', 1920),
('EN-005', 'H.L. Davis', 'Hays', 1940);

-- HOUSEHOLDS (12 rows)
INSERT INTO Heritage_Society.Census_Archive.HOUSEHOLDS VALUES
('HH-001', 'EN-001', 1, 1, '123 Main St', 'Ward 1'),
('HH-002', 'EN-001', 2, 2, 'Main Street', 'Ward 1'),
('HH-003', 'EN-002', 10, 10, 'Congress Ave', 'Ward 3'),
('HH-004', 'EN-002', 11, 11, '400 Congress', 'Ward 3'),
('HH-005', 'EN-003', 5, 5, 'RFD 3', 'Precinct 2'),
('HH-006', 'EN-003', 6, 6, 'Rural Route 3', NULL),
('HH-007', 'EN-004', 1, 1, '500 Guadalupe', 'Ward 2'),
('HH-008', 'EN-004', 2, 2, 'Guadalupe St.', 'Ward 2'),
('HH-009', 'EN-005', 1, 1, '100 Main', 'Precinct 1'),
('HH-010', 'EN-005', 2, 2, 'Rt 1 Box 45', 'Precinct 1'),
('HH-011', 'EN-005', 3, 3, '', NULL),
('HH-012', 'EN-001', 3, 3, NULL, 'Ward 2');

-- PERSONS (50+ rows with all the mess)
INSERT INTO Heritage_Society.Census_Archive.PERSONS VALUES
-- 1870 Census (EN-001)
('P-001', 'HH-001', 'SMITH, John W.', 'Head', '35', 'M', 'Texas', 'Farmer', '1835'),
('P-002', 'HH-001', 'SMITH, Martha A.', 'Wife', '30', 'F', 'Texas', 'At Home', '1840'),
('P-003', 'HH-001', 'SMITH, Ezekiel', 'Son', '10', 'M', 'Tex.', 'At School', '1860'),
('P-004', 'HH-001', 'SMITH, Baby', 'Dau', '3/12', 'F', 'TX', 'None', '1870'),       -- 3 months old
('P-005', 'HH-002', 'O''BRIEN, Patrick', 'Head', '45', 'M', 'Ireland', 'Hostler', 'abt 1825'),
('P-006', 'HH-002', 'O''BRIEN, Bridget', 'W', '40', 'F', 'Ire.', 'Washerwoman', '1830'),
('P-007', 'HH-002', 'O''BRIEN, Sean', 'S', '15', 'M', 'Texas', 'Farm Laborer', '1855'),
('P-008', 'HH-012', 'JONES, Elijah', 'Head', 'abt 55', 'Male', 'Virginia', 'Wheelwright', '181?'),
('P-009', 'HH-012', 'JONES, Mary', 'Wife', '50', 'Female', 'Va.', 'Keeping House', 'abt 1820'),
-- 1900 Census (EN-002)
('P-010', 'HH-003', 'Wilson, Robt. T.', 'Head', '42', 'M', 'New York', 'Druggist', '1858'),
('P-011', 'HH-003', 'Wilson, Sarah J.', 'Wife', '38', 'F', 'N.Y.', 'None', '1862'),
('P-012', 'HH-003', 'Wilson, Robert Jr.', 'Son', '15', 'M', 'Texas', 'At School', '1885'),
('P-013', 'HH-003', 'Wilson, Emily', 'Daughter', '12', 'F', 'Texas', 'At School', '1888'),
('P-014', 'HH-003', 'Chang, Li', 'Servant', '25', 'M', 'China', 'Cook', '1875'),
('P-015', 'HH-004', 'Hernandez, Carlos', 'Head', '50', 'M', 'Mexico', 'Merchant', '1850'),
('P-016', 'HH-004', 'Hernandez, Guadalupe', 'Spouse', '45', 'F', 'Mex.', 'None', '1855'),
('P-017', 'HH-004', 'Hernandez, Rosa', 'Dau', '18', 'F', 'TX', 'Seamstress', '1882'),
-- 1910 Census (EN-003)
('P-018', 'HH-005', 'ANDERSON, Erik', 'Head', '35y', 'M', 'Sweden', 'Farmer (Owner)', '1875'),
('P-019', 'HH-005', 'ANDERSON, Anna', 'Wife', '32', 'f', 'Sverige', 'None', '1878'),  -- Swedish name
('P-020', 'HH-005', 'ANDERSON, Karl', 'Son', '8', 'm', 'Texas', 'None', '1902'),
('P-021', 'HH-005', 'ANDERSON, Infant', 'Son', '2/12', 'M', 'Texas', 'None', '1910'),  -- 2 months
('P-022', 'HH-006', 'MUELLER, Heinrich', 'Head', '48', 'M', 'Deutschland', 'Blacksmith', '1862'),
('P-023', 'HH-006', 'MUELLER, Gretchen', 'Wife', '44', 'F', 'Germany', 'At Home', '1866'),
('P-024', 'HH-006', 'MUELLER, Frieda', 'Daughter', '20', 'F', 'Tex.', 'Telephone Operator', '1890'),
('P-025', 'HH-006', 'BOARDER, Jno. Doe', 'Boarder', 'unknown', 'M', 'Unknown', 'Day Laborer', NULL),
-- 1920 Census (EN-004)
('P-026', 'HH-007', 'Thompson, Chas. A.', 'Head', '55', 'M', 'Texas', 'Banker', '1865'),
('P-027', 'HH-007', 'Thompson, Eleanor', 'Married to head', '50', 'F', 'Texas', 'None', '1870'),
('P-028', 'HH-007', 'Thompson, Chas. Jr.', 'Son', '25', 'M', 'TX', 'Bank Clerk', '1895'),
('P-029', 'HH-007', 'Thompson, Grace', 'Dau', '22', 'F', 'TX', 'Stenographer', '1898'),
('P-030', 'HH-008', 'Tanaka, Ichiro', 'Head', '40', 'M', 'Japan', 'Laundryman', '1880'),
('P-031', 'HH-008', 'Tanaka, Hana', 'W', '35', 'F', 'Japan', 'Assist. Husband', '1885'),
('P-032', 'HH-008', 'Tanaka, George', 'Son', '10', 'M', 'Texas', 'At School', '1910'),
-- 1940 Census (EN-005)
('P-033', 'HH-009', 'Brown, Samuel', 'Head', '45', 'M', 'Texas', 'Pastor', '1895'),
('P-034', 'HH-009', 'Brown, Grace L.', 'Wife', '40', 'F', 'Texas', 'None', '1900'),
('P-035', 'HH-009', 'Brown, James', 'Son', '18', 'M', 'Texas', 'CCC Worker', '1922'),
('P-036', 'HH-009', 'Brown, Ruth', 'Daughter', '15', 'F', 'TX', 'At School', '1925'),
('P-037', 'HH-010', 'Clark, Peter', 'Head', '45', 'M', 'Oklahoma', 'Farmer (Tenant)', '1895'),
('P-038', 'HH-010', 'Clark, Betty', 'Wife', '42', 'F', 'Okla.', 'Unpaid Family Worker', '1898'),
('P-039', 'HH-010', 'Clark, Tom', 'Son', '20', 'M', 'Texas', 'Farm Laborer', '1920'),
('P-040', 'HH-010', 'Clark, Mary', 'Dau', '16', 'F', 'Texas', 'At School', '1924'),
-- Duplicates and errors
('P-041', 'HH-001', 'SMITH, John W.', 'Head', '35', 'M', 'Texas', 'Farmer', '1835'),  -- Exact dupe of P-001
('P-042', 'HH-001', 'John W. Smith', 'Head', '35', 'M', 'TX', 'Farmer', '1835'),        -- Name order swap
-- Age/birth year mismatch
('P-043', 'HH-011', 'Davis, Henry', 'Head', '50', 'M', 'Texas', 'Rancher', '1900'),     -- Age 50 in 1940 = born 1890, not 1900!
('P-044', 'HH-011', 'Davis, Margaret', 'Wife', '45', 'F', 'Texas', 'At Home', '1898'),   -- Matches
-- Edge cases
('P-045', 'HH-011', 'Unknown', 'Boarder', NULL, NULL, 'Unknown', 'None', NULL),
('P-046', 'HH-011', '', '', '', '', '', '', ''),  -- All empty
('P-047', 'HH-999', 'Ghost Person', 'Head', '99', 'M', 'Nowhere', 'None', '1841'),       -- Orphan household
-- More 1870
('P-048', 'HH-002', 'O''BRIEN, Kathleen', 'Dau', '12', 'F', 'Ireland', 'At School', '1858'),
('P-049', 'HH-002', 'O''BRIEN, Infant', 'Son', '6/12', 'M', 'Texas', 'None', '1870'),
('P-050', 'HH-012', 'JONES, Samuel', 'Son', '2', 'M', 'Virginia', 'None', '1868');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Digitize census records in Heritage_Society.Census_Archive.
 *  
 *  1. Bronze: Raw Views of ENUMERATORS, HOUSEHOLDS, PERSONS.
 *  2. Silver: 
 *     - Name: Parse 'SMITH, John W.' -> LAST_NAME='SMITH', FIRST_NAME='John', MIDDLE='W'.
 *       Handle 'Jno.' -> 'John', 'Chas.' -> 'Charles', 'Robt.' -> 'Robert'.
 *     - Relation: Map 'W'/'Spouse'/'Married to head' -> 'WIFE', 'S' -> 'SON', 'Dau'/'Daughter' -> 'DAUGHTER'.
 *     - Age: Parse '3/12' -> 0.25 (years). Parse 'abt 40' -> 40. Validate: Census_Year - Birth_Year ≈ Age.
 *     - Birthplace: Map 'Tex.'/'TX' -> 'Texas', 'Ire.' -> 'Ireland', 'Va.' -> 'Virginia'.
 *     - Occupation: Map 'Hostler' -> 'Stable Hand', 'Wheelwright' -> 'Wheelmaker'.
 *  3. Gold: 
 *     - Household Size Distribution by Census Year.
 *     - Birthplace diversity per decade.
 *  
 *  Show the SQL."
 */
