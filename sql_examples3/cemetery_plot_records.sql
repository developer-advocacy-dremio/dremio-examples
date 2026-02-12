/*
 * Dremio "Messy Data" Challenge: Cemetery Plot Records
 * 
 * Scenario: 
 * Municipal cemetery digitizing 200 years of handwritten records.
 * 3 Tables: SECTIONS, PLOTS, INTERMENTS.
 * 
 * Objective for AI Agent:
 * 1. Multi-Century Date Parsing: Handle '1847-03-15', 'March 15, 1847', '03/15/47', 'Unknown'.
 * 2. Name Order Detection: 'Smith, John' vs 'John Smith' vs 'SMITH JOHN'.
 * 3. Coordinate Mixing: Grid references ('A-12') vs GPS ('40.7128, -74.006') in same column.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Evergreen_Cemetery;
CREATE FOLDER IF NOT EXISTS Evergreen_Cemetery.Registry;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Evergreen_Cemetery.Registry.SECTIONS (
    SECTION_ID VARCHAR,
    SECTION_NAME VARCHAR,
    ESTABLISHED_YEAR INT,
    CAPACITY_PLOTS INT
);

CREATE TABLE IF NOT EXISTS Evergreen_Cemetery.Registry.PLOTS (
    PLOT_ID VARCHAR,
    SECTION_ID VARCHAR,
    PLOT_REF VARCHAR, -- 'A-12', 'Row3-Slot5', '40.7128,-74.006'
    PLOT_SIZE VARCHAR -- 'Single', 'Double', 'Family (4)', 'Cremation Niche'
);

CREATE TABLE IF NOT EXISTS Evergreen_Cemetery.Registry.INTERMENTS (
    INTERMENT_ID VARCHAR,
    PLOT_ID VARCHAR,
    DECEDENT_NAME VARCHAR, -- 'Smith, John' vs 'John Smith' vs 'SMITH JOHN'
    BIRTH_DT_RAW VARCHAR, -- '1847-03-15', 'March 15, 1847', 'Unknown'
    DEATH_DT_RAW VARCHAR,
    NOTES_TXT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- SECTIONS (6 rows)
INSERT INTO Evergreen_Cemetery.Registry.SECTIONS VALUES
('SEC-A', 'Pioneer Section', 1823, 200),
('SEC-B', 'Veterans Memorial', 1865, 500),
('SEC-C', 'Garden of Peace', 1920, 300),
('SEC-D', 'Modern Lawn', 1975, 400),
('SEC-E', 'Columbarium', 2000, 1000),
('SEC-F', 'Childrens Garden', 1900, 100);

-- PLOTS (15 rows with mixed coordinate formats)
INSERT INTO Evergreen_Cemetery.Registry.PLOTS VALUES
('PL-001', 'SEC-A', 'A-01', 'Family (6)'),
('PL-002', 'SEC-A', 'A-02', 'Double'),
('PL-003', 'SEC-A', 'A-03', 'Single'),
('PL-004', 'SEC-B', 'Row1-Slot1', 'Single'),
('PL-005', 'SEC-B', 'Row1-Slot2', 'Single'),
('PL-006', 'SEC-B', 'Row2-Slot1', 'Double'),
('PL-007', 'SEC-C', '40.7501,-73.9972', 'Family (4)'),  -- GPS coordinates
('PL-008', 'SEC-C', '40.7502,-73.9973', 'Single'),
('PL-009', 'SEC-D', 'D-101', 'Single'),
('PL-010', 'SEC-D', 'D-102', 'Double'),
('PL-011', 'SEC-D', 'D-103', 'Cremation Niche'),
('PL-012', 'SEC-E', 'Niche-A-001', 'Cremation Niche'),
('PL-013', 'SEC-E', 'Niche-A-002', 'Cremation Niche'),
('PL-014', 'SEC-F', 'CG-01', 'Single'),
('PL-015', 'SEC-F', 'CG-02', 'Single');

-- INTERMENTS (50+ rows with multi-century date mess and name variations)
INSERT INTO Evergreen_Cemetery.Registry.INTERMENTS VALUES
-- 1800s (handwritten records)
('INT-001', 'PL-001', 'Smith, Ezekiel', 'March 3, 1801', 'November 12, 1867', 'Founder'),
('INT-002', 'PL-001', 'Smith, Martha', 'Unknown', 'December 1869', 'Wife of Ezekiel'),
('INT-003', 'PL-001', 'Ezekiel Smith Jr', '1830', 'Oct 15, 1863', 'Civil War veteran'),
('INT-004', 'PL-002', 'JONES ELIJAH', '1815', '1890', 'Deacon'),
('INT-005', 'PL-002', 'Mary Jones', 'abt 1820', '1895', 'Wife'),
('INT-006', 'PL-003', 'O''Brien, Patrick', '03/17/1845', '06/22/1901', 'Immigrant from Ireland'),
-- Early 1900s (typed records)
('INT-007', 'PL-004', 'Sgt. James Wilson', '1840-02-14', '1918-11-11', 'WWI Veteran'),
('INT-008', 'PL-005', 'Wilson, Robert A.', '12/05/1895', '07/04/1944', 'WWII Veteran, KIA'),
('INT-009', 'PL-006', 'Thompson, Charles', '1870-08-01', '1935-03-22', 'Mayor 1910-1920'),
('INT-010', 'PL-006', 'Thompson, Eleanor', '1875-12-25', '1940-01-15', 'Philanthropist'),
('INT-011', 'PL-007', 'Rosa, Maria del Carmen', '15-06-1910', '03-12-1985', 'DD/MM format'),
('INT-012', 'PL-007', 'Rosa, Antonio', '1908', '1980', 'Year only'),
-- Mid-century
('INT-013', 'PL-008', 'Dr. Helen Park', '1920-05-10', '1990-08-15', 'First woman surgeon'),
('INT-014', 'PL-009', 'John A. Miller', '04/15/1935', '01/20/2005', 'Korean War vet'),
('INT-015', 'PL-009', 'Miller, Dorothy Jean', '1938-07-22', '2010-03-11', 'Teacher'),
('INT-016', 'PL-010', 'Rev. Samuel Brown', '1925-01-01', '2000-12-31', 'Pastor 40 years'),
('INT-017', 'PL-010', 'Brown, Grace L.', '1930-06-15', '2005-09-20', 'Church organist'),
-- Modern (digital records)
('INT-018', 'PL-011', 'Chen, Wei', '1955-03-08', '2020-04-15', 'Cremation'),
('INT-019', 'PL-012', 'Nakamura, Yuki', '1960-11-30', '2021-02-28', 'Niche interment'),
('INT-020', 'PL-013', 'Patel, Arun K.', '1948-09-12', '2022-07-04', 'Niche interment'),
('INT-021', 'PL-014', 'Baby Garcia', 'May 5, 2010', 'May 5, 2010', 'Stillborn'),
('INT-022', 'PL-015', 'Infant Doe', 'Unknown', '2015-01-10', 'Unidentified'),
-- Duplicates and errors
('INT-023', 'PL-001', 'Smith, Ezekiel', 'March 3, 1801', 'November 12, 1867', 'Founder'), -- Exact dupe of INT-001
('INT-024', 'PL-001', 'Ezekiel Smith', 'Mar 3 1801', 'Nov 12 1867', 'Founder'),            -- Same person, different format
('INT-025', 'PL-999', 'Ghost Record', '1900-01-01', '1950-01-01', 'Orphan plot ID'),
-- Edge cases
('INT-026', 'PL-003', 'O''Sullivan, Sean', '1850', 'ca. 1910', 'Circa date'),
('INT-027', 'PL-004', 'Pvt. Thomas Lee', '1843-?-?', '1864-07-03', 'Battle of Gettysburg'),
('INT-028', 'PL-005', 'Lee, William T.', '1920-02-29', '2000-02-29', 'Leap day baby'),     -- Valid leap years
-- Bulk fill
('INT-029', 'PL-009', 'Anderson, Erik', '1945-06-15', '2015-01-20', 'Retired teacher'),
('INT-030', 'PL-009', 'Anderson, Lisa', '1948-03-22', '2018-05-10', 'Nurse'),
('INT-031', 'PL-010', 'Clark, Peter', '1930-11-05', '2002-08-12', 'Farmer'),
('INT-032', 'PL-010', 'Clark, Betty', '1932-04-18', '2008-12-01', 'Homemaker'),
('INT-033', 'PL-011', 'Yamamoto, Ken', '1960-01-15', '2021-06-30', 'Engineer'),
('INT-034', 'PL-012', 'Okafor, Ada', '1975-09-08', '2023-01-15', 'Professor'),
('INT-035', 'PL-013', 'Singh, Raj', '1950-12-20', '2022-11-05', 'Businessman'),
('INT-036', 'PL-004', 'Cpl. Davis, Henry', '1842-08-10', '1865-04-09', 'Civil War'),
('INT-037', 'PL-005', 'Davis, Margaret', '1845-05-20', '1920-10-15', 'War widow'),
('INT-038', 'PL-006', 'Thompson, Chas.', '1900-01-01', '1965-07-04', 'Abbreviated first name'),
('INT-039', 'PL-007', 'del Carmen Rosa, Maria', '15-06-1910', '03-12-1985', 'Alternate name order'),
('INT-040', 'PL-008', 'Park, H.', '1920-05-10', '1990-08-15', 'Initial only'),
('INT-041', 'PL-009', 'Miller, J.A.', '04/15/1935', '01/20/2005', 'Initials variant'),
('INT-042', 'PL-014', 'Baby Boy Martinez', '2012-03-15', '2012-03-15', 'Neonatal'),
('INT-043', 'PL-015', 'Baby Girl Kim', '2018-08-20', '2018-08-21', 'One day old'),
('INT-044', 'PL-001', 'Smith, Abigail', '1835', '1840', 'Child of Ezekiel'),
('INT-045', 'PL-001', 'Smith, Infant', 'Unknown', '1838', 'Unnamed child'),
('INT-046', 'PL-002', 'Jones, Samuel', '1850', '1852', 'Son of Elijah'),
('INT-047', 'PL-003', 'O''Brien, Bridget', '1848', '1910', 'Wife of Patrick'),
('INT-048', 'PL-004', 'Unknown Soldier', 'Unknown', 'Unknown', 'Unidentified remains'),
('INT-049', 'PL-006', 'Thompson III, Charles', '1905', '1970', 'Numeral suffix'),
('INT-050', 'PL-007', 'Rosa, Maria Carmen', NULL, NULL, 'NULL dates');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Digitize the cemetery records in Evergreen_Cemetery.Registry.
 *  
 *  1. Bronze: Raw Views of SECTIONS, PLOTS, INTERMENTS.
 *  2. Silver: 
 *     - Parse Dates: Handle 'March 3, 1801', '03/17/1845', '1830', 'Unknown', 'abt 1820'.
 *       Use CASE WHEN logic to detect format and CAST to DATE where possible, NULL otherwise.
 *     - Normalize Names: Split 'Smith, John' into LAST_NAME and FIRST_NAME columns.
 *       Handle 'SMITH JOHN' (all caps, no comma) and 'John Smith' (natural order).
 *     - Deduplicate: Identify same person entered multiple times (INT-001 vs INT-023 vs INT-024).
 *  3. Gold: 
 *     - Interments per Decade.
 *     - Plot Occupancy Rate (interments vs capacity) per Section.
 *  
 *  Show the SQL."
 */
