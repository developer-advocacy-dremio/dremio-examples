/*
 * Dremio "Messy Data" Challenge: Brewery Batch Records
 * 
 * Scenario: 
 * Craft brewery tracking batch quality from multiple brewing systems.
 * 3 Tables: RECIPES, BATCHES, LAB_RESULTS.
 * 
 * Objective for AI Agent:
 * 1. Gravity Parsing: Normalize OG/FG from '1.052' / '1052' / '13.0°P' (Plato) to SG format.
 * 2. Volume Unit Mixing: Barrels vs gallons vs liters in same column.
 * 3. ABV Calculation Validation: Detect batches where recorded ABV disagrees with OG/FG math.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Lonestar_Brewing;
CREATE FOLDER IF NOT EXISTS Lonestar_Brewing.Production;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Lonestar_Brewing.Production.RECIPES (
    RECIPE_ID VARCHAR,
    RECIPE_NAME VARCHAR,
    STYLE_RAW VARCHAR, -- 'IPA', 'India Pale Ale', 'American IPA', 'ipa'
    TARGET_OG VARCHAR, -- '1.065', '65', '16°P'
    TARGET_FG VARCHAR, -- '1.012', '12', '3°P'
    TARGET_ABV DOUBLE
);

CREATE TABLE IF NOT EXISTS Lonestar_Brewing.Production.BATCHES (
    BATCH_ID VARCHAR,
    RECIPE_ID VARCHAR,
    BREW_DT DATE,
    BREWER VARCHAR,
    VOLUME_RAW VARCHAR, -- '7 bbl', '217 gal', '820 L', '7', NULL
    BATCH_STATUS VARCHAR -- 'Fermenting', 'FERMENTING', 'Packaged', 'Dumped', 'QC Hold'
);

CREATE TABLE IF NOT EXISTS Lonestar_Brewing.Production.LAB_RESULTS (
    LAB_ID VARCHAR,
    BATCH_ID VARCHAR,
    TEST_DT DATE,
    TEST_TYPE VARCHAR, -- 'OG', 'FG', 'pH', 'DO', 'Color'
    RESULT_RAW VARCHAR, -- '1.065', '65', '16.0°P', '4.2', '8 SRM', '12 EBC'
    TESTED_BY VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- RECIPES (8 rows)
INSERT INTO Lonestar_Brewing.Production.RECIPES VALUES
('R-001', 'Hop Highway IPA', 'American IPA', '1.065', '1.012', 6.9),
('R-002', 'Sunset Wheat', 'American Wheat', '1.048', '1.010', 5.0),
('R-003', 'Midnight Stout', 'Stout', '1.072', '1.018', 7.1),
('R-004', 'Pilsner Express', 'Pilsner', '1.045', '1.008', 4.8),
('R-005', 'Haze Craze NEIPA', 'New England IPA', '1.070', '1.015', 7.2),
('R-006', 'Sour Power', 'Berliner Weisse', '1.035', '1.006', 3.8),
('R-007', 'Barleywine Reserve', 'Barleywine', '1.100', '1.025', 9.8),
('R-008', 'Session Blonde', 'Blonde Ale', '1.040', '1.007', 4.3);

-- BATCHES (15 rows with volume mess)
INSERT INTO Lonestar_Brewing.Production.BATCHES VALUES
('B-001', 'R-001', '2023-01-10', 'Jake', '7 bbl', 'Packaged'),
('B-002', 'R-001', '2023-02-15', 'Jake', '217 gal', 'Packaged'),      -- Same vol, different unit
('B-003', 'R-002', '2023-01-20', 'Maria', '7 bbl', 'Packaged'),
('B-004', 'R-003', '2023-02-01', 'Chen', '820 L', 'Packaged'),        -- Liters
('B-005', 'R-004', '2023-02-20', 'Maria', '7', 'Packaged'),            -- No unit!
('B-006', 'R-005', '2023-03-01', 'Jake', '7 bbl', 'Fermenting'),
('B-007', 'R-005', '2023-03-15', 'Jake', '7bbl', 'FERMENTING'),        -- No space, case
('B-008', 'R-006', '2023-04-01', 'Chen', '3.5 bbl', 'Packaged'),
('B-009', 'R-007', '2023-05-01', 'Jake', '3.5 bbl', 'QC Hold'),
('B-010', 'R-008', '2023-05-15', 'Maria', '7 bbl', 'Packaged'),
('B-011', 'R-001', '2023-06-01', 'Chen', '14 bbl', 'Packaged'),       -- Double batch
('B-012', 'R-003', '2023-07-01', 'Jake', NULL, 'Dumped'),              -- NULL volume (dumped)
('B-013', 'R-001', '2023-08-01', 'Maria', '7 bbl', 'Packaged'),
('B-014', 'R-999', '2023-09-01', 'Unknown', '7 bbl', 'Fermenting'),   -- Orphan recipe
('B-015', 'R-002', '2023-01-20', 'Maria', '7 bbl', 'Packaged');       -- Dupe of B-003

-- LAB_RESULTS (50+ rows with gravity/color mess)
INSERT INTO Lonestar_Brewing.Production.LAB_RESULTS VALUES
-- Normal SG format
('L-001', 'B-001', '2023-01-10', 'OG', '1.065', 'Lab'),
('L-002', 'B-001', '2023-01-24', 'FG', '1.012', 'Lab'),
('L-003', 'B-001', '2023-01-10', 'pH', '5.3', 'Lab'),
('L-004', 'B-001', '2023-01-24', 'pH', '4.2', 'Lab'),
-- Short SG (no leading '1.')
('L-005', 'B-002', '2023-02-15', 'OG', '65', 'Jake'),           -- Means 1.065
('L-006', 'B-002', '2023-03-01', 'FG', '13', 'Jake'),           -- Means 1.013
-- Plato degrees
('L-007', 'B-003', '2023-01-20', 'OG', '12.0°P', 'Maria'),     -- Plato
('L-008', 'B-003', '2023-02-03', 'FG', '2.5°P', 'Maria'),
-- Normal again
('L-009', 'B-004', '2023-02-01', 'OG', '1.072', 'Lab'),
('L-010', 'B-004', '2023-02-15', 'FG', '1.018', 'Lab'),
('L-011', 'B-004', '2023-02-01', 'pH', '5.4', 'Chen'),
('L-012', 'B-004', '2023-02-15', 'pH', '4.3', 'Chen'),
-- Color mess (SRM vs EBC)
('L-013', 'B-001', '2023-01-24', 'Color', '8 SRM', 'Lab'),
('L-014', 'B-004', '2023-02-15', 'Color', '70 EBC', 'Lab'),    -- EBC ~ SRM * 1.97
('L-015', 'B-003', '2023-02-03', 'Color', '4', 'Maria'),        -- No unit
('L-016', 'B-005', '2023-02-20', 'OG', '1.045', 'Lab'),
('L-017', 'B-005', '2023-03-06', 'FG', '1.008', 'Lab'),
('L-018', 'B-006', '2023-03-01', 'OG', '1.070', 'Lab'),
('L-019', 'B-006', '2023-03-15', 'FG', '1.015', 'Lab'),
-- ABV disagreement
('L-020', 'B-007', '2023-03-15', 'OG', '1.070', 'Jake'),
('L-021', 'B-007', '2023-03-29', 'FG', '1.015', 'Jake'),
('L-022', 'B-007', '2023-03-29', 'ABV', '5.0', 'Jake'),        -- Should be ~7.2%, recorded 5.0!
-- Sour beer
('L-023', 'B-008', '2023-04-01', 'OG', '1.035', 'Lab'),
('L-024', 'B-008', '2023-04-15', 'FG', '1.006', 'Lab'),
('L-025', 'B-008', '2023-04-15', 'pH', '3.2', 'Lab'),          -- Very sour
-- Barleywine
('L-026', 'B-009', '2023-05-01', 'OG', '1.100', 'Lab'),
('L-027', 'B-009', '2023-06-01', 'FG', '1.028', 'Lab'),        -- Higher than target
('L-028', 'B-009', '2023-06-01', 'pH', '4.0', 'Lab'),
('L-029', 'B-009', '2023-06-01', 'ABV', '9.5', 'Lab'),          -- Matches OG/FG
-- Session
('L-030', 'B-010', '2023-05-15', 'OG', '1.040', 'Lab'),
('L-031', 'B-010', '2023-05-29', 'FG', '1.007', 'Lab'),
-- DO (dissolved oxygen)
('L-032', 'B-001', '2023-01-24', 'DO', '15 ppb', 'Lab'),
('L-033', 'B-011', '2023-06-01', 'DO', '0.02 ppm', 'Lab'),     -- Different DO units
-- Double batch
('L-034', 'B-011', '2023-06-01', 'OG', '1.064', 'Lab'),
('L-035', 'B-011', '2023-06-15', 'FG', '1.013', 'Lab'),
('L-036', 'B-011', '2023-06-15', 'Color', '7 SRM', 'Lab'),
-- Dumped batch
('L-037', 'B-012', '2023-07-01', 'OG', '1.070', 'Lab'),
('L-038', 'B-012', '2023-07-15', 'pH', '7.5', 'Lab'),          -- Way too high - infection
('L-039', 'B-012', '2023-07-15', 'FG', '1.030', 'Lab'),        -- Stuck fermentation
-- Dupe
('L-040', 'B-001', '2023-01-10', 'OG', '1.065', 'Lab'),        -- Exact dupe of L-001
-- Orphan
('L-041', 'B-999', '2023-08-01', 'OG', '1.050', 'Lab'),
-- More normal
('L-042', 'B-013', '2023-08-01', 'OG', '1.066', 'Lab'),
('L-043', 'B-013', '2023-08-15', 'FG', '1.011', 'Lab'),
('L-044', 'B-013', '2023-08-01', 'pH', '5.2', 'Lab'),
('L-045', 'B-013', '2023-08-15', 'pH', '4.1', 'Lab'),
('L-046', 'B-013', '2023-08-15', 'Color', '9 SRM', 'Lab'),
('L-047', 'B-013', '2023-08-15', 'ABV', '7.2', 'Lab'),
('L-048', 'B-013', '2023-08-15', 'DO', '20 ppb', 'Lab'),
('L-049', 'B-010', '2023-05-15', 'Color', '3 SRM', 'Lab'),
('L-050', 'B-008', '2023-04-15', 'Color', '2 SRM', 'Lab');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze brewing data in Lonestar_Brewing.Production.
 *  
 *  1. Bronze: Raw Views of RECIPES, BATCHES, LAB_RESULTS.
 *  2. Silver: 
 *     - Gravity: Normalize to SG. IF < 2 assume SG. IF 2-30 assume Plato (SG = 1 + Plato/258.6). IF > 30 assume short (prefix '1.0').
 *     - Volume: Parse to gallons. 1 bbl = 31 gal. 1 L = 0.264172 gal.
 *     - Color: Standardize to SRM. IF EBC, divide by 1.97.
 *     - ABV Check: Calculate expected ABV = (OG - FG) * 131.25. Flag if |recorded - calculated| > 1.0.
 *  3. Gold: 
 *     - Batch Consistency per Recipe (STDDEV of OG).
 *     - Yield Report: total gallons packaged per month.
 *  
 *  Show the SQL."
 */
