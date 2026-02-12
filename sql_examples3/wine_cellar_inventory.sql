/*
 * Dremio "Messy Data" Challenge: Wine Cellar Inventory
 * 
 * Scenario: 
 * Private wine collection with hand-entered catalog data.
 * 3 Tables: PRODUCERS, BOTTLES, TASTING_NOTES.
 * 
 * Objective for AI Agent:
 * 1. OCR Vintage Cleaning: Fix letter/digit swaps ('20l9' -> '2019', '198O' -> '1980').
 * 2. Region Normalization: Map abbreviations ('Bdx' -> 'Bordeaux', 'Burg' -> 'Burgundy').
 * 3. Score Normalization: Convert 20-point and 5-star scales to 100-point.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Wine_Vault;
CREATE FOLDER IF NOT EXISTS Wine_Vault.Catalog;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Wine_Vault.Catalog.PRODUCERS (
    PRODUCER_ID VARCHAR,
    PRODUCER_NAME VARCHAR,
    REGION_RAW VARCHAR, -- 'Bordeaux', 'Bdx', 'Napa Valley', 'napa'
    COUNTRY VARCHAR
);

CREATE TABLE IF NOT EXISTS Wine_Vault.Catalog.BOTTLES (
    BOTTLE_ID VARCHAR,
    PRODUCER_ID VARCHAR,
    WINE_NAME VARCHAR,
    VINTAGE_RAW VARCHAR, -- '2019', '20l9', '198O', 'NV'
    VARIETAL VARCHAR,
    PRICE_RAW VARCHAR -- '$45.00', '45', '€38', '120 USD'
);

CREATE TABLE IF NOT EXISTS Wine_Vault.Catalog.TASTING_NOTES (
    NOTE_ID VARCHAR,
    BOTTLE_ID VARCHAR,
    REVIEWER VARCHAR,
    SCORE_RAW VARCHAR, -- '92', '17/20', '4.5 stars', 'A-'
    TASTING_DT DATE,
    NOTES_TXT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- PRODUCERS (8 rows)
INSERT INTO Wine_Vault.Catalog.PRODUCERS VALUES
('P-001', 'Chateau Margaux', 'Bordeaux', 'France'),
('P-002', 'Opus One', 'Napa Valley', 'USA'),
('P-003', 'Penfolds', 'Barossa Valley', 'Australia'),
('P-004', 'Domaine Leroy', 'Burgundy', 'France'),
('P-005', 'Antinori', 'Tuscany', 'Italy'),
('P-006', 'Cloudy Bay', 'Marlborough', 'New Zealand'),
('P-007', 'Vega Sicilia', 'Ribera del Duero', 'Spain'),
('P-008', 'Unknown Estate', 'Bdx', 'France');  -- Abbreviated region

-- BOTTLES (20 rows with vintage mess)
INSERT INTO Wine_Vault.Catalog.BOTTLES VALUES
('B-001', 'P-001', 'Grand Vin', '2015', 'Cabernet Blend', '$850.00'),
('B-002', 'P-001', 'Grand Vin', '20l9', 'Cabernet Blend', '$720'),       -- OCR: l for 1
('B-003', 'P-001', 'Pavillon Rouge', '198O', 'Merlot Blend', '€450'),    -- OCR: O for 0
('B-004', 'P-002', 'Opus One', '2018', 'Bordeaux Blend', '400 USD'),
('B-005', 'P-002', 'Opus One', '2O20', 'Bordeaux Blend', '$380'),         -- OCR: O for 0
('B-006', 'P-003', 'Grange', '2017', 'Shiraz', 'AUD 900'),
('B-007', 'P-003', 'Bin 389', '20I8', 'Cab/Shiraz', '75'),               -- OCR: I for 1
('B-008', 'P-004', 'Musigny Grand Cru', '2016', 'Pinot Noir', '€5,200'),
('B-009', 'P-004', 'Romanee-St-Vivant', 'l990', 'Pinot Noir', '€8.500'), -- OCR: l for 1, EU decimal
('B-010', 'P-005', 'Tignanello', '2019', 'Sangiovese Blend', '€130'),
('B-011', 'P-005', 'Solaia', '20l7', 'Cab Sauv', '€350'),                -- OCR: l for 1
('B-012', 'P-006', 'Sauvignon Blanc', '2022', 'Sauvignon Blanc', 'NZD 28'),
('B-013', 'P-006', 'Te Koko', '2O21', 'Sauvignon Blanc', '$35'),          -- OCR: O for 0
('B-014', 'P-007', 'Unico', '2011', 'Tempranillo', '€400'),
('B-015', 'P-007', 'Valbuena', 'NV', 'Tempranillo', '€150'),             -- Non-Vintage
('B-016', 'P-008', 'Reserve Speciale', '????', 'Unknown', NULL),          -- Unknown vintage, NULL price
('B-017', 'P-002', 'Overture', '2020', 'Bordeaux Blend', '$85.00'),
('B-018', 'P-003', 'Bin 28', '2020', 'Shiraz', '$22'),
('B-019', 'P-001', 'Grand Vin', '2015', 'Cabernet Blend', '$850.00'),    -- Dupe of B-001
('B-020', 'P-999', 'Mystery Wine', '2010', 'Red', '$50');                 -- Orphan producer

-- TASTING NOTES (30+ rows with score mess)
INSERT INTO Wine_Vault.Catalog.TASTING_NOTES VALUES
-- 100-point scale
('N-001', 'B-001', 'Parker', '98', '2023-03-01', 'Extraordinary depth and complexity'),
('N-002', 'B-001', 'Spectator', '96', '2023-03-05', 'Ripe and polished'),
('N-003', 'B-004', 'Parker', '95', '2023-04-01', 'Silky and opulent'),
('N-004', 'B-006', 'Halliday', '97', '2023-05-01', 'A benchmark Grange'),
('N-005', 'B-008', 'Burghound', '99', '2023-06-01', 'Perfection in a glass'),
-- 20-point scale (European)
('N-006', 'B-003', 'Bettane', '18/20', '2023-03-10', 'Stunning for its age'),
('N-007', 'B-009', 'RVF', '19/20', '2023-06-10', 'Mythical status'),
('N-008', 'B-010', 'Gambero', '17/20', '2023-07-01', 'Rich and layered'),
('N-009', 'B-014', 'Penin', '19/20', '2023-08-01', 'Benchmark Ribera'),
-- 5-star scale
('N-010', 'B-012', 'Platter', '4.5 stars', '2023-09-01', 'Crisp and vibrant'),
('N-011', 'B-013', 'LocalMag', '4 stars', '2023-09-05', 'Good but not great'),
('N-012', 'B-018', 'Platter', '3.5 stars', '2023-10-01', 'Decent everyday wine'),
-- Letter grades
('N-013', 'B-002', 'BlogReview', 'A-', '2023-04-10', 'Almost perfect'),
('N-014', 'B-005', 'BlogReview', 'B+', '2023-04-15', 'Very good young wine'),
('N-015', 'B-007', 'BlogReview', 'B', '2023-05-10', 'Solid value'),
-- Junk / edge cases
('N-016', 'B-016', 'Unknown', 'N/A', '2023-11-01', 'Could not evaluate'),
('N-017', 'B-016', 'Unknown', '', '2023-11-02', ''),                       -- Empty
('N-018', 'B-015', 'Parker', '90', '2023-08-15', 'Reliable NV bottling'),
('N-019', 'B-017', 'Spectator', '88', '2023-09-20', 'Approachable second wine'),
('N-020', 'B-011', 'Gambero', '16/20', '2023-07-15', 'Needs more time'),
('N-021', 'B-001', 'Parker', '98', '2023-03-01', 'Extraordinary depth and complexity'),  -- Exact dupe
('N-022', 'B-004', 'Halliday', '94', '2023-04-05', 'Polished and elegant'),
('N-023', 'B-006', 'Spectator', '95', '2023-05-05', 'Dense and powerful'),
('N-024', 'B-008', 'Parker', '97', '2023-06-05', 'Ethereal and refined'),
('N-025', 'B-010', 'Spectator', '93', '2023-07-05', 'Tuscan character shines'),
('N-026', 'B-012', 'LocalMag', '4 stars', '2023-09-02', 'Summer sipper'),
('N-027', 'B-014', 'Parker', '96', '2023-08-05', 'Grand and imposing'),
('N-028', 'B-017', 'Parker', '85', '2023-09-25', 'Simple pleasures'),
('N-029', 'B-018', 'Halliday', '89', '2023-10-05', 'Reliable and fruity'),
('N-030', 'B-020', 'Unknown', '75', '2023-11-10', 'Thin and acidic');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Organize the wine collection in Wine_Vault.Catalog.
 *  
 *  1. Bronze: Raw Views of PRODUCERS, BOTTLES, TASTING_NOTES.
 *  2. Silver: 
 *     - Clean Vintage: REPLACE letter 'l'/'I' with '1', 'O' with '0' in VINTAGE_RAW. Cast to INT.
 *     - Normalize Price: Strip currency symbols ('$','€','AUD','NZD','USD'), remove commas, cast to DOUBLE.
 *     - Normalize Score: Convert '18/20' to 90 (score*5), '4.5 stars' to 90 (score*20), 'A-' to 93.
 *  3. Gold: 
 *     - Average Score (100-pt) per Producer.
 *     - Total estimated collection value (SUM of prices).
 *  
 *  Show the SQL."
 */
