/*
 * Dremio "Messy Data" Challenge: Museum Art Provenance
 * 
 * Scenario: 
 * Art museum digitizing provenance records from handwritten catalogs and auction logs.
 * 3 Tables: ARTISTS, ARTWORKS, OWNERSHIP_HISTORY.
 * 
 * Objective for AI Agent:
 * 1. Artist Name Disambiguation: 'Picasso' / 'P. Picasso' / 'Pablo Ruiz Picasso' -> canonical.
 * 2. Dimension Parsing: '24x36 in' / '61 x 91.4 cm' / '24" x 36"' to uniform cm.
 * 3. Price Inflation Adjustment: Historical sale prices need year-aware currency normalization.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Metro_Museum;
CREATE FOLDER IF NOT EXISTS Metro_Museum.Collection;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Metro_Museum.Collection.ARTISTS (
    ARTIST_ID VARCHAR,
    ARTIST_NAME_RAW VARCHAR, -- Multiple formats of same artist
    NATIONALITY VARCHAR, -- 'Spanish', 'ES', 'French', 'FR'
    BIRTH_YEAR INT,
    DEATH_YEAR INT -- NULL if living
);

CREATE TABLE IF NOT EXISTS Metro_Museum.Collection.ARTWORKS (
    ARTWORK_ID VARCHAR,
    ARTIST_ID VARCHAR,
    TITLE VARCHAR,
    MEDIUM_RAW VARCHAR, -- 'Oil on canvas', 'O/C', 'oil/canvas', 'Acrylic'
    DIMENSIONS_RAW VARCHAR, -- '24x36 in', '61 x 91.4 cm', '24" x 36"', NULL
    CREATION_YEAR_RAW VARCHAR, -- '1907', 'ca. 1910', '1905-1910', 'Early 20th C.'
    ACCESSION_NUM VARCHAR
);

CREATE TABLE IF NOT EXISTS Metro_Museum.Collection.OWNERSHIP_HISTORY (
    PROV_ID VARCHAR,
    ARTWORK_ID VARCHAR,
    OWNER_NAME VARCHAR,
    ACQUIRED_DT_RAW VARCHAR, -- '1920', '1920-05', 'ca. 1925', 'before 1930'
    SALE_PRICE_RAW VARCHAR, -- '$500', '£350', '500 francs', '2,500,000', NULL
    SALE_VENUE VARCHAR, -- 'Christie''s', 'Sotheby''s', 'Private', 'Seized', NULL
    NOTES_TXT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- ARTISTS (10 rows with name variants)
INSERT INTO Metro_Museum.Collection.ARTISTS VALUES
('A-001', 'Pablo Ruiz Picasso', 'Spanish', 1881, 1973),
('A-002', 'P. Picasso', 'ES', 1881, 1973),           -- Duplicate artist!
('A-003', 'Claude Monet', 'French', 1840, 1926),
('A-004', 'C. Monet', 'FR', 1840, 1926),              -- Duplicate!
('A-005', 'Georgia O''Keeffe', 'American', 1887, 1986),
('A-006', 'Frida Kahlo', 'Mexican', 1907, 1954),
('A-007', 'Vincent van Gogh', 'Dutch', 1853, 1890),
('A-008', 'V. van Gogh', 'NL', 1853, 1890),           -- Duplicate!
('A-009', 'Unknown', NULL, NULL, NULL),
('A-010', 'Jackson Pollock', 'American', 1912, 1956);

-- ARTWORKS (15 rows with dimension/date mess)
INSERT INTO Metro_Museum.Collection.ARTWORKS VALUES
('W-001', 'A-001', 'Les Demoiselles d''Avignon', 'Oil on canvas', '96 x 92 in', '1907', 'ACC-1939.001'),
('W-002', 'A-002', 'Guernica Study', 'O/C', '24x36 in', '1937', 'ACC-1941.015'),    -- A-002 = A-001
('W-003', 'A-003', 'Water Lilies (Morning)', 'Oil on canvas', '200 x 425 cm', 'ca. 1920', 'ACC-1955.022'),
('W-004', 'A-004', 'Impression, Sunrise (study)', 'oil/canvas', '19" x 25"', '1872-1873', 'ACC-1960.008'),
('W-005', 'A-005', 'Jimson Weed/White Flower No. 1', 'Oil on canvas', '48 x 40 in', '1932', 'ACC-1987.003'),
('W-006', 'A-006', 'The Two Fridas', 'Oil on canvas', '173.5 x 173 cm', '1939', 'ACC-1990.001'),
('W-007', 'A-007', 'Starry Night (copy)', 'Oil on canvas', '73.7 x 92.1 cm', '1889', 'ACC-1941.003'),
('W-008', 'A-008', 'Sunflowers Study', 'O/C', '24 x 18 in', 'Late 1880s', 'ACC-1950.010'),  -- A-008 = A-007
('W-009', 'A-010', 'Number 1A', 'Enamel and metallic paint', '68 × 104 in', '1948', 'ACC-1968.005'),
('W-010', 'A-009', 'Portrait of a Lady', 'Oil on panel', '45 x 30 cm', 'ca. 1500', 'ACC-1920.001'),
('W-011', 'A-005', 'Black Iris', 'Oil on canvas', '36 x 29 7/8 in', '1926', 'ACC-1969.002'),  -- Fraction dims
('W-012', 'A-006', 'Self-Portrait with Thorn Necklace', 'Oil on canvas', '24.41" x 18.5"', '1940', 'ACC-1995.004'),
('W-013', 'A-003', 'Rouen Cathedral', 'Oil on canvas', '100x65cm', '1892-1894', 'ACC-1970.011'),  -- No spaces
('W-014', 'A-001', 'Bull''s Head', 'Bronze sculpture', '33.5 cm (h)', '1942', 'ACC-1975.008'),  -- Height only
('W-015', 'A-010', 'Autumn Rhythm', 'Enamel on canvas', '105 × 207 in', '1950', 'ACC-1957.002');

-- OWNERSHIP_HISTORY (30+ rows with price/date mess)
INSERT INTO Metro_Museum.Collection.OWNERSHIP_HISTORY VALUES
('PR-001', 'W-001', 'Jacques Doucet', '1907', '800 francs', 'Studio Sale', 'Purchased directly from artist'),
('PR-002', 'W-001', 'Jacques Seligmann & Co.', '1924', '$28,000', 'Private', NULL),
('PR-003', 'W-001', 'Museum of Modern Art', '1939', '$24,000', 'Acquisition', 'Gift of Lillie P. Bliss'),
('PR-004', 'W-003', 'Estate of Claude Monet', '1926', NULL, 'Inheritance', 'At artist''s death'),
('PR-005', 'W-003', 'Private Collector (Paris)', 'ca. 1930', '£15,000', 'Christie''s', 'London auction'),
('PR-006', 'W-003', 'Metropolitan Museum', '1955', '$250,000', 'Private', 'Anonymous gift'),
('PR-007', 'W-005', 'Georgia O''Keeffe Foundation', '1987', NULL, 'Bequest', 'Artist''s estate'),
('PR-008', 'W-005', 'Crystal Bridges Museum', '2014', '$44,400,000', 'Sotheby''s', 'Record price for female artist'),
('PR-009', 'W-007', 'Lillie P. Bliss', 'before 1930', '£400', 'Private', 'Uncertain provenance'),
('PR-010', 'W-007', 'Museum of Modern Art', '1941', NULL, 'Bequest', 'Bliss Collection'),
('PR-011', 'W-009', 'Betty Parsons Gallery', '1949', '$1,500', 'Gallery', 'First showing'),
('PR-012', 'W-009', 'MoMA', '1968', '$90,000', 'Sotheby''s', NULL),
('PR-013', 'W-010', 'Royal Collection', 'ca. 1500', NULL, 'Commission', 'Unknown master'),
('PR-014', 'W-010', 'Private Collector (London)', 'Early 1800s', '50 guineas', 'Auction', 'Cleaning revealed signature'),
('PR-015', 'W-010', 'Museum Purchase', '1920', '$15,000', 'Sotheby''s', NULL),
('PR-016', 'W-006', 'Frida Kahlo Museum', '1954', NULL, 'Inheritance', 'Collection of Diego Rivera'),
('PR-017', 'W-011', 'Alfred Stieglitz', '1926', 'Gift', NULL, 'Direct from artist'),
('PR-018', 'W-011', 'Metropolitan Museum', '1969', NULL, 'Bequest', 'Stieglitz Collection'),
('PR-019', 'W-002', 'Private Collector', '1945', '2500 pesetas', 'Private', 'Spain'),
('PR-020', 'W-002', 'Museum Acquisition', '1970', '$180,000', 'Private', 'From Barcelona dealer'),
('PR-021', 'W-004', 'Ambroise Vollard', '1900', '300 francs', 'Gallery', 'Paris'),
('PR-022', 'W-004', 'Private Collector', 'ca. 1935', '£2,500', 'Christie''s', NULL),
('PR-023', 'W-008', 'Johanna van Gogh-Bonger', '1890', NULL, 'Inheritance', 'Widow of Theo van Gogh'),
('PR-024', 'W-008', 'Museum Purchase', '1950', '$35,000', 'Private', NULL),
('PR-025', 'W-012', 'Harry Ransom Center', '1966', '$8,000', 'Private', NULL),
('PR-026', 'W-013', 'Durand-Ruel', '1895', '5000 francs', 'Gallery', 'Paris gallery'),
('PR-027', 'W-013', 'Museum Purchase', '1970', '$1,200,000', 'Sotheby''s', NULL),
('PR-028', 'W-014', 'Private Collection', '1950', '£800', 'Private', 'Paris'),
('PR-029', 'W-015', 'Alfonso Ossorio', '1951', '$3,000', 'Gallery', 'Betty Parsons'),
('PR-030', 'W-015', 'Metropolitan Museum', '1957', '$30,000', 'Private', NULL);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit art provenance in Metro_Museum.Collection.
 *  
 *  1. Bronze: Raw Views of ARTISTS, ARTWORKS, OWNERSHIP_HISTORY.
 *  2. Silver: 
 *     - Artist Dedup: Group 'Picasso'/'P. Picasso'/'Pablo Ruiz Picasso' by matching birth/death years.
 *     - Dimensions: Parse all to width_cm x height_cm. 1 in = 2.54 cm. Handle '29 7/8 in'.
 *     - Medium: Map 'O/C'/'oil/canvas' -> 'Oil on canvas'.
 *     - Creation Date: Parse 'ca. 1910'->'1910', '1905-1910'->'1907' (midpoint).
 *     - Sale Price: Normalize to USD. Handle 'francs', '£', 'guineas', 'pesetas', 'Gift' -> NULL.
 *  3. Gold: 
 *     - Price Appreciation: Ratio of latest to earliest sale price per artwork.
 *     - Provenance Chain: Ordered list of owners per artwork.
 *  
 *  Show the SQL."
 */
