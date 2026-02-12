/*
 * Dremio "Messy Data" Challenge: Botanical Garden Specimens
 * 
 * Scenario: 
 * University botanical garden digitizing 150 years of paper herbarium records.
 * 3 Tables: COLLECTORS, SPECIMENS, IDENTIFICATIONS.
 * 
 * Objective for AI Agent:
 * 1. Taxonomy Synonyms: Map old Latin names to current accepted names.
 * 2. Coordinate Precision: Normalize DMS ('40°26''46"N') vs DD (40.4461) vs text ('near river').
 * 3. Date Ranges: Parse collection dates like '1870s', 'Summer 1923', 'Jun-Jul 1955'.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Herbarium;
CREATE FOLDER IF NOT EXISTS Herbarium.Specimen_Data;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Herbarium.Specimen_Data.COLLECTORS (
    COLLECTOR_ID VARCHAR,
    COLLECTOR_NAME VARCHAR,
    INSTITUTION VARCHAR,
    ACTIVE_YEARS VARCHAR -- '1870-1920', '1950s', NULL
);

CREATE TABLE IF NOT EXISTS Herbarium.Specimen_Data.SPECIMENS (
    SPECIMEN_ID VARCHAR,
    COLLECTOR_ID VARCHAR,
    TAXON_RAW VARCHAR, -- 'Quercus alba', 'Q. alba', 'White Oak', old synonyms
    COLLECTION_DT_RAW VARCHAR, -- '1870-05-15', '1870s', 'Summer 1923', 'Jun-Jul 1955'
    LOCALITY_RAW VARCHAR, -- 'Travis Co., TX', 'near Austin', '30.2672,-97.7431'
    COORD_RAW VARCHAR, -- '40°26''46"N 79°58''56"W', '40.4461,-79.9822', NULL
    HABITAT_TXT VARCHAR
);

CREATE TABLE IF NOT EXISTS Herbarium.Specimen_Data.IDENTIFICATIONS (
    IDENT_ID VARCHAR,
    SPECIMEN_ID VARCHAR,
    DETERMINER VARCHAR,
    TAXON_DETERMINED VARCHAR, -- Updated/corrected name
    IDENT_DT DATE,
    CONFIDENCE_RAW VARCHAR -- 'Confirmed', 'cf.', 'aff.', '?', 'HIGH', '90%'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- COLLECTORS (8 rows)
INSERT INTO Herbarium.Specimen_Data.COLLECTORS VALUES
('COL-001', 'Dr. E. Whitfield', 'Austin Botanical Society', '1865-1910'),
('COL-002', 'Prof. M. Tanaka', 'UT Austin', '1920-1965'),
('COL-003', 'S. Hernandez', 'USDA', '1950s'),
('COL-004', 'Dr. K. Nguyen', 'Texas A&M', '1980-present'),
('COL-005', 'A. Patel', 'Volunteer', '2010-present'),
('COL-006', 'Mrs. J. Adams', 'Garden Club', '1890-1925'),
('COL-007', 'R. Blackwood', 'UT Austin', '1975-2005'),
('COL-008', 'Unknown', NULL, NULL);

-- SPECIMENS (25 rows with messy taxonomy, dates, coords)
INSERT INTO Herbarium.Specimen_Data.SPECIMENS VALUES
('SP-001', 'COL-001', 'Quercus alba', '1870-05-15', 'Travis Co., TX', NULL, 'Bottomland forest'),
('SP-002', 'COL-001', 'Q. alba', '1870s', 'near Austin', NULL, 'creek bank'),
('SP-003', 'COL-001', 'White Oak', 'ca. 1875', 'Austin vicinity', NULL, 'upland prairie edge'),
('SP-004', 'COL-002', 'Prosopis glandulosa', '1925-06-10', 'Bastrop Co.', '30.1100,-97.3300', 'Sandy soil'),
('SP-005', 'COL-002', 'P. juliflora var. glandulosa', 'Summer 1930', 'Bastrop County, Texas', '30°06''36"N 97°19''48"W', 'Mesquite flat'),
('SP-006', 'COL-002', 'Mesquite', 'Jun-Jul 1935', 'nr. Bastrop', NULL, 'roadside'),
('SP-007', 'COL-003', 'Bouteloua gracilis', '1955-08-22', 'Williamson Co., TX', '30.5083,-97.6789', 'Prairie remnant'),
('SP-008', 'COL-003', 'B. gracilis (Kunth) Lag. ex Griffiths', '8/1955', 'Georgetown area', '30°30''N 97°40''W', 'Blackland prairie'),
('SP-009', 'COL-004', 'Quercus fusiformis', '1985-03-15', 'Travis County', '30.2672,-97.7431', 'Live oak woodland'),
('SP-010', 'COL-004', 'Quercus virginiana var. fusiformis', '03/15/1985', 'Austin, TX', '30.2672, -97.7431', 'Escarpment'),
('SP-011', 'COL-005', 'Salvia texana', '2020-04-10', 'Zilker Park, Austin', '30.2660,-97.7730', 'Limestone outcrop'),
('SP-012', 'COL-005', 'Texas Sage', 'April 2020', 'Zilker area', '30.266,-97.773', 'Garden border'),
('SP-013', 'COL-006', 'Castilleja indivisa', 'Spring 1900', 'Travis County', NULL, 'Open prairie'),
('SP-014', 'COL-006', 'Indian Paintbrush', '1900', 'near Austin, Tex.', NULL, 'Meadow'),
('SP-015', 'COL-007', 'Juniperus ashei', '1980-11-20', 'Hays Co.', '29.8833,-97.9414', 'Cedar brake'),
('SP-016', 'COL-007', 'J. ashei J. Buchholz', '11/20/1980', 'San Marcos area', '29°53''N 97°56''W', 'Limestone hill'),
('SP-017', 'COL-008', 'Unknown herb', 'Unknown', 'Texas', NULL, NULL),
('SP-018', 'COL-008', '', '', '', '', ''),  -- All empty
('SP-019', 'COL-004', 'Echinacea angustifolia', '1990-06-15', 'Bastrop Co.', '30.1100,-97.3300', 'Sandy prairie'),
('SP-020', 'COL-005', 'Rudbeckia hirta', '2021-07-04', 'McKinney Falls SP', '30.1823,-97.7221', 'Trail edge'),
('SP-021', 'COL-003', 'Sorghastrum nutans', '1958', 'Williamson Co.', '30.50,-97.68', 'Tall grass'),
('SP-022', 'COL-002', 'Andropogon gerardii', 'late 1940s', 'Central TX', NULL, 'Prairie'),
('SP-023', 'COL-001', 'Quercus stellata', '1880-09', 'Travis County', NULL, 'Post oak savanna'),
('SP-024', 'COL-004', 'Q. stellata Wangenh.', '1988-09-15', 'Bastrop Co.', '30.11,-97.33', 'Sandy woodland'),
('SP-025', 'COL-999', 'Orphan Plant', '2000-01-01', 'Nowhere', NULL, 'No collector match');

-- IDENTIFICATIONS (25+ rows with confidence mess)
INSERT INTO Herbarium.Specimen_Data.IDENTIFICATIONS VALUES
('ID-001', 'SP-001', 'Dr. E. Whitfield', 'Quercus alba L.', '1870-05-15', 'Confirmed'),
('ID-002', 'SP-002', 'Prof. M. Tanaka', 'Quercus alba L.', '1925-01-10', 'Confirmed'),
('ID-003', 'SP-003', 'Prof. M. Tanaka', 'Quercus alba L.', '1925-01-12', 'cf.'),
('ID-004', 'SP-004', 'S. Hernandez', 'Prosopis glandulosa Torr.', '1955-06-01', 'HIGH'),
('ID-005', 'SP-005', 'S. Hernandez', 'Prosopis glandulosa Torr.', '1955-06-05', '90%'),
('ID-006', 'SP-006', 'Dr. K. Nguyen', 'Prosopis glandulosa Torr.', '1985-03-01', 'Confirmed'),
('ID-007', 'SP-007', 'S. Hernandez', 'Bouteloua gracilis (Kunth) Lag.', '1955-09-01', 'Confirmed'),
('ID-008', 'SP-008', 'Dr. K. Nguyen', 'Bouteloua gracilis (Kunth) Lag.', '1986-05-01', 'confirmed'),
('ID-009', 'SP-009', 'Dr. K. Nguyen', 'Quercus fusiformis Small', '1985-04-01', 'Confirmed'),
('ID-010', 'SP-010', 'Dr. K. Nguyen', 'Quercus fusiformis Small', '1985-04-01', '?'),
('ID-011', 'SP-011', 'A. Patel', 'Salvia texana (Scheele) Torr.', '2020-04-15', 'aff.'),
('ID-012', 'SP-012', 'A. Patel', 'Salvia texana (Scheele) Torr.', '2020-04-15', 'Probable'),
('ID-013', 'SP-013', 'Dr. E. Whitfield', 'Castilleja indivisa Engelm.', '1900-04-01', 'Confirmed'),
('ID-014', 'SP-015', 'R. Blackwood', 'Juniperus ashei J. Buchholz', '1980-12-01', 'Confirmed'),
('ID-015', 'SP-016', 'R. Blackwood', 'Juniperus ashei J. Buchholz', '1980-12-01', 'CONFIRMED'),
('ID-016', 'SP-017', 'Unknown', NULL, NULL, 'Unknown'),
('ID-017', 'SP-018', '', '', NULL, ''),
('ID-018', 'SP-019', 'Dr. K. Nguyen', 'Echinacea angustifolia DC.', '1990-07-01', 'Confirmed'),
('ID-019', 'SP-020', 'A. Patel', 'Rudbeckia hirta L.', '2021-07-10', 'Confirmed'),
('ID-020', 'SP-001', 'Dr. K. Nguyen', 'Quercus alba L.', '1986-01-15', 'Confirmed'),  -- Re-determination
('ID-021', 'SP-009', 'A. Patel', 'Quercus virginiana Mill.', '2022-01-01', 'cf.'),     -- Conflicting det!
('ID-022', 'SP-021', 'R. Blackwood', 'Sorghastrum nutans (L.) Nash', '1980-01-01', '90%'),
('ID-023', 'SP-022', 'R. Blackwood', 'Andropogon gerardii Vitman', '1980-01-05', 'Confirmed'),
('ID-024', 'SP-023', 'Prof. M. Tanaka', 'Quercus stellata Wangenh.', '1930-01-01', 'Confirmed'),
('ID-025', 'SP-024', 'Dr. K. Nguyen', 'Quercus stellata Wangenh.', '1988-10-01', 'Confirmed');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Digitize the herbarium in Herbarium.Specimen_Data.
 *  
 *  1. Bronze: Raw Views of COLLECTORS, SPECIMENS, IDENTIFICATIONS.
 *  2. Silver: 
 *     - Taxonomy: Use latest IDENTIFICATION per specimen. Map common names to Latin binomials.
 *     - Dates: Parse '1870s' -> 1870, 'Summer 1923' -> '1923-07-01', 'Jun-Jul 1955' -> '1955-06-01'.
 *     - Coords: Convert DMS to decimal degrees. Flag NULLs and text-only localities.
 *     - Confidence: Map 'cf.'/'aff.'/'?' -> 'UNCERTAIN', 'Confirmed'/'HIGH'/'90%' -> 'CONFIRMED'.
 *  3. Gold: 
 *     - Species Richness per County.
 *     - Collection Timeline (specimens per decade).
 *  
 *  Show the SQL."
 */
