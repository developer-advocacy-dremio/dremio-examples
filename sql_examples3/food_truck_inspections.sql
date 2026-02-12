/*
 * Dremio "Messy Data" Challenge: Food Truck Inspections
 * 
 * Scenario: 
 * City health department tracking mobile food vendors.
 * 3 Tables: TRUCKS, INSPECTORS, INSPECTIONS.
 * 
 * Objective for AI Agent:
 * 1. Score Normalization: Unify letter grades ('A'), numeric (92), and pass/fail into 100-point scale.
 * 2. License Plate OCR Cleaning: Fix 'O'/'0' and 'I'/'1' swaps.
 * 3. Violation Deduplication: Same truck, same violation, same week = single event.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Health;
CREATE FOLDER IF NOT EXISTS City_Health.Mobile_Food;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Health.Mobile_Food.TRUCKS (
    TRUCK_ID VARCHAR,
    BUSINESS_NAME VARCHAR,
    LICENSE_PLATE VARCHAR, -- OCR captured, may have errors
    CUISINE_TYPE VARCHAR,
    PERMIT_NUM VARCHAR
);

CREATE TABLE IF NOT EXISTS City_Health.Mobile_Food.INSPECTORS (
    INSPECTOR_ID VARCHAR,
    INSPECTOR_NAME VARCHAR,
    BADGE_NUM VARCHAR,
    DISTRICT VARCHAR
);

CREATE TABLE IF NOT EXISTS City_Health.Mobile_Food.INSPECTIONS (
    INSP_ID VARCHAR,
    TRUCK_ID VARCHAR,
    INSPECTOR_ID VARCHAR,
    INSP_DT DATE,
    SCORE_RAW VARCHAR, -- 'A', '92', 'Pass', 'B+', '85/100'
    VIOLATION_TXT VARCHAR,
    LAT DOUBLE,
    LON DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- TRUCKS (10 rows)
INSERT INTO City_Health.Mobile_Food.TRUCKS VALUES
('FT-001', 'Taco Loco', 'ABC1234', 'Mexican', 'PM-100'),
('FT-002', 'Burger Bus', 'XYZ5678', 'American', 'PM-101'),
('FT-003', 'Pho Wheels', 'DEF9O12', 'Vietnamese', 'PM-102'),    -- O for 0
('FT-004', 'Pizza Express', 'GH1I345', 'Italian', 'PM-103'),    -- I for 1
('FT-005', 'Seoul Kitchen', 'JKL6789', 'Korean', 'PM-104'),
('FT-006', 'BBQ Baron', 'MNO0I23', 'BBQ', 'PM-105'),            -- O and I swaps
('FT-007', 'Crepe Cart', 'PQR4567', 'French', 'PM-106'),
('FT-008', 'Falafel King', 'STU8901', 'Mediterranean', 'PM-107'),
('FT-009', 'Waffle Wagon', 'VWX2345', 'Breakfast', 'PM-108'),
('FT-010', 'Sushi Roll', 'YZA6789', 'Japanese', 'PM-109');

-- INSPECTORS (5 rows)
INSERT INTO City_Health.Mobile_Food.INSPECTORS VALUES
('INS-01', 'Chen, Lisa', 'B-4401', 'North'),
('INS-02', 'Rodriguez, Marco', 'B-4402', 'South'),
('INS-03', 'Patel, Anita', 'B-4403', 'East'),
('INS-04', 'Thompson, James', 'B-4404', 'West'),
('INS-05', 'Kim, David', 'B-4405', 'Central');

-- INSPECTIONS (50+ rows with score mess and violations)
INSERT INTO City_Health.Mobile_Food.INSPECTIONS VALUES
-- Letter grades
('I-001', 'FT-001', 'INS-01', '2023-01-15', 'A', 'None', 34.0522, -118.2437),
('I-002', 'FT-001', 'INS-01', '2023-04-15', 'A-', 'Minor: handwashing sign missing', 34.0530, -118.2440),
('I-003', 'FT-002', 'INS-02', '2023-01-20', 'B', 'Improper food temp', 34.0600, -118.2500),
('I-004', 'FT-002', 'INS-02', '2023-04-20', 'B+', 'Minor: gloves not worn', 34.0610, -118.2510),
('I-005', 'FT-003', 'INS-03', '2023-01-25', 'C', 'Rodent droppings observed', 34.0700, -118.2600),
-- Numeric scores
('I-006', 'FT-004', 'INS-04', '2023-02-01', '92', 'None', 34.0400, -118.2300),
('I-007', 'FT-005', 'INS-05', '2023-02-05', '88', 'Improper food storage', 34.0500, -118.2350),
('I-008', 'FT-006', 'INS-01', '2023-02-10', '75', 'Cross contamination risk', 34.0550, -118.2380),
('I-009', 'FT-007', 'INS-02', '2023-02-15', '95', 'None', 34.0650, -118.2480),
('I-010', 'FT-008', 'INS-03', '2023-02-20', '100', 'None', 34.0750, -118.2580),
-- Pass / Fail
('I-011', 'FT-009', 'INS-04', '2023-03-01', 'Pass', 'None', 34.0350, -118.2250),
('I-012', 'FT-010', 'INS-05', '2023-03-05', 'Fail', 'No hot water; expired permit', 34.0450, -118.2320),
('I-013', 'FT-009', 'INS-04', '2023-06-01', 'PASS', 'None', 34.0355, -118.2255),   -- Capitalization
('I-014', 'FT-010', 'INS-05', '2023-06-05', 'pass', 'Minor issues resolved', 34.0460, -118.2330),
-- Fraction format
('I-015', 'FT-001', 'INS-01', '2023-07-15', '90/100', 'Minor: cracked tile', 34.0525, -118.2438),
('I-016', 'FT-003', 'INS-03', '2023-07-25', '70/100', 'Repeat: rodent issue', 34.0705, -118.2605),
-- Edge cases
('I-017', 'FT-006', 'INS-01', '2023-08-10', 'N/A', 'Truck not operating', 34.0555, -118.2385),
('I-018', 'FT-004', 'INS-04', '2023-08-01', '', 'Inspection incomplete - rain', 34.0405, -118.2305),
('I-019', 'FT-005', 'INS-05', '2023-08-05', NULL, 'Inspector absent', 34.0505, -118.2355),
-- Violation abbreviations
('I-020', 'FT-002', 'INS-02', '2023-07-20', '80', 'Imp. food temp; No hw sign', 34.0605, -118.2505),
('I-021', 'FT-006', 'INS-01', '2023-05-10', '72', 'X-contam; imp. storage', 34.0560, -118.2390),
-- GPS precision issues
('I-022', 'FT-001', 'INS-02', '2023-10-15', 'A', 'None', 34.1, -118.2),          -- Low precision
('I-023', 'FT-007', 'INS-03', '2023-10-15', '96', 'None', 0.0, 0.0),              -- Null Island
('I-024', 'FT-008', 'INS-04', '2023-10-20', '98', 'None', NULL, NULL),             -- NULL coords
-- Duplicate inspections (same truck, same day)
('I-025', 'FT-001', 'INS-01', '2023-01-15', 'A', 'None', 34.0522, -118.2437),     -- Dupe of I-001
-- Bulk fill
('I-026', 'FT-001', 'INS-03', '2023-09-15', '94', 'None', 34.0528, -118.2442),
('I-027', 'FT-002', 'INS-04', '2023-09-20', '82', 'Minor: thermometer missing', 34.0615, -118.2515),
('I-028', 'FT-003', 'INS-05', '2023-09-25', '65', 'Critical: no refrigeration', 34.0710, -118.2610),
('I-029', 'FT-004', 'INS-01', '2023-10-01', '91', 'None', 34.0408, -118.2308),
('I-030', 'FT-005', 'INS-02', '2023-10-05', '89', 'Minor issues', 34.0508, -118.2358),
('I-031', 'FT-006', 'INS-03', '2023-10-10', '78', 'Repeat violation', 34.0565, -118.2395),
('I-032', 'FT-007', 'INS-04', '2023-10-15', '97', 'None', 34.0655, -118.2485),
('I-033', 'FT-008', 'INS-05', '2023-10-20', '99', 'None', 34.0755, -118.2585),
('I-034', 'FT-009', 'INS-01', '2023-10-25', 'A', 'None', 34.0358, -118.2258),
('I-035', 'FT-010', 'INS-02', '2023-10-30', 'B-', 'Improved but still issues', 34.0465, -118.2335),
('I-036', 'FT-001', 'INS-04', '2023-11-15', '93', 'None', 34.0530, -118.2445),
('I-037', 'FT-002', 'INS-05', '2023-11-20', '84', 'Temp log missing', 34.0620, -118.2520),
('I-038', 'FT-003', 'INS-01', '2023-11-25', '60', 'CLOSURE ORDERED', 34.0715, -118.2615),
('I-039', 'FT-004', 'INS-02', '2023-12-01', '90', 'None', 34.0412, -118.2312),
('I-040', 'FT-005', 'INS-03', '2023-12-05', '87', 'None', 34.0512, -118.2362),
('I-041', 'FT-006', 'INS-04', '2023-12-10', '80', 'Reinspection required', 34.0570, -118.2400),
('I-042', 'FT-007', 'INS-05', '2023-12-15', 'A+', 'Perfect score', 34.0660, -118.2490),
('I-043', 'FT-008', 'INS-01', '2023-12-20', 'A', 'None', 34.0760, -118.2590),
('I-044', 'FT-009', 'INS-02', '2023-12-25', 'Pass', 'None', 34.0362, -118.2262),
('I-045', 'FT-010', 'INS-03', '2023-12-30', 'B', 'Follow up needed', 34.0470, -118.2340),
('I-046', 'FT-999', 'INS-01', '2023-06-15', '85', 'Unregistered truck', 34.0500, -118.2400),
('I-047', 'FT-001', 'INS-99', '2023-06-20', '91', 'None', 34.0535, -118.2450),
('I-048', 'FT-002', 'INS-02', '2023-06-25', '83', 'Minor: floor mat', 34.0625, -118.2525),
('I-049', 'FT-004', 'INS-04', '2023-06-01', '93', 'None', 34.0415, -118.2315),
('I-050', 'FT-005', 'INS-05', '2023-06-10', '86', 'Slight temp deviation', 34.0515, -118.2365);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit food truck inspections in City_Health.Mobile_Food.
 *  
 *  1. Bronze: Raw Views of TRUCKS, INSPECTORS, INSPECTIONS.
 *  2. Silver: 
 *     - Normalize Score: Map 'A'->95, 'B'->85, 'C'->75, 'Pass'->80, 'Fail'->50. Parse '90/100'->90.
 *     - Clean Plates: REPLACE 'O' with '0' and 'I' with '1' in positions that are digits.
 *     - Filter: Remove NULL/empty scores and GPS (0,0).
 *  3. Gold: 
 *     - Average Score Trend per Truck (quarterly).
 *     - Repeat Violators: Trucks with > 2 scores below 80.
 *  
 *  Show the SQL."
 */
