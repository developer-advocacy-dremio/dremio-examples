/*
 * Dremio "Messy Data" Challenge: Marine Vessel AIS
 * 
 * Scenario: 
 * Coast guard aggregating Automatic Identification System (AIS) transponder data.
 * 3 Tables: VESSELS, PORTS, AIS_POSITIONS.
 * 
 * Objective for AI Agent:
 * 1. MMSI Validation: 9-digit Maritime Mobile Service Identity, detect malformed ones.
 * 2. Speed Conversion: Knots vs km/h vs mph mixed in same column.
 * 3. Heading/Course: Detect impossible values (>360°), stationary drift noise.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Coast_Guard;
CREATE FOLDER IF NOT EXISTS Coast_Guard.Vessel_Traffic;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Coast_Guard.Vessel_Traffic.VESSELS (
    MMSI VARCHAR, -- Should be 9 digits, but messy
    VESSEL_NAME VARCHAR,
    VESSEL_TYPE VARCHAR, -- 'Cargo', 'Tanker', 'Fishing', 'Pleasure', 'CARGO', 'cargo'
    FLAG_STATE VARCHAR, -- 'US', 'USA', 'Panama', 'PA', 'MH' (Marshall Islands)
    GROSS_TONNAGE_RAW VARCHAR -- '50000', '50,000', '50K', NULL
);

CREATE TABLE IF NOT EXISTS Coast_Guard.Vessel_Traffic.PORTS (
    PORT_ID VARCHAR,
    PORT_NAME VARCHAR,
    COUNTRY VARCHAR,
    LAT DOUBLE,
    LON DOUBLE
);

CREATE TABLE IF NOT EXISTS Coast_Guard.Vessel_Traffic.AIS_POSITIONS (
    POS_ID VARCHAR,
    MMSI VARCHAR,
    POS_TS TIMESTAMP,
    LAT DOUBLE,
    LON DOUBLE,
    SOG_RAW VARCHAR, -- Speed Over Ground: '12.5 kn', '23.1 km/h', '14.4 mph', '12.5', NULL
    COG DOUBLE, -- Course Over Ground (degrees)
    HEADING DOUBLE, -- True heading (degrees)
    NAV_STATUS VARCHAR -- 'Under way', 'UNDERWAY', 'At anchor', 'Moored', 'MOORED', NULL
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- PORTS (6 rows)
INSERT INTO Coast_Guard.Vessel_Traffic.PORTS VALUES
('PT-001', 'Galveston', 'USA', 29.3013, -94.7977),
('PT-002', 'Houston Ship Channel', 'USA', 29.7604, -95.3698),
('PT-003', 'Corpus Christi', 'USA', 27.8006, -97.3964),
('PT-004', 'New Orleans', 'USA', 29.9511, -90.0715),
('PT-005', 'Freeport', 'USA', 28.9541, -95.3597),
('PT-006', 'Brownsville', 'USA', 25.9991, -97.4469);

-- VESSELS (10 rows with messy types and flags)
INSERT INTO Coast_Guard.Vessel_Traffic.VESSELS VALUES
('366123456', 'MV Gulf Star', 'Cargo', 'US', '50000'),
('366234567', 'SS Bayou Runner', 'Tanker', 'USA', '85,000'),
('366345678', 'FV Red Snapper', 'Fishing', 'US', '250'),
('353456789', 'MT Pacific Spirit', 'TANKER', 'Panama', '120K'),       -- Case + K
('538567890', 'MV Island Trader', 'cargo', 'Marshall Islands', '35000'),
('367890123', 'SY Sea Breeze', 'Pleasure', 'US', '150'),
('12345', 'Unknown Vessel', 'Unknown', NULL, NULL),                    -- Too short MMSI
('3661234567', 'Extra Digit', 'Cargo', 'US', '40000'),                -- 10 digits
('000000000', 'Ghost Ship', 'Unknown', NULL, '0'),                     -- All zeros
('366456789', 'MV Lone Star', 'Cargo', 'United States', '55,000');

-- AIS_POSITIONS (50+ rows)
INSERT INTO Coast_Guard.Vessel_Traffic.AIS_POSITIONS VALUES
-- Normal positions (knots)
('A-001', '366123456', '2023-06-01 08:00:00', 29.30, -94.80, '12.5 kn', 270.0, 268.0, 'Under way'),
('A-002', '366123456', '2023-06-01 08:05:00', 29.30, -94.82, '12.3 kn', 271.0, 269.0, 'Under way'),
('A-003', '366123456', '2023-06-01 08:10:00', 29.30, -94.84, '12.1 kn', 270.5, 268.5, 'Under way'),
('A-004', '366234567', '2023-06-01 09:00:00', 29.50, -94.50, '8.0 kn', 315.0, 313.0, 'Under way'),
('A-005', '366234567', '2023-06-01 09:05:00', 29.51, -94.51, '7.8 kn', 316.0, 314.0, 'UNDERWAY'),
-- Speed in km/h
('A-006', '353456789', '2023-06-01 10:00:00', 28.00, -95.00, '23.1 km/h', 180.0, 178.0, 'Under way'),
('A-007', '353456789', '2023-06-01 10:05:00', 27.98, -95.00, '22.8 km/h', 181.0, 179.0, 'Under way'),
-- Speed in mph
('A-008', '538567890', '2023-06-01 11:00:00', 27.50, -96.00, '14.4 mph', 90.0, 88.0, 'Under way'),
('A-009', '538567890', '2023-06-01 11:05:00', 27.50, -95.98, '14.2 mph', 91.0, 89.0, 'Under Way'),
-- No speed unit
('A-010', '366345678', '2023-06-01 06:00:00', 27.80, -97.40, '5.0', 45.0, 43.0, 'Under way'),
('A-011', '366345678', '2023-06-01 06:05:00', 27.81, -97.39, '4.8', 46.0, 44.0, 'Under way'),
-- At anchor/moored
('A-012', '367890123', '2023-06-01 12:00:00', 29.30, -94.80, '0.0 kn', 0.0, 180.0, 'At anchor'),
('A-013', '367890123', '2023-06-01 12:05:00', 29.30, -94.80, '0.1 kn', 5.0, 182.0, 'At anchor'),  -- Drift
('A-014', '366456789', '2023-06-01 14:00:00', 29.76, -95.37, '0.0 kn', 0.0, 90.0, 'Moored'),
('A-015', '366456789', '2023-06-01 14:05:00', 29.76, -95.37, '0.0 kn', 0.0, 91.0, 'MOORED'),
-- Impossible values
('A-016', '366123456', '2023-06-02 08:00:00', 29.30, -94.90, '12.0 kn', 400.0, 268.0, 'Under way'),  -- COG > 360
('A-017', '366234567', '2023-06-02 09:00:00', 29.55, -94.55, '8.5 kn', 270.0, -10.0, 'Under way'),   -- Negative heading
('A-018', '353456789', '2023-06-02 10:00:00', 27.90, -95.05, '150.0 kn', 180.0, 178.0, 'Under way'), -- 150 knots!
-- GPS errors
('A-019', '366345678', '2023-06-02 06:00:00', 0.0, 0.0, '5.0 kn', 45.0, 43.0, 'Under way'),         -- Null Island
('A-020', '538567890', '2023-06-02 11:00:00', NULL, NULL, '14.0 mph', 90.0, 88.0, 'Under way'),       -- NULL coords
('A-021', '366123456', '2023-06-02 08:15:00', 91.0, -94.90, '12.0 kn', 270.0, 268.0, 'Under way'),   -- Lat > 90
-- Malformed MMSI
('A-022', '12345', '2023-06-01 15:00:00', 29.00, -95.00, '10.0 kn', 180.0, 178.0, 'Under way'),
('A-023', '000000000', '2023-06-01 16:00:00', 28.50, -95.50, '0.0 kn', 0.0, 0.0, NULL),
-- NULL nav status
('A-024', '366123456', '2023-06-03 08:00:00', 29.35, -94.95, '11.0 kn', 265.0, 263.0, NULL),
('A-025', '366234567', '2023-06-03 09:00:00', 29.60, -94.60, NULL, NULL, NULL, NULL),                  -- All NULL
-- Duplicates
('A-026', '366123456', '2023-06-01 08:00:00', 29.30, -94.80, '12.5 kn', 270.0, 268.0, 'Under way'),  -- Dupe of A-001
-- Bulk fill
('A-027', '366123456', '2023-06-03 08:05:00', 29.35, -94.97, '11.2 kn', 266.0, 264.0, 'Under way'),
('A-028', '366123456', '2023-06-03 08:10:00', 29.35, -94.99, '11.0 kn', 267.0, 265.0, 'Under way'),
('A-029', '366234567', '2023-06-03 09:05:00', 29.61, -94.61, '7.5 kn', 316.0, 314.0, 'Under way'),
('A-030', '366234567', '2023-06-03 09:10:00', 29.62, -94.62, '7.3 kn', 317.0, 315.0, 'Under way'),
('A-031', '353456789', '2023-06-03 10:00:00', 27.85, -95.10, '22.0 km/h', 182.0, 180.0, 'Under way'),
('A-032', '353456789', '2023-06-03 10:05:00', 27.83, -95.10, '21.5 km/h', 183.0, 181.0, 'Under way'),
('A-033', '538567890', '2023-06-03 11:00:00', 27.55, -95.90, '13.8 mph', 92.0, 90.0, 'Under way'),
('A-034', '538567890', '2023-06-03 11:05:00', 27.55, -95.88, '13.5 mph', 93.0, 91.0, 'Under way'),
('A-035', '366345678', '2023-06-03 06:00:00', 27.82, -97.38, '4.5 kn', 47.0, 45.0, 'Under way'),
('A-036', '366345678', '2023-06-03 06:05:00', 27.83, -97.37, '4.2 kn', 48.0, 46.0, 'Under way'),
('A-037', '367890123', '2023-06-03 12:00:00', 29.30, -94.80, '0.0 kn', 0.0, 185.0, 'At anchor'),
('A-038', '366456789', '2023-06-03 14:00:00', 29.76, -95.37, '0.0 kn', 0.0, 92.0, 'Moored'),
('A-039', '366123456', '2023-06-04 08:00:00', 29.40, -95.05, '10.5 kn', 260.0, 258.0, 'Under way'),
('A-040', '366123456', '2023-06-04 08:05:00', 29.40, -95.07, '10.3 kn', 261.0, 259.0, 'Under way'),
('A-041', '366234567', '2023-06-04 09:00:00', 29.65, -94.65, '7.0 kn', 318.0, 316.0, 'Under way'),
('A-042', '353456789', '2023-06-04 10:00:00', 27.80, -95.15, '20.0 km/h', 185.0, 183.0, 'Under way'),
('A-043', '538567890', '2023-06-04 11:00:00', 27.60, -95.80, '13.0 mph', 94.0, 92.0, 'Under way'),
('A-044', '366345678', '2023-06-04 06:00:00', 27.85, -97.35, '3.8 kn', 50.0, 48.0, 'Under way'),
('A-045', '366456789', '2023-06-04 14:00:00', 29.76, -95.37, '0.0 kn', 0.0, 93.0, 'Moored'),
('A-046', '366999999', '2023-06-04 15:00:00', 29.00, -95.00, '8.0 kn', 180.0, 178.0, 'Under way'),  -- Orphan MMSI
('A-047', '366123456', '2023-06-05 08:00:00', 29.45, -95.10, '9.8 kn', 255.0, 253.0, 'Under way'),
('A-048', '366234567', '2023-06-05 09:00:00', 29.70, -94.70, '6.5 kn', 320.0, 318.0, 'Under way'),
('A-049', '353456789', '2023-06-05 10:00:00', 27.75, -95.20, '19.5 km/h', 186.0, 184.0, 'Under way'),
('A-050', '538567890', '2023-06-05 11:00:00', 27.65, -95.70, '12.5 mph', 95.0, 93.0, 'Under way');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Track vessel movements in Coast_Guard.Vessel_Traffic.
 *  
 *  1. Bronze: Raw Views of VESSELS, PORTS, AIS_POSITIONS.
 *  2. Silver: 
 *     - MMSI: Validate length = 9 digits. Flag malformed.
 *     - Speed: Normalize to knots. IF 'km/h' THEN value / 1.852. IF 'mph' THEN value / 1.151.
 *     - Heading/COG: Filter > 360 or < 0.
 *     - Nav Status: UPPER. Map 'Under Way' -> 'UNDERWAY', 'At anchor' -> 'ANCHORED'.
 *     - GPS: Filter Lat=0/Lon=0, Lat > 90, NULL coords.
 *  3. Gold: 
 *     - Average Speed by Vessel Type.
 *     - Port Proximity: Flag positions within 1 nautical mile of a port.
 *  
 *  Show the SQL."
 */
