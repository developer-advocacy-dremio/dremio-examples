/*
 * Dremio "Messy Data" Challenge: Amateur Radio Contacts
 * 
 * Scenario: 
 * Ham radio club consolidating QSO (contact) logs from multiple operators.
 * 3 Tables: OPERATORS, STATIONS, QSO_LOG.
 * 
 * Objective for AI Agent:
 * 1. Callsign Canonicalization: Normalize 'W1ABC' / 'w1abc' / 'W1ABC/P' to base callsign.
 * 2. Frequency Normalization: Unify '14.250 MHz' / '14250 kHz' / '14250000' to Hz.
 * 3. Signal Report Parsing: Decode RST ('599', '55N', '5/9') into individual R, S, T components.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Ham_Radio_Club;
CREATE FOLDER IF NOT EXISTS Ham_Radio_Club.QSO_Data;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Ham_Radio_Club.QSO_Data.OPERATORS (
    OPERATOR_ID VARCHAR,
    CALLSIGN VARCHAR,
    OPERATOR_NAME VARCHAR,
    LICENSE_CLASS VARCHAR -- 'Technician', 'General', 'Extra'
);

CREATE TABLE IF NOT EXISTS Ham_Radio_Club.QSO_Data.STATIONS (
    STATION_ID VARCHAR,
    OPERATOR_ID VARCHAR,
    RIG_MODEL VARCHAR,
    ANTENNA VARCHAR,
    GRID_SQUARE VARCHAR -- Maidenhead grid: 'FN31pr'
);

CREATE TABLE IF NOT EXISTS Ham_Radio_Club.QSO_Data.QSO_LOG (
    QSO_ID VARCHAR,
    STATION_ID VARCHAR,
    CONTACT_CALL VARCHAR, -- Other station's callsign (messy)
    FREQ_RAW VARCHAR, -- '14.250 MHz', '14250 kHz', '14250000', '7.040'
    MODE_RAW VARCHAR, -- 'SSB', 'ssb', 'CW', 'FT8', 'USB', 'LSB'
    RST_SENT VARCHAR, -- '599', '59', '5/9', '55N', NULL
    RST_RCVD VARCHAR,
    QSO_TS TIMESTAMP, -- May be UTC or local
    TZ_TAG VARCHAR, -- 'UTC', 'EST', 'PST', NULL
    NOTES_TXT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- OPERATORS (6 rows)
INSERT INTO Ham_Radio_Club.QSO_Data.OPERATORS VALUES
('OP-001', 'W5ABC', 'Alex', 'Extra'),
('OP-002', 'KD5XYZ', 'Bob', 'General'),
('OP-003', 'N5DEF', 'Carol', 'Extra'),
('OP-004', 'KG5GHI', 'Dave', 'Technician'),
('OP-005', 'W5JKL', 'Eve', 'General'),
('OP-006', 'AA5MNO', 'Frank', 'Extra');

-- STATIONS (8 rows)
INSERT INTO Ham_Radio_Club.QSO_Data.STATIONS VALUES
('ST-001', 'OP-001', 'Icom IC-7300', 'Hex Beam', 'EM10dj'),
('ST-002', 'OP-001', 'Yaesu FT-991A', 'Dipole', 'EM10dj'),
('ST-003', 'OP-002', 'Kenwood TS-590SG', 'Vertical', 'EM12kr'),
('ST-004', 'OP-003', 'Elecraft K3S', 'Yagi', 'EM10fm'),
('ST-005', 'OP-004', 'Baofeng UV-5R', 'Rubber Duck', 'EM10dj'),
('ST-006', 'OP-005', 'Icom IC-7610', 'Hex Beam', 'EM13aj'),
('ST-007', 'OP-006', 'Flex 6600', 'SteppIR', 'EM10ck'),
('ST-008', 'OP-002', 'Yaesu FT-817', 'End-Fed', 'EM12kr'); -- Portable

-- QSO LOG (50+ rows with all the mess)
INSERT INTO Ham_Radio_Club.QSO_Data.QSO_LOG VALUES
-- Normal SSB contacts
('Q-001', 'ST-001', 'K1ABC', '14.250 MHz', 'SSB', '59', '59', '2023-06-01 14:00:00', 'UTC', 'Good signal'),
('Q-002', 'ST-001', 'W2DEF', '14.280 MHz', 'SSB', '57', '58', '2023-06-01 14:15:00', 'UTC', 'Slight QSB'),
('Q-003', 'ST-001', 'VE3GHI', '14.300 MHz', 'SSB', '59', '59', '2023-06-01 14:30:00', 'UTC', 'Canada contact'),
-- CW contacts (3-digit RST)
('Q-004', 'ST-004', 'JA1ABC', '7.010 MHz', 'CW', '599', '599', '2023-06-01 22:00:00', 'UTC', 'Japan DX'),
('Q-005', 'ST-004', 'DL2XYZ', '7.025 MHz', 'CW', '579', '589', '2023-06-01 22:30:00', 'UTC', 'Germany'),
('Q-006', 'ST-004', 'G4ABC', '7.030 MHz', 'CW', '559', '569', '2023-06-01 23:00:00', 'UTC', 'England'),
-- Callsign variations (messy!)
('Q-007', 'ST-001', 'w1abc', '14.250 MHz', 'SSB', '59', '59', '2023-06-02 14:00:00', 'UTC', 'Lowercase call'),
('Q-008', 'ST-001', 'W1ABC/P', '14.280 MHz', 'SSB', '57', '58', '2023-06-02 14:15:00', 'UTC', 'Portable suffix'),
('Q-009', 'ST-001', 'W1ABC/M', '14.300 MHz', 'SSB', '55', '56', '2023-06-02 14:30:00', 'UTC', 'Mobile suffix'),
('Q-010', 'ST-003', 'VE3/W1ABC', '14.200 MHz', 'SSB', '59', '59', '2023-06-02 15:00:00', 'UTC', 'Prefix variation'),
-- Frequency format mess
('Q-011', 'ST-002', 'K4DEF', '14250 kHz', 'SSB', '59', '57', '2023-06-03 13:00:00', 'UTC', 'kHz format'),
('Q-012', 'ST-003', 'W6GHI', '14250000', 'SSB', '58', '59', '2023-06-03 14:00:00', 'UTC', 'Hz raw'),
('Q-013', 'ST-006', 'N7JKL', '7.040', 'CW', '599', '599', '2023-06-03 22:00:00', 'UTC', 'No unit'),
('Q-014', 'ST-007', 'W0MNO', '7040', 'CW', '589', '599', '2023-06-03 22:30:00', 'UTC', 'kHz no label'),
('Q-015', 'ST-001', 'VK2ABC', '21.350MHz', 'SSB', '55', '55', '2023-06-04 06:00:00', 'UTC', 'No space'),
('Q-016', 'ST-004', 'ZL1XYZ', '14.1 mhz', 'CW', '599', '599', '2023-06-04 07:00:00', 'UTC', 'Lowercase unit'),
-- RST format mess
('Q-017', 'ST-001', 'K8PQR', '14.280 MHz', 'SSB', '5/9', '5/8', '2023-06-05 14:00:00', 'UTC', 'Slash format'),
('Q-018', 'ST-004', 'W3STU', '7.010 MHz', 'CW', '55N', '57N', '2023-06-05 22:00:00', 'UTC', 'N for noise'),
('Q-019', 'ST-006', 'K2VWX', '14.250 MHz', 'SSB', '5 by 9', '5 by 7', '2023-06-05 14:30:00', 'UTC', 'Verbose'),
('Q-020', 'ST-003', 'W9YZA', '14.300 MHz', 'SSB', '', '59', '2023-06-05 15:00:00', 'UTC', 'Empty sent'),
('Q-021', 'ST-001', 'N4BCD', '14.250 MHz', 'SSB', '59', NULL, '2023-06-05 15:30:00', 'UTC', 'NULL received'),
-- Mode variations
('Q-022', 'ST-002', 'K5EFG', '14.250 MHz', 'ssb', '59', '59', '2023-06-06 14:00:00', 'UTC', 'Lowercase mode'),
('Q-023', 'ST-001', 'W7HIJ', '14.250 MHz', 'USB', '57', '58', '2023-06-06 14:15:00', 'UTC', 'USB = SSB upper'),
('Q-024', 'ST-004', 'DL5KLM', '3.550 MHz', 'LSB', '58', '57', '2023-06-06 20:00:00', 'UTC', 'LSB = SSB lower'),
('Q-025', 'ST-006', 'W1NOP', '14.074 MHz', 'FT8', '-10', '-12', '2023-06-06 14:30:00', 'UTC', 'FT8 uses dB, not RST'),
('Q-026', 'ST-007', 'K3QRS', '14.074 MHz', 'ft8', '-08', '-15', '2023-06-06 14:45:00', 'UTC', 'Lowercase FT8'),
-- Timezone mess
('Q-027', 'ST-003', 'W4TUV', '14.200 MHz', 'SSB', '59', '59', '2023-06-07 09:00:00', 'EST', 'Local time EST'),
('Q-028', 'ST-008', 'K6WXY', '14.280 MHz', 'SSB', '57', '58', '2023-06-07 10:00:00', 'PST', 'Local time PST'),
('Q-029', 'ST-005', 'W8ZAB', '146.520 MHz', 'FM', '5', '5', '2023-06-07 12:00:00', NULL, 'No TZ, VHF'),
('Q-030', 'ST-005', 'KD5CDE', '146.520 MHz', 'FM', '3', '4', '2023-06-07 12:15:00', NULL, 'No TZ'),
-- Duplicates
('Q-031', 'ST-001', 'K1ABC', '14.250 MHz', 'SSB', '59', '59', '2023-06-01 14:00:00', 'UTC', 'Good signal'),  -- Dupe of Q-001
-- Orphans
('Q-032', 'ST-999', 'W5ABC', '14.250 MHz', 'SSB', '59', '59', '2023-06-08 14:00:00', 'UTC', 'Unknown station'),
-- Bulk fill
('Q-033', 'ST-001', 'K9FGH', '21.300 MHz', 'SSB', '59', '59', '2023-06-10 14:00:00', 'UTC', '15m band'),
('Q-034', 'ST-001', 'W0IJK', '21.350 MHz', 'SSB', '57', '58', '2023-06-10 14:30:00', 'UTC', '15m band'),
('Q-035', 'ST-004', 'EA3LMN', '14.010 MHz', 'CW', '599', '599', '2023-06-10 22:00:00', 'UTC', 'Spain'),
('Q-036', 'ST-004', 'F5OPQ', '14.025 MHz', 'CW', '579', '589', '2023-06-10 22:30:00', 'UTC', 'France'),
('Q-037', 'ST-006', 'PY2RST', '14.250 MHz', 'SSB', '55', '56', '2023-06-11 18:00:00', 'UTC', 'Brazil'),
('Q-038', 'ST-007', 'LU3UVW', '14.280 MHz', 'SSB', '57', '57', '2023-06-11 18:30:00', 'UTC', 'Argentina'),
('Q-039', 'ST-001', 'VK4XYZ', '14.200 MHz', 'SSB', '53', '54', '2023-06-12 08:00:00', 'UTC', 'Australia'),
('Q-040', 'ST-003', 'HL1ABC', '14.250 MHz', 'SSB', '55', '56', '2023-06-12 10:00:00', 'UTC', 'South Korea'),
('Q-041', 'ST-004', 'UA3DEF', '7.010 MHz', 'CW', '599', '599', '2023-06-12 20:00:00', 'UTC', 'Russia'),
('Q-042', 'ST-006', 'ZS6GHI', '14.300 MHz', 'SSB', '57', '58', '2023-06-13 16:00:00', 'UTC', 'South Africa'),
('Q-043', 'ST-007', 'A71ABC', '14.200 MHz', 'SSB', '59', '59', '2023-06-13 14:00:00', 'UTC', 'Qatar'),
('Q-044', 'ST-001', 'YB2JKL', '21.250 MHz', 'SSB', '55', '55', '2023-06-14 09:00:00', 'UTC', 'Indonesia'),
('Q-045', 'ST-002', 'HS0MNO', '14.250 MHz', 'SSB', '57', '56', '2023-06-14 10:00:00', 'UTC', 'Thailand'),
('Q-046', 'ST-003', '9V1PQR', '14.280 MHz', 'SSB', '58', '59', '2023-06-14 11:00:00', 'UTC', 'Singapore'),
('Q-047', 'ST-004', 'ON4STU', '7.020 MHz', 'CW', '599', '599', '2023-06-14 21:00:00', 'UTC', 'Belgium'),
('Q-048', 'ST-006', 'OZ1VWX', '14.300 MHz', 'SSB', '59', '58', '2023-06-15 13:00:00', 'UTC', 'Denmark'),
('Q-049', 'ST-007', 'OE1YZA', '14.200 MHz', 'SSB', '57', '57', '2023-06-15 14:00:00', 'UTC', 'Austria'),
('Q-050', 'ST-008', 'K2BCD', '14.250 MHz', 'SSB', '59', '59', '2023-06-15 15:00:00', 'EST', 'Contest QSO');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Organize QSO logs in Ham_Radio_Club.QSO_Data.
 *  
 *  1. Bronze: Raw Views of OPERATORS, STATIONS, QSO_LOG.
 *  2. Silver: 
 *     - Callsign: UPPER(CONTACT_CALL), strip '/P', '/M', 'VE3/' prefixes to get base callsign.
 *     - Frequency: Parse to Hz. IF contains 'MHz' then *1000000. IF contains 'kHz' then *1000. If bare < 1000 assume MHz.
 *     - MODE: UPPER(MODE_RAW). Map 'USB'/'LSB' -> 'SSB'.
 *     - RST: Parse '5/9' -> R=5, S=9. Parse '55N' -> R=5, S=5.
 *  3. Gold: 
 *     - Unique DXCC Entities contacted (by callsign prefix).
 *     - QSOs per Band (derive band from frequency).
 *     - Most Active Operators by QSO count.
 *  
 *  Show the SQL."
 */
