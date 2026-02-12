/*
 * Dremio "Messy Data" Challenge: Auto Body Repair Estimates
 * 
 * Scenario: 
 * Multi-location auto body shop chain merging estimate systems.
 * 3 Tables: VEHICLES, TECHNICIANS, ESTIMATES.
 * 
 * Objective for AI Agent:
 * 1. VIN Validation: Check digit 9 (mod-11 checksum), detect OCR mis-reads.
 * 2. Labor Rate Parsing: '$45/hr' / '45.00' / '$45 per hour' to uniform DOUBLE.
 * 3. Paint Code Matching: OEM codes ('WA8624' GM) vs generic ('Pearl White') vs hex ('#F5F5DC').
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Metro_Auto_Body;
CREATE FOLDER IF NOT EXISTS Metro_Auto_Body.Repair_Data;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Metro_Auto_Body.Repair_Data.VEHICLES (
    VEHICLE_ID VARCHAR,
    VIN_RAW VARCHAR, -- 17 chars, may have OCR errors (O/0, I/1)
    MAKE_RAW VARCHAR, -- 'Toyota', 'TOYOTA', 'Toy.', 'Chevy', 'Chevrolet'
    MODEL_RAW VARCHAR,
    PAINT_CODE_RAW VARCHAR, -- 'WA8624', 'Pearl White', '#F5F5DC', '1G1', NULL
    VEH_YEAR INT
);

CREATE TABLE IF NOT EXISTS Metro_Auto_Body.Repair_Data.TECHNICIANS (
    TECH_ID VARCHAR,
    TECH_NAME VARCHAR,
    SHOP_LOCATION VARCHAR,
    CERT_LEVEL VARCHAR -- 'ASE', 'I-CAR Gold', 'Journeyman', NULL
);

CREATE TABLE IF NOT EXISTS Metro_Auto_Body.Repair_Data.ESTIMATES (
    EST_ID VARCHAR,
    VEHICLE_ID VARCHAR,
    TECH_ID VARCHAR,
    EST_DT DATE,
    DAMAGE_AREA VARCHAR, -- 'Front Bumper', 'Rt Fender', 'R/F', 'LF Door', 'Hood'
    LABOR_HRS_RAW VARCHAR, -- '4.5', '4.5 hrs', '4h30m', NULL
    LABOR_RATE_RAW VARCHAR, -- '$45/hr', '45.00', '$45 per hour', '€55/hr'
    PARTS_COST_RAW VARCHAR, -- '$350.00', '350', '$1,200.50', 'TBD', NULL
    EST_STATUS VARCHAR -- 'Approved', 'APPROVED', 'Pending', 'Rejected', 'Supplement'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- TECHNICIANS (6 rows)
INSERT INTO Metro_Auto_Body.Repair_Data.TECHNICIANS VALUES
('T-001', 'Mike Johnson', 'North Shop', 'ASE'),
('T-002', 'Carlos Ruiz', 'North Shop', 'I-CAR Gold'),
('T-003', 'Dave Chen', 'South Shop', 'Journeyman'),
('T-004', 'Alexis White', 'South Shop', 'ASE'),
('T-005', 'Yuki Tanaka', 'East Shop', 'I-CAR Gold'),
('T-006', 'Trainee', 'North Shop', NULL);

-- VEHICLES (15 rows with VIN and paint mess)
INSERT INTO Metro_Auto_Body.Repair_Data.VEHICLES VALUES
('V-001', '1HGCM82633A123456', 'Honda', 'Accord', 'NH-731P', 2003),
('V-002', '1HGCM82633AI23456', 'Honda', 'Accord', 'Pearl White', 2003),   -- I for 1 in VIN
('V-003', 'WBAPH5C55BA123456', 'BMW', '328i', 'A76', 2011),
('V-004', '1G1YY22G965123456', 'Chevrolet', 'Corvette', 'WA8624', 2006),
('V-005', '1G1YY22G965I23456', 'Chevy', 'Vette', '#CC0000', 2006),        -- Abbrev + I for 1
('V-006', '5YJ3E1EA1JF123456', 'Tesla', 'Model 3', 'PBSB', 2018),
('V-007', '5YJ3E1EAOJF123456', 'TESLA', 'Model 3', 'Solid Black', 2018), -- O for 0
('V-008', 'JN1TBNT30Z0123456', 'Nissan', 'Sentra', 'QAB', 2010),
('V-009', '2T1BURHE5JC123456', 'Toyota', 'Corolla', '1F7', 2018),
('V-010', '2T1BURHE5JCI23456', 'TOYOTA', 'Corolla', 'Classic Silver', 2018), -- I for 1
('V-011', 'WDDGF8AB7DA123456', 'Mercedes-Benz', 'C300', '775', 2013),
('V-012', 'WDDGF8AB7DA123456', 'Mercedes', 'C-Class', 'Iridium Silver', 2013), -- Same VIN, diff name
('V-013', '3VWDP7AJ5DM123456', 'Volkswagen', 'Jetta', 'LA7W', 2013),
('V-014', '', 'Ford', 'F-150', 'UX', 2020),                              -- Empty VIN
('V-015', NULL, 'Toy.', 'Camry', NULL, 2015);                             -- NULL VIN + abbrev

-- ESTIMATES (50+ rows)
INSERT INTO Metro_Auto_Body.Repair_Data.ESTIMATES VALUES
-- Normal estimates
('E-001', 'V-001', 'T-001', '2023-03-01', 'Front Bumper', '4.5', '$55/hr', '$350.00', 'Approved'),
('E-002', 'V-001', 'T-001', '2023-03-01', 'Hood', '6.0 hrs', '$55/hr', '$480.00', 'Approved'),
('E-003', 'V-003', 'T-002', '2023-03-05', 'Rt Fender', '3.5', '$60/hr', '$520.00', 'Approved'),
('E-004', 'V-003', 'T-002', '2023-03-05', 'R/F Door', '5.0', '$60 per hour', '$380.00', 'Approved'),
('E-005', 'V-004', 'T-003', '2023-03-10', 'Rear Bumper', '3.0 hrs', '$50/hr', '$850.00', 'Approved'),
('E-006', 'V-006', 'T-004', '2023-03-15', 'LF Fender', '8.0', '$75/hr', '$1,200.50', 'Approved'),  -- Tesla
('E-007', 'V-006', 'T-004', '2023-03-15', 'L/F Door', '6.5 hrs', '$75/hr', '$980.00', 'Approved'),
-- Status mess
('E-008', 'V-008', 'T-005', '2023-04-01', 'Front Bumper', '4.0', '$55/hr', '$300.00', 'APPROVED'),
('E-009', 'V-009', 'T-001', '2023-04-05', 'Hood', '5.5', '55.00', '$420.00', 'Pending'),           -- No $ in rate
('E-010', 'V-009', 'T-001', '2023-04-10', 'Hood', '5.5', '$55/hr', '$420.00', 'Approved'),          -- Updated
-- Supplement (additional damage found)
('E-011', 'V-001', 'T-001', '2023-03-08', 'Radiator Support', '3.0', '$55/hr', 'TBD', 'Supplement'),
('E-012', 'V-006', 'T-004', '2023-03-20', 'Battery Pack Shield', '2.0', '$75/hr', 'TBD', 'Supplement'),
-- Labor mess
('E-013', 'V-011', 'T-002', '2023-04-15', 'Rear Quarter', '8h30m', '$65/hr', '$1,500', 'Approved'), -- h/m format
('E-014', 'V-011', 'T-002', '2023-04-15', 'Trunk Lid', '4h', '$65/hr', '$600', 'Approved'),
('E-015', 'V-013', 'T-003', '2023-05-01', 'Front Bumper', '4.0', '€55/hr', '$320.00', 'Pending'),   -- Euro rate
-- Parts_cost mess
('E-016', 'V-009', 'T-001', '2023-05-05', 'Tail Light', '1.5', '$55/hr', '250', 'Approved'),        -- No $
('E-017', 'V-004', 'T-003', '2023-05-10', 'Side Mirror', '1.0', '$50/hr', '$0', 'Approved'),         -- Zero
('E-018', 'V-008', 'T-005', '2023-05-15', 'Windshield', '2.0', '$55/hr', NULL, 'Pending'),           -- NULL
-- Duplicates
('E-019', 'V-001', 'T-001', '2023-03-01', 'Front Bumper', '4.5', '$55/hr', '$350.00', 'Approved'),  -- Dupe of E-001
-- Orphans
('E-020', 'V-999', 'T-001', '2023-06-01', 'Hood', '5.0', '$55/hr', '$400.00', 'Pending'),
('E-021', 'V-001', 'T-999', '2023-06-05', 'Door', '3.0', '$55/hr', '$300.00', 'Pending'),
-- Damage area inconsistency
('E-022', 'V-003', 'T-002', '2023-06-10', 'Right Fender', '3.5', '$60/hr', '$520.00', 'Approved'),  -- 'Right Fender' vs 'Rt Fender'
('E-023', 'V-004', 'T-003', '2023-06-15', 'R. Bumper', '3.0', '$50/hr', '$850.00', 'Approved'),     -- 'R. Bumper' vs 'Rear Bumper'
-- Bulk fill
('E-024', 'V-001', 'T-001', '2023-07-01', 'Front Bumper', '2.0', '$55/hr', '$200.00', 'Approved'),
('E-025', 'V-003', 'T-002', '2023-07-05', 'Hood', '4.0', '$60/hr', '$350.00', 'Approved'),
('E-026', 'V-004', 'T-003', '2023-07-10', 'Front Bumper', '3.5', '$50/hr', '$400.00', 'Approved'),
('E-027', 'V-006', 'T-004', '2023-07-15', 'Rear Bumper', '5.0', '$75/hr', '$950.00', 'Approved'),
('E-028', 'V-008', 'T-005', '2023-07-20', 'Hood', '4.0', '$55/hr', '$380.00', 'Approved'),
('E-029', 'V-009', 'T-001', '2023-07-25', 'Rt Fender', '3.0', '$55/hr', '$300.00', 'Approved'),
('E-030', 'V-011', 'T-002', '2023-08-01', 'Front Bumper', '4.5', '$65/hr', '$450.00', 'Approved'),
('E-031', 'V-013', 'T-003', '2023-08-05', 'Rear Bumper', '3.0', '$50/hr', '$280.00', 'Approved'),
('E-032', 'V-001', 'T-004', '2023-08-10', 'Side Mirror', '1.0', '$55/hr', '$180.00', 'Approved'),
('E-033', 'V-003', 'T-005', '2023-08-15', 'Tail Light', '1.5', '$60/hr', '$220.00', 'Approved'),
('E-034', 'V-004', 'T-001', '2023-08-20', 'LF Fender', '4.0', '$55/hr', '$500.00', 'Approved'),
('E-035', 'V-006', 'T-002', '2023-08-25', 'Hood', '7.0', '$75/hr', '$1,100.00', 'Approved'),
('E-036', 'V-008', 'T-003', '2023-09-01', 'Rear Quarter', '6.0', '$55/hr', '$800.00', 'Approved'),
('E-037', 'V-009', 'T-004', '2023-09-05', 'Front Bumper', '4.0', '$55/hr', '$350.00', 'Approved'),
('E-038', 'V-011', 'T-005', '2023-09-10', 'Hood', '5.0', '$65/hr', '$450.00', 'Approved'),
('E-039', 'V-013', 'T-001', '2023-09-15', 'LF Door', '4.5', '$55/hr', '$380.00', 'Approved'),
('E-040', 'V-001', 'T-002', '2023-09-20', 'Windshield', '2.0', '$60/hr', '$400.00', 'Approved'),
('E-041', 'V-003', 'T-003', '2023-09-25', 'Rear Bumper', '3.5', '$60/hr', '$420.00', 'Approved'),
('E-042', 'V-004', 'T-004', '2023-10-01', 'Hood', '5.0', '$55/hr', '$500.00', 'Pending'),
('E-043', 'V-006', 'T-005', '2023-10-05', 'Front Bumper', '6.0', '$75/hr', '$900.00', 'Pending'),
('E-044', 'V-008', 'T-001', '2023-10-10', 'Side Mirror', '1.0', '$55/hr', '$150.00', 'Approved'),
('E-045', 'V-009', 'T-002', '2023-10-15', 'Rear Quarter', '5.5', '$60/hr', '$700.00', 'Approved'),
('E-046', 'V-011', 'T-003', '2023-10-20', 'Front Bumper', '4.0', '$65/hr', '$400.00', 'Approved'),
('E-047', 'V-013', 'T-004', '2023-10-25', 'Hood', '4.5', '$55/hr', '$350.00', 'Approved'),
('E-048', 'V-014', 'T-005', '2023-11-01', 'Bed Side', '6.0', '$55/hr', '$600.00', 'Approved'),
('E-049', 'V-015', 'T-001', '2023-11-05', 'Front Bumper', '3.0', '$55/hr', '$280.00', 'Rejected'),
('E-050', 'V-015', 'T-006', '2023-11-10', 'Front Bumper', '4.0', '$45/hr', '$280.00', 'Pending');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze auto body repairs in Metro_Auto_Body.Repair_Data.
 *  
 *  1. Bronze: Raw Views of VEHICLES, TECHNICIANS, ESTIMATES.
 *  2. Silver: 
 *     - VIN: REPLACE 'O'->'0', 'I'->'1'. Validate length=17.
 *     - Make: Map 'Chevy' -> 'Chevrolet', 'Toy.' -> 'Toyota'. UPPER all.
 *     - Labor Hrs: Parse '4h30m'->4.5, '4.5 hrs'->4.5. CAST to DOUBLE.
 *     - Labor Rate: Strip '$', '/hr', 'per hour'. CAST to DOUBLE.
 *     - Parts Cost: Strip '$' and commas. Handle 'TBD' -> NULL.
 *     - Damage Area: Normalize 'Rt'/'R/F'/'R.' -> 'Right', 'LF'/'L/F' -> 'Left Front'.
 *  3. Gold: 
 *     - Total Repair Cost per Vehicle (SUM of labor + parts across estimates).
 *     - Technician Productivity: Avg estimates per month.
 *  
 *  Show the SQL."
 */
