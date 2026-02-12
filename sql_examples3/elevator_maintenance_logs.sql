/*
 * Dremio "Messy Data" Challenge: Elevator Maintenance Logs
 * 
 * Scenario: 
 * Property management company consolidating elevator service records.
 * 3 Tables: BUILDINGS, ELEVATORS, WORK_ORDERS.
 * 
 * Objective for AI Agent:
 * 1. Floor Normalization: Unify 'B2' / '-2' / 'SB' / 'Sub-Basement' to standard notation.
 * 2. Priority Harmonization: Map '1' / 'URGENT' / 'P1' / 'Emergency' to unified levels.
 * 3. Schedule Conflict Detection: Identify overlapping maintenance windows for same elevator.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Metro_Properties;
CREATE FOLDER IF NOT EXISTS Metro_Properties.Elevator_Ops;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Metro_Properties.Elevator_Ops.BUILDINGS (
    BUILDING_ID VARCHAR,
    BUILDING_NAME VARCHAR,
    ADDRESS VARCHAR,
    FLOORS_ABOVE INT,
    FLOORS_BELOW INT
);

CREATE TABLE IF NOT EXISTS Metro_Properties.Elevator_Ops.ELEVATORS (
    ELEVATOR_ID VARCHAR,
    BUILDING_ID VARCHAR,
    ELEV_NUM VARCHAR, -- '1', '2', 'A', 'Freight'
    CAPACITY_LBS INT,
    INSTALL_YEAR INT
);

CREATE TABLE IF NOT EXISTS Metro_Properties.Elevator_Ops.WORK_ORDERS (
    WO_ID VARCHAR,
    ELEVATOR_ID VARCHAR,
    TECH_ID VARCHAR, -- Old format: '1001', new format: 'T-1001'
    PRIORITY_RAW VARCHAR, -- '1', 'URGENT', 'P1', 'Emergency', 'Low', '3'
    REPORTED_FLOOR VARCHAR, -- 'B2', '-2', 'SB', 'Lobby', '15', 'PH'
    ISSUE_DESC VARCHAR,
    SCHED_START TIMESTAMP,
    SCHED_END TIMESTAMP,
    WO_STATUS VARCHAR -- 'Open', 'OPEN', 'Closed', 'In Progress', 'IP'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- BUILDINGS (5 rows)
INSERT INTO Metro_Properties.Elevator_Ops.BUILDINGS VALUES
('BLD-001', 'One Metro Tower', '100 Main St', 40, 3),
('BLD-002', 'Parkside Plaza', '200 Park Ave', 25, 2),
('BLD-003', 'Harbor View', '300 Water St', 15, 1),
('BLD-004', 'University Center', '400 Campus Dr', 10, 2),
('BLD-005', 'Grand Hotel', '500 Broadway', 30, 3);

-- ELEVATORS (10 rows)
INSERT INTO Metro_Properties.Elevator_Ops.ELEVATORS VALUES
('ELV-001', 'BLD-001', '1', 3500, 2005),
('ELV-002', 'BLD-001', '2', 3500, 2005),
('ELV-003', 'BLD-001', 'Freight', 8000, 2000),
('ELV-004', 'BLD-002', 'A', 3000, 2010),
('ELV-005', 'BLD-002', 'B', 3000, 2010),
('ELV-006', 'BLD-003', '1', 2500, 2015),
('ELV-007', 'BLD-004', '1', 2500, 2018),
('ELV-008', 'BLD-004', '2', 2500, 2018),
('ELV-009', 'BLD-005', '1', 4000, 2008),
('ELV-010', 'BLD-005', 'Service', 6000, 2003);

-- WORK_ORDERS (50+ rows with all the mess)
INSERT INTO Metro_Properties.Elevator_Ops.WORK_ORDERS VALUES
-- Priority variations
('WO-001', 'ELV-001', '1001', '1', '15', 'Door not closing', '2023-01-10 08:00:00', '2023-01-10 12:00:00', 'Closed'),
('WO-002', 'ELV-001', 'T-1001', 'URGENT', 'Lobby', 'Stuck between floors', '2023-01-15 09:00:00', '2023-01-15 11:00:00', 'Closed'),
('WO-003', 'ELV-002', '1002', 'P1', 'B1', 'Emergency call stuck', '2023-01-20 10:00:00', '2023-01-20 14:00:00', 'Closed'),
('WO-004', 'ELV-002', 'T-1002', 'Emergency', '-1', 'Fire alarm override', '2023-02-01 07:00:00', '2023-02-01 10:00:00', 'Closed'),
('WO-005', 'ELV-003', '1003', 'Low', 'B3', 'Routine inspection', '2023-02-05 09:00:00', '2023-02-05 17:00:00', 'Closed'),
('WO-006', 'ELV-003', 'T-1003', '3', 'SB', 'Light bulb replacement', '2023-02-10 13:00:00', '2023-02-10 14:00:00', 'Closed'),
('WO-007', 'ELV-004', '1004', 'P2', 'L', 'Noise complaint', '2023-02-15 10:00:00', '2023-02-15 12:00:00', 'Closed'),
('WO-008', 'ELV-004', 'T-1004', 'Medium', 'G', 'Leveling issue', '2023-02-20 08:00:00', '2023-02-20 16:00:00', 'Closed'),
('WO-009', 'ELV-005', '1005', '2', '10', 'Annual inspection', '2023-03-01 09:00:00', '2023-03-01 17:00:00', 'Closed'),
('WO-010', 'ELV-005', 'T-1005', 'Normal', '15', 'Cable inspection', '2023-03-05 09:00:00', '2023-03-05 17:00:00', 'Closed'),
-- Floor notation mess
('WO-011', 'ELV-006', '1006', '1', 'B1', 'Motor overheating', '2023-03-10 08:00:00', '2023-03-10 12:00:00', 'Closed'),
('WO-012', 'ELV-006', 'T-1006', 'URGENT', 'Basement', 'Same motor issue', '2023-03-12 08:00:00', '2023-03-12 16:00:00', 'Closed'),
('WO-013', 'ELV-007', '1007', 'P1', 'Sub-Basement', 'Water leak in pit', '2023-03-15 07:00:00', '2023-03-15 11:00:00', 'Closed'),
('WO-014', 'ELV-007', 'T-1007', '1', '-2', 'Sump pump failure', '2023-03-20 08:00:00', '2023-03-20 12:00:00', 'Closed'),
('WO-015', 'ELV-008', '1008', 'Low', 'PH', 'Penthouse button sticking', '2023-03-25 10:00:00', '2023-03-25 11:00:00', 'Closed'),
('WO-016', 'ELV-008', 'T-1008', '3', 'Penthouse', 'Same button issue', '2023-03-28 10:00:00', '2023-03-28 11:30:00', 'Closed'),
-- Overlapping schedules (conflict!)
('WO-017', 'ELV-009', '1009', 'P2', '20', 'Annual cert inspection', '2023-04-01 09:00:00', '2023-04-01 17:00:00', 'Open'),
('WO-018', 'ELV-009', 'T-1009', 'URGENT', '5', 'Emergency entrapment', '2023-04-01 11:00:00', '2023-04-01 13:00:00', 'Open'),  -- Overlaps WO-017!
('WO-019', 'ELV-010', '1010', '2', '-3', 'Hydraulic fluid leak', '2023-04-05 08:00:00', '2023-04-05 16:00:00', 'In Progress'),
('WO-020', 'ELV-010', 'T-1010', 'Medium', '-3', 'Shaft cleaning', '2023-04-05 10:00:00', '2023-04-05 14:00:00', 'IP'),  -- Overlaps WO-019!
-- Status inconsistency
('WO-021', 'ELV-001', '1001', 'Low', '30', 'Routine maintenance', '2023-04-10 09:00:00', '2023-04-10 17:00:00', 'OPEN'),
('WO-022', 'ELV-002', 'T-1002', 'P3', '25', 'Button panel cleaning', '2023-04-15 10:00:00', '2023-04-15 12:00:00', 'open'),
('WO-023', 'ELV-003', '1003', 'Low', 'B2', 'Lubrication', '2023-04-20 09:00:00', '2023-04-20 11:00:00', 'In Progress'),
('WO-024', 'ELV-004', 'T-1004', '3', 'G', 'Mirror replacement', '2023-04-25 14:00:00', '2023-04-25 15:00:00', 'IP'),
-- Duplicates
('WO-025', 'ELV-001', '1001', '1', '15', 'Door not closing', '2023-01-10 08:00:00', '2023-01-10 12:00:00', 'Closed'),  -- Dupe of WO-001
-- Orphans
('WO-026', 'ELV-999', '1001', 'P1', '5', 'Unknown elevator', '2023-05-01 08:00:00', '2023-05-01 12:00:00', 'Open'),
('WO-027', 'ELV-001', 'T-9999', 'Low', '10', 'Unknown tech', '2023-05-05 09:00:00', '2023-05-05 11:00:00', 'Open'),
-- Bulk fill
('WO-028', 'ELV-001', 'T-1001', 'Low', '20', 'Routine check', '2023-05-10 09:00:00', '2023-05-10 11:00:00', 'Closed'),
('WO-029', 'ELV-002', '1002', '2', '18', 'Cable tension check', '2023-05-15 09:00:00', '2023-05-15 12:00:00', 'Closed'),
('WO-030', 'ELV-003', 'T-1003', 'Low', 'B1', 'Pit cleanup', '2023-05-20 09:00:00', '2023-05-20 11:00:00', 'Closed'),
('WO-031', 'ELV-004', '1004', '2', '8', 'Noise investigation', '2023-05-25 10:00:00', '2023-05-25 14:00:00', 'Closed'),
('WO-032', 'ELV-005', 'T-1005', 'Low', '12', 'Annual inspection', '2023-06-01 09:00:00', '2023-06-01 17:00:00', 'Closed'),
('WO-033', 'ELV-006', '1006', 'P2', '5', 'Door alignment', '2023-06-05 10:00:00', '2023-06-05 14:00:00', 'Closed'),
('WO-034', 'ELV-007', 'T-1007', 'Normal', '3', 'Safety check', '2023-06-10 09:00:00', '2023-06-10 12:00:00', 'Closed'),
('WO-035', 'ELV-008', '1008', 'Low', '7', 'Fan replacement', '2023-06-15 13:00:00', '2023-06-15 15:00:00', 'Closed'),
('WO-036', 'ELV-009', 'T-1009', '2', '15', 'Weight sensor recalibration', '2023-06-20 09:00:00', '2023-06-20 13:00:00', 'Closed'),
('WO-037', 'ELV-010', '1010', 'Low', '-2', 'Hydraulic top-up', '2023-06-25 09:00:00', '2023-06-25 11:00:00', 'Closed'),
('WO-038', 'ELV-001', 'T-1001', 'P2', '35', 'Intercom repair', '2023-07-01 10:00:00', '2023-07-01 12:00:00', 'Closed'),
('WO-039', 'ELV-002', '1002', 'Low', '22', 'Light upgrade to LED', '2023-07-05 09:00:00', '2023-07-05 17:00:00', 'Closed'),
('WO-040', 'ELV-003', 'T-1003', '3', 'B2', 'Rust treatment', '2023-07-10 09:00:00', '2023-07-10 15:00:00', 'Closed'),
('WO-041', 'ELV-004', '1004', 'P1', 'G', 'Entrapment event', '2023-07-15 16:00:00', '2023-07-15 18:00:00', 'Closed'),
('WO-042', 'ELV-005', 'T-1005', 'URGENT', '20', 'Brake failure', '2023-07-20 08:00:00', '2023-07-20 20:00:00', 'Closed'),
('WO-043', 'ELV-006', '1006', 'Low', '10', 'Cosmetic update', '2023-07-25 09:00:00', '2023-07-25 17:00:00', 'Closed'),
('WO-044', 'ELV-007', 'T-1007', '2', '5', 'Door sensor calibration', '2023-08-01 10:00:00', '2023-08-01 12:00:00', 'Closed'),
('WO-045', 'ELV-008', '1008', 'Low', '9', 'Floor indicator fix', '2023-08-05 14:00:00', '2023-08-05 16:00:00', 'Closed'),
('WO-046', 'ELV-009', 'T-1009', 'Normal', '25', 'Governor test', '2023-08-10 09:00:00', '2023-08-10 13:00:00', 'Closed'),
('WO-047', 'ELV-010', '1010', '2', '-1', 'Valve inspection', '2023-08-15 09:00:00', '2023-08-15 11:00:00', 'Closed'),
('WO-048', 'ELV-001', 'T-1001', 'Low', '5', 'Handrail tightening', '2023-08-20 10:00:00', '2023-08-20 11:00:00', 'Closed'),
('WO-049', 'ELV-002', '1002', 'P3', '1', 'Phone line test', '2023-08-25 09:00:00', '2023-08-25 10:00:00', 'Closed'),
('WO-050', 'ELV-003', 'T-1003', 'Low', 'B3', 'Full overhaul planning', '2023-09-01 09:00:00', '2023-09-01 17:00:00', 'Open');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Manage elevator maintenance in Metro_Properties.Elevator_Ops.
 *  
 *  1. Bronze: Raw Views of BUILDINGS, ELEVATORS, WORK_ORDERS.
 *  2. Silver: 
 *     - Floor Normal: Map 'B1'/'Basement'/'-1' -> -1, 'B2'/'-2'/'SB'/'Sub-Basement' -> -2, 'L'/'G'/'Lobby' -> 0, 'PH'/'Penthouse' -> max floor + 1.
 *     - Priority: Map '1'/'P1'/'URGENT'/'Emergency' -> 'CRITICAL', '2'/'P2'/'Medium'/'Normal' -> 'STANDARD', '3'/'P3'/'Low' -> 'ROUTINE'.
 *     - Status: UPPER(WO_STATUS). Map 'IP' -> 'IN PROGRESS'.
 *  3. Gold: 
 *     - Schedule Conflicts: Self-join WORK_ORDERS for same elevator with overlapping SCHED_START/SCHED_END.
 *     - MTTR (Mean Time To Repair) per building.
 *  
 *  Show the SQL."
 */
