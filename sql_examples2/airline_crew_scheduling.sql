/*
 * Transportation: Airline Crew Scheduling
 * 
 * Scenario:
 * Tracking crew duty hours, rest periods, and pairing legality to prevent violations.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AirlineOpsDB;
CREATE FOLDER IF NOT EXISTS AirlineOpsDB.Crewing;
CREATE FOLDER IF NOT EXISTS AirlineOpsDB.Crewing.Bronze;
CREATE FOLDER IF NOT EXISTS AirlineOpsDB.Crewing.Silver;
CREATE FOLDER IF NOT EXISTS AirlineOpsDB.Crewing.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- CrewRoster Table
CREATE TABLE IF NOT EXISTS AirlineOpsDB.Crewing.Bronze.CrewRoster (
    CrewID VARCHAR,
    FullName VARCHAR,
    CrewRole VARCHAR, -- Pilot, First Officer, Cabin Crew
    BaseAirport VARCHAR
);

INSERT INTO AirlineOpsDB.Crewing.Bronze.CrewRoster VALUES
('C-001', 'John Smith', 'Pilot', 'JFK'),
('C-002', 'Jane Doe', 'First Officer', 'JFK'),
('C-003', 'Bob Brown', 'Cabin Crew', 'JFK'),
('C-004', 'Alice Green', 'Cabin Crew', 'JFK'),
('C-005', 'Mike White', 'Cabin Crew', 'JFK'),
('C-006', 'Sarah Black', 'Pilot', 'LHR'),
('C-007', 'Tom Blue', 'First Officer', 'LHR'),
('C-008', 'Emily Red', 'Cabin Crew', 'LHR'),
('C-009', 'David Orange', 'Cabin Crew', 'LHR'),
('C-010', 'Lisa Purple', 'Cabin Crew', 'LHR'),
('C-011', 'Kevin Grey', 'Pilot', 'LAX'),
('C-012', 'Nancy Pink', 'First Officer', 'LAX'),
('C-013', 'Chris Yellow', 'Cabin Crew', 'LAX'),
('C-014', 'Pat Gold', 'Cabin Crew', 'LAX'),
('C-015', 'Kelly Silver', 'Cabin Crew', 'LAX'),
('C-016', 'George Cyan', 'Pilot', 'ORD'),
('C-017', 'Maria Magenta', 'First Officer', 'ORD'),
('C-018', 'Steve Lime', 'Cabin Crew', 'ORD'),
('C-019', 'Rachel Olive', 'Cabin Crew', 'ORD'),
('C-020', 'Frank Teal', 'Cabin Crew', 'ORD'),
('C-021', 'Amy Indigo', 'Pilot', 'MIA'),
('C-022', 'Brian Violet', 'First Officer', 'MIA'),
('C-023', 'Cathy Rose', 'Cabin Crew', 'MIA'),
('C-024', 'Dan Brown', 'Cabin Crew', 'MIA'),
('C-025', 'Eva Black', 'Cabin Crew', 'MIA'),
('C-026', 'Fred Green', 'Pilot', 'DFW'),
('C-027', 'Gina White', 'First Officer', 'DFW'),
('C-028', 'Hank Blue', 'Cabin Crew', 'DFW'),
('C-029', 'Iris Red', 'Cabin Crew', 'DFW'),
('C-030', 'Jack Orange', 'Cabin Crew', 'DFW'),
('C-031', 'Kara Purple', 'Pilot', 'SEA'),
('C-032', 'Leo Grey', 'First Officer', 'SEA'),
('C-033', 'Mia Pink', 'Cabin Crew', 'SEA'),
('C-034', 'Nick Yellow', 'Cabin Crew', 'SEA'),
('C-035', 'Olga Gold', 'Cabin Crew', 'SEA'),
('C-036', 'Paul Silver', 'Pilot', 'SFO'),
('C-037', 'Queen Cyan', 'First Officer', 'SFO'),
('C-038', 'Rob Magenta', 'Cabin Crew', 'SFO'),
('C-039', 'Sue Lime', 'Cabin Crew', 'SFO'),
('C-040', 'Tim Olive', 'Cabin Crew', 'SFO'),
('C-041', 'Uma Teal', 'Pilot', 'BOS'),
('C-042', 'Victor Indigo', 'First Officer', 'BOS'),
('C-043', 'Wendy Violet', 'Cabin Crew', 'BOS'),
('C-044', 'Xander Rose', 'Cabin Crew', 'BOS'),
('C-045', 'Yara Brown', 'Cabin Crew', 'BOS'),
('C-046', 'Zack Black', 'Pilot', 'ATL'),
('C-047', 'Abby Green', 'First Officer', 'ATL'),
('C-048', 'Ben White', 'Cabin Crew', 'ATL'),
('C-049', 'Cara Blue', 'Cabin Crew', 'ATL'),
('C-050', 'Doug Red', 'Cabin Crew', 'ATL');

-- FlightSegments Table
CREATE TABLE IF NOT EXISTS AirlineOpsDB.Crewing.Bronze.FlightSegments (
    SegmentID INT,
    CrewID VARCHAR,
    FlightNumber VARCHAR,
    DepartureTime TIMESTAMP,
    ArrivalTime TIMESTAMP,
    DutyType VARCHAR -- Flight, Deadhead, Reserve
);

INSERT INTO AirlineOpsDB.Crewing.Bronze.FlightSegments VALUES
(101, 'C-001', 'BA101', '2025-06-01 08:00:00', '2025-06-01 16:00:00', 'Flight'),
(102, 'C-002', 'BA101', '2025-06-01 08:00:00', '2025-06-01 16:00:00', 'Flight'),
(103, 'C-003', 'BA101', '2025-06-01 08:00:00', '2025-06-01 16:00:00', 'Flight'),
(104, 'C-001', 'BA102', '2025-06-02 10:00:00', '2025-06-02 18:00:00', 'Flight'), -- Return
(105, 'C-006', 'VS201', '2025-06-01 09:00:00', '2025-06-01 20:00:00', 'Flight'), -- Long haul
(106, 'C-006', 'VS201', '2025-06-02 09:00:00', '2025-06-02 10:00:00', 'Deadhead'),
(107, 'C-011', 'AA301', '2025-06-01 07:00:00', '2025-06-01 12:00:00', 'Flight'),
(108, 'C-011', 'AA302', '2025-06-01 14:00:00', '2025-06-01 19:00:00', 'Flight'),
(109, 'C-016', 'UA401', '2025-06-01 06:00:00', '2025-06-01 22:00:00', 'Flight'), -- Very long duty, risk?
(110, 'C-021', 'DL501', '2025-06-01 08:00:00', '2025-06-01 11:00:00', 'Flight'),
(111, 'C-021', 'DL502', '2025-06-01 12:00:00', '2025-06-01 15:00:00', 'Flight'),
(112, 'C-021', 'DL503', '2025-06-01 16:00:00', '2025-06-01 19:00:00', 'Flight'),
(113, 'C-026', 'SW601', '2025-06-01 09:00:00', '2025-06-01 10:00:00', 'Reserve'),
(114, 'C-031', 'AS701', '2025-06-01 08:00:00', '2025-06-01 12:00:00', 'Flight'),
(115, 'C-036', 'UA801', '2025-06-01 10:00:00', '2025-06-01 23:00:00', 'Flight'), -- 13 hours
(116, 'C-041', 'JB901', '2025-06-01 07:00:00', '2025-06-01 09:00:00', 'Flight'),
(117, 'C-041', 'JB902', '2025-06-01 10:00:00', '2025-06-01 12:00:00', 'Flight'),
(118, 'C-041', 'JB903', '2025-06-01 13:00:00', '2025-06-01 15:00:00', 'Flight'),
(119, 'C-041', 'JB904', '2025-06-01 16:00:00', '2025-06-01 18:00:00', 'Flight'),
(120, 'C-046', 'DL001', '2025-06-01 23:00:00', '2025-06-02 07:00:00', 'Flight'), -- Red eye
(121, 'C-002', 'BA101', '2025-06-05 08:00:00', '2025-06-05 16:00:00', 'Flight'),
(122, 'C-003', 'BA101', '2025-06-05 08:00:00', '2025-06-05 16:00:00', 'Flight'),
(123, 'C-004', 'BA103', '2025-06-01 08:00:00', '2025-06-01 16:00:00', 'Flight'),
(124, 'C-005', 'BA103', '2025-06-01 08:00:00', '2025-06-01 16:00:00', 'Flight'),
(125, 'C-007', 'VS202', '2025-06-01 09:00:00', '2025-06-01 20:00:00', 'Flight'),
(126, 'C-008', 'VS202', '2025-06-01 09:00:00', '2025-06-01 20:00:00', 'Flight'),
(127, 'C-012', 'AA303', '2025-06-01 07:00:00', '2025-06-01 12:00:00', 'Flight'),
(128, 'C-013', 'AA303', '2025-06-01 07:00:00', '2025-06-01 12:00:00', 'Flight'),
(129, 'C-017', 'UA402', '2025-06-01 06:00:00', '2025-06-01 22:00:00', 'Flight'),
(130, 'C-022', 'DL504', '2025-06-01 08:00:00', '2025-06-01 11:00:00', 'Flight'),
(131, 'C-027', 'SW602', '2025-06-01 09:00:00', '2025-06-01 10:00:00', 'Flight'),
(132, 'C-032', 'AS702', '2025-06-01 08:00:00', '2025-06-01 12:00:00', 'Flight'),
(133, 'C-037', 'UA802', '2025-06-01 10:00:00', '2025-06-01 23:00:00', 'Flight'),
(134, 'C-042', 'JB905', '2025-06-01 07:00:00', '2025-06-01 09:00:00', 'Flight'),
(135, 'C-047', 'DL002', '2025-06-01 23:00:00', '2025-06-02 07:00:00', 'Flight'),
(136, 'C-001', 'BA105', '2025-06-05 08:00:00', '2025-06-05 16:00:00', 'Flight'),
(137, 'C-006', 'VS205', '2025-06-05 09:00:00', '2025-06-05 20:00:00', 'Flight'),
(138, 'C-011', 'AA305', '2025-06-05 07:00:00', '2025-06-05 12:00:00', 'Flight'),
(139, 'C-016', 'UA405', '2025-06-05 06:00:00', '2025-06-05 22:00:00', 'Flight'),
(140, 'C-021', 'DL505', '2025-06-05 08:00:00', '2025-06-05 11:00:00', 'Flight'),
(141, 'C-026', 'SW605', '2025-06-05 09:00:00', '2025-06-05 10:00:00', 'Flight'),
(142, 'C-031', 'AS705', '2025-06-05 08:00:00', '2025-06-05 12:00:00', 'Flight'),
(143, 'C-036', 'UA805', '2025-06-05 10:00:00', '2025-06-05 23:00:00', 'Flight'),
(144, 'C-041', 'JB905', '2025-06-05 07:00:00', '2025-06-05 09:00:00', 'Flight'),
(145, 'C-046', 'DL005', '2025-06-05 23:00:00', '2025-06-06 07:00:00', 'Flight'),
(146, 'C-050', 'DL005', '2025-06-05 23:00:00', '2025-06-06 07:00:00', 'Flight'),
(147, 'C-049', 'DL005', '2025-06-05 23:00:00', '2025-06-06 07:00:00', 'Flight'),
(148, 'C-048', 'DL005', '2025-06-05 23:00:00', '2025-06-06 07:00:00', 'Flight'),
(149, 'C-045', 'JB905', '2025-06-05 07:00:00', '2025-06-05 09:00:00', 'Flight'),
(150, 'C-044', 'JB905', '2025-06-05 07:00:00', '2025-06-05 09:00:00', 'Flight');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Duty Time Calculations
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AirlineOpsDB.Crewing.Silver.DailyDuty AS
SELECT 
    f.CrewID,
    r.FullName,
    r.CrewRole,
    CAST(f.DepartureTime AS DATE) AS DutyDate,
    -- Calculate duty hours from minutes variance (more robust standard SQL for Dremio)
    -- (Unix Timestamp diff / 3600)
    (EXTRACT(EPOCH FROM f.ArrivalTime) - EXTRACT(EPOCH FROM f.DepartureTime)) / 3600.0 AS TotalDutyHours
FROM AirlineOpsDB.Crewing.Bronze.FlightSegments f
JOIN AirlineOpsDB.Crewing.Bronze.CrewRoster r ON f.CrewID = r.CrewID
GROUP BY f.CrewID, r.FullName, r.CrewRole, CAST(f.DepartureTime AS DATE), f.ArrivalTime, f.DepartureTime;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Legality Violations
-------------------------------------------------------------------------------

-- Identifying crew who exceeded 14 hours of duty in a single day
CREATE OR REPLACE VIEW AirlineOpsDB.Crewing.Gold.Violations AS
SELECT 
    CrewID,
    FullName,
    CrewRole,
    DutyDate,
    SUM(TotalDutyHours) AS TotalDutyHours,
    'Exceeded 14h Limit' AS ViolationType
FROM AirlineOpsDB.Crewing.Silver.DailyDuty
GROUP BY CrewID, FullName, CrewRole, DutyDate
HAVING SUM(TotalDutyHours) > 14.0;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all pilots who have a duty period longer than 12 hours from the AirlineOpsDB.Crewing.Silver.DailyDuty view."

PROMPT 2:
"Count the number of 'Deadhead' segments for crew member 'C-006' in the Bronze layer."

PROMPT 3:
"Show all violations for the 'Cabin Crew' role in the Gold layer."
*/
