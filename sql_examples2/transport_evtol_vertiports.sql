/*
 * Transportation: Urban Air Mobility (UAM) Vertiports
 * 
 * Scenario:
 * Managing approach, landing, and charging schedules for electric vertical takeoff and landing (eVTOL) air Taxis.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS UrbanAirDB;
CREATE FOLDER IF NOT EXISTS UrbanAirDB.Vertiports;
CREATE FOLDER IF NOT EXISTS UrbanAirDB.Vertiports.Bronze;
CREATE FOLDER IF NOT EXISTS UrbanAirDB.Vertiports.Silver;
CREATE FOLDER IF NOT EXISTS UrbanAirDB.Vertiports.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Vertiport Schedule
-------------------------------------------------------------------------------

-- LandingSlots Table
CREATE TABLE IF NOT EXISTS UrbanAirDB.Vertiports.Bronze.LandingSlots (
    SlotID INT,
    VertiportCode VARCHAR, -- NYC-01, LON-02
    VehicleID VARCHAR,
    ScheduledTime TIMESTAMP,
    ActualTime TIMESTAMP,
    ChargingRequired BOOLEAN,
    PassengerCount INT
);

INSERT INTO UrbanAirDB.Vertiports.Bronze.LandingSlots VALUES
(1, 'NYC-01', 'EV-101', '2026-05-01 08:00:00', '2026-05-01 08:00:00', true, 4),
(2, 'NYC-01', 'EV-102', '2026-05-01 08:05:00', '2026-05-01 08:06:00', false, 3),
(3, 'NYC-01', 'EV-103', '2026-05-01 08:10:00', '2026-05-01 08:15:00', true, 2), -- Late
(4, 'NYC-01', 'EV-104', '2026-05-01 08:15:00', '2026-05-01 08:25:00', true, 4), -- Very Late
(5, 'LON-02', 'EV-201', '2026-05-01 08:00:00', '2026-05-01 07:55:00', false, 1), -- Early
(6, 'LON-02', 'EV-202', '2026-05-01 08:05:00', '2026-05-01 08:05:00', true, 2),
(7, 'NYC-01', 'EV-105', '2026-05-01 08:20:00', '2026-05-01 08:20:00', false, 4),
(8, 'NYC-01', 'EV-106', '2026-05-01 08:25:00', '2026-05-01 08:28:00', true, 3),
(9, 'NYC-01', 'EV-107', '2026-05-01 08:30:00', '2026-05-01 08:31:00', false, 2),
(10, 'NYC-01', 'EV-108', '2026-05-01 08:35:00', '2026-05-01 08:45:00', false, 1), -- Late
(11, 'LON-02', 'EV-203', '2026-05-01 08:10:00', '2026-05-01 08:10:00', true, 3),
(12, 'LON-02', 'EV-204', '2026-05-01 08:15:00', '2026-05-01 08:20:00', false, 4), -- Late
(13, 'NYC-01', 'EV-109', '2026-05-01 08:40:00', '2026-05-01 08:40:00', true, 4),
(14, 'NYC-01', 'EV-110', '2026-05-01 08:45:00', '2026-05-01 08:50:00', true, 2),
(15, 'LON-02', 'EV-205', '2026-05-01 08:20:00', '2026-05-01 08:20:00', false, 1),
(16, 'NYC-01', 'EV-111', '2026-05-01 08:50:00', '2026-05-01 08:55:00', false, 3),
(17, 'NYC-01', 'EV-112', '2026-05-01 08:55:00', '2026-05-01 09:05:00', true, 4), -- Late
(18, 'LON-02', 'EV-206', '2026-05-01 08:25:00', '2026-05-01 08:25:00', true, 2),
(19, 'LON-02', 'EV-207', '2026-05-01 08:30:00', '2026-05-01 08:32:00', false, 3),
(20, 'NYC-01', 'EV-113', '2026-05-01 09:00:00', '2026-05-01 09:00:00', true, 4),
(21, 'NYC-01', 'EV-114', '2026-05-01 09:05:00', '2026-05-01 09:05:00', false, 1),
(22, 'LON-02', 'EV-208', '2026-05-01 08:35:00', '2026-05-01 08:40:00', true, 4),
(23, 'LON-02', 'EV-209', '2026-05-01 08:40:00', '2026-05-01 08:40:00', false, 2),
(24, 'NYC-01', 'EV-115', '2026-05-01 09:10:00', '2026-05-01 09:12:00', true, 3),
(25, 'NYC-01', 'EV-116', '2026-05-01 09:15:00', '2026-05-01 09:25:00', true, 4), -- Late
(26, 'LON-02', 'EV-210', '2026-05-01 08:45:00', '2026-05-01 08:45:00', true, 1),
(27, 'NYC-01', 'EV-117', '2026-05-01 09:20:00', '2026-05-01 09:20:00', false, 2),
(28, 'NYC-01', 'EV-118', '2026-05-01 09:25:00', '2026-05-01 09:30:00', true, 3),
(29, 'LON-02', 'EV-211', '2026-05-01 08:50:00', '2026-05-01 08:50:00', false, 4),
(30, 'LON-02', 'EV-212', '2026-05-01 08:55:00', '2026-05-01 08:58:00', true, 2),
(31, 'NYC-01', 'EV-119', '2026-05-01 09:30:00', '2026-05-01 09:35:00', false, 4),
(32, 'NYC-01', 'EV-120', '2026-05-01 09:35:00', '2026-05-01 09:35:00', true, 1),
(33, 'LON-02', 'EV-213', '2026-05-01 09:00:00', '2026-05-01 09:05:00', true, 3),
(34, 'LON-02', 'EV-214', '2026-05-01 09:05:00', '2026-05-01 09:05:00', false, 4),
(35, 'NYC-01', 'EV-121', '2026-05-01 09:40:00', '2026-05-01 09:40:00', true, 2),
(36, 'NYC-01', 'EV-122', '2026-05-01 09:45:00', '2026-05-01 09:50:00', true, 3),
(37, 'LON-02', 'EV-215', '2026-05-01 09:10:00', '2026-05-01 09:10:00', false, 1),
(38, 'LON-02', 'EV-216', '2026-05-01 09:15:00', '2026-05-01 09:20:00', true, 2),
(39, 'NYC-01', 'EV-123', '2026-05-01 09:50:00', '2026-05-01 09:55:00', false, 4),
(40, 'NYC-01', 'EV-124', '2026-05-01 09:55:00', '2026-05-01 10:00:00', true, 2),
(41, 'LON-02', 'EV-217', '2026-05-01 09:20:00', '2026-05-01 09:20:00', false, 3),
(42, 'LON-02', 'EV-218', '2026-05-01 09:25:00', '2026-05-01 09:30:00', true, 4),
(43, 'NYC-01', 'EV-125', '2026-05-01 10:00:00', '2026-05-01 10:00:00', false, 1),
(44, 'NYC-01', 'EV-126', '2026-05-01 10:05:00', '2026-05-01 10:10:00', true, 3),
(45, 'LON-02', 'EV-219', '2026-05-01 09:30:00', '2026-05-01 09:30:00', false, 2),
(46, 'LON-02', 'EV-220', '2026-05-01 09:35:00', '2026-05-01 09:40:00', true, 4),
(47, 'NYC-01', 'EV-127', '2026-05-01 10:10:00', '2026-05-01 10:15:00', false, 2),
(48, 'NYC-01', 'EV-128', '2026-05-01 10:15:00', '2026-05-01 10:20:00', true, 3),
(49, 'LON-02', 'EV-221', '2026-05-01 09:40:00', '2026-05-01 09:40:00', false, 1),
(50, 'LON-02', 'EV-222', '2026-05-01 09:45:00', '2026-05-01 09:50:00', true, 4);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: On-Time Performance (OTP)
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW UrbanAirDB.Vertiports.Silver.Arrivals AS
SELECT 
    VertiportCode,
    VehicleID,
    ScheduledTime,
    ActualTime,
    PassengerCount,
    ChargingRequired,
    -- Extract Epoch for difference in minutes
    (EXTRACT(EPOCH FROM ActualTime) - EXTRACT(EPOCH FROM ScheduledTime)) / 60.0 AS DelayMinutes,
    CASE 
        WHEN (EXTRACT(EPOCH FROM ActualTime) - EXTRACT(EPOCH FROM ScheduledTime)) > 5 THEN 'Late'
        WHEN (EXTRACT(EPOCH FROM ActualTime) - EXTRACT(EPOCH FROM ScheduledTime)) < -5 THEN 'Early'
        ELSE 'On Time'
    END AS ArrivalStatus
FROM UrbanAirDB.Vertiports.Bronze.LandingSlots;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Capacity Planning
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW UrbanAirDB.Vertiports.Gold.PortUtilization AS
SELECT 
    VertiportCode,
    COUNT(VehicleID) AS TotalArrivals,
    AVG(DelayMinutes) AS AvgDelay,
    SUM(PassengerCount) AS TotalPax,
    SUM(CASE WHEN ChargingRequired = true THEN 1 ELSE 0 END) AS ChargingDemand,
    -- Calculate "Turnaround Pressure"
    (CAST(SUM(CASE WHEN ChargingRequired = true THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(VehicleID)) * 100 AS ChargingLoadPct
FROM UrbanAirDB.Vertiports.Silver.Arrivals
GROUP BY VertiportCode;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Compare the 'ChargingLoadPct' between 'NYC-01' and 'LON-02' airports in the Gold utilization report."

PROMPT 2:
"List all vehicles arriving 'Late' with a delay greater than 5 minutes in the Silver layer."

PROMPT 3:
"Calculate the total number of passengers handled by 'NYC-01' so far."
*/
