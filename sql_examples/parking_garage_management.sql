/*
 * Parking Garage Management Demo
 * 
 * Scenario:
 * Monitoring occupancy, duration of stay, and revenue by vehicle type.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize pricing and space utilization.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ParkingDB;
CREATE FOLDER IF NOT EXISTS ParkingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ParkingDB.Silver;
CREATE FOLDER IF NOT EXISTS ParkingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ParkingDB.Bronze.Spaces (
    SpaceID INT,
    FloorLevel INT,
    Type VARCHAR -- 'Standard', 'Compact', 'EV', 'Handicap'
);

CREATE TABLE IF NOT EXISTS ParkingDB.Bronze.Sessions (
    TicketID INT,
    SpaceID INT,
    EntryTime TIMESTAMP,
    ExitTime TIMESTAMP,
    RateCode VARCHAR -- 'Hourly', 'Daily', 'Monthly'
);

INSERT INTO ParkingDB.Bronze.Spaces VALUES
(101, 1, 'Standard'),
(102, 1, 'EV');

INSERT INTO ParkingDB.Bronze.Sessions VALUES
(1, 101, '2025-01-01 08:00:00', '2025-01-01 10:00:00', 'Hourly'),
(2, 102, '2025-01-01 09:00:00', '2025-01-01 17:00:00', 'Daily');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ParkingDB.Silver.Occupancy AS
SELECT 
    s.TicketID,
    sp.FloorLevel,
    sp.Type,
    s.EntryTime,
    s.ExitTime,
    TIMESTAMPDIFF(MINUTE, s.EntryTime, s.ExitTime) / 60.0 AS DurationHours,
    CASE 
        WHEN s.RateCode = 'Hourly' THEN (TIMESTAMPDIFF(MINUTE, s.EntryTime, s.ExitTime) / 60.0) * 5.0
        WHEN s.RateCode = 'Daily' THEN 25.0
        ELSE 0 
    END AS Revenue
FROM ParkingDB.Bronze.Sessions s
JOIN ParkingDB.Bronze.Spaces sp ON s.SpaceID = sp.SpaceID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ParkingDB.Gold.RevenueByFloor AS
SELECT 
    FloorLevel,
    COUNT(TicketID) AS TotalCars,
    AVG(DurationHours) AS AvgStay,
    SUM(Revenue) AS TotalRevenue
FROM ParkingDB.Silver.Occupancy
GROUP BY FloorLevel;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which FloorLevel generated the most TotalRevenue in ParkingDB.Gold.RevenueByFloor?"
*/
