/*
 * Urban Planning: Bike Share Logistics
 * 
 * Scenario:
 * Analyzing bike share station rebalancing needs by calculating net flow (Returns vs Rentals).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CityTransitDB;
CREATE FOLDER IF NOT EXISTS CityTransitDB.Mobility;
CREATE FOLDER IF NOT EXISTS CityTransitDB.Mobility.Bronze;
CREATE FOLDER IF NOT EXISTS CityTransitDB.Mobility.Silver;
CREATE FOLDER IF NOT EXISTS CityTransitDB.Mobility.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- Stations Table
CREATE TABLE IF NOT EXISTS CityTransitDB.Mobility.Bronze.Stations (
    StationID INT,
    StationName VARCHAR,
    Capacity INT,
    Latitude DOUBLE,
    Longitude DOUBLE
);

INSERT INTO CityTransitDB.Mobility.Bronze.Stations VALUES
(1, 'Central Park South', 50, 40.76, -73.97),
(2, 'Times Square', 40, 40.75, -73.98),
(3, 'Union Square', 35, 40.73, -73.99),
(4, 'Penn Station', 60, 40.75, -73.99),
(5, 'Grand Central', 55, 40.75, -73.97),
(6, 'Wall St', 30, 40.70, -74.00),
(7, 'Brooklyn Bridge', 25, 40.70, -73.99),
(8, 'Williamsburg', 20, 40.71, -73.96),
(9, 'Hudson Yards', 30, 40.75, -74.00),
(10, 'SoHo', 25, 40.72, -74.00),
(11, 'East Village', 20, 40.72, -73.98),
(12, 'Chelsea Piers', 25, 40.74, -74.00),
(13, 'Columbus Circle', 45, 40.76, -73.98),
(14, 'Hell''s Kitchen', 30, 40.76, -73.99),
(15, 'Upper West Side', 35, 40.78, -73.97),
(16, 'Upper East Side', 35, 40.77, -73.95),
(17, 'Harlem 125th', 30, 40.80, -73.94),
(18, 'Columbia Univ', 40, 40.80, -73.96),
(19, 'Battery Park', 25, 40.70, -74.01),
(20, 'Tribeca', 20, 40.71, -74.00),
(21, 'Chinatown', 25, 40.71, -73.99),
(22, 'Lower East Side', 20, 40.71, -73.98),
(23, 'Gramercy', 20, 40.73, -73.98),
(24, 'Flatiron', 25, 40.74, -73.98),
(25, 'Murray Hill', 20, 40.74, -73.97),
(26, 'Midtown East', 30, 40.75, -73.96),
(27, 'Midtown West', 35, 40.76, -73.99),
(28, 'Lincoln Center', 30, 40.77, -73.98),
(29, 'Morningside', 25, 40.80, -73.95),
(30, 'Washington Heights', 25, 40.84, -73.93),
(31, 'Inwood', 20, 40.86, -73.92),
(32, 'DUMBO', 25, 40.70, -73.98),
(33, 'Brooklyn Heights', 25, 40.69, -73.99),
(34, 'Cobble Hill', 20, 40.68, -73.99),
(35, 'Carroll Gardens', 20, 40.68, -73.99),
(36, 'Park Slope', 30, 40.67, -73.97),
(37, 'Prospect Park', 35, 40.66, -73.96),
(38, 'Fort Greene', 25, 40.69, -73.97),
(39, 'Bushwick', 20, 40.70, -73.92),
(40, 'Astoria', 30, 40.76, -73.91),
(41, 'LIC', 35, 40.74, -73.94),
(42, 'Sunnyside', 25, 40.74, -73.91),
(43, 'Greenpoint', 25, 40.72, -73.95),
(44, 'Red Hook', 20, 40.67, -74.01),
(45, 'Roosevelt Island', 20, 40.75, -73.95),
(46, 'Governor''s Island', 25, 40.68, -74.01),
(47, 'Staten Island Ferry', 30, 40.64, -74.07),
(48, 'Hoboken Path', 30, 40.73, -74.02),
(49, 'Jersey City Exch', 35, 40.71, -74.03),
(50, 'Newport', 30, 40.72, -74.03);

-- RideLog Table
CREATE TABLE IF NOT EXISTS CityTransitDB.Mobility.Bronze.RideLog (
    RideID INT,
    StartStationID INT,
    EndStationID INT,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    UserType VARCHAR -- Subscriber, Guest
);

INSERT INTO CityTransitDB.Mobility.Bronze.RideLog VALUES
(1001, 1, 5, '2025-06-01 08:00:00', '2025-06-01 08:20:00', 'Subscriber'),
(1002, 1, 6, '2025-06-01 08:05:00', '2025-06-01 08:35:00', 'Subscriber'),
(1003, 2, 4, '2025-06-01 08:10:00', '2025-06-01 08:15:00', 'Guest'),
(1004, 3, 1, '2025-06-01 09:00:00', '2025-06-01 09:30:00', 'Subscriber'), -- Return to 1
(1005, 4, 1, '2025-06-01 09:10:00', '2025-06-01 09:35:00', 'Subscriber'), -- Return to 1
(1006, 5, 1, '2025-06-01 09:20:00', '2025-06-01 09:40:00', 'Guest'),      -- Return to 1
(1007, 6, 2, '2025-06-01 10:00:00', '2025-06-01 10:45:00', 'Subscriber'),
(1008, 1, 10, '2025-06-01 11:00:00', '2025-06-01 11:30:00', 'Subscriber'), -- Rental from 1
(1009, 1, 11, '2025-06-01 11:05:00', '2025-06-01 11:35:00', 'Guest'),      -- Rental from 1
(1010, 10, 12, '2025-06-01 12:00:00', '2025-06-01 12:15:00', 'Subscriber'),
(1011, 15, 1, '2025-06-01 13:00:00', '2025-06-01 13:20:00', 'Guest'),      -- Return to 1
(1012, 16, 17, '2025-06-01 13:10:00', '2025-06-01 13:30:00', 'Subscriber'),
(1013, 1, 20, '2025-06-01 14:00:00', '2025-06-01 14:40:00', 'Subscriber'), -- Rental from 1
(1014, 25, 26, '2025-06-01 15:00:00', '2025-06-01 15:10:00', 'Subscriber'),
(1015, 30, 31, '2025-06-01 15:30:00', '2025-06-01 15:45:00', 'Guest'),
(1016, 40, 41, '2025-06-01 16:00:00', '2025-06-01 16:15:00', 'Subscriber'),
(1017, 1, 5, '2025-06-01 16:30:00', '2025-06-01 16:50:00', 'Subscriber'), -- Rental from 1
(1018, 50, 1, '2025-06-01 17:00:00', '2025-06-01 17:40:00', 'Subscriber'), -- Return to 1
(1019, 45, 1, '2025-06-01 17:10:00', '2025-06-01 17:50:00', 'Guest'),      -- Return to 1
(1020, 2, 3, '2025-06-01 18:00:00', '2025-06-01 18:15:00', 'Subscriber'),
(1021, 3, 2, '2025-06-01 18:30:00', '2025-06-01 18:45:00', 'Subscriber'),
(1022, 5, 2, '2025-06-01 19:00:00', '2025-06-01 19:20:00', 'Subscriber'),
(1023, 7, 8, '2025-06-01 19:15:00', '2025-06-01 19:35:00', 'Guest'),
(1024, 8, 7, '2025-06-01 19:45:00', '2025-06-01 20:05:00', 'Guest'),
(1025, 9, 10, '2025-06-01 20:00:00', '2025-06-01 20:30:00', 'Subscriber'),
(1026, 12, 13, '2025-06-01 08:30:00', '2025-06-01 08:50:00', 'Subscriber'),
(1027, 13, 14, '2025-06-01 09:00:00', '2025-06-01 09:10:00', 'Subscriber'),
(1028, 14, 15, '2025-06-01 09:30:00', '2025-06-01 09:50:00', 'Subscriber'),
(1029, 15, 16, '2025-06-01 10:00:00', '2025-06-01 10:15:00', 'Subscriber'),
(1030, 16, 17, '2025-06-01 10:30:00', '2025-06-01 10:45:00', 'Guest'),
(1031, 17, 18, '2025-06-01 11:00:00', '2025-06-01 11:15:00', 'Subscriber'),
(1032, 18, 19, '2025-06-01 11:30:00', '2025-06-01 12:00:00', 'Subscriber'),
(1033, 19, 20, '2025-06-01 12:30:00', '2025-06-01 12:45:00', 'Subscriber'),
(1034, 20, 21, '2025-06-01 13:00:00', '2025-06-01 13:20:00', 'Guest'),
(1035, 21, 22, '2025-06-01 13:30:00', '2025-06-01 13:45:00', 'Subscriber'),
(1036, 22, 23, '2025-06-01 14:00:00', '2025-06-01 14:15:00', 'Subscriber'),
(1037, 23, 24, '2025-06-01 14:30:00', '2025-06-01 14:45:00', 'Subscriber'),
(1038, 24, 25, '2025-06-01 15:00:00', '2025-06-01 15:15:00', 'Subscriber'),
(1039, 25, 26, '2025-06-01 15:30:00', '2025-06-01 15:45:00', 'Guest'),
(1040, 26, 27, '2025-06-01 16:00:00', '2025-06-01 16:20:00', 'Subscriber'),
(1041, 27, 28, '2025-06-01 16:30:00', '2025-06-01 16:50:00', 'Subscriber'),
(1042, 28, 29, '2025-06-01 17:00:00', '2025-06-01 17:20:00', 'Subscriber'),
(1043, 29, 30, '2025-06-01 17:30:00', '2025-06-01 17:50:00', 'Subscriber'),
(1044, 30, 31, '2025-06-01 18:00:00', '2025-06-01 18:20:00', 'Guest'),
(1045, 31, 32, '2025-06-01 18:30:00', '2025-06-01 19:00:00', 'Subscriber'),
(1046, 32, 33, '2025-06-01 19:30:00', '2025-06-01 19:45:00', 'Subscriber'),
(1047, 33, 34, '2025-06-01 20:00:00', '2025-06-01 20:15:00', 'Subscriber'),
(1048, 34, 35, '2025-06-01 20:30:00', '2025-06-01 20:45:00', 'Subscriber'),
(1049, 35, 36, '2025-06-01 21:00:00', '2025-06-01 21:20:00', 'Subscriber'),
(1050, 36, 1, '2025-06-01 21:30:00', '2025-06-01 22:00:00', 'Guest'); -- Return to 1

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Flow Calculation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CityTransitDB.Mobility.Silver.StationFlow AS
SELECT 
    StartStationID AS StationID,
    COUNT(*) * -1 AS NetChange -- Rentals decrease stock
FROM CityTransitDB.Mobility.Bronze.RideLog
GROUP BY StartStationID
UNION ALL
SELECT 
    EndStationID AS StationID,
    COUNT(*) AS NetChange -- Returns increase stock
FROM CityTransitDB.Mobility.Bronze.RideLog
GROUP BY EndStationID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Rebalancing Needs
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CityTransitDB.Mobility.Gold.RebalancingAlerts AS
SELECT 
    s.StationName,
    s.Capacity,
    SUM(f.NetChange) AS NetBikeFlow,
    CASE 
        WHEN SUM(f.NetChange) < -5 THEN 'Needs Restocking'
        WHEN SUM(f.NetChange) > 5 THEN 'Needs Pickup'
        ELSE 'Balanced' 
    END AS Status
FROM CityTransitDB.Mobility.Bronze.Stations s
JOIN CityTransitDB.Mobility.Silver.StationFlow f ON s.StationID = f.StationID
GROUP BY s.StationName, s.Capacity;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all stations that are flagged as 'Needs Restocking' from the CityTransitDB.Mobility.Gold.RebalancingAlerts view."

PROMPT 2:
"Calculate the total distance traveled for all 'Guest' users (approximate using lat/long) from the Silver layer."

PROMPT 3:
"Show the busiest 5 stations by total rental volume."
*/
