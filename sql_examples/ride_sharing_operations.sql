/*
 * Ride Sharing Operations Demo
 * 
 * Scenario:
 * Tracking rides, fare calculation, and surge pricing impact.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Balance driver supply with rider demand.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RideShareDB;
CREATE FOLDER IF NOT EXISTS RideShareDB.Bronze;
CREATE FOLDER IF NOT EXISTS RideShareDB.Silver;
CREATE FOLDER IF NOT EXISTS RideShareDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RideShareDB.Bronze.Drivers (
    DriverID INT,
    Name VARCHAR,
    Rating DOUBLE,
    JoinDate DATE
);

CREATE TABLE IF NOT EXISTS RideShareDB.Bronze.Rides (
    RideID INT,
    DriverID INT,
    RiderID INT,
    PickupTime TIMESTAMP,
    DropoffTime TIMESTAMP,
    BaseFare DOUBLE,
    SurgeMultiplier DOUBLE,
    DistanceMiles DOUBLE
);

INSERT INTO RideShareDB.Bronze.Drivers VALUES
(1, 'Fast Eddie', 4.9, '2023-01-01'),
(2, 'Slow Sam', 4.2, '2023-06-01');

INSERT INTO RideShareDB.Bronze.Rides VALUES
(101, 1, 500, '2025-01-01 18:00:00', '2025-01-01 18:20:00', 10.0, 1.5, 5.0), -- Surge
(102, 2, 501, '2025-01-01 19:00:00', '2025-01-01 19:15:00', 8.0, 1.0, 3.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RideShareDB.Silver.TripDetails AS
SELECT 
    r.RideID,
    d.Name AS DriverName,
    r.DistanceMiles,
    TIMESTAMPDIFF(MINUTE, r.PickupTime, r.DropoffTime) AS DurationMin,
    (r.BaseFare * r.SurgeMultiplier) AS TotalFare,
    r.SurgeMultiplier
FROM RideShareDB.Bronze.Rides r
JOIN RideShareDB.Bronze.Drivers d ON r.DriverID = d.DriverID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RideShareDB.Gold.DriverEarnings AS
SELECT 
    DriverName,
    COUNT(RideID) AS TotalRides,
    AVG(SurgeMultiplier) AS AvgSurgeSeen,
    SUM(TotalFare) * 0.75 AS DriverPayout -- Assuming 75% split
FROM RideShareDB.Silver.TripDetails
GROUP BY DriverName;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate total DriverPayout for 'Fast Eddie' in RideShareDB.Gold.DriverEarnings."
*/
