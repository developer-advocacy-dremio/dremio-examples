/*
 * Automotive Fleet Telemetry Demo
 * 
 * Scenario:
 * A logistics company tracks vehicle fleet performance and driver behavior.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize fuel efficiency and reduce maintenance costs.
 * 
 * Note: Assumes a catalog named 'FleetDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FleetDB;
CREATE FOLDER IF NOT EXISTS FleetDB.Bronze;
CREATE FOLDER IF NOT EXISTS FleetDB.Silver;
CREATE FOLDER IF NOT EXISTS FleetDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Telemetry Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FleetDB.Bronze.Vehicles (
    VehicleID VARCHAR,
    Model VARCHAR,
    "Year" INT,
    PurchaseDate DATE
);

CREATE TABLE IF NOT EXISTS FleetDB.Bronze.Trips (
    TripID INT,
    VehicleID VARCHAR,
    DriverID INT,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    DistanceMiles DOUBLE,
    FuelConsumedGallons DOUBLE
);

CREATE TABLE IF NOT EXISTS FleetDB.Bronze.MaintenanceEvents (
    EventID INT,
    VehicleID VARCHAR,
    EventDate DATE,
    Cost DOUBLE,
    Description VARCHAR
);

-- 1.2 Populate Bronze Tables
INSERT INTO FleetDB.Bronze.Vehicles (VehicleID, Model, "Year", PurchaseDate) VALUES
('V-101', 'Freightliner Cascadia', 2022, '2022-01-15'),
('V-102', 'Volvo VNL', 2021, '2021-05-20'),
('V-103', 'Peterbilt 579', 2023, '2023-03-10');

INSERT INTO FleetDB.Bronze.Trips (TripID, VehicleID, DriverID, StartTime, EndTime, DistanceMiles, FuelConsumedGallons) VALUES
(1, 'V-101', 501, '2025-06-01 08:00:00', '2025-06-01 12:00:00', 250.0, 35.0),
(2, 'V-102', 502, '2025-06-01 09:00:00', '2025-06-01 14:00:00', 300.0, 42.0),
(3, 'V-103', 503, '2025-06-02 07:00:00', '2025-06-02 11:00:00', 200.0, 24.0),
(4, 'V-101', 501, '2025-06-03 08:00:00', '2025-06-03 13:00:00', 320.0, 48.0);

INSERT INTO FleetDB.Bronze.MaintenanceEvents (EventID, VehicleID, EventDate, Cost, Description) VALUES
(1, 'V-102', '2024-12-01', 500.00, 'Tire Replacement'),
(2, 'V-101', '2025-01-15', 150.00, 'Oil Change');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Efficiency Metrics
-------------------------------------------------------------------------------

-- 2.1 View: Trip_Efficiency
-- Calculates MPG per trip.
CREATE OR REPLACE VIEW FleetDB.Silver.Trip_Efficiency AS
SELECT
    t.TripID,
    t.VehicleID,
    v.Model,
    t.DistanceMiles,
    t.FuelConsumedGallons,
    (t.DistanceMiles / NULLIF(t.FuelConsumedGallons, 0)) AS MPG
FROM FleetDB.Bronze.Trips t
JOIN FleetDB.Bronze.Vehicles v ON t.VehicleID = v.VehicleID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Fleet Performance
-------------------------------------------------------------------------------

-- 3.1 View: Vehicle_Performance
-- Aggregates metrics by vehicle.
CREATE OR REPLACE VIEW FleetDB.Gold.Vehicle_Performance AS
SELECT
    VehicleID,
    Model,
    SUM(DistanceMiles) AS TotalMiles,
    AVG(MPG) AS AvgMPG,
    COUNT(TripID) AS TotalTrips
FROM FleetDB.Silver.Trip_Efficiency
GROUP BY VehicleID, Model;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Fuel Efficiency):
"Which vehicle has the lowest AvgMPG in FleetDB.Gold.Vehicle_Performance?"

PROMPT 2 (Maintenance):
"List all maintenance events for vehicles with AvgMPG < 6.0."
*/
