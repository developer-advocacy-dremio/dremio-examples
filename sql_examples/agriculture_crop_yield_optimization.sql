/*
 * Agriculture Crop Yield Optimization Demo
 * 
 * Scenario:
 * A farming cooperative uses sensors to optimize irrigation and harvest timing.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Maximize Yield Per Acre and minimize water usage.
 * 
 * Note: Assumes a catalog named 'AgricultureDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AgricultureDB.Bronze;
CREATE FOLDER IF NOT EXISTS AgricultureDB.Silver;
CREATE FOLDER IF NOT EXISTS AgricultureDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Field Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AgricultureDB.Bronze.Fields (
    FieldID INT,
    LocationName VARCHAR,
    Acres DOUBLE,
    SoilType VARCHAR -- 'Clay', 'Loam', 'Sandy'
);

CREATE TABLE IF NOT EXISTS AgricultureDB.Bronze.CropCycles (
    CycleID INT,
    FieldID INT,
    CropName VARCHAR,
    PlantDate DATE,
    Season VARCHAR
);

CREATE TABLE IF NOT EXISTS AgricultureDB.Bronze.SensorReadings (
    ReadingID INT,
    FieldID INT,
    "Timestamp" TIMESTAMP,
    MoistureLevel DOUBLE, -- %
    Temperature DOUBLE
);

CREATE TABLE IF NOT EXISTS AgricultureDB.Bronze.Harvests (
    HarvestID INT,
    CycleID INT,
    HarvestDate DATE,
    YieldTons DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO AgricultureDB.Bronze.Fields (FieldID, LocationName, Acres, SoilType) VALUES
(1, 'North 40', 40.0, 'Loam'),
(2, 'River Bottom', 60.0, 'Clay'),
(3, 'Sunny Hill', 35.0, 'Sandy');

INSERT INTO AgricultureDB.Bronze.CropCycles (CycleID, FieldID, CropName, PlantDate, Season) VALUES
(101, 1, 'Corn', '2025-04-15', 'Spring'),
(102, 2, 'Soybeans', '2025-05-01', 'Spring'),
(103, 3, 'Wheat', '2025-03-20', 'Spring');

INSERT INTO AgricultureDB.Bronze.SensorReadings (ReadingID, FieldID, "Timestamp", MoistureLevel, Temperature) VALUES
(1, 1, '2025-06-01 06:00:00', 35.0, 60.0), -- Good
(2, 2, '2025-06-01 06:00:00', 45.0, 58.0), -- High moisture
(3, 3, '2025-06-01 06:00:00', 15.0, 65.0), -- Drought stress!
(4, 1, '2025-06-02 06:00:00', 34.0, 62.0);

INSERT INTO AgricultureDB.Bronze.Harvests (HarvestID, CycleID, HarvestDate, YieldTons) VALUES
(1, 101, '2025-09-15', 200.0),
(2, 102, '2025-10-01', 150.0),
(3, 103, '2025-07-15', 80.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Growth Tracking
-------------------------------------------------------------------------------

-- 2.1 View: Field_Conditions
-- Averages sensor data daily.
CREATE OR REPLACE VIEW AgricultureDB.Silver.Field_Conditions AS
SELECT
    FieldID,
    CAST("Timestamp" AS DATE) AS Day,
    AVG(MoistureLevel) AS AvgMoisture,
    AVG(Temperature) AS AvgTemp,
    CASE WHEN AVG(MoistureLevel) < 20 THEN 'IRRIGATE' ELSE 'OK' END AS ActionRequired
FROM AgricultureDB.Bronze.SensorReadings
GROUP BY FieldID, CAST("Timestamp" AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Yield Analysis
-------------------------------------------------------------------------------

-- 3.1 View: Crop_Efficiency
-- Calculates Yield per Acre.
CREATE OR REPLACE VIEW AgricultureDB.Gold.Crop_Efficiency AS
SELECT
    f.LocationName,
    c.CropName,
    c.Season,
    h.YieldTons,
    f.Acres,
    (h.YieldTons / f.Acres) AS YieldPerAcre
FROM AgricultureDB.Bronze.Harvests h
JOIN AgricultureDB.Bronze.CropCycles c ON h.CycleID = c.CycleID
JOIN AgricultureDB.Bronze.Fields f ON c.FieldID = f.FieldID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Drought Warning):
"Which fields need irrigation? Check AgricultureDB.Silver.Field_Conditions for 'IRRIGATE' action."

PROMPT 2 (Efficiency):
"Rank crops by YieldPerAcre using AgricultureDB.Gold.Crop_Efficiency."
*/
