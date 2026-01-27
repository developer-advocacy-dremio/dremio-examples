/*
 * Utilities Smart Water Metering Demo
 * 
 * Scenario:
 * Tracking water consumption, detecting leaks via continuous flow, and monitoring pressure zones.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS UtilitiesDB;
CREATE FOLDER IF NOT EXISTS UtilitiesDB.Water;
CREATE FOLDER IF NOT EXISTS UtilitiesDB.Water.Bronze;
CREATE FOLDER IF NOT EXISTS UtilitiesDB.Water.Silver;
CREATE FOLDER IF NOT EXISTS UtilitiesDB.Water.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS UtilitiesDB.Water.Bronze.MeterReads (
    MeterID INT,
    ZoneID VARCHAR,
    ReadingValue DOUBLE, -- Gallons
    ReadingTime TIMESTAMP,
    PressurePSI DOUBLE
);

INSERT INTO UtilitiesDB.Water.Bronze.MeterReads VALUES
(101, 'Z1', 1005.0, '2025-01-01 02:00:00', 60.5), -- Night flow (leak?)
(101, 'Z1', 1010.0, '2025-01-01 03:00:00', 60.2), -- Increasing
(102, 'Z1', 5000.0, '2025-01-01 02:00:00', 58.0),
(102, 'Z1', 5000.0, '2025-01-01 03:00:00', 58.1), -- Static (no flow)
(103, 'Z2', 250.0, '2025-01-01 02:00:00', 45.0), -- Low pressure
(103, 'Z2', 255.0, '2025-01-01 03:00:00', 44.5),
(104, 'Z2', 800.0, '2025-01-01 02:00:00', 46.0),
(104, 'Z2', 800.0, '2025-01-01 03:00:00', 46.0),
(105, 'Z3', 3000.0, '2025-01-01 08:00:00', 70.0), -- High pressure
(106, 'Z1', 1200.0, '2025-01-01 04:00:00', 59.0),
(101, 'Z1', 1015.0, '2025-01-01 04:00:00', 60.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW UtilitiesDB.Water.Silver.ConsumptionMetrics AS
SELECT 
    MeterID,
    ZoneID,
    ReadingTime,
    ReadingValue,
    PressurePSI,
    -- Simple Lag to get previous reading
    LAG(ReadingValue) OVER (PARTITION BY MeterID ORDER BY ReadingTime) AS PrevReading,
    (ReadingValue - LAG(ReadingValue) OVER (PARTITION BY MeterID ORDER BY ReadingTime)) AS HourlyUsage
FROM UtilitiesDB.Water.Bronze.MeterReads;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW UtilitiesDB.Water.Gold.LeakDetection AS
SELECT 
    MeterID,
    ZoneID,
    AVG(PressurePSI) AS AvgPressure,
    SUM(HourlyUsage) AS TotalUsage,
    -- Usage between 2AM-4AM often indicates leaks
    SUM(CASE 
        WHEN EXTRACT(HOUR FROM ReadingTime) BETWEEN 2 AND 4 THEN HourlyUsage 
        ELSE 0 
    END) AS NightUsage
FROM UtilitiesDB.Water.Silver.ConsumptionMetrics
GROUP BY MeterID, ZoneID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Identify MeterIDs with high NightUsage (> 5 gallons) in UtilitiesDB.Water.Gold.LeakDetection indicating potential leaks."
*/
