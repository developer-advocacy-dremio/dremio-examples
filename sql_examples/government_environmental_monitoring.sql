/*
 * Government Environmental Monitoring Demo
 * 
 * Scenario:
 * Tracking Air Quality Index (AQI) and emission compliance metrics.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Environment;
CREATE FOLDER IF NOT EXISTS RetailDB.Environment.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Environment.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Environment.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Environment.Bronze.SensorData (
    ReadingID INT,
    Location VARCHAR,
    SensorType VARCHAR, -- AQI, WaterPH, Noise
    Value DOUBLE,
    Timestamp TIMESTAMP
);

INSERT INTO RetailDB.Environment.Bronze.SensorData VALUES
(1, 'Downtown', 'AQI', 45.0, '2025-01-01 08:00:00'), -- Good
(2, 'Downtown', 'AQI', 55.0, '2025-01-01 12:00:00'), -- Moderate
(3, 'IndustrialPark', 'AQI', 120.0, '2025-01-01 08:00:00'), -- Unhealthy
(4, 'IndustrialPark', 'AQI', 155.0, '2025-01-01 14:00:00'), -- Unhealthy
(5, 'Reservoir', 'WaterPH', 7.2, '2025-01-01 09:00:00'),
(6, 'Reservoir', 'WaterPH', 6.8, '2025-01-01 15:00:00'),
(7, 'Downtown', 'Noise', 65.0, '2025-01-01 18:00:00'),
(8, 'IndustrialPark', 'Noise', 85.0, '2025-01-01 10:00:00'),
(9, 'Suburb', 'AQI', 30.0, '2025-01-01 08:00:00'),
(10, 'Suburb', 'AQI', 35.0, '2025-01-01 12:00:00'),
(11, 'Downtown', 'AQI', 105.0, '2025-01-01 17:00:00'); -- Traffic peak

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Environment.Silver.AQI_Levels AS
SELECT 
    Location,
    Value AS AQI,
    Timestamp,
    CASE 
        WHEN Value <= 50 THEN 'Good'
        WHEN Value <= 100 THEN 'Moderate'
        WHEN Value <= 150 THEN 'Unhealthy for Sensitive'
        ELSE 'Unhealthy'
    END AS Category
FROM RetailDB.Environment.Bronze.SensorData
WHERE SensorType = 'AQI';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Environment.Gold.DailyReport AS
SELECT 
    Location,
    AVG(AQI) AS AvgAQI,
    MAX(AQI) AS PeakAQI,
    SUM(CASE WHEN Category = 'Unhealthy' THEN 1 ELSE 0 END) AS UnhealthyReadings
FROM RetailDB.Environment.Silver.AQI_Levels
GROUP BY Location;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which location had the highest PeakAQI in RetailDB.Environment.Gold.DailyReport?"
*/
