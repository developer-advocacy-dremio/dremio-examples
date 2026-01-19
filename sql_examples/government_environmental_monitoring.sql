/*
    Dremio High-Volume SQL Pattern: Government Environmental Monitoring
    
    Business Scenario:
    Tracking Air Quality Index (AQI) and emission compliance metrics in real-time.
    Alerting on "Unhealthy" conditions.
    
    Data Story:
    We track Sensor Readings across different Locations.
    
    Medallion Architecture:
    - Bronze: SensorData.
      *Volume*: 50+ records.
    - Silver: AQI_Levels (Categorization).
    - Gold: DailyReport (Aggregated Health Stats).
    
    Key Dremio Features:
    - Case Logic
    - Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentEnvDB;
CREATE FOLDER IF NOT EXISTS GovernmentEnvDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentEnvDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentEnvDB.Gold;
USE GovernmentEnvDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentEnvDB.Bronze.SensorData (
    ReadingID STRING,
    Location STRING,
    SensorType STRING, -- AQI, WaterPH, Noise
    Value DOUBLE,
    Timestamp TIMESTAMP
);

INSERT INTO GovernmentEnvDB.Bronze.SensorData VALUES
('R1', 'Downtown', 'AQI', 45.0, TIMESTAMP '2025-01-01 08:00:00'),
('R2', 'Downtown', 'AQI', 55.0, TIMESTAMP '2025-01-01 12:00:00'),
('R3', 'IndustrialPark', 'AQI', 120.0, TIMESTAMP '2025-01-01 08:00:00'),
('R4', 'IndustrialPark', 'AQI', 155.0, TIMESTAMP '2025-01-01 14:00:00'),
('R5', 'Reservoir', 'WaterPH', 7.2, TIMESTAMP '2025-01-01 09:00:00'),
('R6', 'Reservoir', 'WaterPH', 6.8, TIMESTAMP '2025-01-01 15:00:00'),
('R7', 'Downtown', 'Noise', 65.0, TIMESTAMP '2025-01-01 18:00:00'),
('R8', 'IndustrialPark', 'Noise', 85.0, TIMESTAMP '2025-01-01 10:00:00'),
('R9', 'Suburb', 'AQI', 30.0, TIMESTAMP '2025-01-01 08:00:00'),
('R10', 'Suburb', 'AQI', 35.0, TIMESTAMP '2025-01-01 12:00:00'),
('R11', 'Downtown', 'AQI', 105.0, TIMESTAMP '2025-01-01 17:00:00'),
('R12', 'IndustrialPark', 'AQI', 130.0, TIMESTAMP '2025-01-02 08:00:00'),
('R13', 'Downtown', 'AQI', 42.0, TIMESTAMP '2025-01-02 08:00:00'),
('R14', 'Suburb', 'AQI', 25.0, TIMESTAMP '2025-01-02 08:00:00'),
('R15', 'Reservoir', 'WaterPH', 7.1, TIMESTAMP '2025-01-02 09:00:00'),
('R16', 'IndustrialPark', 'Noise', 88.0, TIMESTAMP '2025-01-02 10:00:00'),
('R17', 'Downtown', 'Noise', 60.0, TIMESTAMP '2025-01-02 18:00:00'),
('R18', 'IndustrialPark', 'AQI', 160.0, TIMESTAMP '2025-01-02 14:00:00'),
('R19', 'Downtown', 'AQI', 50.0, TIMESTAMP '2025-01-02 12:00:00'),
('R20', 'Suburb', 'AQI', 32.0, TIMESTAMP '2025-01-02 12:00:00'),
('R21', 'IndustrialPark', 'AQI', 110.0, TIMESTAMP '2025-01-03 08:00:00'),
('R22', 'Downtown', 'AQI', 48.0, TIMESTAMP '2025-01-03 08:00:00'),
('R23', 'Suburb', 'AQI', 28.0, TIMESTAMP '2025-01-03 08:00:00'),
('R24', 'Reservoir', 'WaterPH', 7.3, TIMESTAMP '2025-01-03 09:00:00'),
('R25', 'IndustrialPark', 'Noise', 82.0, TIMESTAMP '2025-01-03 10:00:00'),
('R26', 'Downtown', 'Noise', 62.0, TIMESTAMP '2025-01-03 18:00:00'),
('R27', 'IndustrialPark', 'AQI', 145.0, TIMESTAMP '2025-01-03 14:00:00'),
('R28', 'Downtown', 'AQI', 58.0, TIMESTAMP '2025-01-03 12:00:00'),
('R29', 'Suburb', 'AQI', 38.0, TIMESTAMP '2025-01-03 12:00:00'),
('R30', 'IndustrialPark', 'AQI', 125.0, TIMESTAMP '2025-01-04 08:00:00'),
('R31', 'Downtown', 'AQI', 40.0, TIMESTAMP '2025-01-04 08:00:00'),
('R32', 'Suburb', 'AQI', 22.0, TIMESTAMP '2025-01-04 08:00:00'),
('R33', 'Reservoir', 'WaterPH', 7.0, TIMESTAMP '2025-01-04 09:00:00'),
('R34', 'IndustrialPark', 'Noise', 80.0, TIMESTAMP '2025-01-04 10:00:00'),
('R35', 'Downtown', 'Noise', 58.0, TIMESTAMP '2025-01-04 18:00:00'),
('R36', 'IndustrialPark', 'AQI', 152.0, TIMESTAMP '2025-01-04 14:00:00'),
('R37', 'Downtown', 'AQI', 52.0, TIMESTAMP '2025-01-04 12:00:00'),
('R38', 'Suburb', 'AQI', 31.0, TIMESTAMP '2025-01-04 12:00:00'),
('R39', 'IndustrialPark', 'AQI', 115.0, TIMESTAMP '2025-01-05 08:00:00'),
('R40', 'Downtown', 'AQI', 44.0, TIMESTAMP '2025-01-05 08:00:00'),
('R41', 'Suburb', 'AQI', 26.0, TIMESTAMP '2025-01-05 08:00:00'),
('R42', 'Reservoir', 'WaterPH', 7.4, TIMESTAMP '2025-01-05 09:00:00'),
('R43', 'IndustrialPark', 'Noise', 86.0, TIMESTAMP '2025-01-05 10:00:00'),
('R44', 'Downtown', 'Noise', 64.0, TIMESTAMP '2025-01-05 18:00:00'),
('R45', 'IndustrialPark', 'AQI', 158.0, TIMESTAMP '2025-01-05 14:00:00'),
('R46', 'Downtown', 'AQI', 54.0, TIMESTAMP '2025-01-05 12:00:00'),
('R47', 'Suburb', 'AQI', 34.0, TIMESTAMP '2025-01-05 12:00:00'),
('R48', 'Downtown', 'AQI', 110.0, TIMESTAMP '2025-01-05 17:00:00'),
('R49', 'IndustrialPark', 'AQI', 165.0, TIMESTAMP '2025-01-05 15:00:00'), -- Spike
('R50', 'Suburb', 'AQI', 40.0, TIMESTAMP '2025-01-05 17:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: AQI Categorization
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentEnvDB.Silver.AQI_Levels AS
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
FROM GovernmentEnvDB.Bronze.SensorData
WHERE SensorType = 'AQI';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Daily Summary
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentEnvDB.Gold.DailyReport AS
SELECT 
    Location,
    AVG(AQI) AS AvgAQI,
    MAX(AQI) AS PeakAQI,
    SUM(CASE WHEN Category = 'Unhealthy' THEN 1 ELSE 0 END) AS UnhealthyReadings
FROM GovernmentEnvDB.Silver.AQI_Levels
GROUP BY Location;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which location had the highest PeakAQI?"
    2. "List all Unhealthy readings."
    3. "Show average AQI by Location."
*/
