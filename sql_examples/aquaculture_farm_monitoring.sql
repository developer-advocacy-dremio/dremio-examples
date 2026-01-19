/*
 * Aquaculture Farm Monitoring & Water Quality Demo
 * 
 * Scenario:
 * A fish farm needs to monitor water quality (dissolved oxygen, temp) and feed rates
 * to optimize fish growth and prevent mortality events.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Tanks: Tank metadata (Capacity, Species, InstallDate).
 * - Sensor_Readings: IoT stream (Temp, pH, DissolvedOxygen).
 * - Harvests: Yield results (Weight, Count, Date).
 * 
 * Silver Layer:
 * - Daily_Water_Quality: Aggregated daily averages from sensors.
 * - Growth_Metrics: Feed Conversion Ratio (FCR) logic (implied from logs).
 * 
 * Gold Layer:
 * - Tank_Health_Scorecard: Grading tanks based on stability.
 * - Harvest_Yield_Analysis: Yield per tank vs environmental factors.
 * 
 * Note: Assumes a catalog named 'AquaDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AquaDB;
CREATE FOLDER IF NOT EXISTS AquaDB.Bronze;
CREATE FOLDER IF NOT EXISTS AquaDB.Silver;
CREATE FOLDER IF NOT EXISTS AquaDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS AquaDB.Bronze.Tanks (
    TankID INT,
    Location VARCHAR,
    CapacityLiters INT,
    Species VARCHAR, -- Salmon, Tilapia, Shrimp
    StockingDate DATE,
    InitialCount INT
);

CREATE TABLE IF NOT EXISTS AquaDB.Bronze.Sensor_Readings (
    ReadingID INT,
    TankID INT,
    Timestamp TIMESTAMP,
    TemperatureC DOUBLE,
    DissolvedOxygen DOUBLE, -- mg/L
    pHLevel DOUBLE,
    TurbidityNTU DOUBLE
);

CREATE TABLE IF NOT EXISTS AquaDB.Bronze.Harvests (
    HarvestID INT,
    TankID INT,
    HarvestDate DATE,
    TotalWeightKG DOUBLE,
    FishCount INT,
    QualityGrade VARCHAR -- A, B, C
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Tanks
INSERT INTO AquaDB.Bronze.Tanks (TankID, Location, CapacityLiters, Species, StockingDate, InitialCount) VALUES
(1, 'Zone_A', 50000, 'Salmon', '2024-01-01', 5000),
(2, 'Zone_A', 50000, 'Salmon', '2024-01-01', 5000),
(3, 'Zone_A', 50000, 'Salmon', '2024-02-01', 4800),
(4, 'Zone_B', 30000, 'Tilapia', '2024-03-01', 10000),
(5, 'Zone_B', 30000, 'Tilapia', '2024-03-01', 9500),
(6, 'Zone_C', 10000, 'Shrimp', '2024-04-01', 50000),
(7, 'Zone_C', 10000, 'Shrimp', '2024-04-01', 50000),
(8, 'Zone_D', 40000, 'Trout', '2024-05-01', 3000),
(9, 'Zone_D', 40000, 'Trout', '2024-05-15', 3000),
(10, 'Zone_E', 20000, 'Catfish', '2024-06-01', 6000);

-- Insert 50 records into AquaDB.Bronze.Harvests
INSERT INTO AquaDB.Bronze.Harvests (HarvestID, TankID, HarvestDate, TotalWeightKG, FishCount, QualityGrade) VALUES
(1, 1, '2024-06-01', 12000.5, 4800, 'A'), -- Partial harvest
(2, 2, '2024-06-05', 11500.0, 4900, 'A'),
(3, 3, '2024-07-01', 10000.0, 4500, 'B'),
(4, 4, '2024-06-15', 5000.0, 9000, 'A'),
(5, 5, '2024-06-15', 4800.0, 9200, 'B'),
(6, 6, '2024-07-20', 1000.0, 45000, 'A'), -- Shrimp are light
(7, 4, '2024-08-01', 2000.0, 1000, 'A'), -- Remainder
(8, 7, '2024-07-25', 950.0, 44000, 'B'),
(9, 8, '2024-09-01', 6000.0, 2900, 'A'),
(10, 9, '2024-09-15', 5800.0, 2850, 'A'),
(11, 10, '2024-10-01', 8000.0, 5800, 'B'),
(12, 1, '2024-12-01', 13000.0, 5000, 'A'), -- New cycle
(13, 2, '2024-12-05', 12500.0, 5000, 'A'),
(14, 3, '2025-01-01', 11000.0, 4800, 'C'), -- Bad yielding
(15, 4, '2025-01-15', 5500.0, 10000, 'A'),
(16, 5, '2025-01-15', 5200.0, 9500, 'B'),
(17, 6, '2025-02-01', 1100.0, 48000, 'A'),
(18, 7, '2025-02-05', 1050.0, 47000, 'A'),
(19, 8, '2025-03-01', 6200.0, 2950, 'A'),
(20, 9, '2025-03-15', 6100.0, 2900, 'A'),
(21, 10, '2025-04-01', 8500.0, 5900, 'A'),
(22, 1, '2025-06-01', 12800.0, 4900, 'A'),
(23, 2, '2025-06-05', 12200.0, 4950, 'A'),
(24, 3, '2025-07-01', 10500.0, 4600, 'B'),
(25, 4, '2025-08-01', 5300.0, 9800, 'A'),
(26, 5, '2025-08-15', 5000.0, 9300, 'B'),
(27, 6, '2025-09-01', 1080.0, 46000, 'A'),
(28, 7, '2025-09-05', 1000.0, 45000, 'B'),
(29, 8, '2025-10-01', 6100.0, 2920, 'A'),
(30, 9, '2025-10-15', 6000.0, 2880, 'A'),
(31, 10, '2025-11-01', 8200.0, 5850, 'B'),
(32, 1, '2025-12-01', 12900.0, 4950, 'A'),
(33, 2, '2025-12-05', 12400.0, 4980, 'A'),
(34, 3, '2026-01-01', 10800.0, 4700, 'B'),
(35, 4, '2026-02-01', 5400.0, 9900, 'A'),
(36, 5, '2026-02-15', 5100.0, 9400, 'B'),
(37, 6, '2026-03-01', 1090.0, 47000, 'A'),
(38, 7, '2026-03-05', 1020.0, 46000, 'B'),
(39, 8, '2026-04-01', 6150.0, 2930, 'A'),
(40, 9, '2026-04-15', 6050.0, 2890, 'A'),
(41, 10, '2026-05-01', 8300.0, 5880, 'A'),
(42, 1, '2026-06-01', 13100.0, 4990, 'A'),
(43, 2, '2026-06-05', 12600.0, 5001, 'A'), -- Growth
(44, 3, '2026-07-01', 11000.0, 4800, 'B'),
(45, 4, '2026-07-15', 5600.0, 9950, 'A'),
(46, 5, '2026-07-15', 5300.0, 9550, 'B'),
(47, 6, '2026-08-01', 1150.0, 49000, 'A'),
(48, 7, '2026-08-05', 1100.0, 48000, 'A'),
(49, 8, '2026-09-01', 6300.0, 2980, 'A'),
(50, 9, '2026-09-15', 6200.0, 2950, 'A');

-- Insert 50 records into AquaDB.Bronze.Sensor_Readings
-- Simulating varying conditions across tanks
INSERT INTO AquaDB.Bronze.Sensor_Readings (ReadingID, TankID, Timestamp, TemperatureC, DissolvedOxygen, pHLevel, TurbidityNTU) VALUES
(1, 1, '2025-01-01 08:00:00', 12.5, 9.5, 7.2, 5.0),
(2, 1, '2025-01-01 12:00:00', 13.0, 9.2, 7.3, 5.5),
(3, 1, '2025-01-01 16:00:00', 12.8, 9.0, 7.2, 6.0),
(4, 2, '2025-01-01 08:00:00', 12.4, 9.4, 7.2, 4.8),
(5, 2, '2025-01-01 12:00:00', 12.9, 9.1, 7.3, 5.2),
(6, 3, '2025-01-01 08:00:00', 11.0, 6.5, 6.8, 15.0), -- Stress! Low DO
(7, 3, '2025-01-01 12:00:00', 11.5, 6.2, 6.7, 18.0), -- Worsening
(8, 4, '2025-01-01 08:00:00', 25.0, 5.5, 7.5, 10.0), -- Tilapia warmer
(9, 4, '2025-01-01 12:00:00', 26.0, 5.2, 7.6, 12.0),
(10, 5, '2025-01-01 08:00:00', 25.5, 5.4, 7.5, 10.5),
(11, 6, '2025-01-01 08:00:00', 28.0, 4.5, 8.0, 20.0), -- Shrimp
(12, 6, '2025-01-01 12:00:00', 29.0, 4.2, 8.1, 22.0),
(13, 7, '2025-01-01 08:00:00', 28.2, 4.4, 7.9, 19.0),
(14, 8, '2025-01-01 08:00:00', 14.0, 10.0, 7.0, 3.0), -- Trout pristine
(15, 8, '2025-01-01 12:00:00', 14.5, 9.8, 7.0, 3.5),
(16, 9, '2025-01-01 08:00:00', 14.2, 9.9, 7.1, 3.2),
(17, 10, '2025-01-01 08:00:00', 22.0, 4.0, 7.4, 30.0), -- Catfish muddy
(18, 1, '2025-01-02 08:00:00', 12.4, 9.4, 7.2, 5.0),
(19, 1, '2025-01-02 12:00:00', 12.9, 9.1, 7.3, 5.2),
(20, 3, '2025-01-02 08:00:00', 11.2, 6.8, 6.9, 14.0), -- Recovery
(21, 6, '2025-01-02 08:00:00', 28.5, 4.4, 8.0, 21.0),
(22, 1, '2025-01-03 08:00:00', 12.3, 9.5, 7.2, 5.1),
(23, 1, '2025-01-03 12:00:00', 12.8, 9.2, 7.3, 5.3),
(24, 2, '2025-01-02 08:00:00', 12.5, 9.3, 7.2, 4.9),
(25, 3, '2025-01-03 08:00:00', 11.5, 7.0, 7.0, 10.0),
(26, 4, '2025-01-02 12:00:00', 25.5, 5.3, 7.5, 11.0),
(27, 5, '2025-01-02 12:00:00', 25.0, 5.5, 7.5, 11.0),
(28, 6, '2025-01-03 08:00:00', 28.8, 4.3, 8.0, 21.5),
(29, 8, '2025-01-02 08:00:00', 14.1, 9.9, 7.0, 3.1),
(30, 9, '2025-01-02 08:00:00', 14.3, 9.8, 7.1, 3.3),
(31, 10, '2025-01-02 08:00:00', 21.5, 4.2, 7.4, 28.0),
(32, 1, '2025-01-04 08:00:00', 12.6, 9.4, 7.2, 5.0),
(33, 2, '2025-01-03 08:00:00', 12.4, 9.4, 7.2, 5.0),
(34, 3, '2025-01-04 08:00:00', 11.8, 7.5, 7.1, 8.0),
(35, 4, '2025-01-03 08:00:00', 25.2, 5.4, 7.5, 10.5),
(36, 6, '2025-01-04 08:00:00', 29.2, 4.1, 8.1, 23.0),
(37, 8, '2025-01-03 08:00:00', 14.2, 9.8, 7.0, 3.2),
(38, 1, '2025-01-05 08:00:00', 12.5, 9.5, 7.2, 5.0),
(39, 3, '2025-01-05 08:00:00', 12.0, 8.0, 7.2, 6.0), -- Stabilized
(40, 6, '2025-01-05 08:00:00', 28.5, 4.4, 8.0, 20.0),
(41, 1, '2025-01-06 08:00:00', 12.4, 9.4, 7.2, 5.1),
(42, 2, '2025-01-04 08:00:00', 12.5, 9.3, 7.2, 4.9),
(43, 3, '2025-01-06 08:00:00', 12.2, 8.5, 7.2, 5.5),
(44, 4, '2025-01-04 08:00:00', 25.1, 5.5, 7.5, 10.2),
(45, 6, '2025-01-06 08:00:00', 28.0, 4.5, 8.0, 19.5),
(46, 8, '2025-01-04 08:00:00', 14.0, 9.9, 7.0, 3.1),
(47, 10, '2025-01-03 08:00:00', 22.2, 3.9, 7.4, 31.0),
(48, 1, '2025-01-07 08:00:00', 12.3, 9.5, 7.2, 5.0),
(49, 2, '2025-01-05 08:00:00', 12.6, 9.2, 7.3, 5.0),
(50, 3, '2025-01-07 08:00:00', 12.5, 9.0, 7.2, 5.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Monitoring & Trends
-------------------------------------------------------------------------------

-- 2.1 Daily Water Quality
CREATE OR REPLACE VIEW AquaDB.Silver.Daily_Water_Quality AS
SELECT
    TankID,
    TO_DATE(Timestamp) AS ReadDate,
    AVG(TemperatureC) AS AvgTemp,
    AVG(DissolvedOxygen) AS AvgDO,
    MIN(DissolvedOxygen) AS MinDO,
    MAX(TurbidityNTU) AS MaxTurbidity,
    CASE 
        WHEN MIN(DissolvedOxygen) < 5.0 AND TankID IN (1,2,3,8,9) THEN 'Critical' -- Salmonids need high DO
        WHEN MIN(DissolvedOxygen) < 3.0 THEN 'Critical' -- Tolerant species
        ELSE 'Good'
    END AS Status
FROM AquaDB.Bronze.Sensor_Readings
GROUP BY TankID, TO_DATE(Timestamp);

-- 2.2 Harvest Yields
CREATE OR REPLACE VIEW AquaDB.Silver.Harvest_Stats AS
SELECT
    h.HarvestID,
    t.Species,
    t.Location,
    h.HarvestDate,
    h.TotalWeightKG,
    h.FishCount,
    (h.TotalWeightKG / h.FishCount) AS ArgWeightPerFish
FROM AquaDB.Bronze.Harvests h
JOIN AquaDB.Bronze.Tanks t ON h.TankID = t.TankID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Farm Operations Scorecard
-------------------------------------------------------------------------------

-- 3.1 Tank Health Scorecard
CREATE OR REPLACE VIEW AquaDB.Gold.Tank_Health_Scorecard AS
SELECT
    t.TankID,
    t.Species,
    AVG(dwq.AvgDO) AS AvgDO_7Day,
    COUNT(CASE WHEN dwq.Status = 'Critical' THEN 1 END) AS CriticalDays
FROM AquaDB.Bronze.Tanks t
LEFT JOIN AquaDB.Silver.Daily_Water_Quality dwq ON t.TankID = dwq.TankID
GROUP BY t.TankID, t.Species;

-- 3.2 Species Yield Analysis
CREATE OR REPLACE VIEW AquaDB.Gold.Species_Yield_Analysis AS
SELECT
    Species,
    SUM(TotalWeightKG) AS TotalYieldKG,
    AVG(ArgWeightPerFish) AS AvgFishMakeWeight
FROM AquaDB.Silver.Harvest_Stats
GROUP BY Species;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Critical Check):
"Identify any tanks in AquaDB.Gold.Tank_Health_Scorecard that have had more than 0 CriticalDays."

PROMPT 2 (Yields):
"What is the total yield in KG for 'Salmon' in AquaDB.Gold.Species_Yield_Analysis?"

PROMPT 3 (Sensor Drilldown):
"From AquaDB.Silver.Daily_Water_Quality, show me the MinDO for TankID 3 arranged by date."
*/
