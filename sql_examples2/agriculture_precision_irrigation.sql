/*
 * Agriculture: Precision Irrigation Telemetry
 * 
 * Scenario:
 * Analyzing soil moisture sensor data to optimize irrigation schedules and conserve water.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AgriTechDB;
CREATE FOLDER IF NOT EXISTS AgriTechDB.Farming;
CREATE FOLDER IF NOT EXISTS AgriTechDB.Farming.Bronze;
CREATE FOLDER IF NOT EXISTS AgriTechDB.Farming.Silver;
CREATE FOLDER IF NOT EXISTS AgriTechDB.Farming.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Sensor Data
-------------------------------------------------------------------------------

-- Sensors Table: IoT Device Metadata
CREATE TABLE IF NOT EXISTS AgriTechDB.Farming.Bronze.Sensors (
    SensorID VARCHAR,
    FieldID INT,
    CropType VARCHAR,
    InstallationDate DATE,
    Status VARCHAR
);

INSERT INTO AgriTechDB.Farming.Bronze.Sensors VALUES
('S-101', 1, 'Corn', '2024-03-01', 'Active'),
('S-102', 1, 'Corn', '2024-03-01', 'Active'),
('S-103', 1, 'Corn', '2024-03-01', 'Maintenance'),
('S-201', 2, 'Soybeans', '2024-04-15', 'Active'),
('S-202', 2, 'Soybeans', '2024-04-15', 'Active'),
('S-203', 2, 'Soybeans', '2024-04-15', 'Active'),
('S-301', 3, 'Wheat', '2024-02-10', 'Active'),
('S-302', 3, 'Wheat', '2024-02-10', 'Active'),
('S-401', 4, 'Cotton', '2024-05-01', 'Active'),
('S-402', 4, 'Cotton', '2024-05-01', 'Active'),
('S-501', 5, 'Rice', '2024-04-20', 'Active'),
('S-502', 5, 'Rice', '2024-04-20', 'Offline'),
('S-601', 6, 'Barley', '2024-03-10', 'Active'),
('S-602', 6, 'Barley', '2024-03-10', 'Active'),
('S-701', 7, 'Potatoes', '2024-04-05', 'Active'),
('S-702', 7, 'Potatoes', '2024-04-05', 'Active'),
('S-801', 8, 'Tomatoes', '2024-05-10', 'Active'),
('S-802', 8, 'Tomatoes', '2024-05-10', 'Active'),
('S-901', 9, 'Corn', '2024-03-01', 'Active'),
('S-902', 9, 'Corn', '2024-03-01', 'Active'),
('S-1001', 10, 'Soybeans', '2024-04-15', 'Active'),
('S-1002', 10, 'Soybeans', '2024-04-15', 'Active'),
('S-1101', 11, 'Oats', '2024-03-20', 'Active'),
('S-1102', 11, 'Oats', '2024-03-20', 'Active'),
('S-1201', 12, 'Sunflowers', '2024-05-15', 'Active'),
('S-1202', 12, 'Sunflowers', '2024-05-15', 'Active'),
('S-1301', 13, 'Sorghum', '2024-04-25', 'Active'),
('S-1302', 13, 'Sorghum', '2024-04-25', 'Active'),
('S-1401', 14, 'Alfalfa', '2024-03-15', 'Active'),
('S-1402', 14, 'Alfalfa', '2024-03-15', 'Active'),
('S-1501', 15, 'Peppers', '2024-05-20', 'Active'),
('S-1502', 15, 'Peppers', '2024-05-20', 'Active'),
('S-1601', 16, 'Onions', '2024-04-10', 'Active'),
('S-1602', 16, 'Onions', '2024-04-10', 'Active'),
('S-1701', 17, 'Garlic', '2024-03-05', 'Active'),
('S-1702', 17, 'Garlic', '2024-03-05', 'Active'),
('S-1801', 18, 'Carrots', '2024-04-01', 'Active'),
('S-1802', 18, 'Carrots', '2024-04-01', 'Active'),
('S-1901', 19, 'Corn', '2024-03-01', 'Active'),
('S-1902', 19, 'Corn', '2024-03-01', 'Active'),
('S-2001', 20, 'Soybeans', '2024-04-15', 'Active'),
('S-2002', 20, 'Soybeans', '2024-04-15', 'Active'),
('S-2101', 21, 'Wheat', '2024-02-10', 'Active'),
('S-2102', 21, 'Wheat', '2024-02-10', 'Active'),
('S-2201', 22, 'Cotton', '2024-05-01', 'Active'),
('S-2202', 22, 'Cotton', '2024-05-01', 'Active'),
('S-2301', 23, 'Rice', '2024-04-20', 'Active'),
('S-2302', 23, 'Rice', '2024-04-20', 'Active'),
('S-2401', 24, 'Barley', '2024-03-10', 'Active'),
('S-2402', 24, 'Barley', '2024-03-10', 'Active');

-- SensorReadings Table: Daily average moisture and temp
CREATE TABLE IF NOT EXISTS AgriTechDB.Farming.Bronze.SensorReadings (
    ReadingID INT,
    SensorID VARCHAR,
    SoilMoisturePct DOUBLE, -- 0 to 100
    SoilTempC DOUBLE,
    Timestamp TIMESTAMP
);

INSERT INTO AgriTechDB.Farming.Bronze.SensorReadings VALUES
(1, 'S-101', 25.5, 18.0, '2025-06-01 08:00:00'),
(2, 'S-101', 24.0, 22.0, '2025-06-01 14:00:00'),
(3, 'S-101', 22.5, 16.0, '2025-06-02 08:00:00'), -- Drying out
(4, 'S-102', 26.0, 18.2, '2025-06-01 08:00:00'),
(5, 'S-201', 45.0, 20.0, '2025-06-01 09:00:00'), -- Soybeans like it wetter
(6, 'S-202', 44.5, 20.1, '2025-06-01 09:00:00'),
(7, 'S-301', 15.0, 25.0, '2025-06-01 10:00:00'), -- Too dry?
(8, 'S-302', 16.0, 24.5, '2025-06-01 10:00:00'),
(9, 'S-401', 35.0, 28.0, '2025-06-01 11:00:00'),
(10, 'S-501', 80.0, 26.0, '2025-06-01 12:00:00'), -- Rice paddy, normal
(11, 'S-101', 55.0, 19.0, '2025-06-03 08:00:00'), -- After Irrigation
(12, 'S-601', 30.0, 15.0, '2025-06-01 08:00:00'),
(13, 'S-701', 28.0, 17.0, '2025-06-01 08:00:00'),
(14, 'S-801', 32.0, 22.0, '2025-06-01 08:00:00'),
(15, 'S-901', 25.0, 18.0, '2025-06-01 08:00:00'),
(16, 'S-1001', 43.0, 20.0, '2025-06-01 09:00:00'),
(17, 'S-1101', 29.0, 16.0, '2025-06-01 09:00:00'),
(18, 'S-1201', 31.0, 21.0, '2025-06-01 10:00:00'),
(19, 'S-1301', 27.0, 23.0, '2025-06-01 10:00:00'),
(20, 'S-1401', 33.0, 19.0, '2025-06-01 11:00:00'),
(21, 'S-1501', 34.0, 24.0, '2025-06-01 12:00:00'),
(22, 'S-1601', 30.0, 18.0, '2025-06-01 08:00:00'),
(23, 'S-1701', 29.0, 17.5, '2025-06-01 08:00:00'),
(24, 'S-1801', 32.5, 20.0, '2025-06-01 08:00:00'),
(25, 'S-1901', 24.0, 18.5, '2025-06-02 08:00:00'),
(26, 'S-2001', 42.0, 20.5, '2025-06-02 09:00:00'),
(27, 'S-2101', 14.0, 25.5, '2025-06-02 10:00:00'),
(28, 'S-2201', 34.0, 28.5, '2025-06-02 11:00:00'),
(29, 'S-2301', 79.0, 26.5, '2025-06-02 12:00:00'),
(30, 'S-2401', 29.0, 15.5, '2025-06-02 08:00:00'),
(31, 'S-101', 20.0, 18.5, '2025-06-04 08:00:00'), -- Drying
(32, 'S-201', 40.0, 21.0, '2025-06-04 09:00:00'),
(33, 'S-301', 12.0, 26.0, '2025-06-04 10:00:00'), -- Critical Dry
(34, 'S-401', 33.0, 29.0, '2025-06-04 11:00:00'),
(35, 'S-501', 75.0, 27.0, '2025-06-04 12:00:00'),
(36, 'S-601', 28.0, 16.0, '2025-06-04 08:00:00'),
(37, 'S-701', 26.0, 18.0, '2025-06-04 08:00:00'),
(38, 'S-801', 30.0, 23.0, '2025-06-04 08:00:00'),
(39, 'S-901', 22.0, 19.0, '2025-06-04 08:00:00'),
(40, 'S-1001', 40.0, 21.0, '2025-06-04 09:00:00'),
(41, 'S-1101', 27.0, 17.0, '2025-06-04 09:00:00'),
(42, 'S-1201', 29.0, 22.0, '2025-06-04 10:00:00'),
(43, 'S-1301', 25.0, 24.0, '2025-06-04 10:00:00'),
(44, 'S-1401', 31.0, 20.0, '2025-06-04 11:00:00'),
(45, 'S-1501', 32.0, 25.0, '2025-06-04 12:00:00'),
(46, 'S-1601', 28.0, 19.0, '2025-06-04 08:00:00'),
(47, 'S-1701', 27.0, 18.0, '2025-06-04 08:00:00'),
(48, 'S-1801', 30.0, 21.0, '2025-06-04 08:00:00'),
(49, 'S-1901', 22.0, 19.0, '2025-06-04 08:00:00'),
(50, 'S-2001', 39.0, 21.0, '2025-06-04 09:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: State Determination
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AgriTechDB.Farming.Silver.MoistureAnalysis AS
SELECT 
    s.FieldID,
    s.CropType,
    r.SensorID,
    r.SoilMoisturePct,
    r.SoilTempC,
    r.Timestamp,
    CASE 
        WHEN s.CropType = 'Corn' AND r.SoilMoisturePct < 25 THEN 'Irrigation Needed'
        WHEN s.CropType = 'Soybeans' AND r.SoilMoisturePct < 40 THEN 'Irrigation Needed'
        WHEN s.CropType = 'Wheat' AND r.SoilMoisturePct < 15 THEN 'Critical Water Stress'
        WHEN s.CropType = 'Rice' AND r.SoilMoisturePct < 70 THEN 'Refill Paddy'
        ELSE 'Optimal'
    END AS MoistureStatus
FROM AgriTechDB.Farming.Bronze.Sensors s
JOIN AgriTechDB.Farming.Bronze.SensorReadings r ON s.SensorID = r.SensorID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Irrigation Alerts
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AgriTechDB.Farming.Gold.IrrigationAlerts AS
SELECT 
    FieldID,
    CropType,
    COUNT(DISTINCT SensorID) AS SensorCount,
    AVG(SoilMoisturePct) AS AvgFieldMoisture,
    MAX(Timestamp) AS LastReading,
    MoistureStatus
FROM AgriTechDB.Farming.Silver.MoistureAnalysis
WHERE MoistureStatus != 'Optimal'
GROUP BY FieldID, CropType, MoistureStatus;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Based on the AgriTechDB.Farming.Gold.IrrigationAlerts view, which fields require immediate irrigation?"

PROMPT 2:
"Show the average soil moisture trend for FieldID 1 over the last 3 days using the Silver layer."

PROMPT 3:
"List all sensors currently reporting 'Critical Water Stress' for Wheat crops."
*/
