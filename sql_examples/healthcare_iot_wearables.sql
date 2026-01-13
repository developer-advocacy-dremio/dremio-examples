/*
 * IoT Wearable Health Demo
 * 
 * Scenario:
 * Monitoring high-frequency vitals (Heart Heart, Oxygen Saturation) from wearable devices.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.IoTHealth;
CREATE FOLDER IF NOT EXISTS RetailDB.IoTHealth.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.IoTHealth.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.IoTHealth.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.IoTHealth.Bronze.DeviceReadings (
    ReadingID INT,
    DeviceID VARCHAR,
    PatientID INT,
    Metric VARCHAR, -- HeartRate, SpO2
    Value INT,
    Timestamp TIMESTAMP
);

INSERT INTO RetailDB.IoTHealth.Bronze.DeviceReadings VALUES
(1, 'DEV-A', 101, 'HeartRate', 72, '2025-01-01 10:00:00'),
(2, 'DEV-A', 101, 'HeartRate', 75, '2025-01-01 10:01:00'),
(3, 'DEV-A', 101, 'SpO2', 98, '2025-01-01 10:02:00'),
(4, 'DEV-B', 102, 'HeartRate', 110, '2025-01-01 10:00:00'), -- High
(5, 'DEV-B', 102, 'HeartRate', 115, '2025-01-01 10:01:00'), -- Elevating
(6, 'DEV-B', 102, 'SpO2', 95, '2025-01-01 10:02:00'),
(7, 'DEV-C', 103, 'HeartRate', 60, '2025-01-01 10:00:00'),
(8, 'DEV-C', 103, 'SpO2', 99, '2025-01-01 10:01:00'),
(9, 'DEV-D', 104, 'HeartRate', 85, '2025-01-01 10:00:00'),
(10, 'DEV-D', 104, 'SpO2', 92, '2025-01-01 10:05:00'), -- Low
(11, 'DEV-A', 101, 'HeartRate', 80, '2025-01-01 10:05:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.IoTHealth.Silver.Anomalies AS
SELECT 
    DeviceID,
    PatientID,
    Metric,
    Value,
    Timestamp,
    CASE 
        WHEN Metric = 'HeartRate' AND Value > 100 THEN 'Tachycardia'
        WHEN Metric = 'HeartRate' AND Value < 50 THEN 'Bradycardia'
        WHEN Metric = 'SpO2' AND Value < 94 THEN 'Hypoxia'
        ELSE 'Normal'
    END AS AlertType
FROM RetailDB.IoTHealth.Bronze.DeviceReadings;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.IoTHealth.Gold.DailyAlerts AS
SELECT 
    PatientID,
    AlertType,
    COUNT(*) AS Occurrences,
    MIN(Timestamp) AS FirstOccurrence,
    MAX(Timestamp) AS LastOccurrence
FROM RetailDB.IoTHealth.Silver.Anomalies
WHERE AlertType <> 'Normal'
GROUP BY PatientID, AlertType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all patients from RetailDB.IoTHealth.Gold.DailyAlerts who had 'Hypoxia' alerts."
*/
