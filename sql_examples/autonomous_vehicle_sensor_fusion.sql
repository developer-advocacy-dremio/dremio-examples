/*
 * Autonomous Vehicle Sensor Fusion & Safety Demo
 * 
 * Scenario:
 * An autonomous vehicle (AV) fleet operator needs to validate sensor performance by matching
 * disengagement reports (manual takeovers) with sensor discrepancies.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Test_Drives: Sessions of AV operation (VehicleID, Route, Conditions).
 * - Disengagements: Log of when safety driver took control.
 * - Sensor_Logs: Stream of object detections from LiDAR, Radar, and Camera.
 * 
 * Silver Layer:
 * - Sensor_Discrepancies: Moments where different sensors disagreed (e.g., Phantom braking).
 * - Drive_Quality_Metrics: Disengagements per 1000 miles.
 * 
 * Gold Layer:
 * - Sensor_Reliability_Score: Grading sensor stacks by failure rate.
 * - Safety_Incident_Report: Correlations between disagreement and disengagements.
 * 
 * Note: Assumes a catalog named 'AVSensorDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AVSensorDB;
CREATE FOLDER IF NOT EXISTS AVSensorDB.Bronze;
CREATE FOLDER IF NOT EXISTS AVSensorDB.Silver;
CREATE FOLDER IF NOT EXISTS AVSensorDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS AVSensorDB.Bronze.Test_Drives (
    DriveID INT,
    VehicleID VARCHAR,
    SoftwareVersion VARCHAR,
    RouteID VARCHAR,
    WeatherCondition VARCHAR,
    TotalMiles DOUBLE,
    Date DATE
);

CREATE TABLE IF NOT EXISTS AVSensorDB.Bronze.Disengagements (
    EventID INT,
    DriveID INT,
    Timestamp TIMESTAMP,
    Reason VARCHAR, -- 'Perception Error', 'Planning Error', 'Precautionary'
    DriverNotes VARCHAR
);

CREATE TABLE IF NOT EXISTS AVSensorDB.Bronze.Sensor_Logs (
    LogID INT,
    DriveID INT,
    Timestamp TIMESTAMP,
    LidarObjectCount INT,
    RadarObjectCount INT,
    CameraObjectCount INT,
    ConfidenceScore DOUBLE
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Test Drives
INSERT INTO AVSensorDB.Bronze.Test_Drives (DriveID, VehicleID, SoftwareVersion, RouteID, WeatherCondition, TotalMiles, Date) VALUES
(1, 'AV-001', 'v2.0', 'Route_Downtown', 'Sunny', 50.5, '2025-01-01'),
(2, 'AV-001', 'v2.0', 'Route_Highway', 'Rain', 120.0, '2025-01-02'),
(3, 'AV-002', 'v2.1', 'Route_Suburban', 'Cloudy', 45.0, '2025-01-01'),
(4, 'AV-003', 'v2.0', 'Route_Downtown', 'Fog', 30.0, '2025-01-03'),
(5, 'AV-002', 'v2.1', 'Route_Highway', 'Sunny', 150.0, '2025-01-04'),
(6, 'AV-001', 'v2.2', 'Route_Downtown', 'Night', 40.0, '2025-02-01'),
(7, 'AV-003', 'v2.2', 'Route_Suburban', 'Night', 35.0, '2025-02-01'),
(8, 'AV-001', 'v2.2', 'Route_Highway', 'Sunny', 130.0, '2025-02-02'),
(9, 'AV-002', 'v2.1', 'Route_Downtown', 'Rain', 25.0, '2025-01-05'),
(10, 'AV-003', 'v2.0', 'Route_Suburban', 'Cloudy', 40.0, '2025-01-06');

-- Insert 50 records into AVSensorDB.Bronze.Disengagements
INSERT INTO AVSensorDB.Bronze.Disengagements (EventID, DriveID, Timestamp, Reason, DriverNotes) VALUES
(1, 1, '2025-01-01 10:15:00', 'Perception Error', 'Car ignored pedestrian'),
(2, 2, '2025-01-02 14:30:00', 'Precautionary', 'Heavy rain, sensor noise'),
(3, 4, '2025-01-03 09:00:00', 'Planning Error', 'Stuck at intersection'),
(4, 6, '2025-02-01 20:00:00', 'Perception Error', 'Ghost object'),
(5, 7, '2025-02-01 21:00:00', 'Precautionary', 'Erratic behavior'),
(6, 9, '2025-01-05 11:00:00', 'Perception Error', 'Missed traffic light'),
-- Dummy fillers
(7, 1, '2025-01-01 10:30:00', 'Precautionary', 'Narrow lane'),
(8, 2, '2025-01-02 15:00:00', 'Planning Error', 'Missed exit'),
(9, 3, '2025-01-01 13:00:00', 'Precautionary', 'Cyclist nearby'),
(10, 5, '2025-01-04 12:00:00', 'Planning Error', 'Hard braking'),
(11, 8, '2025-02-02 10:00:00', 'Precautionary', 'Construction zone'),
(12, 1, '2025-01-01 11:00:00', 'Perception Error', 'Sign misread'),
(13, 2, '2025-01-02 15:30:00', 'Planning Error', 'Lane drift'),
(14, 3, '2025-01-01 13:30:00', 'Precautionary', 'Emergency vehicle'),
(15, 4, '2025-01-03 09:30:00', 'Perception Error', 'Fog obstruction'),
(16, 5, '2025-01-04 12:30:00', 'Planning Error', 'Speeding'),
(17, 6, '2025-02-01 20:30:00', 'Perception Error', 'Glare'),
(18, 7, '2025-02-01 21:30:00', 'Precautionary', 'Pothole'),
(19, 8, '2025-02-02 10:30:00', 'Planning Error', 'Tailgating'),
(20, 9, '2025-01-05 11:30:00', 'Perception Error', 'Reflection'),
(21, 10, '2025-01-06 08:00:00', 'Precautionary', 'School bus'),
(22, 1, '2025-01-01 11:30:00', 'Planning Error', 'Merging fail'),
(23, 2, '2025-01-02 16:00:00', 'Precautionary', 'Hydroplaning'),
(24, 3, '2025-01-01 14:00:00', 'Perception Error', 'Animal crossing'),
(25, 4, '2025-01-03 10:00:00', 'Precautionary', 'Low visibility'),
(26, 5, '2025-01-04 13:00:00', 'Planning Error', 'Wrong turn'),
(27, 6, '2025-02-01 21:00:00', 'Perception Error', 'Shadow'),
(28, 7, '2025-02-01 22:00:00', 'Precautionary', 'Jaywalker'),
(29, 8, '2025-02-02 11:00:00', 'Planning Error', 'Hesitation'),
(30, 9, '2025-01-05 12:00:00', 'Perception Error', 'Camera blocked'),
(31, 10, '2025-01-06 08:30:00', 'Precautionary', 'Debris'),
(32, 1, '2025-01-01 12:00:00', 'Planning Error', 'Unprotected left'),
(33, 2, '2025-01-02 16:30:00', 'Precautionary', 'Wind gust'),
(34, 3, '2025-01-01 14:30:00', 'Perception Error', 'Cone confusing'),
(35, 4, '2025-01-03 10:30:00', 'Precautionary', 'Slippery'),
(36, 5, '2025-01-04 13:30:00', 'Planning Error', 'Overshoot'),
(37, 6, '2025-02-01 21:30:00', 'Perception Error', 'Headlights'),
(38, 7, '2025-02-01 22:30:00', 'Precautionary', 'Police stop'),
(39, 8, '2025-02-02 11:30:00', 'Planning Error', 'Lane change'),
(40, 9, '2025-01-05 12:30:00', 'Perception Error', 'Lens flare'),
(41, 10, '2025-01-06 09:00:00', 'Precautionary', 'Trash can'),
(42, 1, '2025-01-01 12:30:00', 'Planning Error', 'Roundabout'),
(43, 2, '2025-01-02 17:00:00', 'Precautionary', 'Hail'),
(44, 3, '2025-01-01 15:00:00', 'Perception Error', 'Bird'),
(45, 4, '2025-01-03 11:00:00', 'Precautionary', 'Smoke'),
(46, 5, '2025-01-04 14:00:00', 'Planning Error', 'U-turn'),
(47, 6, '2025-02-01 22:00:00', 'Perception Error', 'Dark object'),
(48, 7, '2025-02-01 23:00:00', 'Precautionary', 'Drunk driver'),
(49, 8, '2025-02-02 12:00:00', 'Planning Error', 'Exit ramp'),
(50, 9, '2025-01-05 13:00:00', 'Perception Error', 'Rain drop');

-- Insert 50 records into AVSensorDB.Bronze.Sensor_Logs
INSERT INTO AVSensorDB.Bronze.Sensor_Logs (LogID, DriveID, Timestamp, LidarObjectCount, RadarObjectCount, CameraObjectCount, ConfidenceScore) VALUES
(1, 1, '2025-01-01 10:15:00', 5, 5, 2, 0.4), -- Discrepancy! Camera sees less vs Lidar/Radar
(2, 2, '2025-01-02 14:30:00', 0, 1, 0, 0.3),
(3, 4, '2025-01-03 09:00:00', 10, 10, 10, 0.95),
(4, 6, '2025-02-01 20:00:00', 1, 0, 1, 0.5), -- Ghost object
(5, 7, '2025-02-01 21:00:00', 3, 3, 3, 0.8),
(6, 1, '2025-01-01 10:15:05', 5, 5, 5, 0.9), -- Resolved
(7, 1, '2025-01-01 10:15:10', 5, 5, 5, 0.9),
(8, 1, '2025-01-01 10:15:15', 5, 5, 5, 0.9),
(9, 1, '2025-01-01 10:15:20', 4, 4, 4, 0.9),
(10, 2, '2025-01-02 14:30:10', 0, 0, 0, 0.9),
(11, 2, '2025-01-02 14:30:20', 0, 0, 0, 0.9),
(12, 6, '2025-02-01 20:00:05', 0, 0, 0, 0.9), -- Vanished
(13, 1, '2025-01-01 11:00:00', 2, 2, 0, 0.2), -- Sign missed
(14, 1, '2025-01-01 11:00:05', 2, 2, 2, 0.9),
(15, 3, '2025-01-01 13:00:00', 1, 1, 1, 0.9),
(16, 3, '2025-01-01 13:05:00', 1, 1, 1, 0.9),
(17, 3, '2025-01-01 13:10:00', 1, 1, 1, 0.9),
(18, 9, '2025-01-05 11:00:00', 3, 3, 0, 0.1), -- Camera blocked/missed light
(19, 9, '2025-01-05 11:00:05', 3, 3, 3, 0.9),
(20, 8, '2025-02-02 10:00:00', 5, 5, 5, 0.9),
(21, 8, '2025-02-02 10:00:05', 5, 5, 5, 0.9),
(22, 10, '2025-01-06 08:00:00', 2, 2, 2, 0.9),
(23, 10, '2025-01-06 08:00:05', 2, 2, 2, 0.9),
(24, 1, '2025-01-01 12:00:00', 8, 8, 8, 0.9),
(25, 4, '2025-01-03 09:30:00', 4, 4, 1, 0.3), -- Fog issue
(26, 4, '2025-01-03 09:30:05', 4, 4, 4, 0.8),
(27, 4, '2025-01-03 09:30:10', 4, 4, 4, 0.8),
(28, 5, '2025-01-04 12:00:00', 1, 1, 1, 0.9),
(29, 5, '2025-01-04 12:00:05', 1, 1, 1, 0.9),
(30, 2, '2025-01-02 15:00:00', 0, 0, 0, 0.9),
(31, 2, '2025-01-02 15:00:05', 0, 0, 0, 0.9),
(32, 6, '2025-02-01 21:00:00', 1, 0, 1, 0.4), -- Shadow issue
(33, 6, '2025-02-01 21:00:05', 0, 0, 0, 0.9),
(34, 7, '2025-02-01 22:00:00', 1, 1, 1, 0.9),
(35, 7, '2025-02-01 22:00:05', 1, 1, 1, 0.9),
(36, 8, '2025-02-02 11:00:00', 2, 2, 2, 0.9),
(37, 9, '2025-01-05 12:00:00', 1, 1, 0, 0.1), -- Lens problem
(38, 9, '2025-01-05 12:00:05', 1, 1, 1, 0.9),
(39, 10, '2025-01-06 09:00:00', 1, 1, 1, 0.9),
(40, 1, '2025-01-01 12:30:00', 6, 6, 6, 0.9),
(41, 1, '2025-01-01 12:30:05', 6, 6, 6, 0.9),
(42, 3, '2025-01-01 14:00:00', 1, 1, 0, 0.3), -- Animal missed by cam?
(43, 3, '2025-01-01 14:00:05', 1, 1, 1, 0.9),
(44, 2, '2025-01-02 16:00:00', 0, 0, 0, 0.9),
(45, 5, '2025-01-04 13:00:00', 2, 2, 2, 0.9),
(46, 5, '2025-01-04 13:00:05', 2, 2, 2, 0.9),
(47, 8, '2025-02-02 12:00:00', 3, 3, 3, 0.9),
(48, 7, '2025-02-01 23:00:00', 1, 1, 1, 0.9),
(49, 4, '2025-01-03 11:00:00', 0, 1, 0, 0.4), -- Smoke
(50, 4, '2025-01-03 11:00:05', 0, 0, 0, 0.9);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Fault & Reliability Analysis
-------------------------------------------------------------------------------

-- 2.1 Sensor Discrepancies
-- Identifies logs where object counts differ significantly between sensors
CREATE OR REPLACE VIEW AVSensorDB.Silver.Sensor_Discrepancies AS
SELECT
    LogID,
    DriveID,
    Timestamp,
    LidarObjectCount,
    RadarObjectCount,
    CameraObjectCount,
    ConfidenceScore,
    CASE
        WHEN LidarObjectCount <> RadarObjectCount OR LidarObjectCount <> CameraObjectCount 
        THEN 'Mismatch'
        ELSE 'Sync'
    END AS SyncStatus
FROM AVSensorDB.Bronze.Sensor_Logs
WHERE (LidarObjectCount <> RadarObjectCount OR LidarObjectCount <> CameraObjectCount)
   OR ConfidenceScore < 0.6;

-- 2.2 Drive Quality Metrics
CREATE OR REPLACE VIEW AVSensorDB.Silver.Drive_Quality AS
SELECT
    d.DriveID,
    d.VehicleID,
    d.SoftwareVersion,
    d.TotalMiles,
    COUNT(dis.EventID) AS DisengagementCount,
    (COUNT(dis.EventID) / d.TotalMiles) * 1000 AS DE_Per_1000_Miles
FROM AVSensorDB.Bronze.Test_Drives d
LEFT JOIN AVSensorDB.Bronze.Disengagements dis ON d.DriveID = dis.DriveID
GROUP BY d.DriveID, d.VehicleID, d.SoftwareVersion, d.TotalMiles;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Safety & Reliability
-------------------------------------------------------------------------------

-- 3.1 Safety Incident Report
-- Correlates sensor mismatch with actual disengagements within 5 seconds
CREATE OR REPLACE VIEW AVSensorDB.Gold.Safety_Incident_Report AS
SELECT
    dis.EventID,
    dis.DriveID,
    dis.Timestamp AS DisengagementTime,
    dis.Reason,
    sd.SyncStatus,
    sd.ConfidenceScore
FROM AVSensorDB.Bronze.Disengagements dis
JOIN AVSensorDB.Silver.Sensor_Discrepancies sd 
  ON dis.DriveID = sd.DriveID 
  AND ABS(EXTRACT(SECOND FROM (dis.Timestamp - sd.Timestamp))) < 5;

-- 3.2 Sensor Stack Reliability Score
CREATE OR REPLACE VIEW AVSensorDB.Gold.Sensor_Reliability_Score AS
SELECT
    d.VehicleID,
    d.SoftwareVersion,
    COUNT(sd.LogID) AS MismatchEvents,
    AVG(sd.ConfidenceScore) AS AvgConfidence
FROM AVSensorDB.Bronze.Test_Drives d
JOIN AVSensorDB.Silver.Sensor_Discrepancies sd ON d.DriveID = sd.DriveID
GROUP BY d.VehicleID, d.SoftwareVersion;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Software Check):
"Compare the DE_Per_1000_Miles for 'v2.0' vs 'v2.2' using AVSensorDB.Silver.Drive_Quality."

PROMPT 2 (Sensor Failures):
"List incidents in AVSensorDB.Gold.Safety_Incident_Report where SyncStatus is 'Mismatch'."

PROMPT 3 (Vehicle Health):
"Which VehicleID has the lowest AvgConfidence in AVSensorDB.Gold.Sensor_Reliability_Score?"
*/
