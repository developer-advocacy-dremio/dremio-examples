/*
 * Space Satellite Telemetry & Debris Tracking Demo
 * 
 * Scenario:
 * A satellite operator needs to monitor the health of their LEO (Low Earth Orbit) constellation
 * and receive alerts for potential collisions with tracked orbital debris.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Satellites: Static metadata about the fleet (ID, Model, LaunchDate).
 * - Telemetry_Stream: High-frequency logs (Battery, SolarAngle, Position).
 * - Orbital_Debris: External catalog of tracked debris objects.
 * 
 * Silver Layer:
 * - Cleaned_Telemetry: Joins telemetry with satellite metadata, filters anomalies.
 * - Conjunction_Analysis: Calculates distance between satellites and debris.
 * 
 * Gold Layer:
 * - Fleet_Health_Dashboard: Aggregated battery and solar performance.
 * - Collision_Risk_Alerts: High-priority warnings for close approaches.
 * 
 * Note: Assumes a catalog named 'SpaceDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SpaceDB;
CREATE FOLDER IF NOT EXISTS SpaceDB.Bronze;
CREATE FOLDER IF NOT EXISTS SpaceDB.Silver;
CREATE FOLDER IF NOT EXISTS SpaceDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS SpaceDB.Bronze.Satellites (
    SatID INT,
    Model VARCHAR,
    LaunchDate DATE,
    OrbitType VARCHAR,
    Status VARCHAR
);

CREATE TABLE IF NOT EXISTS SpaceDB.Bronze.Telemetry_Stream (
    LogID INT,
    SatID INT,
    Timestamp TIMESTAMP,
    BatteryLevel DOUBLE,
    SolarArrayAngle DOUBLE,
    Latitude DOUBLE,
    Longitude DOUBLE,
    AltitudeKm DOUBLE
);

CREATE TABLE IF NOT EXISTS SpaceDB.Bronze.Orbital_Debris (
    DebrisID INT,
    ObjectName VARCHAR,
    SizeClass VARCHAR,
    Latitude DOUBLE,
    Longitude DOUBLE,
    AltitudeKm DOUBLE,
    TrackedDate DATE
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Satellites (Small catalog for context)
INSERT INTO SpaceDB.Bronze.Satellites (SatID, Model, LaunchDate, OrbitType, Status) VALUES
(101, 'Saturn-V1', '2020-05-12', 'LEO', 'Active'),
(102, 'Saturn-V1', '2020-05-12', 'LEO', 'Active'),
(103, 'Saturn-V2', '2021-08-23', 'LEO', 'Active'),
(104, 'Saturn-V2', '2021-11-05', 'LEO', 'Maintenance'),
(105, 'Jupiter-X', '2022-01-15', 'MEO', 'Active'),
(106, 'Jupiter-X', '2022-03-30', 'MEO', 'Active'),
(107, 'Mars-O1', '2023-06-10', 'LEO', 'Active'),
(108, 'Mars-O1', '2023-09-22', 'LEO', 'Inactive'),
(109, 'Neptune-S', '2024-02-14', 'GEO', 'Active'),
(110, 'Neptune-S', '2024-05-01', 'GEO', 'Active');

-- Insert 50 records into SpaceDB.Bronze.Telemetry_Stream
INSERT INTO SpaceDB.Bronze.Telemetry_Stream (LogID, SatID, Timestamp, BatteryLevel, SolarArrayAngle, Latitude, Longitude, AltitudeKm) VALUES
(1, 101, '2025-01-01 12:00:00', 98.5, 45.2, 34.05, -118.24, 550.1),
(2, 101, '2025-01-01 12:05:00', 98.4, 46.1, 36.10, -115.10, 550.2),
(3, 102, '2025-01-01 12:00:00', 88.2, 30.5, 40.71, -74.00, 545.0),
(4, 102, '2025-01-01 12:05:00', 87.9, 31.2, 42.36, -71.05, 545.1),
(5, 103, '2025-01-01 12:00:00', 99.1, 60.0, 51.50, -0.12, 560.3),
(6, 103, '2025-01-01 12:05:00', 99.0, 60.5, 48.85, 2.35, 560.4),
(7, 101, '2025-01-01 12:10:00', 98.2, 47.0, 38.50, -110.50, 550.3),
(8, 104, '2025-01-01 12:00:00', 45.0, 15.0, -33.86, 151.20, 530.0),
(9, 104, '2025-01-01 12:05:00', 44.5, 14.8, -37.81, 144.96, 529.8),
(10, 105, '2025-01-01 12:00:00', 95.0, 55.5, 35.68, 139.69, 20200.0),
(11, 105, '2025-01-01 12:05:00', 94.9, 56.0, 37.56, 126.97, 20200.1),
(12, 106, '2025-01-01 12:00:00', 92.5, 50.2, 19.43, -99.13, 20150.0),
(13, 107, '2025-01-01 12:00:00', 96.6, 42.1, 55.75, 37.61, 555.5),
(14, 107, '2025-01-01 12:05:00', 96.4, 43.0, 59.93, 30.33, 555.6),
(15, 108, '2025-01-01 12:00:00', 0.0, 0.0, 1.35, 103.81, 540.0), -- Dead satellite
(16, 109, '2025-01-01 12:00:00', 100.0, 75.0, 0.0, -10.0, 35786.0),
(17, 110, '2025-01-01 12:00:00', 99.8, 74.5, 0.0, 20.0, 35786.0),
(18, 101, '2025-01-01 12:15:00', 98.0, 48.0, 40.0, -105.0, 550.4),
(19, 102, '2025-01-01 12:10:00', 87.5, 32.0, 45.0, -68.0, 545.2),
(20, 103, '2025-01-01 12:10:00', 98.9, 61.0, 45.0, 5.0, 560.5),
(21, 104, '2025-01-01 12:10:00', 44.0, 14.5, -40.0, 140.0, 529.6),
(22, 101, '2025-01-01 12:20:00', 97.8, 49.0, 42.0, -100.0, 550.5),
(23, 105, '2025-01-01 12:10:00', 94.8, 56.5, 39.0, 120.0, 20200.2),
(24, 106, '2025-01-01 12:05:00', 92.3, 50.8, 22.0, -95.0, 20150.1),
(25, 109, '2025-01-01 12:05:00', 99.9, 75.1, 0.0, -10.0, 35786.0), -- GEO stational
(26, 110, '2025-01-01 12:05:00', 99.7, 74.6, 0.0, 20.0, 35786.0),
(27, 103, '2025-01-01 12:15:00', 98.8, 61.5, 42.0, 8.0, 560.6),
(28, 107, '2025-01-01 12:10:00', 96.2, 44.0, 62.0, 25.0, 555.7),
(29, 101, '2025-01-01 12:25:00', 97.5, 50.0, 45.0, -95.0, 550.6),
(30, 102, '2025-01-01 12:15:00', 87.0, 32.5, 48.0, -65.0, 545.3),
(31, 105, '2025-01-01 12:15:00', 94.7, 57.0, 41.0, 115.0, 20200.3),
(32, 106, '2025-01-01 12:10:00', 92.1, 51.2, 25.0, -90.0, 20150.2),
(33, 104, '2025-01-01 12:15:00', 43.5, 14.0, -42.0, 135.0, 529.4),
(34, 107, '2025-01-01 12:15:00', 96.0, 45.0, 65.0, 20.0, 555.8),
(35, 103, '2025-01-01 12:20:00', 98.7, 62.0, 39.0, 10.0, 560.7),
(36, 101, '2025-01-01 12:30:00', 97.2, 51.0, 48.0, -90.0, 550.7),
(37, 101, '2025-01-01 12:35:00', 97.0, 52.0, 50.0, -85.0, 550.8),
(38, 102, '2025-01-01 12:20:00', 86.5, 33.0, 51.0, -62.0, 545.4),
(39, 105, '2025-01-01 12:20:00', 94.6, 57.5, 43.0, 110.0, 20200.4),
(40, 106, '2025-01-01 12:15:00', 91.9, 51.8, 28.0, -85.0, 20150.3),
(41, 104, '2025-01-01 12:20:00', 43.0, 13.8, -44.0, 130.0, 529.2),
(42, 109, '2025-01-01 12:10:00', 99.8, 75.2, 0.0, -10.0, 35786.0),
(43, 110, '2025-01-01 12:10:00', 99.6, 74.7, 0.0, 20.0, 35786.0),
(44, 103, '2025-01-01 12:25:00', 98.6, 62.5, 36.0, 12.0, 560.8),
(45, 107, '2025-01-01 12:20:00', 95.8, 46.0, 68.0, 15.0, 555.9),
(46, 101, '2025-01-01 12:40:00', 96.8, 53.0, 52.0, -80.0, 550.9),
(47, 102, '2025-01-01 12:25:00', 86.0, 33.5, 54.0, -59.0, 545.5),
(48, 105, '2025-01-01 12:25:00', 94.5, 58.0, 45.0, 105.0, 20200.5),
(49, 106, '2025-01-01 12:20:00', 91.7, 52.2, 31.0, -80.0, 20150.4),
(50, 104, '2025-01-01 12:25:00', 42.5, 13.5, -46.0, 125.0, 529.0);

-- Insert 50 records into SpaceDB.Bronze.Orbital_Debris
INSERT INTO SpaceDB.Bronze.Orbital_Debris (DebrisID, ObjectName, SizeClass, Latitude, Longitude, AltitudeKm, TrackedDate) VALUES
(1, 'DEB_1999_02', 'Small', 34.10, -118.20, 550.1, '2025-01-01'), -- Collision risk with Sat 101
(2, 'DEB_2001_15', 'Medium', 10.0, 10.0, 600.0, '2025-01-01'),
(3, 'DEB_1985_99', 'Large', -45.0, 120.0, 800.0, '2025-01-01'),
(4, 'DEB_2010_44', 'Small', 50.0, -10.0, 560.3, '2025-01-01'), -- Collision risk Sat 103?
(5, 'DEB_2022_01', 'Medium', 0.0, 0.0, 300.0, '2025-01-01'),
(6, 'DEB_2005_33', 'Small', 36.15, -115.05, 550.2, '2025-01-01'), -- Close to Sat 101
(7, 'DEB_1998_12', 'Large', 40.0, -74.0, 540.0, '2025-01-01'),
(8, 'DEB_2015_09', 'Small', -30.0, 150.0, 530.0, '2025-01-01'),
(9, 'DEB_2018_21', 'Medium', 20.0, 100.0, 700.0, '2025-01-01'),
(10, 'DEB_2000_05', 'Small', 5.0, 5.0, 400.0, '2025-01-01'),
(11, 'DEB_1990_XX', 'Medium', 80.0, 0.0, 900.0, '2025-01-01'),
(12, 'DEB_2011_55', 'Large', -80.0, 0.0, 900.0, '2025-01-01'),
(13, 'DEB_2020_88', 'Small', 48.8, 2.3, 560.4, '2025-01-01'), -- Sat 103 Risk
(14, 'DEB_2003_11', 'Medium', 30.0, 30.0, 500.0, '2025-01-01'),
(15, 'DEB_2007_67', 'Small', -20.0, -20.0, 450.0, '2025-01-01'),
(16, 'DEB_2016_22', 'Large', 15.0, 15.0, 650.0, '2025-01-01'),
(17, 'DEB_1975_01', 'Medium', 60.0, 60.0, 1000.0, '2025-01-01'),
(18, 'DEB_2023_04', 'Small', -60.0, -60.0, 1000.0, '2025-01-01'),
(19, 'DEB_2019_19', 'Small', 38.55, -110.55, 550.3, '2025-01-01'),
(20, 'DEB_2009_30', 'Medium', 25.0, 25.0, 580.0, '2025-01-01'),
(21, 'DEB_2012_14', 'Large', -25.0, -25.0, 580.0, '2025-01-01'),
(22, 'DEB_2004_02', 'Small', 12.0, 12.0, 480.0, '2025-01-01'),
(23, 'DEB_2017_89', 'Medium', -12.0, -12.0, 480.0, '2025-01-01'),
(24, 'DEB_1995_77', 'Small', 42.0, -71.0, 545.1, '2025-01-01'),
(25, 'DEB_2021_31', 'Large', -33.8, 151.2, 530.1, '2025-01-01'),
(26, 'DEB_2006_45', 'Small', 35.0, 139.0, 20200.0, '2025-01-01'), -- MEO debris?? Rare but adding.
(27, 'DEB_2013_16', 'Medium', 55.0, 37.0, 555.5, '2025-01-01'),
(28, 'DEB_2008_90', 'Small', -5.0, 105.0, 540.0, '2025-01-01'),
(29, 'DEB_2002_08', 'Large', 0.0, -10.0, 35780.0, '2025-01-01'), -- Near Geo
(30, 'DEB_2014_52', 'Small', 45.0, 45.0, 750.0, '2025-01-01'),
(31, 'DEB_2024_03', 'Medium', -45.0, -45.0, 750.0, '2025-01-01'),
(32, 'DEB_1982_11', 'Small', 34.0, -118.0, 555.0, '2025-01-01'),
(33, 'DEB_2010_02', 'Large', 40.0, -73.0, 545.0, '2025-01-01'),
(34, 'DEB_2020_20', 'Small', 51.0, 0.0, 560.5, '2025-01-01'),
(35, 'DEB_2011_11', 'Medium', -34.0, 151.0, 535.0, '2025-01-01'),
(36, 'DEB_2005_05', 'Small', 36.0, 140.0, 500.0, '2025-01-01'),
(37, 'DEB_1999_99', 'Large', 20.0, -99.0, 600.0, '2025-01-01'),
(38, 'DEB_2015_15', 'Small', 56.0, 38.0, 555.0, '2025-01-01'),
(39, 'DEB_2003_33', 'Medium', 0.1, 103.0, 540.0, '2025-01-01'),
(40, 'DEB_2019_07', 'Small', 1.0, -11.0, 35785.0, '2025-01-01'),
(41, 'DEB_2022_12', 'Small', 34.0, -118.4, 550.2, '2025-01-01'), -- Sat 101 again
(42, 'DEB_2001_88', 'Medium', 40.1, -74.1, 545.0, '2025-01-01'),
(43, 'DEB_2023_50', 'Large', 51.5, -0.1, 560.3, '2025-01-01'), -- Direct hit risk?
(44, 'DEB_2014_77', 'Small', -37.8, 145.0, 529.8, '2025-01-01'),
(45, 'DEB_2008_23', 'Medium', 35.7, 139.7, 510.0, '2025-01-01'),
(46, 'DEB_1997_34', 'Small', 19.4, -99.1, 520.0, '2025-01-01'),
(47, 'DEB_2021_05', 'Large', 55.7, 37.6, 555.5, '2025-01-01'),
(48, 'DEB_2011_92', 'Small', 1.3, 103.8, 542.0, '2025-01-01'),
(49, 'DEB_2000_18', 'Medium', -0.5, -10.5, 35786.0, '2025-01-01'),
(50, 'DEB_2025_01', 'Small', 0.5, 20.5, 35786.0, '2025-01-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Cleaned & Enriched
-------------------------------------------------------------------------------

-- 2.1 Cleaned Telemetry (Join with Satellites, Flag Anomalies)
CREATE OR REPLACE VIEW SpaceDB.Silver.Cleaned_Telemetry AS
SELECT
    t.LogID,
    t.SatID,
    s.Model,
    s.Status,
    t.Timestamp,
    t.BatteryLevel,
    t.SolarArrayAngle,
    t.Latitude,
    t.Longitude,
    t.AltitudeKm,
    CASE
        WHEN t.BatteryLevel < 20 THEN 'Critical'
        WHEN t.BatteryLevel < 50 THEN 'Low'
        ELSE 'Normal'
    END AS PowerStatus
FROM SpaceDB.Bronze.Telemetry_Stream t
JOIN SpaceDB.Bronze.Satellites s ON t.SatID = s.SatID
WHERE s.Status != 'Inactive'; -- Exclude dead satellites from active monitoring

-- 2.2 Conjunction Analysis (Cross Join to find close approaches)
-- WARNING: This is a simplified distance calculation for demo purposes.
-- Real orbital mechanics requires Propagators (SGP4).
-- Here we imply collision risk if Lat/Lon/Alt are within tight thresholds.
CREATE OR REPLACE VIEW SpaceDB.Silver.Conjunction_Analysis AS
SELECT
    t.SatID,
    t.Timestamp,
    d.ObjectName AS DebrisName,
    t.Latitude AS SatLat,
    d.Latitude AS DebrisLat,
    t.AltitudeKm AS SatAlt,
    d.AltitudeKm AS DebrisAlt,
    -- Approx Euclidian distance in 'units' (Not real Scale)
    SQRT(POWER(t.Latitude - d.Latitude, 2) + POWER(t.Longitude - d.Longitude, 2) + POWER(t.AltitudeKm - d.AltitudeKm, 2)) AS ProximityScore
FROM SpaceDB.Bronze.Telemetry_Stream t
CROSS JOIN SpaceDB.Bronze.Orbital_Debris d
WHERE ABS(t.AltitudeKm - d.AltitudeKm) < 5.0 -- Only check objects within 5km vert.
  AND ABS(t.Latitude - d.Latitude) < 1.0; -- Coarse filter

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Business Insights
-------------------------------------------------------------------------------

-- 3.1 Fleet Health Dashboard
CREATE OR REPLACE VIEW SpaceDB.Gold.Fleet_Health_Dashboard AS
SELECT
    Model,
    Status,
    AVG(BatteryLevel) AS AvgBattery,
    MIN(BatteryLevel) AS MinBattery,
    AVG(SolarArrayAngle) AS AvgSolarAngle,
    COUNT(DISTINCT SatID) AS SatCount
FROM SpaceDB.Silver.Cleaned_Telemetry
GROUP BY Model, Status;

-- 3.2 Collision Risk Alerts
CREATE OR REPLACE VIEW SpaceDB.Gold.Collision_Risk_Alerts AS
SELECT
    SatID,
    DebrisName,
    MIN(ProximityScore) AS ClosestApproach,
    MAX(Timestamp) AS TimeOfApproach,
    'Urgent' AS AlertLevel
FROM SpaceDB.Silver.Conjunction_Analysis
WHERE ProximityScore < 0.5 -- Threat threshold
GROUP BY SatID, DebrisName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Health Check):
"Show me the average battery level for all 'Active' satellites in the SpaceDB.Gold.Fleet_Health_Dashboard, broken down by Model."

PROMPT 2 (Collision Warning):
"List all active collision alerts from SpaceDB.Gold.Collision_Risk_Alerts where the AlertLevel is 'Urgent'. ordered by ClosestApproach ascending."

PROMPT 3 (Telemetry Trends):
"From SpaceDB.Silver.Cleaned_Telemetry, show me the trend of BatteryLevel over time for SatID 104."
*/
