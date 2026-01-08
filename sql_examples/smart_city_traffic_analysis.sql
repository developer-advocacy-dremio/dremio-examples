/*
 * Smart City Traffic Analysis Demo
 * 
 * Scenario:
 * A city traffic management center monitors congestion and incidents.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize traffic flow and reduce incident response times.
 * 
 * Note: Assumes a catalog named 'SmartCityDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SmartCityDB;
CREATE FOLDER IF NOT EXISTS SmartCityDB.Bronze;
CREATE FOLDER IF NOT EXISTS SmartCityDB.Silver;
CREATE FOLDER IF NOT EXISTS SmartCityDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Sensor Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SmartCityDB.Bronze.Intersections (
    IntersectionID VARCHAR,
    Location VARCHAR,
    Latitude DOUBLE,
    Longitude DOUBLE
);

CREATE TABLE IF NOT EXISTS SmartCityDB.Bronze.TrafficSensors (
    SensorID INT,
    IntersectionID VARCHAR,
    "Timestamp" TIMESTAMP,
    VehicleCount INT,
    AvgSpeed DOUBLE
);

CREATE TABLE IF NOT EXISTS SmartCityDB.Bronze.Incidents (
    IncidentID INT,
    IntersectionID VARCHAR,
    ReportTime TIMESTAMP,
    ClearedTime TIMESTAMP,
    Type VARCHAR -- 'Accident', 'Stalled Vehicle'
);

-- 1.2 Populate Bronze Tables
INSERT INTO SmartCityDB.Bronze.Intersections (IntersectionID, Location, Latitude, Longitude) VALUES
('INT-001', 'Main St & 1st Ave', 40.7128, -74.0060),
('INT-002', 'Broadway & 5th St', 40.7150, -74.0090),
('INT-003', 'Park Ln & Elm St', 40.7200, -74.0100);

INSERT INTO SmartCityDB.Bronze.TrafficSensors (SensorID, IntersectionID, "Timestamp", VehicleCount, AvgSpeed) VALUES
(1, 'INT-001', '2025-07-04 08:00:00', 150, 25.0),
(2, 'INT-001', '2025-07-04 08:15:00', 300, 10.0), -- Congestion
(3, 'INT-002', '2025-07-04 08:00:00', 50, 35.0),
(4, 'INT-003', '2025-07-04 08:00:00', 40, 30.0),
(5, 'INT-001', '2025-07-04 08:30:00', 400, 5.0); -- Gridlock

INSERT INTO SmartCityDB.Bronze.Incidents (IncidentID, IntersectionID, ReportTime, ClearedTime, Type) VALUES
(1, 'INT-001', '2025-07-04 08:10:00', '2025-07-04 09:00:00', 'Accident');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Flow Analysis
-------------------------------------------------------------------------------

-- 2.1 View: Congestion_Monitor
-- Calculates Congestion Index (VehicleCount / AvgSpeed).
CREATE OR REPLACE VIEW SmartCityDB.Silver.Congestion_Monitor AS
SELECT
    s."Timestamp",
    i.Location,
    s.VehicleCount,
    s.AvgSpeed,
    (s.VehicleCount / NULLIF(s.AvgSpeed, 0)) AS CongestionIndex
FROM SmartCityDB.Bronze.TrafficSensors s
JOIN SmartCityDB.Bronze.Intersections i ON s.IntersectionID = i.IntersectionID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Operational Metrics
-------------------------------------------------------------------------------

-- 3.1 View: Incident_Impact
-- Correlates incidents with maximum congestion levels.
CREATE OR REPLACE VIEW SmartCityDB.Gold.Incident_Impact AS
SELECT
    inc.IncidentID,
    inc.Type,
    i.Location,
    TIMESTAMPDIFF(MINUTE, inc.ReportTime, inc.ClearedTime) AS DurationMinutes,
    MAX(cm.CongestionIndex) AS MaxCongestionDuringIncident
FROM SmartCityDB.Bronze.Incidents inc
JOIN SmartCityDB.Bronze.Intersections i ON inc.IntersectionID = i.IntersectionID
JOIN SmartCityDB.Silver.Congestion_Monitor cm ON inc.IntersectionID = i.IntersectionID
WHERE cm."Timestamp" BETWEEN inc.ReportTime AND inc.ClearedTime
GROUP BY inc.IncidentID, inc.Type, i.Location, inc.ReportTime, inc.ClearedTime;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Congestion Map):
"Using SmartCityDB.Silver.Congestion_Monitor, find the top 5 times and locations with the highest CongestionIndex."

PROMPT 2 (Incident Response):
"What was the average duration of 'Accident' type incidents in SmartCityDB.Gold.Incident_Impact?"
*/
