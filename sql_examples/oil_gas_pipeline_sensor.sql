/*
 * Oil & Gas Pipeline Monitoring Demo
 * 
 * Scenario:
 * An energy company monitors pipeline pressure to detect leaks and blockages.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Ensure safety and operational efficiency.
 * 
 * Note: Assumes a catalog named 'OilGasDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS OilGasDB;
CREATE FOLDER IF NOT EXISTS OilGasDB.Bronze;
CREATE FOLDER IF NOT EXISTS OilGasDB.Silver;
CREATE FOLDER IF NOT EXISTS OilGasDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Sensor Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS OilGasDB.Bronze.Pipelines (
    PipelineID VARCHAR,
    Name VARCHAR,
    Region VARCHAR,
    InstallYear INT
);

CREATE TABLE IF NOT EXISTS OilGasDB.Bronze.Sensors (
    SensorID VARCHAR,
    PipelineID VARCHAR,
    LocationKM DOUBLE, -- Distance from source
    Type VARCHAR -- 'Pressure', 'Flow', 'Temperature'
);

CREATE TABLE IF NOT EXISTS OilGasDB.Bronze.PressureReadings (
    ReadingID INT,
    SensorID VARCHAR,
    "Timestamp" TIMESTAMP,
    ValuePSI DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO OilGasDB.Bronze.Pipelines (PipelineID, Name, Region, InstallYear) VALUES
('P-1', 'Keystone X', 'Midwest', 2010),
('P-2', 'Coastal Flow', 'Gulf', 2015);

INSERT INTO OilGasDB.Bronze.Sensors (SensorID, PipelineID, LocationKM, Type) VALUES
('S-101', 'P-1', 10.5, 'Pressure'),
('S-102', 'P-1', 50.0, 'Pressure'),
('S-201', 'P-2', 5.0, 'Pressure');

INSERT INTO OilGasDB.Bronze.PressureReadings (ReadingID, SensorID, "Timestamp", ValuePSI) VALUES
(1, 'S-101', '2025-04-01 08:00:00', 1200.0),
(2, 'S-102', '2025-04-01 08:00:00', 1150.0), -- Normal drop
(3, 'S-101', '2025-04-01 09:00:00', 1195.0),
(4, 'S-102', '2025-04-01 09:00:00', 800.0), -- Sudden drop! Leak?
(5, 'S-201', '2025-04-01 08:30:00', 1100.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Anomaly Detection
-------------------------------------------------------------------------------

-- 2.1 View: Pressure_Analysis
-- Flags unsafe pressure levels (< 900 or > 1300).
CREATE OR REPLACE VIEW OilGasDB.Silver.Pressure_Analysis AS
SELECT
    r.ReadingID,
    s.PipelineID,
    s.SensorID,
    r."Timestamp",
    r.ValuePSI,
    CASE 
        WHEN r.ValuePSI < 900 THEN 'LOW_PRESSURE_ALERT'
        WHEN r.ValuePSI > 1300 THEN 'HIGH_PRESSURE_ALERT'
        ELSE 'NORMAL'
    END AS Status
FROM OilGasDB.Bronze.PressureReadings r
JOIN OilGasDB.Bronze.Sensors s ON r.SensorID = s.SensorID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Operational Safety
-------------------------------------------------------------------------------

-- 3.1 View: Pipeline_Health
-- Aggregates alerts per pipeline.
CREATE OR REPLACE VIEW OilGasDB.Gold.Pipeline_Health AS
SELECT
    PipelineID,
    COUNT(ReadingID) AS TotalReadings,
    SUM(CASE WHEN Status LIKE '%ALERT' THEN 1 ELSE 0 END) AS AlertCount,
    AVG(ValuePSI) AS AvgPressure
FROM OilGasDB.Silver.Pressure_Analysis
GROUP BY PipelineID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Leak Detection):
"Show me all alerts from OilGasDB.Silver.Pressure_Analysis where Status is 'LOW_PRESSURE_ALERT'."

PROMPT 2 (Safety Audit):
"Which pipeline has the highest AlertCount in OilGasDB.Gold.Pipeline_Health?"
*/
