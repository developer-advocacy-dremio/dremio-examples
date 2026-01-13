/*
 * Data Center Power Usage Demo
 * 
 * Scenario:
 * Monitoring PUE (Power Usage Effectiveness), rack temperature, and HVAC load.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Reduce energy costs and prevent overheating incidents.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS DataCenterDB;
CREATE FOLDER IF NOT EXISTS DataCenterDB.Bronze;
CREATE FOLDER IF NOT EXISTS DataCenterDB.Silver;
CREATE FOLDER IF NOT EXISTS DataCenterDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DataCenterDB.Bronze.PowerMeters (
    MeterID VARCHAR,
    Zone VARCHAR,
    "Timestamp" TIMESTAMP,
    LoadKilowatts DOUBLE,
    Type VARCHAR -- 'IT_Load', 'Cooling', 'Lighting'
);

CREATE TABLE IF NOT EXISTS DataCenterDB.Bronze.EnvironmentalSensors (
    SensorID VARCHAR,
    RackID VARCHAR,
    "Timestamp" TIMESTAMP,
    TemperatureC DOUBLE,
    HumidityPct DOUBLE
);

INSERT INTO DataCenterDB.Bronze.PowerMeters VALUES
('M1', 'ZoneA', '2025-07-01 12:00:00', 500.0, 'IT_Load'),
('M2', 'ZoneA', '2025-07-01 12:00:00', 250.0, 'Cooling'),
('M3', 'ZoneA', '2025-07-01 12:00:00', 50.0, 'Lighting'),
('M1', 'ZoneA', '2025-07-01 13:00:00', 520.0, 'IT_Load'),
('M2', 'ZoneA', '2025-07-01 13:00:00', 260.0, 'Cooling');

INSERT INTO DataCenterDB.Bronze.EnvironmentalSensors VALUES
('S1', 'R101', '2025-07-01 12:00:00', 24.0, 45.0),
('S2', 'R102', '2025-07-01 12:00:00', 26.5, 42.0),
('S1', 'R101', '2025-07-01 13:00:00', 24.5, 46.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW DataCenterDB.Silver.HourlyPUE AS
SELECT 
    CAST("Timestamp" AS TIMESTAMP) AS HourBlock,
    SUM(CASE WHEN Type = 'IT_Load' THEN LoadKilowatts ELSE 0 END) AS IT_Load,
    SUM(LoadKilowatts) AS Total_Load,
    (SUM(LoadKilowatts) / NULLIF(SUM(CASE WHEN Type = 'IT_Load' THEN LoadKilowatts ELSE 0 END), 0)) AS PUE
FROM DataCenterDB.Bronze.PowerMeters
GROUP BY CAST("Timestamp" AS TIMESTAMP);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW DataCenterDB.Gold.EfficiencyReport AS
SELECT 
    DATE_TRUNC('DAY', HourBlock) AS Day,
    AVG(PUE) AS AvgDailyPUE,
    MAX(PUE) AS MaxDailyPUE
FROM DataCenterDB.Silver.HourlyPUE
GROUP BY DATE_TRUNC('DAY', HourBlock);

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What was the max PUE recorded on '2025-07-01' in DataCenterDB.Gold.EfficiencyReport?"
*/
