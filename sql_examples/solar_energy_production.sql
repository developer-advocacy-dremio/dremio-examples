/*
 * Solar Energy Production Demo
 * 
 * Scenario:
 * Monitoring solar panel efficiency, battery storage, and grid export.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize battery charging cycles and predict output.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SolarDB;
CREATE FOLDER IF NOT EXISTS SolarDB.Bronze;
CREATE FOLDER IF NOT EXISTS SolarDB.Silver;
CREATE FOLDER IF NOT EXISTS SolarDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SolarDB.Bronze.PanelReadings (
    PanelID VARCHAR,
    SiteID VARCHAR,
    "Timestamp" TIMESTAMP,
    OutputWatts DOUBLE,
    EfficiencyPct DOUBLE
);

CREATE TABLE IF NOT EXISTS SolarDB.Bronze.BatteryStorage (
    BatteryID VARCHAR,
    SiteID VARCHAR,
    "Timestamp" TIMESTAMP,
    ChargeLevelPct DOUBLE,
    GridExportWatts DOUBLE
);

INSERT INTO SolarDB.Bronze.PanelReadings VALUES
('P-01', 'SiteA', '2025-06-21 12:00:00', 350.5, 22.0),
('P-02', 'SiteA', '2025-06-21 12:00:00', 340.0, 21.5);

INSERT INTO SolarDB.Bronze.BatteryStorage VALUES
('B-01', 'SiteA', '2025-06-21 12:00:00', 85.0, 500.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SolarDB.Silver.SiteGeneration AS
SELECT 
    SiteID,
    DATE_TRUNC('HOUR', "Timestamp") AS HourBlock,
    SUM(OutputWatts) / 1000.0 AS TotalKWhGenerated,
    AVG(EfficiencyPct) AS AvgPanelEfficiency
FROM SolarDB.Bronze.PanelReadings
GROUP BY SiteID, DATE_TRUNC('HOUR', "Timestamp");

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SolarDB.Gold.DailyYield AS
SELECT 
    SiteID,
    CAST(HourBlock AS DATE) AS Day,
    SUM(TotalKWhGenerated) AS TotalDailyKWh,
    MAX(AvgPanelEfficiency) AS PeakEfficiency
FROM SolarDB.Silver.SiteGeneration
GROUP BY SiteID, CAST(HourBlock AS DATE);

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What was the TotalDailyKWh for 'SiteA' on '2025-06-21' in SolarDB.Gold.DailyYield?"
*/
