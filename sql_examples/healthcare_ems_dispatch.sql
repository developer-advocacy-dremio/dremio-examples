/*
    Dremio High-Volume SQL Pattern: Healthcare EMS Dispatch Optimization
    
    Business Scenario:
    Emergency Medical Services (EMS) need to minimize "Chute Time" (dispatch to wheels rolling)
    and "Travel Time" to improve survival rates.
    
    Data Story:
    We ingest CAD (Computer Aided Dispatch) logs.
    
    Medallion Architecture:
    - Bronze: CAD_Logs (Call, Dispatch, Arrival Times).
      *Volume*: 50+ records.
    - Silver: Response Metrics (Chute Time & Travel Time).
    - Gold: Coverage Heatmap by Zip Code.
    
    Key Dremio Features:
    - TIMESTAMPDIFF for multiple intervals
    - Aggregation by Zip
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareEMSDB;
CREATE FOLDER IF NOT EXISTS HealthcareEMSDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareEMSDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareEMSDB.Gold;
USE HealthcareEMSDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareEMSDB.Bronze.CAD_Logs (
    IncidentID STRING,
    ZipCode STRING,
    CallReceivedTime TIMESTAMP,
    UnitDispatchedTime TIMESTAMP,
    UnitEnRouteTime TIMESTAMP,
    UnitOnSceneTime TIMESTAMP,
    Priority STRING -- ALPHA, BRAVO, CHARLIE, DELTA, ECHO (Highest)
);

-- Insert 50+ records
INSERT INTO HealthcareEMSDB.Bronze.CAD_Logs VALUES
('INC001', '10001', TIMESTAMP '2025-01-18 08:00:00', TIMESTAMP '2025-01-18 08:01:00', TIMESTAMP '2025-01-18 08:02:00', TIMESTAMP '2025-01-18 08:08:00', 'ECHO'), -- 6m travel (Good)
('INC002', '10002', TIMESTAMP '2025-01-18 08:15:00', TIMESTAMP '2025-01-18 08:16:30', TIMESTAMP '2025-01-18 08:18:00', TIMESTAMP '2025-01-18 08:28:00', 'DELTA'), -- 10m travel
('INC003', '10001', TIMESTAMP '2025-01-18 08:30:00', TIMESTAMP '2025-01-18 08:31:00', TIMESTAMP '2025-01-18 08:32:00', TIMESTAMP '2025-01-18 08:39:00', 'CHARLIE'),
('INC004', '10003', TIMESTAMP '2025-01-18 09:00:00', TIMESTAMP '2025-01-18 09:02:00', TIMESTAMP '2025-01-18 09:04:00', TIMESTAMP '2025-01-18 09:20:00', 'ALPHA'), -- 16m travel (Long)
('INC005', '10001', TIMESTAMP '2025-01-18 09:15:00', TIMESTAMP '2025-01-18 09:16:00', TIMESTAMP '2025-01-18 09:17:00', TIMESTAMP '2025-01-18 09:22:00', 'ECHO');
-- Insert bulk data
INSERT INTO HealthcareEMSDB.Bronze.CAD_Logs
SELECT 
  'INC' || CAST(rn + 5 AS STRING),
  CASE WHEN rn % 3 = 0 THEN '10001' WHEN rn % 3 = 1 THEN '10002' ELSE '10003' END,
  TIMESTAMP '2025-01-18 10:00:00',
  TIMESTAMP '2025-01-18 10:01:00',
  TIMESTAMP '2025-01-18 10:02:00',
  DATE_ADD(TIMESTAMP '2025-01-18 10:02:00', CAST((rn % 15) + 5 AS INT) * 1000 * 60), -- Random travel time 5-20 mins
  CASE WHEN rn % 5 = 0 THEN 'ECHO' ELSE 'DELTA' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Response Metrics
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareEMSDB.Silver.ResponseMetrics AS
SELECT
    IncidentID,
    ZipCode,
    Priority,
    TIMESTAMPDIFF(SECOND, UnitDispatchedTime, UnitEnRouteTime) AS ChuteTimeSec, -- Goal < 60s
    TIMESTAMPDIFF(MINUTE, UnitEnRouteTime, UnitOnSceneTime) AS TravelTimeMin,
    TIMESTAMPDIFF(MINUTE, CallReceivedTime, UnitOnSceneTime) AS TotalResponseTimeMin
FROM HealthcareEMSDB.Bronze.CAD_Logs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Coverage Heatmap
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareEMSDB.Gold.CoverageHeatmap AS
SELECT
    ZipCode,
    COUNT(IncidentID) AS TotalCalls,
    AVG(TotalResponseTimeMin) AS AvgResponseTime,
    MAX(TotalResponseTimeMin) AS MaxResponseTime,
    SUM(CASE WHEN TotalResponseTimeMin > 9 THEN 1 ELSE 0 END) AS CallsOver9Min, -- Standard SLA
    (SUM(CASE WHEN TotalResponseTimeMin > 9 THEN 1 ELSE 0 END) / CAST(COUNT(IncidentID) AS DOUBLE)) * 100 AS PercentSLA_Breach
FROM HealthcareEMSDB.Silver.ResponseMetrics
WHERE Priority IN ('DELTA', 'ECHO') -- Focus on life-threatening
GROUP BY ZipCode;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me the Zip Codes with the highest percentage of SLA breaches."
    2. "What is the average Chute Time?"
    3. "Map the call volume by Zip Code."
    4. "List all ECHO priority calls that took longer than 9 minutes."
*/
