/*
    Dremio High-Volume SQL Pattern: Healthcare Home Visiting Nurse Routing
    
    Business Scenario:
    Home Health agencies need to group patients geographically to minimize nurse travel time.
    
    Data Story:
    We have Patient Locations (Zip/Cluster) and Visit Orders.
    
    Medallion Architecture:
    - Bronze: PatientLocations, VisitOrders.
      *Volume*: 50+ records.
    - Silver: RouteEfficiency (Grouping visits by Cluster).
    - Gold: DailyRouteOptimization (Suggested assignments).
    
    Key Dremio Features:
    - Aggregation by Zone
    - Date filtering
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareHomeDB;
CREATE FOLDER IF NOT EXISTS HealthcareHomeDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareHomeDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareHomeDB.Gold;
USE HealthcareHomeDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareHomeDB.Bronze.PatientLocations (
    PatientID STRING,
    ZipCode STRING,
    GeoClusterID INT -- Pre-calced zone 1-5
);

-- Bulk Patients
INSERT INTO HealthcareHomeDB.Bronze.PatientLocations
SELECT 
  'P' || CAST(rn + 100 AS STRING),
  CASE WHEN (rn % 5) = 0 THEN '10001' 
       WHEN (rn % 5) = 1 THEN '10002' 
       WHEN (rn % 5) = 2 THEN '10003' 
       WHEN (rn % 5) = 3 THEN '10004' 
       ELSE '10005' END,
  (rn % 5) + 1
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareHomeDB.Bronze.VisitOrders (
    OrderID STRING,
    PatientID STRING,
    VisitDate DATE,
    VisitType STRING -- WoundCare, PT, Assessment
);

-- Bulk Visits for tomorrow
INSERT INTO HealthcareHomeDB.Bronze.VisitOrders
SELECT 
  'O' || CAST(rn + 500 AS STRING),
  'P' || CAST(rn + 100 AS STRING),
  DATE '2025-01-20',
  CASE WHEN rn % 3 = 0 THEN 'WoundCare' WHEN rn % 3 = 1 THEN 'PT' ELSE 'Assessment' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Route Efficiency
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareHomeDB.Silver.DailyClusterLoad AS
SELECT
    v.VisitDate,
    p.GeoClusterID,
    p.ZipCode,
    COUNT(v.OrderID) AS VisitCount,
    -- Est 45 mins per visit + 15 mins travel intracluster
    COUNT(v.OrderID) * 60 AS EstTotalMinutes
FROM HealthcareHomeDB.Bronze.VisitOrders v
JOIN HealthcareHomeDB.Bronze.PatientLocations p ON v.PatientID = p.PatientID
WHERE v.VisitDate = DATE '2025-01-20'
GROUP BY v.VisitDate, p.GeoClusterID, p.ZipCode;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Route Assignments
-------------------------------------------------------------------------------
-- Simple heuristic: One nurse can do ~480 mins (8 hours)
-- We calculate how many nurses needed per cluster
CREATE OR REPLACE VIEW HealthcareHomeDB.Gold.NurseStaffingNeeds AS
SELECT
    VisitDate,
    GeoClusterID,
    VisitCount,
    EstTotalMinutes,
    CEIL(CAST(EstTotalMinutes AS DOUBLE) / 420.0) AS NursesNeeded -- 7 hours clinical time allowed
FROM HealthcareHomeDB.Silver.DailyClusterLoad;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "How many nurses do we need for Cluster 1 on 2025-01-20?"
    2. "Show the estimated workload (minutes) by Zip Code."
    3. "List assignments for GeoClusterID 3."
*/
