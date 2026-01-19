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

INSERT INTO HealthcareHomeDB.Bronze.PatientLocations VALUES
('P001', '10001', 1),
('P002', '10001', 1),
('P003', '10002', 2),
('P004', '10002', 2),
('P005', '10003', 3),
('P006', '10004', 4),
('P007', '10004', 4),
('P008', '10005', 5),
('P009', '10001', 1),
('P010', '10002', 2),
('P011', '10003', 3),
('P012', '10004', 4),
('P013', '10005', 5),
('P014', '10001', 1),
('P015', '10002', 2),
('P016', '10003', 3),
('P017', '10004', 4),
('P018', '10005', 5),
('P019', '10001', 1),
('P020', '10002', 2),
('P021', '10003', 3),
('P022', '10004', 4),
('P023', '10005', 5),
('P024', '10001', 1),
('P025', '10002', 2),
('P026', '10003', 3),
('P027', '10004', 4),
('P028', '10005', 5),
('P029', '10001', 1),
('P030', '10002', 2),
('P031', '10003', 3),
('P032', '10004', 4),
('P033', '10005', 5),
('P034', '10001', 1),
('P035', '10002', 2),
('P036', '10003', 3),
('P037', '10004', 4),
('P038', '10005', 5),
('P039', '10001', 1),
('P040', '10002', 2),
('P041', '10003', 3),
('P042', '10004', 4),
('P043', '10005', 5),
('P044', '10001', 1),
('P045', '10002', 2),
('P046', '10003', 3),
('P047', '10004', 4),
('P048', '10005', 5),
('P049', '10001', 1),
('P050', '10002', 2);

CREATE OR REPLACE TABLE HealthcareHomeDB.Bronze.VisitOrders (
    OrderID STRING,
    PatientID STRING,
    VisitDate DATE,
    VisitType STRING -- WoundCare, PT, Assessment
);

INSERT INTO HealthcareHomeDB.Bronze.VisitOrders VALUES
('O001', 'P001', DATE '2025-01-20', 'WoundCare'),
('O002', 'P002', DATE '2025-01-20', 'PT'),
('O003', 'P003', DATE '2025-01-20', 'Assessment'),
('O004', 'P004', DATE '2025-01-20', 'WoundCare'),
('O005', 'P005', DATE '2025-01-20', 'PT'),
('O006', 'P006', DATE '2025-01-20', 'Assessment'),
('O007', 'P007', DATE '2025-01-20', 'WoundCare'),
('O008', 'P008', DATE '2025-01-20', 'PT'),
('O009', 'P009', DATE '2025-01-20', 'Assessment'),
('O010', 'P010', DATE '2025-01-20', 'WoundCare'),
('O011', 'P011', DATE '2025-01-20', 'PT'),
('O012', 'P012', DATE '2025-01-20', 'Assessment'),
('O013', 'P013', DATE '2025-01-20', 'WoundCare'),
('O014', 'P014', DATE '2025-01-20', 'PT'),
('O015', 'P015', DATE '2025-01-20', 'Assessment'),
('O016', 'P016', DATE '2025-01-20', 'WoundCare'),
('O017', 'P017', DATE '2025-01-20', 'PT'),
('O018', 'P018', DATE '2025-01-20', 'Assessment'),
('O019', 'P019', DATE '2025-01-20', 'WoundCare'),
('O020', 'P020', DATE '2025-01-20', 'PT'),
('O021', 'P021', DATE '2025-01-20', 'Assessment'),
('O022', 'P022', DATE '2025-01-20', 'WoundCare'),
('O023', 'P023', DATE '2025-01-20', 'PT'),
('O024', 'P024', DATE '2025-01-20', 'Assessment'),
('O025', 'P025', DATE '2025-01-20', 'WoundCare'),
('O026', 'P026', DATE '2025-01-20', 'PT'),
('O027', 'P027', DATE '2025-01-20', 'Assessment'),
('O028', 'P028', DATE '2025-01-20', 'WoundCare'),
('O029', 'P029', DATE '2025-01-20', 'PT'),
('O030', 'P030', DATE '2025-01-20', 'Assessment'),
('O031', 'P031', DATE '2025-01-20', 'WoundCare'),
('O032', 'P032', DATE '2025-01-20', 'PT'),
('O033', 'P033', DATE '2025-01-20', 'Assessment'),
('O034', 'P034', DATE '2025-01-20', 'WoundCare'),
('O035', 'P035', DATE '2025-01-20', 'PT'),
('O036', 'P036', DATE '2025-01-20', 'Assessment'),
('O037', 'P037', DATE '2025-01-20', 'WoundCare'),
('O038', 'P038', DATE '2025-01-20', 'PT'),
('O039', 'P039', DATE '2025-01-20', 'Assessment'),
('O040', 'P040', DATE '2025-01-20', 'WoundCare'),
('O041', 'P041', DATE '2025-01-20', 'PT'),
('O042', 'P042', DATE '2025-01-20', 'Assessment'),
('O043', 'P043', DATE '2025-01-20', 'WoundCare'),
('O044', 'P044', DATE '2025-01-20', 'PT'),
('O045', 'P045', DATE '2025-01-20', 'Assessment'),
('O046', 'P046', DATE '2025-01-20', 'WoundCare'),
('O047', 'P047', DATE '2025-01-20', 'PT'),
('O048', 'P048', DATE '2025-01-20', 'Assessment'),
('O049', 'P049', DATE '2025-01-20', 'WoundCare'),
('O050', 'P050', DATE '2025-01-20', 'PT');

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
