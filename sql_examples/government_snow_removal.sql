/*
    Dremio High-Volume SQL Pattern: Government Snow Removal
    
    Business Scenario:
    During storms, optimizing "Plow Routes" ensuring primary roads are cleared first is vital.
    Tracking "Salt Usage" helps manage inventory and environmental impact.
    
    Data Story:
    We track Plow Trucks and Route Completion Logs.
    
    Medallion Architecture:
    - Bronze: Trucks, RouteLogs.
      *Volume*: 50+ records.
    - Silver: ShiftSummary (Miles plowed, Salt used).
    - Gold: StormEfficiency (Cost per Mile).
    
    Key Dremio Features:
    - Aggregation
    - Priority Filtering
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentSnowDB;
CREATE FOLDER IF NOT EXISTS GovernmentSnowDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentSnowDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentSnowDB.Gold;
USE GovernmentSnowDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentSnowDB.Bronze.Trucks (
    TruckID STRING,
    DepotID STRING,
    CapacityTons DOUBLE
);

INSERT INTO GovernmentSnowDB.Bronze.Trucks VALUES
('T1', 'Depot_North', 10.0),
('T2', 'Depot_South', 12.0),
('T3', 'Depot_Central', 8.0),
('T4', 'Depot_North', 10.0),
('T5', 'Depot_South', 12.0);

CREATE OR REPLACE TABLE GovernmentSnowDB.Bronze.RouteLogs (
    LogID STRING,
    TruckID STRING,
    RouteType STRING, -- Primary, Secondary
    MilesPlowed DOUBLE,
    SaltUsedTons DOUBLE,
    ShiftDate DATE
);

-- Bulk Logs (50 Records)
INSERT INTO GovernmentSnowDB.Bronze.RouteLogs VALUES
('LOG1001', 'T1', 'Primary', 50.0, 5.0, DATE '2025-01-15'),
('LOG1002', 'T1', 'Primary', 45.0, 4.5, DATE '2025-01-16'),
('LOG1003', 'T2', 'Primary', 60.0, 6.0, DATE '2025-01-15'),
('LOG1004', 'T2', 'Secondary', 30.0, 3.0, DATE '2025-01-16'),
('LOG1005', 'T3', 'Secondary', 40.0, 4.0, DATE '2025-01-15'),
('LOG1006', 'T3', 'Secondary', 35.0, 3.5, DATE '2025-01-16'),
('LOG1007', 'T4', 'Primary', 55.0, 5.5, DATE '2025-01-15'),
('LOG1008', 'T4', 'Primary', 50.0, 5.0, DATE '2025-01-16'),
('LOG1009', 'T5', 'Primary', 65.0, 6.5, DATE '2025-01-15'),
('LOG1010', 'T5', 'Secondary', 25.0, 2.5, DATE '2025-01-16'),
('LOG1011', 'T1', 'Secondary', 20.0, 2.0, DATE '2025-01-15'),
('LOG1012', 'T2', 'Primary', 10.0, 1.0, DATE '2025-01-17'), -- Cleanup
('LOG1013', 'T3', 'Secondary', 15.0, 1.5, DATE '2025-01-17'),
('LOG1014', 'T4', 'Primary', 12.0, 1.2, DATE '2025-01-17'),
('LOG1015', 'T5', 'Secondary', 10.0, 1.0, DATE '2025-01-17'),
('LOG1016', 'T1', 'Primary', 52.0, 5.2, DATE '2025-02-01'), -- Next storm
('LOG1017', 'T2', 'Primary', 62.0, 6.2, DATE '2025-02-01'),
('LOG1018', 'T3', 'Secondary', 38.0, 3.8, DATE '2025-02-01'),
('LOG1019', 'T4', 'Primary', 58.0, 5.8, DATE '2025-02-01'),
('LOG1020', 'T5', 'Secondary', 28.0, 2.8, DATE '2025-02-01'),
('LOG1021', 'T1', 'Secondary', 22.0, 2.2, DATE '2025-02-02'),
('LOG1022', 'T2', 'Secondary', 32.0, 3.2, DATE '2025-02-02'),
('LOG1023', 'T3', 'Secondary', 35.0, 3.5, DATE '2025-02-02'),
('LOG1024', 'T4', 'Secondary', 25.0, 2.5, DATE '2025-02-02'),
('LOG1025', 'T5', 'Primary', 10.0, 1.0, DATE '2025-02-02'),
('LOG1026', 'T1', 'Primary', 48.0, 4.8, DATE '2024-12-10'), -- Early storm
('LOG1027', 'T1', 'Secondary', 18.0, 1.8, DATE '2024-12-11'),
('LOG1028', 'T2', 'Primary', 58.0, 5.8, DATE '2024-12-10'),
('LOG1029', 'T3', 'Secondary', 36.0, 3.6, DATE '2024-12-10'),
('LOG1030', 'T4', 'Primary', 53.0, 5.3, DATE '2024-12-10'),
('LOG1031', 'T5', 'Primary', 60.0, 6.0, DATE '2024-12-10'),
('LOG1032', 'T2', 'Secondary', 28.0, 2.8, DATE '2024-12-11'),
('LOG1033', 'T3', 'Primary', 10.0, 1.0, DATE '2024-12-11'),
('LOG1034', 'T4', 'Secondary', 22.0, 2.2, DATE '2024-12-11'),
('LOG1035', 'T5', 'Secondary', 26.0, 2.6, DATE '2024-12-11'),
('LOG1036', 'T1', 'Primary', 50.0, 5.0, DATE '2025-01-25'),
('LOG1037', 'T2', 'Primary', 60.0, 6.0, DATE '2025-01-25'),
('LOG1038', 'T3', 'Primary', 15.0, 1.5, DATE '2025-01-25'),
('LOG1039', 'T4', 'Primary', 55.0, 5.5, DATE '2025-01-25'),
('LOG1040', 'T5', 'Primary', 65.0, 6.5, DATE '2025-01-25'),
('LOG1041', 'T1', 'Secondary', 20.0, 2.0, DATE '2025-01-26'),
('LOG1042', 'T2', 'Secondary', 30.0, 3.0, DATE '2025-01-26'),
('LOG1043', 'T3', 'Secondary', 40.0, 4.0, DATE '2025-01-26'),
('LOG1044', 'T4', 'Secondary', 25.0, 2.5, DATE '2025-01-26'),
('LOG1045', 'T5', 'Secondary', 28.0, 2.8, DATE '2025-01-26'),
('LOG1046', 'T1', 'Primary', 5.0, 0.5, DATE '2025-01-27'),
('LOG1047', 'T2', 'Primary', 6.0, 0.6, DATE '2025-01-27'),
('LOG1048', 'T3', 'Secondary', 10.0, 1.0, DATE '2025-01-27'),
('LOG1049', 'T4', 'Primary', 5.0, 0.5, DATE '2025-01-27'),
('LOG1050', 'T5', 'Primary', 6.0, 0.6, DATE '2025-01-27');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Shift Totals
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentSnowDB.Silver.TruckPerformance AS
SELECT
    TruckID,
    ShiftDate,
    SUM(MilesPlowed) AS TotalMiles,
    SUM(SaltUsedTons) AS TotalSalt
FROM GovernmentSnowDB.Bronze.RouteLogs
GROUP BY TruckID, ShiftDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Operational Efficiency
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentSnowDB.Gold.StormCost AS
SELECT
    r.RouteType,
    SUM(r.MilesPlowed) AS TotalMiles,
    SUM(r.SaltUsedTons) AS TotalSalt,
    (SUM(r.SaltUsedTons) / SUM(r.MilesPlowed)) AS SaltPerMile,
    AVG(t.CapacityTons) AS AvgFleetCap -- Context
FROM GovernmentSnowDB.Bronze.RouteLogs r
JOIN GovernmentSnowDB.Bronze.Trucks t ON r.TruckID = t.TruckID
GROUP BY r.RouteType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Route Type consumed the most salt?"
    2. "Show total miles plowed by Truck ID."
    3. "Calculate average salt usage per mile."
*/
