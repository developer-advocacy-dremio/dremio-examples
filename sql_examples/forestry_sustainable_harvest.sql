/*
 * Forestry Sustainable Harvest & Lidar Analysis Demo
 * 
 * Scenario:
 * A timber company uses aerial LiDAR scans to estimate forest biomass, plan sustainable 
 * harvest cycles, and ensure compliance with conservation zones.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Forest_Zones: Geo-defined zones (ZoneID, Type, Area).
 * - Lidar_Scans: Raw point cloud derived metrics (TreeHeight, Density).
 * - Harvest_Plans: Scheduled cutting activities.
 * 
 * Silver Layer:
 * - Biomass_Estimates: Calculated biomass volume based on LiDAR density/height.
 * - Sustainable_Yield: Yield vs. Regrowth rate logic.
 * 
 * Gold Layer:
 * - Harvest_Schedule_Optimization: Prioritization of zones ready for harvest.
 * - Conservation_Compliance: Verification that Protected zones are untouched.
 * 
 * Note: Assumes a catalog named 'ForestDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ForestDB;
CREATE FOLDER IF NOT EXISTS ForestDB.Bronze;
CREATE FOLDER IF NOT EXISTS ForestDB.Silver;
CREATE FOLDER IF NOT EXISTS ForestDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS ForestDB.Bronze.Forest_Zones (
    ZoneID INT,
    RegionName VARCHAR,
    ZoneType VARCHAR, -- 'Commercial', 'Protected', 'Regrowth'
    AreaHectares DOUBLE,
    LastHarvestDate DATE
);

CREATE TABLE IF NOT EXISTS ForestDB.Bronze.Lidar_Scans (
    ScanID INT,
    ZoneID INT,
    ScanDate DATE,
    AvgTreeHeightM DOUBLE,
    CanopyDensityPct DOUBLE,
    GroundSlopeDegrees DOUBLE
);

CREATE TABLE IF NOT EXISTS ForestDB.Bronze.Harvest_Plans (
    PlanID INT,
    ZoneID INT,
    ScheduledDate DATE,
    TargetVolumeCBM DOUBLE, -- Cubic Meters
    Status VARCHAR
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Forest Zones
INSERT INTO ForestDB.Bronze.Forest_Zones (ZoneID, RegionName, ZoneType, AreaHectares, LastHarvestDate) VALUES
(1, 'North_Ridge', 'Commercial', 500.0, '2010-05-15'),
(2, 'North_Ridge', 'Commercial', 600.0, '2012-08-20'),
(3, 'North_Ridge', 'Protected', 200.0, NULL), -- Never harvested
(4, 'East_Valley', 'Regrowth', 400.0, '2020-01-10'),
(5, 'East_Valley', 'Commercial', 450.0, '2005-06-30'), -- Old growth ready?
(6, 'West_Slope', 'Commercial', 700.0, '2015-09-15'),
(7, 'West_Slope', 'Protected', 300.0, NULL),
(8, 'South_Basin', 'Commercial', 550.0, '2018-11-01'),
(9, 'South_Basin', 'Regrowth', 350.0, '2022-03-20'),
(10, 'High_Plateau', 'Protected', 800.0, NULL);

-- Insert 50 records into ForestDB.Bronze.Lidar_Scans
INSERT INTO ForestDB.Bronze.Lidar_Scans (ScanID, ZoneID, ScanDate, AvgTreeHeightM, CanopyDensityPct, GroundSlopeDegrees) VALUES
(1, 1, '2025-01-15', 25.0, 85.0, 10.0),
(2, 2, '2025-01-15', 22.0, 80.0, 15.0),
(3, 3, '2025-01-15', 35.0, 95.0, 25.0), -- Old growth protected
(4, 4, '2025-01-16', 5.0, 40.0, 5.0), -- Regrowth young
(5, 5, '2025-01-16', 30.0, 90.0, 12.0), -- Ready for harvest
(6, 6, '2025-01-17', 18.0, 70.0, 30.0), -- Steering slope
(7, 7, '2025-01-17', 28.0, 88.0, 35.0),
(8, 8, '2025-01-18', 12.0, 60.0, 8.0),
(9, 9, '2025-01-18', 3.0, 30.0, 5.0),
(10, 10, '2025-01-19', 32.0, 92.0, 20.0),
-- Repeat scans for history or sub-blocks... simplifying to quarterly scans
(11, 1, '2024-10-15', 24.8, 84.0, 10.0),
(12, 2, '2024-10-15', 21.8, 79.0, 15.0),
(13, 3, '2024-10-15', 34.8, 94.0, 25.0),
(14, 4, '2024-10-16', 4.5, 38.0, 5.0),
(15, 5, '2024-10-16', 29.8, 89.0, 12.0),
(16, 6, '2024-10-17', 17.8, 69.0, 30.0),
(17, 7, '2024-10-17', 27.8, 87.0, 35.0),
(18, 8, '2024-10-18', 11.5, 59.0, 8.0),
(19, 9, '2024-10-18', 2.8, 28.0, 5.0),
(20, 10, '2024-10-19', 31.8, 91.0, 20.0),
(21, 1, '2024-07-15', 24.5, 83.0, 10.0),
(22, 2, '2024-07-15', 21.5, 78.0, 15.0),
(23, 3, '2024-07-15', 34.5, 93.0, 25.0),
(24, 4, '2024-07-16', 4.0, 35.0, 5.0),
(25, 5, '2024-07-16', 29.5, 88.0, 12.0),
(26, 6, '2024-07-17', 17.5, 68.0, 30.0),
(27, 7, '2024-07-17', 27.5, 86.0, 35.0),
(28, 8, '2024-07-18', 11.0, 58.0, 8.0),
(29, 9, '2024-07-18', 2.5, 25.0, 5.0),
(30, 10, '2024-07-19', 31.5, 90.0, 20.0),
(31, 1, '2024-04-15', 24.2, 82.0, 10.0),
(32, 2, '2024-04-15', 21.2, 77.0, 15.0),
(33, 3, '2024-04-15', 34.2, 92.0, 25.0),
(34, 4, '2024-04-16', 3.5, 30.0, 5.0),
(35, 5, '2024-04-16', 29.2, 87.0, 12.0),
(36, 6, '2024-04-17', 17.2, 67.0, 30.0),
(37, 7, '2024-04-17', 27.2, 85.0, 35.0),
(38, 8, '2024-04-18', 10.5, 57.0, 8.0),
(39, 9, '2024-04-18', 2.2, 22.0, 5.0),
(40, 10, '2024-04-19', 31.2, 89.0, 20.0),
(41, 1, '2024-01-15', 24.0, 81.0, 10.0), -- Winter Growth pause
(42, 2, '2024-01-15', 21.0, 76.0, 15.0),
(43, 3, '2024-01-15', 34.0, 91.0, 25.0),
(44, 4, '2024-01-16', 3.2, 28.0, 5.0),
(45, 5, '2024-01-16', 29.0, 86.0, 12.0),
(46, 6, '2024-01-17', 17.0, 66.0, 30.0),
(47, 7, '2024-01-17', 27.0, 84.0, 35.0),
(48, 8, '2024-01-18', 10.0, 56.0, 8.0),
(49, 9, '2024-01-18', 2.0, 20.0, 5.0),
(50, 10, '2024-01-19', 31.0, 88.0, 20.0);

-- Insert 5 records into Harvest_Plans
INSERT INTO ForestDB.Bronze.Harvest_Plans (PlanID, ZoneID, ScheduledDate, TargetVolumeCBM, Status) VALUES
(1, 5, '2025-06-01', 5000.0, 'Approved'),
(2, 6, '2025-07-01', 3000.0, 'Review'), -- Slopes might be too steep
(3, 1, '2025-08-01', 4000.0, 'Planned'),
(4, 9, '2026-06-01', 200.0, 'Draft'),
(5, 3, '2025-09-01', 1000.0, 'Rejected'); -- Protected zone!

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Estimations
-------------------------------------------------------------------------------

-- 2.1 Biomass Estimates
-- Simple formula: Volume = Area * Height * Density * Factor
CREATE OR REPLACE VIEW ForestDB.Silver.Biomass_Estimates AS
SELECT
    l.ScanID,
    l.ScanDate,
    z.ZoneID,
    z.ZoneType,
    z.AreaHectares,
    l.AvgTreeHeightM,
    l.CanopyDensityPct,
    -- Hypothetical Biomass Calculation
    (z.AreaHectares * l.AvgTreeHeightM * (l.CanopyDensityPct / 100.0) * 0.5) AS EstimatedBiomassCBM
FROM ForestDB.Bronze.Lidar_Scans l
JOIN ForestDB.Bronze.Forest_Zones z ON l.ZoneID = z.ZoneID
WHERE l.ScanDate = '2025-01-15' OR l.ScanDate BETWEEN '2025-01-16' AND '2025-01-19'; -- Latest scan logic simplified

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Planning & Compliance
-------------------------------------------------------------------------------

-- 3.1 Harvest Schedule Optimization
CREATE OR REPLACE VIEW ForestDB.Gold.Harvest_Opportunity AS
SELECT
    be.ZoneID,
    be.EstimatedBiomassCBM,
    z.LastHarvestDate,
    DATEDIFF(day, z.LastHarvestDate, CURRENT_DATE) / 365.0 AS YearsSinceHarvest,
    CASE
        WHEN be.EstimatedBiomassCBM > 5000 AND (DATEDIFF(day, z.LastHarvestDate, CURRENT_DATE) / 365.0) > 15 THEN 'Prime Harvest'
        WHEN z.ZoneType = 'Protected' THEN 'No Harvest - Protected'
        ELSE 'Grow'
    END AS Recommendation
FROM ForestDB.Silver.Biomass_Estimates be
JOIN ForestDB.Bronze.Forest_Zones z ON be.ZoneID = z.ZoneID;

-- 3.2 Compliance Check
CREATE OR REPLACE VIEW ForestDB.Gold.Compliance_Alerts AS
SELECT
    hp.PlanID,
    hp.ZoneID,
    z.ZoneType,
    hp.Status,
    'Violation Alert' AS AlertType
FROM ForestDB.Bronze.Harvest_Plans hp
JOIN ForestDB.Bronze.Forest_Zones z ON hp.ZoneID = z.ZoneID
WHERE z.ZoneType = 'Protected' AND hp.Status != 'Rejected';

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Find Sales):
"Show me all zones marked as 'Prime Harvest' in ForestDB.Gold.Harvest_Opportunity."

PROMPT 2 (Compliance):
"Are there any active harvest plans in Protected zones? Check ForestDB.Gold.Compliance_Alerts."

PROMPT 3 (Biomass):
"What is the total EstimatedBiomassCBM for Commercial zones in ForestDB.Silver.Biomass_Estimates?"
*/
