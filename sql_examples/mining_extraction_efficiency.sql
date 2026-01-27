/*
 * Mining Extraction Efficiency Demo
 * 
 * Scenario:
 * Analyzing ore grade quality and equipment haul cycle times.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MiningDB;
CREATE FOLDER IF NOT EXISTS MiningDB.Mining;
CREATE FOLDER IF NOT EXISTS MiningDB.Mining.Bronze;
CREATE FOLDER IF NOT EXISTS MiningDB.Mining.Silver;
CREATE FOLDER IF NOT EXISTS MiningDB.Mining.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MiningDB.Mining.Bronze.HaulCycles (
    CycleID INT,
    TruckID INT,
    LoadTime TIMESTAMP,
    DumpTime TIMESTAMP,
    TonsHaul DOUBLE,
    OreGrade DOUBLE, -- Percentage metal content
    PitLocation VARCHAR
);

INSERT INTO MiningDB.Mining.Bronze.HaulCycles VALUES
(1, 501, '2025-01-01 08:00:00', '2025-01-01 08:30:00', 250.0, 1.2, 'Pit_A'),
(2, 502, '2025-01-01 08:15:00', '2025-01-01 08:50:00', 240.0, 1.1, 'Pit_A'),
(3, 503, '2025-01-01 08:10:00', '2025-01-01 08:35:00', 255.0, 2.5, 'Pit_B'), -- High grade
(4, 501, '2025-01-01 09:00:00', '2025-01-01 09:25:00', 248.0, 1.3, 'Pit_A'),
(5, 502, '2025-01-01 09:15:00', '2025-01-01 09:55:00', 242.0, 0.9, 'Pit_A'), -- Slow cycle
(6, 503, '2025-01-01 09:00:00', '2025-01-01 09:25:00', 260.0, 2.4, 'Pit_B'),
(7, 504, '2025-01-01 08:30:00', '2025-01-01 09:00:00', 230.0, 0.5, 'Pit_C'), -- Waste?
(8, 504, '2025-01-01 09:30:00', '2025-01-01 10:00:00', 235.0, 0.6, 'Pit_C'),
(9, 501, '2025-01-01 10:00:00', '2025-01-01 10:28:00', 252.0, 1.2, 'Pit_A'),
(10, 503, '2025-01-01 10:00:00', '2025-01-01 10:25:00', 258.0, 2.3, 'Pit_B'),
(11, 502, '2025-01-01 10:20:00', '2025-01-01 10:55:00', 245.0, 1.0, 'Pit_A');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MiningDB.Mining.Silver.ProductionMetrics AS
SELECT 
    CycleID,
    TruckID,
    PitLocation,
    TonsHaul,
    OreGrade,
    (TonsHaul * OreGrade / 100.0) AS MetalContentTons,
    DATE_DIFF(CAST(DumpTime AS TIMESTAMP), CAST(LoadTime AS TIMESTAMP)) AS CycleMinutes
FROM MiningDB.Mining.Bronze.HaulCycles;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MiningDB.Mining.Gold.PitPerformance AS
SELECT 
    PitLocation,
    COUNT(*) AS HaulCount,
    SUM(TonsHaul) AS TotalOreMoved,
    SUM(MetalContentTons) AS TotalMetal,
    AVG(CycleMinutes) AS AvgCycleTime,
    AVG(OreGrade) AS AvgGrade
FROM MiningDB.Mining.Silver.ProductionMetrics
GROUP BY PitLocation;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which PitLocation produces the highest TotalMetal in MiningDB.Mining.Gold.PitPerformance?"
*/
