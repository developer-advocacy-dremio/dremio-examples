/*
 * Dremio Deep Sea Shipwreck Salvage Example
 * 
 * Domain: Maritime Archaeology & Salvage
 * Scenario: 
 * A salvage vessel deploys an ROV (Remote Operated Vehicle) to a wreck site (e.g., Titanic depth).
 * Logs track "Depth_Meters", "Manipulator_Pressure", and "Artifact_ID" scanning.
 * The goal is to maximize "Artifact Value Recovered" while minimizing "ROV Battery Drain".
 * 
 * Complexity: Medium (Pressure calculations, battery efficiency metrics)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Deep_Blue_Salvage;
CREATE FOLDER IF NOT EXISTS Deep_Blue_Salvage.Sources;
CREATE FOLDER IF NOT EXISTS Deep_Blue_Salvage.Bronze;
CREATE FOLDER IF NOT EXISTS Deep_Blue_Salvage.Silver;
CREATE FOLDER IF NOT EXISTS Deep_Blue_Salvage.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Deep_Blue_Salvage.Sources.Wreck_Catalog (
    WreckID VARCHAR,
    Ship_Name VARCHAR, -- 'SS Gairsoppa'
    Sunk_Date DATE,
    Depth_Meters INT,
    Estimated_Cargo_Value_Millions DOUBLE
);

CREATE TABLE IF NOT EXISTS Deep_Blue_Salvage.Sources.ROV_Dive_Logs (
    LogID VARCHAR,
    WreckID VARCHAR,
    Dive_Start_Time TIMESTAMP,
    Max_Depth_Reached INT,
    Bottom_Time_Minutes INT,
    Artifacts_recovered_Count INT,
    Battery_Start_Pct INT,
    Battery_End_Pct INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Wrecks
INSERT INTO Deep_Blue_Salvage.Sources.Wreck_Catalog VALUES
('WRK-001', 'SS Gairsoppa', '1941-02-17', 4700, 200.0), -- Silver bullion
('WRK-002', 'San Jose Galleon', '1708-06-08', 600, 17000.0), -- Gold coins
('WRK-003', 'RMS Republic', '1909-01-24', 80, 1000.0); -- Gold Eagle coins

-- Seed Dives
INSERT INTO Deep_Blue_Salvage.Sources.ROV_Dive_Logs VALUES
('D-001', 'WRK-001', '2023-06-01 08:00:00', 4700, 120, 5, 100, 40),
('D-002', 'WRK-001', '2023-06-02 08:00:00', 4700, 130, 8, 100, 35),
('D-003', 'WRK-001', '2023-06-03 08:00:00', 4700, 100, 2, 100, 50),
('D-004', 'WRK-002', '2023-07-01 08:00:00', 600, 300, 50, 100, 20), -- Shallower, longer time
('D-005', 'WRK-002', '2023-07-02 08:00:00', 600, 280, 45, 100, 25),
('D-006', 'WRK-003', '2023-08-01 08:00:00', 80, 400, 100, 100, 10), -- Very shallow
-- Fill 50
('D-007', 'WRK-001', '2023-06-04 08:00:00', 4700, 120, 4, 100, 42),
('D-008', 'WRK-001', '2023-06-05 08:00:00', 4700, 120, 6, 100, 41),
('D-009', 'WRK-001', '2023-06-06 08:00:00', 4700, 120, 5, 100, 40),
('D-010', 'WRK-001', '2023-06-07 08:00:00', 4700, 120, 3, 100, 45),
('D-011', 'WRK-001', '2023-06-08 08:00:00', 4700, 120, 2, 100, 48),
('D-012', 'WRK-001', '2023-06-09 08:00:00', 4700, 120, 0, 100, 50), -- Failed grab
('D-013', 'WRK-001', '2023-06-10 08:00:00', 4700, 120, 1, 100, 49),
('D-014', 'WRK-001', '2023-06-11 08:00:00', 4700, 120, 4, 100, 42),
('D-015', 'WRK-001', '2023-06-12 08:00:00', 4700, 120, 5, 100, 40),
('D-016', 'WRK-001', '2023-06-13 08:00:00', 4700, 120, 6, 100, 38),
('D-017', 'WRK-002', '2023-07-03 08:00:00', 600, 300, 40, 100, 22),
('D-018', 'WRK-002', '2023-07-04 08:00:00', 600, 300, 42, 100, 21),
('D-019', 'WRK-002', '2023-07-05 08:00:00', 600, 300, 38, 100, 23),
('D-020', 'WRK-002', '2023-07-06 08:00:00', 600, 300, 35, 100, 25),
('D-021', 'WRK-002', '2023-07-07 08:00:00', 600, 300, 48, 100, 18),
('D-022', 'WRK-002', '2023-07-08 08:00:00', 600, 300, 50, 100, 15),
('D-023', 'WRK-002', '2023-07-09 08:00:00', 600, 300, 45, 100, 19),
('D-024', 'WRK-002', '2023-07-10 08:00:00', 600, 300, 44, 100, 20),
('D-025', 'WRK-002', '2023-07-11 08:00:00', 600, 300, 46, 100, 19),
('D-026', 'WRK-002', '2023-07-12 08:00:00', 600, 300, 41, 100, 22),
('D-027', 'WRK-003', '2023-08-02 08:00:00', 80, 400, 90, 100, 12),
('D-028', 'WRK-003', '2023-08-03 08:00:00', 80, 400, 80, 100, 15),
('D-029', 'WRK-003', '2023-08-04 08:00:00', 80, 400, 85, 100, 14),
('D-030', 'WRK-003', '2023-08-05 08:00:00', 80, 400, 88, 100, 13),
('D-031', 'WRK-003', '2023-08-06 08:00:00', 80, 400, 92, 100, 11),
('D-032', 'WRK-003', '2023-08-07 08:00:00', 80, 400, 95, 100, 10),
('D-033', 'WRK-003', '2023-08-08 08:00:00', 80, 400, 70, 100, 18),
('D-034', 'WRK-003', '2023-08-09 08:00:00', 80, 400, 75, 100, 16),
('D-035', 'WRK-003', '2023-08-10 08:00:00', 80, 400, 78, 100, 15),
('D-036', 'WRK-003', '2023-08-11 08:00:00', 80, 400, 82, 100, 14),
('D-037', 'WRK-001', '2023-06-14 08:00:00', 4700, 120, 3, 100, 45),
('D-038', 'WRK-001', '2023-06-15 08:00:00', 4700, 120, 4, 100, 43),
('D-039', 'WRK-001', '2023-06-16 08:00:00', 4700, 120, 5, 100, 41),
('D-040', 'WRK-001', '2023-06-17 08:00:00', 4700, 120, 6, 100, 39),
('D-041', 'WRK-001', '2023-06-18 08:00:00', 4700, 120, 7, 100, 37),
('D-042', 'WRK-001', '2023-06-19 08:00:00', 4700, 120, 8, 100, 35),
('D-043', 'WRK-001', '2023-06-20 08:00:00', 4700, 120, 5, 100, 40),
('D-044', 'WRK-001', '2023-06-21 08:00:00', 4700, 120, 4, 100, 42),
('D-045', 'WRK-001', '2023-06-22 08:00:00', 4700, 120, 3, 100, 44),
('D-046', 'WRK-001', '2023-06-23 08:00:00', 4700, 120, 2, 100, 46),
('D-047', 'WRK-001', '2023-06-24 08:00:00', 4700, 120, 1, 100, 48),
('D-048', 'WRK-001', '2023-06-25 08:00:00', 4700, 120, 0, 100, 50),
('D-049', 'WRK-001', '2023-06-26 08:00:00', 4700, 120, 1, 100, 49),
('D-050', 'WRK-001', '2023-06-27 08:00:00', 4700, 120, 2, 100, 47);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Deep_Blue_Salvage.Bronze.Bronze_Wrecks AS SELECT * FROM Deep_Blue_Salvage.Sources.Wreck_Catalog;
CREATE OR REPLACE VIEW Deep_Blue_Salvage.Bronze.Bronze_Dives AS SELECT * FROM Deep_Blue_Salvage.Sources.ROV_Dive_Logs;

-- 4b. SILVER LAYER (Efficiency Metrics)
CREATE OR REPLACE VIEW Deep_Blue_Salvage.Silver.Silver_Dive_Stats AS
SELECT
    d.LogID,
    d.WreckID,
    w.Ship_Name,
    d.Max_Depth_Reached,
    d.Bottom_Time_Minutes,
    d.Artifacts_recovered_Count,
    -- Battery Usage
    (d.Battery_Start_Pct - d.Battery_End_Pct) as Battery_Consumed_Pct,
    -- Efficiency: Artifacts per Battery %
    ROUND(d.Artifacts_recovered_Count / NULLIF((d.Battery_Start_Pct - d.Battery_End_Pct), 0), 2) as Artifacts_Per_Battery_Unit
FROM Deep_Blue_Salvage.Bronze.Bronze_Dives d
JOIN Deep_Blue_Salvage.Bronze.Bronze_Wrecks w ON d.WreckID = w.WreckID;

-- 4c. GOLD LAYER (Project Yield)
CREATE OR REPLACE VIEW Deep_Blue_Salvage.Gold.Gold_Project_Yield AS
SELECT
    Ship_Name,
    COUNT(LogID) as Total_Dives,
    SUM(Artifacts_recovered_Count) as Total_Artifacts,
    AVG(Artifacts_Per_Battery_Unit) as Avg_ROV_Efficiency,
    SUM(Bottom_Time_Minutes) as Total_Bottom_Time_Min
FROM Deep_Blue_Salvage.Silver.Silver_Dive_Stats
GROUP BY Ship_Name
ORDER BY Total_Artifacts DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Dive_Stats' to calculate the average battery consumption per minute of bottom time 
 * for the 'SS Gairsoppa' (Deep) vs 'RMS Republic' (Shallow). 
 * Quantify how depth impacts battery drain."
 */
