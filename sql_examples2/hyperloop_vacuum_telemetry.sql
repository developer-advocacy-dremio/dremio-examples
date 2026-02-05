/*
 * Dremio Hyperloop Vacuum Telemetry Example
 * 
 * Domain: Future Transportation
 * Scenario: 
 * A Hyperloop system runs pods in a vacuum tube.
 * Sensors track "Tube Pressure" (Pascals), "Maglev Gap" (mm), and "Pod G-Force".
 * Safety requires maintaining near-vacuum and stable levitation.
 * 
 * Complexity: Medium (Physics safety thresholds)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Vacuum_Transport;
CREATE FOLDER IF NOT EXISTS Vacuum_Transport.Sources;
CREATE FOLDER IF NOT EXISTS Vacuum_Transport.Bronze;
CREATE FOLDER IF NOT EXISTS Vacuum_Transport.Silver;
CREATE FOLDER IF NOT EXISTS Vacuum_Transport.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Vacuum_Transport.Sources.Pod_Fleet (
    PodID VARCHAR,
    Model_Version VARCHAR, -- 'v1.0', 'v2-Cargo'
    Max_Speed_Kmh INT
);

CREATE TABLE IF NOT EXISTS Vacuum_Transport.Sources.Test_Runs (
    RunID VARCHAR,
    PodID VARCHAR,
    Tube_Section VARCHAR, -- 'Sector_A', 'Sector_B'
    Tube_Pressure_Pa DOUBLE, -- Target < 100 Pa
    Maglev_Gap_mm DOUBLE, -- Target 15-20 mm
    Max_G_Force DOUBLE, -- Target < 2.0
    Run_Status VARCHAR -- 'Success', 'Abort'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Pods
INSERT INTO Vacuum_Transport.Sources.Pod_Fleet VALUES
('POD-01', 'v1.0', 1000),
('POD-02', 'v2-Cargo', 800);

-- Seed Runs
INSERT INTO Vacuum_Transport.Sources.Test_Runs VALUES
('R-001', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-002', 'POD-01', 'Sector_B', 95.0, 14.0, 1.5, 'Success'), -- Gap narrowing
('R-003', 'POD-01', 'Sector_A', 150.0, 15.0, 1.1, 'Abort'), -- Pressure leak
('R-004', 'POD-02', 'Sector_A', 80.0, 18.0, 0.8, 'Success'),
('R-005', 'POD-02', 'Sector_B', 85.0, 10.0, 1.8, 'Abort'), -- Gap dangerous
-- Fill 50
('R-006', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-007', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-008', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-009', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-010', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-011', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-012', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-013', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-014', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-015', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-016', 'POD-01', 'Sector_A', 200.0, 15.0, 1.0, 'Abort'),
('R-017', 'POD-01', 'Sector_A', 210.0, 15.0, 1.0, 'Abort'),
('R-018', 'POD-01', 'Sector_A', 220.0, 15.0, 1.0, 'Abort'),
('R-019', 'POD-01', 'Sector_A', 180.0, 15.0, 1.0, 'Abort'),
('R-020', 'POD-01', 'Sector_A', 190.0, 15.0, 1.0, 'Abort'),
('R-021', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-022', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-023', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-024', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-025', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-026', 'POD-02', 'Sector_B', 88.0, 12.0, 1.5, 'Success'), -- Tight gap
('R-027', 'POD-02', 'Sector_B', 88.0, 12.0, 1.5, 'Success'),
('R-028', 'POD-02', 'Sector_B', 88.0, 12.0, 1.5, 'Success'),
('R-029', 'POD-02', 'Sector_B', 88.0, 12.0, 1.5, 'Success'),
('R-030', 'POD-02', 'Sector_B', 88.0, 12.0, 1.5, 'Success'),
('R-031', 'POD-02', 'Sector_B', 90.0, 8.0, 2.0, 'Abort'), -- Crash risk
('R-032', 'POD-02', 'Sector_B', 90.0, 8.0, 2.0, 'Abort'),
('R-033', 'POD-02', 'Sector_B', 90.0, 8.0, 2.0, 'Abort'),
('R-034', 'POD-02', 'Sector_B', 90.0, 8.0, 2.0, 'Abort'),
('R-035', 'POD-02', 'Sector_B', 90.0, 8.0, 2.0, 'Abort'),
('R-036', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-037', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-038', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-039', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-040', 'POD-01', 'Sector_A', 90.0, 15.0, 1.2, 'Success'),
('R-041', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-042', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-043', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-044', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-045', 'POD-01', 'Sector_B', 92.0, 15.0, 1.3, 'Success'),
('R-046', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-047', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-048', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-049', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success'),
('R-050', 'POD-02', 'Sector_A', 85.0, 18.0, 0.9, 'Success');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Vacuum_Transport.Bronze.Bronze_Fleet AS SELECT * FROM Vacuum_Transport.Sources.Pod_Fleet;
CREATE OR REPLACE VIEW Vacuum_Transport.Bronze.Bronze_Runs AS SELECT * FROM Vacuum_Transport.Sources.Test_Runs;

-- 4b. SILVER LAYER (Safety Checks)
CREATE OR REPLACE VIEW Vacuum_Transport.Silver.Silver_Run_Analysis AS
SELECT
    r.RunID,
    r.PodID,
    p.Model_Version,
    r.Tube_Pressure_Pa,
    r.Maglev_Gap_mm,
    r.Max_G_Force,
    r.Run_Status,
    -- Failure Root Cause
    CASE 
        WHEN r.Run_Status = 'Abort' AND r.Tube_Pressure_Pa > 120 THEN 'Vacuum Leak'
        WHEN r.Run_Status = 'Abort' AND r.Maglev_Gap_mm < 12 THEN 'Maglev Instability'
        WHEN r.Run_Status = 'Abort' AND r.Max_G_Force > 1.8 THEN 'Passenger G-Limit'
        ELSE 'N/A'
    END as Failure_Mode
FROM Vacuum_Transport.Bronze.Bronze_Runs r
JOIN Vacuum_Transport.Bronze.Bronze_Fleet p ON r.PodID = p.PodID;

-- 4c. GOLD LAYER (Reliability Report)
CREATE OR REPLACE VIEW Vacuum_Transport.Gold.Gold_Safety_Report AS
SELECT
    Model_Version,
    COUNT(*) as Total_Runs,
    SUM(CASE WHEN Run_Status = 'Success' THEN 1 ELSE 0 END) as Success_Count,
    -- System Reliability
    ROUND(CAST(SUM(CASE WHEN Run_Status = 'Success' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100, 2) as Reliability_Percent,
    -- Vacuum Performance
    AVG(Tube_Pressure_Pa) as Avg_Pressure_Pa
FROM Vacuum_Transport.Silver.Silver_Run_Analysis
GROUP BY Model_Version;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Compare 'Avg_Pressure_Pa' between 'Gold_Safety_Report' models. 
 * Identify if the Cargo model (v2) runs at higher pressures than v1.0, impacting efficiency."
 */
