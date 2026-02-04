/*
 * Dremio Professional Fireworks Show Example
 * 
 * Domain: Entertainment & Event Safety
 * Scenario: 
 * A Pyrotechnic company designs huge shows. They sequence "Shells" (Mortars) to music.
 * They track "Launch Timestamp", "Shell Size" (in inches), and "Burst Height" to ensure
 * debris falls in the "Safety Zone".
 * 
 * Complexity: Medium (Timing synchronization, safety radius geometry simulation)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Pyrotechnics_Ops;
CREATE FOLDER IF NOT EXISTS Pyrotechnics_Ops.Sources;
CREATE FOLDER IF NOT EXISTS Pyrotechnics_Ops.Bronze;
CREATE FOLDER IF NOT EXISTS Pyrotechnics_Ops.Silver;
CREATE FOLDER IF NOT EXISTS Pyrotechnics_Ops.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Pyrotechnics_Ops.Sources.Show_Manifest (
    ShowID VARCHAR,
    Event_Name VARCHAR, -- 'NYE 2024', '4th of July'
    Music_Track VARCHAR,
    Total_Duration_Sec INT
);

CREATE TABLE IF NOT EXISTS Pyrotechnics_Ops.Sources.Firing_Script (
    CueID VARCHAR,
    ShowID VARCHAR,
    Fire_Timestamp_Offset_Sec DOUBLE, -- 1.5s into the song
    Shell_Size_Inches INT, -- 3, 4, 5, 8, 10
    Effect_Type VARCHAR, -- 'Peony', 'Willow', 'Salute'
    Rack_Position VARCHAR,
    Did_Fire_Successfully BOOLEAN
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Show
INSERT INTO Pyrotechnics_Ops.Sources.Show_Manifest VALUES
('SHOW-001', 'NYE 2024', 'Overture_1812', 600); -- 10 min

-- Seed Script (Finale Sequence)
INSERT INTO Pyrotechnics_Ops.Sources.Firing_Script VALUES
('CUE-001', 'SHOW-001', 0.0, 3, 'Comet', 'R1-1', true),
('CUE-002', 'SHOW-001', 1.0, 3, 'Comet', 'R1-2', true),
('CUE-003', 'SHOW-001', 2.0, 3, 'Comet', 'R1-3', true),
('CUE-004', 'SHOW-001', 5.0, 4, 'Peony Red', 'R2-1', true),
('CUE-005', 'SHOW-001', 5.5, 4, 'Peony Blue', 'R2-2', true),
('CUE-006', 'SHOW-001', 10.0, 5, 'Willow Gold', 'R3-1', true),
('CUE-007', 'SHOW-001', 12.0, 5, 'Willow Gold', 'R3-2', true),
('CUE-008', 'SHOW-001', 15.0, 3, 'Salute', 'R1-4', true), -- BOOM
('CUE-009', 'SHOW-001', 15.2, 3, 'Salute', 'R1-5', true),
('CUE-010', 'SHOW-001', 15.4, 3, 'Salute', 'R1-6', true),
-- Finale barrage (Simulated rapid fire)
('CUE-011', 'SHOW-001', 580.0, 3, 'Crossette', 'R4-1', true),
('CUE-012', 'SHOW-001', 580.1, 3, 'Crossette', 'R4-2', true),
('CUE-013', 'SHOW-001', 580.2, 3, 'Crossette', 'R4-3', true),
('CUE-014', 'SHOW-001', 580.3, 3, 'Crossette', 'R4-4', true),
('CUE-015', 'SHOW-001', 580.4, 3, 'Crossette', 'R4-5', true),
('CUE-016', 'SHOW-001', 580.5, 3, 'Crossette', 'R4-6', true),
('CUE-017', 'SHOW-001', 581.0, 4, 'Brocade', 'R5-1', true),
('CUE-018', 'SHOW-001', 581.2, 4, 'Brocade', 'R5-2', true),
('CUE-019', 'SHOW-001', 581.4, 4, 'Brocade', 'R5-3', true),
('CUE-020', 'SHOW-001', 581.6, 4, 'Brocade', 'R5-4', true),
('CUE-021', 'SHOW-001', 582.0, 5, 'Dahlia', 'R6-1', true),
('CUE-022', 'SHOW-001', 582.5, 5, 'Dahlia', 'R6-2', true),
('CUE-023', 'SHOW-001', 583.0, 6, 'Ring', 'R7-1', true),
('CUE-024', 'SHOW-001', 583.5, 6, 'Ring', 'R7-2', true),
('CUE-025', 'SHOW-001', 584.0, 8, 'Shell of Shells', 'R8-1', true), -- Big one
('CUE-026', 'SHOW-001', 585.0, 8, 'Shell of Shells', 'R8-2', true),
('CUE-027', 'SHOW-001', 586.0, 10, 'Titanium Salute', 'R9-1', true), -- Huge
('CUE-028', 'SHOW-001', 587.0, 10, 'Titanium Salute', 'R9-2', false), -- Dud!
('CUE-029', 'SHOW-001', 590.0, 3, 'Mine', 'R1-7', true),
('CUE-030', 'SHOW-001', 590.1, 3, 'Mine', 'R1-8', true),
('CUE-031', 'SHOW-001', 590.2, 3, 'Mine', 'R1-9', true),
('CUE-032', 'SHOW-001', 590.3, 3, 'Mine', 'R1-10', true),
('CUE-033', 'SHOW-001', 590.4, 3, 'Mine', 'R1-11', true),
('CUE-034', 'SHOW-001', 590.5, 3, 'Mine', 'R1-12', true),
('CUE-035', 'SHOW-001', 591.0, 4, 'Whistle', 'R2-3', true),
('CUE-036', 'SHOW-001', 591.2, 4, 'Whistle', 'R2-4', true),
('CUE-037', 'SHOW-001', 591.4, 4, 'Whistle', 'R2-5', true),
('CUE-038', 'SHOW-001', 591.6, 4, 'Whistle', 'R2-6', true),
('CUE-039', 'SHOW-001', 592.0, 5, 'Palm', 'R3-3', true),
('CUE-040', 'SHOW-001', 592.5, 5, 'Palm', 'R3-4', true),
('CUE-041', 'SHOW-001', 593.0, 6, 'Ghost', 'R7-3', true),
('CUE-042', 'SHOW-001', 593.5, 6, 'Ghost', 'R7-4', true),
('CUE-043', 'SHOW-001', 594.0, 6, 'Ghost', 'R7-5', true),
('CUE-044', 'SHOW-001', 595.0, 10, 'Kamuro', 'R9-3', true),
('CUE-045', 'SHOW-001', 596.0, 10, 'Kamuro', 'R9-4', true),
('CUE-046', 'SHOW-001', 597.0, 10, 'Kamuro', 'R9-5', true),
('CUE-047', 'SHOW-001', 598.0, 10, 'Kamuro', 'R9-6', true),
('CUE-048', 'SHOW-001', 599.0, 3, 'Salute Finale', 'R1-13', true),
('CUE-049', 'SHOW-001', 599.1, 3, 'Salute Finale', 'R1-14', true),
('CUE-050', 'SHOW-001', 599.2, 3, 'Salute Finale', 'R1-15', true);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Pyrotechnics_Ops.Bronze.Bronze_Manifest AS SELECT * FROM Pyrotechnics_Ops.Sources.Show_Manifest;
CREATE OR REPLACE VIEW Pyrotechnics_Ops.Bronze.Bronze_Script AS SELECT * FROM Pyrotechnics_Ops.Sources.Firing_Script;

-- 4b. SILVER LAYER (Safety & Height Analysis)
CREATE OR REPLACE VIEW Pyrotechnics_Ops.Silver.Silver_Safety_Trajectory AS
SELECT
    s.CueID,
    s.ShowID,
    s.Fire_Timestamp_Offset_Sec,
    s.Shell_Size_Inches,
    s.Effect_Type,
    -- Rule of thumb: 100 ft rise per inch of shell
    s.Shell_Size_Inches * 100 as Approx_Burst_Height_Ft,
    -- Fallout radius approx 0.75 * burst height drift
    (s.Shell_Size_Inches * 100) * 0.75 as Fallout_Zone_Radius_Ft,
    s.Did_Fire_Successfully
FROM Pyrotechnics_Ops.Bronze.Bronze_Script s;

-- 4c. GOLD LAYER (Post-Show Report)
CREATE OR REPLACE VIEW Pyrotechnics_Ops.Gold.Gold_Performance_Report AS
SELECT
    ShowID,
    COUNT(*) as Total_Cues,
    SUM(CASE WHEN Did_Fire_Successfully = false THEN 1 ELSE 0 END) as Misfire_Count,
    MAX(Shell_Size_Inches) as Largest_Shell_Inches,
    MAX(Approx_Burst_Height_Ft) as Max_Altitude_Reached_Ft,
    -- Calculated Success Rate
    ROUND(CAST(SUM(CASE WHEN Did_Fire_Successfully THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100, 2) as Success_Rate_Percent
FROM Pyrotechnics_Ops.Silver.Silver_Safety_Trajectory
GROUP BY ShowID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Review 'Silver_Safety_Trajectory' to find any cues where 'Fallout_Zone_Radius_Ft' > 600 ft. 
 * Ensure these larger shells (8-10 inch) were fired at least 580 seconds into the show (Finale)."
 */
