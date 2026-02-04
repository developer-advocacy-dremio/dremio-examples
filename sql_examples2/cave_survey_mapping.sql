/*
 * Dremio Cave Survey Mapping Example
 * 
 * Domain: Geology & Speleology
 * Scenario: 
 * Cavers survey a cave system using lasers. They record "Stations" (points), 
 * "Azimuth" (compass direction), "Inclination" (slope), and "Distance" (meters).
 * The goal is to close "Loops" (accuracy check) and map Total Depth.
 * 
 * Complexity: Medium (Trigonometry for vector components, z-depth calc)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Spelunking_Geo;
CREATE FOLDER IF NOT EXISTS Spelunking_Geo.Sources;
CREATE FOLDER IF NOT EXISTS Spelunking_Geo.Bronze;
CREATE FOLDER IF NOT EXISTS Spelunking_Geo.Silver;
CREATE FOLDER IF NOT EXISTS Spelunking_Geo.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Spelunking_Geo.Sources.Survey_Trips (
    TripID VARCHAR,
    Cave_Name VARCHAR,
    Survey_Date DATE,
    Team_Leader VARCHAR,
    Survey_Grade VARCHAR -- 'BCRA-3', 'BCRA-5' (High accuracy)
);

CREATE TABLE IF NOT EXISTS Spelunking_Geo.Sources.Survey_Shots (
    ShotID VARCHAR,
    TripID VARCHAR,
    From_Station VARCHAR, -- 'A1'
    To_Station VARCHAR, -- 'A2'
    Distance_Meters DOUBLE,
    Azimuth_Degrees DOUBLE, -- 0-360
    Inclination_Degrees DOUBLE, -- +90 (Up) to -90 (Down)
    Passage_Type VARCHAR -- 'Walking', 'Crawlway', 'Pit'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Trips
INSERT INTO Spelunking_Geo.Sources.Survey_Trips VALUES
('TRIP-001', 'Mammoth Cave System', '2023-01-01', 'Floyd', 'BCRA-5'),
('TRIP-002', 'Lechuguilla', '2023-02-01', 'Hazel', 'BCRA-5');

-- Seed Shots (Main Trunk Passage A)
INSERT INTO Spelunking_Geo.Sources.Survey_Shots VALUES
('S-001', 'TRIP-001', 'ENT', 'A1', 10.0, 180.0, -5.0, 'Walking'), -- Entrance slope
('S-002', 'TRIP-001', 'A1', 'A2', 15.0, 185.0, 0.0, 'Walking'),
('S-003', 'TRIP-001', 'A2', 'A3', 12.5, 190.0, 2.0, 'Walking'),
('S-004', 'TRIP-001', 'A3', 'A4', 8.0, 170.0, -10.0, 'Crawlway'),
('S-005', 'TRIP-001', 'A4', 'A5', 25.0, 180.0, 0.0, 'Walking'),
('S-006', 'TRIP-001', 'A5', 'A6', 30.0, 175.0, -2.0, 'Walking'),
('S-007', 'TRIP-001', 'A6', 'A7', 5.0, 90.0, -90.0, 'Pit'), -- 5m Drop
('S-008', 'TRIP-001', 'A7', 'A8', 10.0, 180.0, 0.0, 'Walking'), -- Bottom of pit
('S-009', 'TRIP-001', 'A8', 'A9', 12.0, 185.0, 5.0, 'Walking'),
('S-010', 'TRIP-001', 'A9', 'A10', 15.0, 190.0, 0.0, 'Walking'),
-- Side Passage B
('S-011', 'TRIP-001', 'A5', 'B1', 6.0, 270.0, 0.0, 'Crawlway'), -- Junction
('S-012', 'TRIP-001', 'B1', 'B2', 5.0, 260.0, -5.0, 'Crawlway'),
('S-013', 'TRIP-001', 'B2', 'B3', 4.0, 250.0, 10.0, 'Crawlway'),
('S-014', 'TRIP-001', 'B3', 'B4', 8.0, 270.0, 0.0, 'Walking'),
('S-015', 'TRIP-001', 'B4', 'B5', 10.0, 280.0, -20.0, 'Walking'), -- Steep
-- Lechuguilla Discovery
('S-016', 'TRIP-002', 'ENT', 'Z1', 20.0, 0.0, -45.0, 'Walking'), -- Steep entrance
('S-017', 'TRIP-002', 'Z1', 'Z2', 15.0, 10.0, -10.0, 'Walking'),
('S-018', 'TRIP-002', 'Z2', 'Z3', 18.0, 350.0, 0.0, 'Walking'),
('S-019', 'TRIP-002', 'Z3', 'Z4', 50.0, 5.0, 0.0, 'Borehole'), -- Big passage
('S-020', 'TRIP-002', 'Z4', 'Z5', 45.0, 15.0, 2.0, 'Borehole'),
-- Loop Closure (A10 connects to ENT roughly?)
('S-021', 'TRIP-001', 'A10', 'A11', 20.0, 360.0, 10.0, 'Walking'), -- Heading back North
('S-022', 'TRIP-001', 'A11', 'A12', 20.0, 350.0, 5.0, 'Walking'),
('S-023', 'TRIP-001', 'A12', 'A13', 20.0, 10.0, 5.0, 'Walking'),
-- Fill 50
('S-024', 'TRIP-001', 'A13', 'A14', 10.0, 0.0, 0.0, 'Walking'),
('S-025', 'TRIP-001', 'A14', 'A15', 10.0, 0.0, 0.0, 'Walking'),
('S-026', 'TRIP-001', 'A15', 'A16', 10.0, 0.0, 0.0, 'Walking'),
('S-027', 'TRIP-001', 'A16', 'A17', 10.0, 0.0, 0.0, 'Walking'),
('S-028', 'TRIP-001', 'A17', 'A18', 10.0, 0.0, 0.0, 'Walking'),
('S-029', 'TRIP-001', 'A18', 'A19', 10.0, 0.0, 0.0, 'Walking'),
('S-030', 'TRIP-001', 'A19', 'A20', 10.0, 0.0, 0.0, 'Walking'),
('S-031', 'TRIP-002', 'Z5', 'Z6', 10.0, 0.0, 0.0, 'Borehole'),
('S-032', 'TRIP-002', 'Z6', 'Z7', 10.0, 0.0, 0.0, 'Borehole'),
('S-033', 'TRIP-002', 'Z7', 'Z8', 10.0, 0.0, 0.0, 'Borehole'),
('S-034', 'TRIP-002', 'Z8', 'Z9', 10.0, 0.0, 0.0, 'Borehole'),
('S-035', 'TRIP-002', 'Z9', 'Z10', 10.0, 0.0, 0.0, 'Borehole'),
('S-036', 'TRIP-001', 'B5', 'B6', 5.0, 90.0, 0.0, 'Walking'),
('S-037', 'TRIP-001', 'B6', 'B7', 5.0, 90.0, 0.0, 'Walking'),
('S-038', 'TRIP-001', 'B7', 'B8', 5.0, 90.0, 0.0, 'Walking'),
('S-039', 'TRIP-001', 'B8', 'B9', 5.0, 90.0, 0.0, 'Walking'),
('S-040', 'TRIP-001', 'B9', 'B10', 5.0, 90.0, 0.0, 'Walking'),
('S-041', 'TRIP-001', 'C1', 'C2', 2.0, 180.0, -10.0, 'Squeeze'), -- Tight
('S-042', 'TRIP-001', 'C2', 'C3', 2.0, 180.0, -10.0, 'Squeeze'),
('S-043', 'TRIP-001', 'C3', 'C4', 2.0, 180.0, -10.0, 'Squeeze'),
('S-044', 'TRIP-001', 'C4', 'C5', 2.0, 180.0, -10.0, 'Squeeze'),
('S-045', 'TRIP-001', 'C5', 'C6', 2.0, 180.0, -10.0, 'Squeeze'),
('S-046', 'TRIP-001', 'C6', 'C7', 2.0, 180.0, -10.0, 'Squeeze'),
('S-047', 'TRIP-001', 'C7', 'C8', 2.0, 180.0, -10.0, 'Squeeze'),
('S-048', 'TRIP-001', 'C8', 'C9', 2.0, 180.0, -10.0, 'Squeeze'),
('S-049', 'TRIP-001', 'C9', 'C10', 2.0, 180.0, -10.0, 'Squeeze'),
('S-050', 'TRIP-001', 'C10', 'C11', 2.0, 180.0, -10.0, 'Squeeze');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Spelunking_Geo.Bronze.Bronze_Trips AS SELECT * FROM Spelunking_Geo.Sources.Survey_Trips;
CREATE OR REPLACE VIEW Spelunking_Geo.Bronze.Bronze_Shots AS SELECT * FROM Spelunking_Geo.Sources.Survey_Shots;

-- 4b. SILVER LAYER (Vector Calculation)
CREATE OR REPLACE VIEW Spelunking_Geo.Silver.Silver_Vector_Components AS
SELECT
    s.TripID,
    s.From_Station,
    s.To_Station,
    s.Distance_Meters,
    s.Azimuth_Degrees,
    s.Inclination_Degrees,
    -- Trig to get XYZ deltas (Pseudo-code logic valid in Dremio SQL)
    -- DZ = Distance * SIN(Inclination)
    ROUND(s.Distance_Meters * SIN(RADIANS(s.Inclination_Degrees)), 2) as Delta_Depth_Z,
    -- Horizontal Length = Distance * COS(Inclination)
    -- DX = Horiz * SIN(Azimuth)
    -- DY = Horiz * COS(Azimuth)
    ROUND((s.Distance_Meters * COS(RADIANS(s.Inclination_Degrees))) * SIN(RADIANS(s.Azimuth_Degrees)), 2) as Delta_East_X,
    ROUND((s.Distance_Meters * COS(RADIANS(s.Inclination_Degrees))) * COS(RADIANS(s.Azimuth_Degrees)), 2) as Delta_North_Y,
    s.Passage_Type
FROM Spelunking_Geo.Bronze.Bronze_Shots s;

-- 4c. GOLD LAYER (Cave Statistics)
CREATE OR REPLACE VIEW Spelunking_Geo.Gold.Gold_Cave_Metrics AS
SELECT
    t.Cave_Name,
    COUNT(*) as Total_Shots,
    SUM(v.Distance_Meters) as Total_Cave_Length_Meters,
    SUM(CASE WHEN v.Passage_Type = 'Pit' THEN v.Distance_Meters ELSE 0 END) as Total_Vertical_Drop_Meters,
    MIN(v.Delta_Depth_Z) as Max_Single_Pitch_Depth,
    SUM(CASE WHEN v.Passage_Type = 'Crawlway' OR v.Passage_Type = 'Squeeze' THEN v.Distance_Meters ELSE 0 END) as Total_Misery_Meters
FROM Spelunking_Geo.Silver.Silver_Vector_Components v
JOIN Spelunking_Geo.Bronze.Bronze_Trips t ON v.TripID = t.TripID
GROUP BY t.Cave_Name;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Vector_Components' to find the deepest point (Sum of Z deltas) relative to the Entrance (ENT). 
 * List the sequence of stations leading to that depth."
 */
