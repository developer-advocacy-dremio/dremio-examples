/*
 * Dremio Formula 1 Race Strategy Example
 * 
 * Domain: Motorsports & Real-Time Logistics
 * Scenario: 
 * F1 teams track "Tyre Degradation" (lap times getting slower) and "Fuel Load".
 * They must decide the optimal "Pit Window" to undercut opponents.
 * Simulation models calculated "Predicted Lap Time" vs "Actual Lap Time".
 * 
 * Complexity: Medium (Window functions for lap deltas, cumulative sums)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS F1_Telemetry;
CREATE FOLDER IF NOT EXISTS F1_Telemetry.Sources;
CREATE FOLDER IF NOT EXISTS F1_Telemetry.Bronze;
CREATE FOLDER IF NOT EXISTS F1_Telemetry.Silver;
CREATE FOLDER IF NOT EXISTS F1_Telemetry.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS F1_Telemetry.Sources.Race_Config (
    RaceID VARCHAR,
    Circuit_Name VARCHAR, -- 'Monaco', 'Silverstone'
    Total_Laps INT,
    Pit_Loss_Seconds DOUBLE -- Time lost driving through pit lane (e.g., 20s)
);

CREATE TABLE IF NOT EXISTS F1_Telemetry.Sources.Car_Laps (
    LapID VARCHAR,
    RaceID VARCHAR,
    Driver VARCHAR,
    Lap_Number INT,
    Tyre_Compound VARCHAR, -- 'Soft', 'Medium', 'Hard'
    Tyre_Age_Laps INT,
    Lap_Time_Seconds DOUBLE,
    Fuel_Load_Kg DOUBLE,
    Pit_Stop BOOLEAN
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Race
INSERT INTO F1_Telemetry.Sources.Race_Config VALUES
('RACE-001', 'Silverstone', 52, 22.5);

-- Seed Laps (Verstappen vs Hamilton simulation)
-- Driver A: Starts Soft, Pits Lap 15.
-- Driver B: Starts Medium, Pits Lap 20.
INSERT INTO F1_Telemetry.Sources.Car_Laps VALUES
('L-101', 'RACE-001', 'Verstappen', 1, 'Soft', 1, 90.5, 100.0, false),
('L-102', 'RACE-001', 'Verstappen', 2, 'Soft', 2, 89.0, 98.5, false), -- Faster as tires warm
('L-103', 'RACE-001', 'Verstappen', 3, 'Soft', 3, 88.5, 97.0, false),
('L-104', 'RACE-001', 'Verstappen', 4, 'Soft', 4, 88.5, 95.5, false),
('L-105', 'RACE-001', 'Verstappen', 5, 'Soft', 5, 88.6, 94.0, false), -- Deg starts
('L-106', 'RACE-001', 'Verstappen', 6, 'Soft', 6, 88.8, 92.5, false),
('L-107', 'RACE-001', 'Verstappen', 7, 'Soft', 7, 89.0, 91.0, false),
('L-108', 'RACE-001', 'Verstappen', 8, 'Soft', 8, 89.3, 89.5, false),
('L-109', 'RACE-001', 'Verstappen', 9, 'Soft', 9, 89.6, 88.0, false),
('L-110', 'RACE-001', 'Verstappen', 10, 'Soft', 10, 90.0, 86.5, false), -- Drop off
('L-111', 'RACE-001', 'Verstappen', 11, 'Soft', 11, 90.5, 85.0, false),
('L-112', 'RACE-001', 'Verstappen', 12, 'Soft', 12, 91.0, 83.5, false),
('L-113', 'RACE-001', 'Verstappen', 13, 'Soft', 13, 91.5, 82.0, false),
('L-114', 'RACE-001', 'Verstappen', 14, 'Soft', 14, 92.0, 80.5, false), -- Box Box
('L-115', 'RACE-001', 'Verstappen', 15, 'Soft', 15, 114.5, 79.0, true), -- Pit stop (+22.5s)
('L-116', 'RACE-001', 'Verstappen', 16, 'Hard', 1, 93.0, 77.5, false), -- Out lap, cold hard tires
('L-117', 'RACE-001', 'Verstappen', 17, 'Hard', 2, 89.5, 76.0, false), -- Up to speed
('L-118', 'RACE-001', 'Verstappen', 18, 'Hard', 3, 89.0, 74.5, false),

('L-201', 'RACE-001', 'Hamilton', 1, 'Medium', 1, 91.0, 100.0, false),
('L-202', 'RACE-001', 'Hamilton', 2, 'Medium', 2, 90.0, 98.5, false),
('L-203', 'RACE-001', 'Hamilton', 3, 'Medium', 3, 89.5, 97.0, false),
('L-204', 'RACE-001', 'Hamilton', 4, 'Medium', 4, 89.4, 95.5, false),
('L-205', 'RACE-001', 'Hamilton', 5, 'Medium', 5, 89.4, 94.0, false),
('L-206', 'RACE-001', 'Hamilton', 6, 'Medium', 6, 89.5, 92.5, false),
('L-207', 'RACE-001', 'Hamilton', 7, 'Medium', 7, 89.6, 91.0, false), -- Slower degradation
('L-208', 'RACE-001', 'Hamilton', 8, 'Medium', 8, 89.7, 89.5, false),
('L-209', 'RACE-001', 'Hamilton', 9, 'Medium', 9, 89.8, 88.0, false),
('L-210', 'RACE-001', 'Hamilton', 10, 'Medium', 10, 89.9, 86.5, false),
('L-211', 'RACE-001', 'Hamilton', 11, 'Medium', 11, 90.0, 85.0, false), -- Passing Max's times now
('L-212', 'RACE-001', 'Hamilton', 12, 'Medium', 12, 90.1, 83.5, false),
('L-213', 'RACE-001', 'Hamilton', 13, 'Medium', 13, 90.2, 82.0, false),
('L-214', 'RACE-001', 'Hamilton', 14, 'Medium', 14, 90.4, 80.5, false),
('L-215', 'RACE-001', 'Hamilton', 15, 'Medium', 15, 90.6, 79.0, false), -- Staying out
('L-216', 'RACE-001', 'Hamilton', 16, 'Medium', 16, 90.8, 77.5, false),
('L-217', 'RACE-001', 'Hamilton', 17, 'Medium', 17, 91.0, 76.0, false),
('L-218', 'RACE-001', 'Hamilton', 18, 'Medium', 18, 91.5, 74.5, false), -- Losing time to Max's new hards
('L-219', 'RACE-001', 'Hamilton', 19, 'Medium', 19, 92.0, 73.0, false),
('L-220', 'RACE-001', 'Hamilton', 20, 'Medium', 20, 114.0, 71.5, true), -- Pit stop

-- More laps to fill 50...
('L-119', 'RACE-001', 'Verstappen', 19, 'Hard', 4, 88.8, 73.0, false),
('L-120', 'RACE-001', 'Verstappen', 20, 'Hard', 5, 88.7, 71.5, false),
('L-121', 'RACE-001', 'Verstappen', 21, 'Hard', 6, 88.6, 70.0, false),
('L-122', 'RACE-001', 'Verstappen', 22, 'Hard', 7, 88.6, 68.5, false),
('L-123', 'RACE-001', 'Verstappen', 23, 'Hard', 8, 88.7, 67.0, false),
('L-221', 'RACE-001', 'Hamilton', 21, 'Hard', 1, 92.0, 70.0, false), -- Out lap
('L-222', 'RACE-001', 'Hamilton', 22, 'Hard', 2, 89.0, 68.5, false),
('L-223', 'RACE-001', 'Hamilton', 23, 'Hard', 3, 88.5, 67.0, false),
('L-224', 'RACE-001', 'Hamilton', 24, 'Hard', 4, 88.4, 65.5, false),
('L-225', 'RACE-001', 'Hamilton', 25, 'Hard', 5, 88.4, 64.0, false);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW F1_Telemetry.Bronze.Bronze_Laps AS SELECT * FROM F1_Telemetry.Sources.Car_Laps;
CREATE OR REPLACE VIEW F1_Telemetry.Bronze.Bronze_Config AS SELECT * FROM F1_Telemetry.Sources.Race_Config;

-- 4b. SILVER LAYER (Race Pace Analysis)
CREATE OR REPLACE VIEW F1_Telemetry.Silver.Silver_Lap_Deltas AS
SELECT
    l.*,
    -- Calculate Lap Delta (Difference from previous lap)
    l.Lap_Time_Seconds - LAG(l.Lap_Time_Seconds) OVER (PARTITION BY l.Driver ORDER BY l.Lap_Number) as Time_Delta_Prev_Lap,
    -- Cumulative Race Time
    SUM(l.Lap_Time_Seconds) OVER (PARTITION BY l.Driver ORDER BY l.Lap_Number) as Total_Race_Time
FROM F1_Telemetry.Bronze.Bronze_Laps l;

-- 4c. GOLD LAYER (Undercut Strategy)
CREATE OR REPLACE VIEW F1_Telemetry.Gold.Gold_Gap_To_Leader AS
SELECT
    l.Lap_Number,
    l.Driver,
    l.Total_Race_Time,
    -- Assume Verstappen is leader for simplicity, calc gap
    l.Total_Race_Time - FIRST_VALUE(l.Total_Race_Time) OVER (PARTITION BY l.Lap_Number ORDER BY l.Total_Race_Time ASC) as Gap_To_Leader_Seconds,
    l.Tyre_Compound,
    l.Pit_Stop
FROM F1_Telemetry.Silver.Silver_Lap_Deltas l;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Lap_Deltas' to find the 'Crossover Point' where Verstappen's old Soft tires 
 * became slower than the projected time of new Hard tires (approx 89.0s). 
 * Did he pit at the optimal lap?"
 */
