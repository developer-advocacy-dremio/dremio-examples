/*
 * Dremio Bunker Life Support Systems Example
 * 
 * Domain: Survival Engineering & Safety
 * Scenario: 
 * A fallout shelter monitors life support.
 * We track "CO2_PPM" (Scrubber health), "Water_Recycling_Pct", and "Battery_Volts".
 * The goal is to maximize "Autonomy_Days" before needing external resupply.
 * 
 * Complexity: Low (Threshold monitoring)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Doomsday_Preppers;
CREATE FOLDER IF NOT EXISTS Doomsday_Preppers.Sources;
CREATE FOLDER IF NOT EXISTS Doomsday_Preppers.Bronze;
CREATE FOLDER IF NOT EXISTS Doomsday_Preppers.Silver;
CREATE FOLDER IF NOT EXISTS Doomsday_Preppers.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Doomsday_Preppers.Sources.Shelter_Specs (
    ShelterID VARCHAR,
    Capacity_Persons INT,
    Generator_Type VARCHAR -- 'Diesel', 'Solar+Battery'
);

CREATE TABLE IF NOT EXISTS Doomsday_Preppers.Sources.Atmosphere_Log (
    LogID VARCHAR,
    ShelterID VARCHAR,
    Log_Date DATE,
    CO2_PPM INT, -- Target < 1000
    O2_Percent DOUBLE, -- Target ~21%
    Water_Tank_Level_Liters INT,
    Waste_Water_Recycled_Liters INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Shelters
INSERT INTO Doomsday_Preppers.Sources.Shelter_Specs VALUES
('SH-01', 4, 'Solar+Battery'),
('SH-02', 10, 'Diesel');

-- Seed Logs
INSERT INTO Doomsday_Preppers.Sources.Atmosphere_Log VALUES
('L-001', 'SH-01', '2025-01-01', 450, 21.0, 5000, 50),
('L-002', 'SH-01', '2025-01-02', 460, 20.9, 4950, 48),
('L-003', 'SH-01', '2025-01-03', 470, 20.9, 4900, 52),
('L-004', 'SH-01', '2025-01-15', 1200, 19.5, 4000, 40), -- Scrubber failure?
('L-005', 'SH-02', '2025-01-01', 500, 21.0, 10000, 100),
-- Fill 50
('L-006', 'SH-01', '2025-01-04', 450, 21.0, 4850, 50),
('L-007', 'SH-01', '2025-01-05', 450, 21.0, 4800, 50),
('L-008', 'SH-01', '2025-01-06', 450, 21.0, 4750, 50),
('L-009', 'SH-01', '2025-01-07', 450, 21.0, 4700, 50),
('L-010', 'SH-01', '2025-01-08', 450, 21.0, 4650, 50),
('L-011', 'SH-01', '2025-01-09', 450, 21.0, 4600, 50),
('L-012', 'SH-01', '2025-01-10', 450, 21.0, 4550, 50),
('L-013', 'SH-01', '2025-01-11', 450, 21.0, 4500, 50),
('L-014', 'SH-01', '2025-01-12', 450, 21.0, 4450, 50),
('L-015', 'SH-01', '2025-01-13', 450, 21.0, 4400, 50),
('L-016', 'SH-01', '2025-01-14', 800, 20.0, 4350, 45), -- Warning
('L-017', 'SH-02', '2025-01-02', 500, 21.0, 9900, 100),
('L-018', 'SH-02', '2025-01-03', 500, 21.0, 9800, 100),
('L-019', 'SH-02', '2025-01-04', 500, 21.0, 9700, 100),
('L-020', 'SH-02', '2025-01-05', 500, 21.0, 9600, 100),
('L-021', 'SH-02', '2025-01-06', 500, 21.0, 9500, 100),
('L-022', 'SH-02', '2025-01-07', 500, 21.0, 9400, 100),
('L-023', 'SH-02', '2025-01-08', 500, 21.0, 9300, 100),
('L-024', 'SH-02', '2025-01-09', 500, 21.0, 9200, 100),
('L-025', 'SH-02', '2025-01-10', 500, 21.0, 9100, 100),
('L-026', 'SH-01', '2025-01-16', 1500, 19.0, 3950, 30), -- Critical CO2
('L-027', 'SH-01', '2025-01-17', 2000, 18.5, 3900, 20), -- Danger
('L-028', 'SH-01', '2025-01-18', 450, 21.0, 3850, 50), -- Fixed
('L-029', 'SH-01', '2025-01-19', 450, 21.0, 3800, 50),
('L-030', 'SH-01', '2025-01-20', 450, 21.0, 3750, 50),
('L-031', 'SH-02', '2025-01-11', 500, 21.0, 9000, 100),
('L-032', 'SH-02', '2025-01-12', 500, 21.0, 8900, 100),
('L-033', 'SH-02', '2025-01-13', 500, 21.0, 8800, 100),
('L-034', 'SH-02', '2025-01-14', 500, 21.0, 8700, 100),
('L-035', 'SH-02', '2025-01-15', 500, 21.0, 8600, 100),
('L-036', 'SH-02', '2025-01-16', 500, 21.0, 8500, 100),
('L-037', 'SH-02', '2025-01-17', 500, 21.0, 8400, 100),
('L-038', 'SH-02', '2025-01-18', 500, 21.0, 8300, 100),
('L-039', 'SH-02', '2025-01-19', 500, 21.0, 8200, 100),
('L-040', 'SH-02', '2025-01-20', 500, 21.0, 8100, 100),
('L-041', 'SH-01', '2025-02-01', 450, 21.0, 3700, 50),
('L-042', 'SH-01', '2025-02-02', 450, 21.0, 3650, 50),
('L-043', 'SH-01', '2025-02-03', 450, 21.0, 3600, 50),
('L-044', 'SH-01', '2025-02-04', 450, 21.0, 3550, 50),
('L-045', 'SH-01', '2025-02-05', 450, 21.0, 3500, 50),
('L-046', 'SH-02', '2025-02-01', 500, 21.0, 8000, 100),
('L-047', 'SH-02', '2025-02-02', 500, 21.0, 7900, 100),
('L-048', 'SH-02', '2025-02-03', 500, 21.0, 7800, 100),
('L-049', 'SH-02', '2025-02-04', 500, 21.0, 7700, 100),
('L-050', 'SH-02', '2025-02-05', 500, 21.0, 7600, 100);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Doomsday_Preppers.Bronze.Bronze_Specs AS SELECT * FROM Doomsday_Preppers.Sources.Shelter_Specs;
CREATE OR REPLACE VIEW Doomsday_Preppers.Bronze.Bronze_Logs AS SELECT * FROM Doomsday_Preppers.Sources.Atmosphere_Log;

-- 4b. SILVER LAYER (Habitability Index)
CREATE OR REPLACE VIEW Doomsday_Preppers.Silver.Silver_Life_Status AS
SELECT
    l.LogID,
    l.ShelterID,
    l.Log_Date,
    l.CO2_PPM,
    l.O2_Percent,
    -- Status
    CASE 
        WHEN l.CO2_PPM > 2000 OR l.O2_Percent < 19.0 THEN 'EVACUATE FAIL'
        WHEN l.CO2_PPM > 1000 THEN 'WARNING'
        ELSE 'SAFE'
    END as Air_Quality,
    -- Recycling Efficiency
    ROUND(l.Waste_Water_Recycled_Liters / CAST(50 AS DOUBLE) * 100, 1) as Water_System_Efficiency_Pct -- Assuming 50L target/day
FROM Doomsday_Preppers.Bronze.Bronze_Logs l;

-- 4c. GOLD LAYER (Sustainability Report)
CREATE OR REPLACE VIEW Doomsday_Preppers.Gold.Gold_Autonomy_Projection AS
SELECT
    s.Shelter_ID,
    AVG(l.Water_System_Efficiency_Pct) as Avg_Water_Recycling,
    COUNT(CASE WHEN l.Air_Quality != 'SAFE' THEN 1 END) as Unsafe_Days,
    MAX(l.CO2_PPM) as Peak_CO2
FROM Doomsday_Preppers.Silver.Silver_Life_Status l
JOIN Doomsday_Preppers.Bronze.Bronze_Specs s ON l.ShelterID = s.ShelterID
GROUP BY s.Shelter_ID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Review 'Gold_Autonomy_Projection' to identify shelters with 'Peak_CO2' > 1500. 
 * Correlate this with 'Generator_Type' to see if 'Solar' systems struggle with scrubber power loads."
 */
