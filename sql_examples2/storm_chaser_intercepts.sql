/*
 * Dremio Storm Chaser Intercepts Example
 * 
 * Domain: Meteorology & Extreme Weather
 * Scenario: 
 * Research teams deploy "Probes" in the path of tornadoes.
 * They log "Intercepts" (tornado proximity), "Vortex Signatures" (Doppler radar), 
 * and "Damage Surveys" (EF Scale).
 * 
 * Complexity: Medium (Geospatial proximity logic simulations, success rate tracking)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Storm_Data;
CREATE FOLDER IF NOT EXISTS Storm_Data.Sources;
CREATE FOLDER IF NOT EXISTS Storm_Data.Bronze;
CREATE FOLDER IF NOT EXISTS Storm_Data.Silver;
CREATE FOLDER IF NOT EXISTS Storm_Data.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Storm_Data.Sources.Chase_Teams (
    TeamID VARCHAR,
    Vehicle_Callsign VARCHAR, -- 'Dominator 1', 'TIV 2'
    Equipment_Type VARCHAR, -- 'Armored Tank', 'Radar Truck', 'Scout'
    Driver VARCHAR
);

CREATE TABLE IF NOT EXISTS Storm_Data.Sources.Intercept_Logs (
    LogID VARCHAR,
    TeamID VARCHAR,
    Storm_Date DATE,
    Target_Storm_Cell VARCHAR, -- 'El Reno Supercell'
    Deploy_Lat DOUBLE,
    Deploy_Lon DOUBLE,
    Tornado_Visual BOOLEAN,
    Measured_Wind_Speed_Mph INT,
    Probe_Survived BOOLEAN
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Teams
INSERT INTO Storm_Data.Sources.Chase_Teams VALUES
('TEAM-001', 'Dominator 1', 'Armored Tank', 'Reed'),
('TEAM-002', 'TIV 2', 'Armored Tank', 'Sean'),
('TEAM-003', 'Scout 1', 'SUV', 'Tim'),
('TEAM-004', 'Radar 1', 'Radar Truck', 'Josh');

-- Seed Intercepts
INSERT INTO Storm_Data.Sources.Intercept_Logs VALUES
('LOG-001', 'TEAM-001', '2023-05-20', 'Moore Supercell', 35.3, -97.5, true, 180, true), -- Success
('LOG-002', 'TEAM-001', '2023-05-21', 'Shawnee Supercell', 35.4, -97.0, true, 150, true),
('LOG-003', 'TEAM-001', '2023-05-31', 'El Reno Supercell', 35.5, -98.0, true, 290, false), -- Probe crushed
('LOG-004', 'TEAM-002', '2023-05-20', 'Moore Supercell', 35.35, -97.4, true, 200, true),
('LOG-005', 'TEAM-002', '2023-05-25', 'Dodge City Cycles', 37.7, -100.0, true, 120, true),
('LOG-006', 'TEAM-003', '2023-05-20', 'Moore Supercell', 35.2, -97.6, false, 60, true), -- Missed visual
('LOG-007', 'TEAM-004', '2023-05-20', 'Moore Supercell', 35.1, -97.7, true, 80, true), -- Radar standoff
('LOG-008', 'TEAM-001', '2023-06-01', 'Panhandle Hook', 36.0, -101.0, false, 50, true), -- Bust
('LOG-009', 'TEAM-001', '2023-06-02', 'Amarillo Hailer', 35.2, -101.8, false, 70, true),
('LOG-010', 'TEAM-001', '2023-06-03', 'Lubbock Dust', 33.5, -101.8, false, 40, true),
-- Filling Rows
('LOG-011', 'TEAM-002', '2023-06-01', 'Panhandle Hook', 36.0, -101.0, false, 55, true),
('LOG-012', 'TEAM-002', '2023-06-02', 'Amarillo Hailer', 35.2, -101.8, true, 90, true),
('LOG-013', 'TEAM-003', '2023-06-01', 'Panhandle Hook', 36.1, -101.1, false, 0, true),
('LOG-014', 'TEAM-004', '2023-06-01', 'Panhandle Hook', 36.0, -101.2, false, 45, true),
('LOG-015', 'TEAM-001', '2023-04-10', 'Dixie Alley', 32.0, -90.0, true, 130, true),
('LOG-016', 'TEAM-001', '2023-04-12', 'Dixie Alley', 33.0, -88.0, false, 60, true),
('LOG-017', 'TEAM-002', '2023-04-10', 'Dixie Alley', 32.1, -90.1, true, 140, false), -- Probe damaged
('LOG-018', 'TEAM-003', '2023-04-10', 'Dixie Alley', 31.9, -89.9, false, 50, true),
('LOG-019', 'TEAM-001', '2023-03-20', 'Texas Dryline', 34.0, -100.0, false, 30, true),
('LOG-020', 'TEAM-001', '2023-03-21', 'Texas Dryline', 34.0, -99.0, false, 0, true), -- Blue sky bust
('LOG-021', 'TEAM-001', '2023-03-22', 'Oklahoma Cap', 35.0, -98.0, true, 110, true),
('LOG-022', 'TEAM-001', '2023-03-23', 'Kansas Warm Front', 38.0, -97.0, false, 50, true),
('LOG-023', 'TEAM-002', '2023-03-22', 'Oklahoma Cap', 35.0, -98.1, true, 115, true),
('LOG-024', 'TEAM-002', '2023-03-23', 'Kansas Warm Front', 38.0, -96.9, false, 40, true),
('LOG-025', 'TEAM-003', '2023-03-22', 'Oklahoma Cap', 34.9, -98.2, false, 60, true),
('LOG-026', 'TEAM-001', '2023-05-15', 'Nebraska High Risk', 40.0, -100.0, true, 160, true),
('LOG-027', 'TEAM-001', '2023-05-16', 'Iowa Derech', 41.0, -95.0, false, 80, true),
('LOG-028', 'TEAM-002', '2023-05-15', 'Nebraska High Risk', 40.1, -99.9, true, 170, true),
('LOG-029', 'TEAM-003', '2023-05-15', 'Nebraska High Risk', 39.9, -100.1, false, 70, true),
('LOG-030', 'TEAM-004', '2023-05-15', 'Nebraska High Risk', 40.2, -99.8, true, 90, true),
('LOG-031', 'TEAM-001', '2023-06-10', 'Northern Plains', 44.0, -103.0, true, 120, true),
('LOG-032', 'TEAM-001', '2023-06-11', 'Northern Plains', 45.0, -102.0, false, 40, true),
('LOG-033', 'TEAM-002', '2023-06-10', 'Northern Plains', 44.1, -103.1, true, 125, true),
('LOG-034', 'TEAM-001', '2023-06-15', 'Canada Setup', 50.0, -110.0, true, 140, true),
('LOG-035', 'TEAM-001', '2023-06-16', 'Canada Setup', 50.0, -109.0, false, 50, true),
('LOG-036', 'TEAM-002', '2023-06-15', 'Canada Setup', 50.1, -110.1, true, 130, true),
('LOG-037', 'TEAM-001', '2023-06-20', 'Upslope Flow', 40.0, -104.0, true, 100, true),
('LOG-038', 'TEAM-001', '2023-06-21', 'Upslope Flow', 40.0, -104.0, true, 110, true),
('LOG-039', 'TEAM-002', '2023-06-20', 'Upslope Flow', 40.0, -104.0, true, 105, true),
('LOG-040', 'TEAM-003', '2023-06-20', 'Upslope Flow', 40.0, -104.0, false, 50, true),
('LOG-041', 'TEAM-004', '2023-06-20', 'Upslope Flow', 40.0, -104.0, true, 70, true),
('LOG-042', 'TEAM-001', '2023-05-01', 'Target Practice', 35.0, -97.0, false, 20, true),
('LOG-043', 'TEAM-002', '2023-05-01', 'Target Practice', 35.0, -97.0, false, 20, true),
('LOG-044', 'TEAM-001', '2023-05-02', 'Maintenance Day', 35.0, -97.0, false, 0, true),
('LOG-045', 'TEAM-002', '2023-05-02', 'Maintenance Day', 35.0, -97.0, false, 0, true),
('LOG-046', 'TEAM-001', '2023-05-03', 'Travel Day', 35.0, -97.0, false, 0, true),
('LOG-047', 'TEAM-001', '2023-05-04', 'Travel Day', 35.0, -97.0, false, 0, true),
('LOG-048', 'TEAM-001', '2023-05-05', 'Travel Day', 35.0, -97.0, false, 0, true),
('LOG-049', 'TEAM-001', '2023-05-06', 'Travel Day', 35.0, -97.0, false, 0, true),
('LOG-050', 'TEAM-001', '2023-05-07', 'Travel Day', 35.0, -97.0, false, 0, true);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Storm_Data.Bronze.Bronze_Teams AS SELECT * FROM Storm_Data.Sources.Chase_Teams;
CREATE OR REPLACE VIEW Storm_Data.Bronze.Bronze_Intercepts AS SELECT * FROM Storm_Data.Sources.Intercept_Logs;

-- 4b. SILVER LAYER (Calculated Intensity)
CREATE OR REPLACE VIEW Storm_Data.Silver.Silver_Storm_Impact AS
SELECT
    i.LogID,
    t.Vehicle_Callsign,
    t.Equipment_Type,
    i.Storm_Date,
    i.Target_Storm_Cell,
    i.Measured_Wind_Speed_Mph,
    -- EF Scale Estimation based on wind speed
    CASE 
        WHEN i.Measured_Wind_Speed_Mph >= 200 THEN 'EF5'
        WHEN i.Measured_Wind_Speed_Mph >= 166 THEN 'EF4'
        WHEN i.Measured_Wind_Speed_Mph >= 136 THEN 'EF3'
        WHEN i.Measured_Wind_Speed_Mph >= 111 THEN 'EF2'
        WHEN i.Measured_Wind_Speed_Mph >= 86 THEN 'EF1'
        WHEN i.Measured_Wind_Speed_Mph >= 65 THEN 'EF0'
        ELSE 'Non-Tornadic'
    END as Estimated_EF_Scale,
    i.Probe_Survived
FROM Storm_Data.Bronze.Bronze_Intercepts i
JOIN Storm_Data.Bronze.Bronze_Teams t ON i.TeamID = t.TeamID;

-- 4c. GOLD LAYER (Probe Reliability)
CREATE OR REPLACE VIEW Storm_Data.Gold.Gold_Interceptor_Stats AS
SELECT
    Vehicle_Callsign,
    Equipment_Type,
    COUNT(*) as Total_Deployments,
    SUM(CASE WHEN Measured_Wind_Speed_Mph > 100 THEN 1 ELSE 0 END) as Violent_Intercepts,
    SUM(CASE WHEN Probe_Survived = false THEN 1 ELSE 0 END) as Equipment_Lost,
    CAST(SUM(CASE WHEN Probe_Survived = false THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 as Failure_Rate_Percent
FROM Storm_Data.Silver.Silver_Storm_Impact
GROUP BY Vehicle_Callsign, Equipment_Type;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Compare 'Violent_Intercepts' counts in 'Gold_Interceptor_Stats' between 'Armored Tank' and 'SUV' types. 
 * Calculate the risk ratio of deployment failure."
 */
