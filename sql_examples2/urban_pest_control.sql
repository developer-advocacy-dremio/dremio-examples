/*
 * Dremio Urban Pest Control Example
 * 
 * Domain: City Services & Public Health
 * Scenario: 
 * A city tracks pest reports (Rats, Cockroaches) and manages "Bait Stations".
 * They log "Consumption Rates" of bait and "Activity Indexes" to identify hotspots.
 * Visualization helps deploy exterminators efficiently.
 * 
 * Complexity: Low (Heatmap-style aggregation)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Sanitation;
CREATE FOLDER IF NOT EXISTS City_Sanitation.Sources;
CREATE FOLDER IF NOT EXISTS City_Sanitation.Bronze;
CREATE FOLDER IF NOT EXISTS City_Sanitation.Silver;
CREATE FOLDER IF NOT EXISTS City_Sanitation.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Sanitation.Sources.Bait_Stations (
    StationID VARCHAR,
    Zone VARCHAR, -- 'Downtown', 'Harbor', 'Residential-A'
    Install_Date DATE,
    Trap_Type VARCHAR -- 'Snap Trap', 'Poison Box', 'Electronic'
);

CREATE TABLE IF NOT EXISTS City_Sanitation.Sources.Service_Logs (
    LogID VARCHAR,
    StationID VARCHAR,
    Service_Date DATE,
    Bait_Consumed_Percent INT, -- 0-100%
    Activity_Signs VARCHAR, -- 'Burrows', 'Droppings', 'None'
     Technician VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Stations
INSERT INTO City_Sanitation.Sources.Bait_Stations VALUES
('BS-001', 'Downtown', '2023-01-01', 'Poison Box'),
('BS-002', 'Downtown', '2023-01-01', 'Poison Box'),
('BS-003', 'Harbor', '2023-01-01', 'Snap Trap'),
('BS-004', 'Residential-A', '2023-02-01', 'Electronic'),
('BS-005', 'Harbor', '2023-01-15', 'Poison Box');

-- Seed Logs
INSERT INTO City_Sanitation.Sources.Service_Logs VALUES
('L-001', 'BS-001', '2023-06-01', 100, 'Burrows', 'Mario'),
('L-002', 'BS-001', '2023-06-08', 50, 'Droppings', 'Mario'),
('L-003', 'BS-001', '2023-06-15', 20, 'None', 'Mario'), -- Reducing
('L-004', 'BS-002', '2023-06-01', 80, 'Burrows', 'Luigi'),
('L-005', 'BS-002', '2023-06-08', 90, 'Burrows', 'Luigi'), -- Persistent
('L-006', 'BS-003', '2023-06-01', 10, 'None', 'Peach'),
('L-007', 'BS-003', '2023-06-08', 0, 'None', 'Peach'),
('L-008', 'BS-004', '2023-06-01', 5, 'None', 'Toad'),
('L-009', 'BS-005', '2023-06-01', 100, 'Droppings', 'Peach'),
('L-010', 'BS-005', '2023-06-08', 100, 'Droppings', 'Peach'), -- Hotspot
-- Fill Rows
('L-011', 'BS-001', '2023-06-22', 10, 'None', 'Mario'),
('L-012', 'BS-001', '2023-06-29', 0, 'None', 'Mario'),
('L-013', 'BS-002', '2023-06-15', 60, 'Droppings', 'Luigi'),
('L-014', 'BS-002', '2023-06-22', 40, 'None', 'Luigi'),
('L-015', 'BS-002', '2023-06-29', 20, 'None', 'Luigi'),
('L-016', 'BS-005', '2023-06-15', 90, 'Droppings', 'Peach'),
('L-017', 'BS-005', '2023-06-22', 80, 'Droppings', 'Peach'),
('L-018', 'BS-005', '2023-06-29', 70, 'Burrows', 'Peach'),
('L-019', 'BS-001', '2023-05-01', 100, 'Burrows', 'Mario'),
('L-020', 'BS-001', '2023-05-08', 100, 'Burrows', 'Mario'),
('L-021', 'BS-001', '2023-05-15', 90, 'Burrows', 'Mario'),
('L-022', 'BS-001', '2023-05-22', 80, 'Burrows', 'Mario'),
('L-023', 'BS-002', '2023-05-01', 90, 'Burrows', 'Luigi'),
('L-024', 'BS-002', '2023-05-08', 90, 'Burrows', 'Luigi'),
('L-025', 'BS-002', '2023-05-15', 90, 'Burrows', 'Luigi'),
('L-026', 'BS-003', '2023-05-01', 20, 'None', 'Peach'),
('L-027', 'BS-003', '2023-05-08', 10, 'None', 'Peach'),
('L-028', 'BS-004', '2023-05-01', 10, 'None', 'Toad'),
('L-029', 'BS-004', '2023-05-08', 5, 'None', 'Toad'),
('L-030', 'BS-005', '2023-05-01', 100, 'Droppings', 'Peach'),
('L-031', 'BS-005', '2023-05-08', 100, 'Droppings', 'Peach'),
('L-032', 'BS-005', '2023-05-15', 100, 'Droppings', 'Peach'),
('L-033', 'BS-005', '2023-05-22', 100, 'Droppings', 'Peach'),
('L-034', 'BS-001', '2023-04-01', 50, 'Droppings', 'Mario'),
('L-035', 'BS-001', '2023-04-08', 60, 'Droppings', 'Mario'),
('L-036', 'BS-001', '2023-04-15', 70, 'Droppings', 'Mario'),
('L-037', 'BS-001', '2023-04-22', 80, 'Droppings', 'Mario'),
('L-038', 'BS-002', '2023-04-01', 50, 'Droppings', 'Luigi'),
('L-039', 'BS-002', '2023-04-08', 60, 'Droppings', 'Luigi'),
('L-040', 'BS-002', '2023-04-15', 70, 'Droppings', 'Luigi'),
('L-041', 'BS-003', '2023-04-01', 0, 'None', 'Peach'),
('L-042', 'BS-004', '2023-04-01', 0, 'None', 'Toad'),
('L-043', 'BS-005', '2023-04-01', 90, 'Droppings', 'Peach'),
('L-044', 'BS-005', '2023-04-08', 95, 'Droppings', 'Peach'),
('L-045', 'BS-005', '2023-04-15', 100, 'Droppings', 'Peach'),
('L-046', 'BS-001', '2023-03-01', 10, 'None', 'Mario'),
('L-047', 'BS-002', '2023-03-01', 10, 'None', 'Luigi'),
('L-048', 'BS-003', '2023-03-01', 0, 'None', 'Peach'),
('L-049', 'BS-004', '2023-03-01', 0, 'None', 'Toad'),
('L-050', 'BS-005', '2023-03-01', 80, 'Droppings', 'Peach');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW City_Sanitation.Bronze.Bronze_Stations AS SELECT * FROM City_Sanitation.Sources.Bait_Stations;
CREATE OR REPLACE VIEW City_Sanitation.Bronze.Bronze_Logs AS SELECT * FROM City_Sanitation.Sources.Service_Logs;

-- 4b. SILVER LAYER (Activity Scoring)
CREATE OR REPLACE VIEW City_Sanitation.Silver.Silver_Infestation_Levels AS
SELECT
    l.LogID,
    s.Zone,
    s.StationID,
    l.Service_Date,
    l.Bait_Consumed_Percent,
    l.Activity_Signs,
    -- Score: 0-10 based on evidence
    CASE 
        WHEN l.Activity_Signs = 'Burrows' THEN 10
        WHEN l.Activity_Signs = 'Droppings' THEN 7
        WHEN l.Bait_Consumed_Percent > 50 THEN 5
        WHEN l.Bait_Consumed_Percent > 10 THEN 2
        ELSE 0
    END as Infestation_Score
FROM City_Sanitation.Bronze.Bronze_Logs l
JOIN City_Sanitation.Bronze.Bronze_Stations s ON l.StationID = s.StationID;

-- 4c. GOLD LAYER (Hotspot Map)
CREATE OR REPLACE VIEW City_Sanitation.Gold.Gold_Zone_Hotspots AS
SELECT
    Zone,
    AVG(Infestation_Score) as Avg_Score,
    MAX(Infestation_Score) as Peak_Score,
    SUM(Bait_Consumed_Percent) as Total_Bait_Usage,
    CASE 
        WHEN AVG(Infestation_Score) > 5 THEN 'HIGH PRIORITY'
        WHEN AVG(Infestation_Score) > 2 THEN 'MONITOR'
        ELSE 'CONTROLLED'
    END as Zone_Status
FROM City_Sanitation.Silver.Silver_Infestation_Levels
WHERE Service_Date > CURRENT_DATE - 30 -- Monthly rolling
GROUP BY Zone;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "From 'Gold_Zone_Hotspots', identify which Zone has the highest 'Peak_Score' and generate a SQL query 
 * to list the Technician responsible for those stations in 'Silver_Infestation_Levels'."
 */
