/*
 * Dremio Volcanology Magma Monitor Example
 * 
 * Domain: Geology & Disaster Planning
 * Scenario: 
 * Seismic sensors (Seismometers) and Gas Spectrometers monitor a dormant volcano.
 * Rising "Tremor Amplitude" combined with increased "SO2 Flux" (Sulfur Dioxide) 
 * indicates magma moving toward the surface.
 * The system triggers alert levels (Green, Yellow, Orange, Red).
 * 
 * Complexity: Medium (Multi-sensor correlation, threshold logic)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Volcano_Watch;
CREATE FOLDER IF NOT EXISTS Volcano_Watch.Sources;
CREATE FOLDER IF NOT EXISTS Volcano_Watch.Bronze;
CREATE FOLDER IF NOT EXISTS Volcano_Watch.Silver;
CREATE FOLDER IF NOT EXISTS Volcano_Watch.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Volcano_Watch.Sources.Sensor_Locations (
    SensorID VARCHAR,
    Volcano_Name VARCHAR, -- 'Mt. Rainier', 'Mt. St. Helens'
    Sensor_Type VARCHAR, -- 'Seismic', 'Gas', 'Tiltmeter'
    Distance_From_Caldera_Km DOUBLE,
    Install_Date DATE
);

CREATE TABLE IF NOT EXISTS Volcano_Watch.Sources.Telemetry_Log (
    LogID VARCHAR,
    SensorID VARCHAR,
    Reading_Timestamp TIMESTAMP,
    Value_Metric DOUBLE, -- Generic value column
    Unit VARCHAR -- 'mm/s' (velocity), 'tonnes/day' (flux), 'microradians' (tilt)
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Sensors
INSERT INTO Volcano_Watch.Sources.Sensor_Locations VALUES
('SENS-001', 'Mt. Rainier', 'Seismic', 5.0, '2020-01-01'),
('SENS-002', 'Mt. Rainier', 'Seismic', 10.0, '2020-01-01'),
('SENS-003', 'Mt. Rainier', 'Gas', 0.5, '2021-06-01'), -- Crater rim
('SENS-004', 'Mt. Rainier', 'Tiltmeter', 2.0, '2021-06-01'),
('SENS-005', 'Mt. St. Helens', 'Seismic', 3.0, '2019-01-01');

-- Seed Telemetry (Simulating an awakening event at Rainier)
-- Seismic: Baseline < 10 mm/s. Pre-eruption > 50 mm/s.
-- Gas: SO2 Baseline < 100 t/d. Pre-eruption > 1000 t/d.
-- Normal day
INSERT INTO Volcano_Watch.Sources.Telemetry_Log VALUES
('LOG-001', 'SENS-001', '2023-11-01 08:00:00', 5.2, 'mm/s'),
('LOG-002', 'SENS-001', '2023-11-01 09:00:00', 5.1, 'mm/s'),
('LOG-003', 'SENS-001', '2023-11-01 10:00:00', 5.3, 'mm/s'),
('LOG-004', 'SENS-003', '2023-11-01 08:00:00', 50.0, 'tonnes/day'),
('LOG-005', 'SENS-003', '2023-11-01 12:00:00', 55.0, 'tonnes/day'),
('LOG-006', 'SENS-004', '2023-11-01 08:00:00', 0.1, 'microradians'),
-- Swarm starts
('LOG-007', 'SENS-001', '2023-11-02 08:00:00', 15.0, 'mm/s'),
('LOG-008', 'SENS-001', '2023-11-02 09:00:00', 20.0, 'mm/s'),
('LOG-009', 'SENS-001', '2023-11-02 10:00:00', 25.0, 'mm/s'), -- Increasing tremor
('LOG-010', 'SENS-002', '2023-11-02 08:00:00', 12.0, 'mm/s'), 
('LOG-011', 'SENS-003', '2023-11-02 08:00:00', 200.0, 'tonnes/day'), -- Gas rising
('LOG-012', 'SENS-003', '2023-11-02 12:00:00', 300.0, 'tonnes/day'),
('LOG-013', 'SENS-004', '2023-11-02 08:00:00', 5.0, 'microradians'), -- Deformation (inflation)
-- Intensity builds
('LOG-014', 'SENS-001', '2023-11-03 08:00:00', 45.0, 'mm/s'),
('LOG-015', 'SENS-001', '2023-11-03 09:00:00', 60.0, 'mm/s'), -- RED ALERT level
('LOG-016', 'SENS-003', '2023-11-03 08:00:00', 1500.0, 'tonnes/day'), -- Massive degassing
('LOG-017', 'SENS-004', '2023-11-03 08:00:00', 20.0, 'microradians'),
-- St. Helens stays quiet
('LOG-018', 'SENS-005', '2023-11-01 08:00:00', 2.0, 'mm/s'),
('LOG-019', 'SENS-005', '2023-11-02 08:00:00', 2.1, 'mm/s'),
('LOG-020', 'SENS-005', '2023-11-03 08:00:00', 1.9, 'mm/s'),
-- Filling history
('LOG-021', 'SENS-001', '2023-10-31 08:00:00', 5.0, 'mm/s'),
('LOG-022', 'SENS-001', '2023-10-31 12:00:00', 5.0, 'mm/s'),
('LOG-023', 'SENS-003', '2023-10-31 08:00:00', 40.0, 'tonnes/day'),
('LOG-024', 'SENS-004', '2023-10-31 08:00:00', 0.0, 'microradians'),
('LOG-025', 'SENS-001', '2023-10-30 08:00:00', 5.0, 'mm/s'),
('LOG-026', 'SENS-003', '2023-10-30 08:00:00', 40.0, 'tonnes/day'),
('LOG-027', 'SENS-001', '2023-10-29 08:00:00', 5.0, 'mm/s'),
('LOG-028', 'SENS-001', '2023-10-28 08:00:00', 5.0, 'mm/s'),
('LOG-029', 'SENS-001', '2023-10-27 08:00:00', 5.0, 'mm/s'),
('LOG-030', 'SENS-001', '2023-10-26 08:00:00', 5.0, 'mm/s'),
('LOG-031', 'SENS-003', '2023-10-26 08:00:00', 40.0, 'tonnes/day'),
('LOG-032', 'SENS-001', '2023-10-25 08:00:00', 5.0, 'mm/s'),
('LOG-033', 'SENS-001', '2023-10-24 08:00:00', 5.0, 'mm/s'),
('LOG-034', 'SENS-001', '2023-10-23 08:00:00', 5.0, 'mm/s'),
('LOG-035', 'SENS-001', '2023-10-22 08:00:00', 5.0, 'mm/s'),
('LOG-036', 'SENS-001', '2023-10-21 08:00:00', 5.0, 'mm/s'),
('LOG-037', 'SENS-001', '2023-10-20 08:00:00', 5.0, 'mm/s'),
('LOG-038', 'SENS-003', '2023-10-20 08:00:00', 40.0, 'tonnes/day'),
('LOG-039', 'SENS-004', '2023-10-20 08:00:00', 0.0, 'microradians'),
('LOG-040', 'SENS-001', '2023-10-19 08:00:00', 5.0, 'mm/s'),
('LOG-041', 'SENS-001', '2023-10-18 08:00:00', 5.0, 'mm/s'),
('LOG-042', 'SENS-001', '2023-10-17 08:00:00', 5.0, 'mm/s'),
('LOG-043', 'SENS-001', '2023-10-16 08:00:00', 5.0, 'mm/s'),
('LOG-044', 'SENS-001', '2023-10-15 08:00:00', 5.0, 'mm/s'),
('LOG-045', 'SENS-001', '2023-10-14 08:00:00', 5.0, 'mm/s'),
('LOG-046', 'SENS-001', '2023-10-13 08:00:00', 5.0, 'mm/s'),
('LOG-047', 'SENS-001', '2023-10-12 08:00:00', 5.0, 'mm/s'),
('LOG-048', 'SENS-003', '2023-10-12 08:00:00', 40.0, 'tonnes/day'),
('LOG-049', 'SENS-001', '2023-10-11 08:00:00', 5.0, 'mm/s'),
('LOG-050', 'SENS-001', '2023-10-10 08:00:00', 5.0, 'mm/s');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Volcano_Watch.Bronze.Bronze_Sensors AS SELECT * FROM Volcano_Watch.Sources.Sensor_Locations;
CREATE OR REPLACE VIEW Volcano_Watch.Bronze.Bronze_Telemetry AS SELECT * FROM Volcano_Watch.Sources.Telemetry_Log;

-- 4b. SILVER LAYER (Standardized Metrics)
CREATE OR REPLACE VIEW Volcano_Watch.Silver.Silver_Unified_Monitoring AS
SELECT
    t.LogID,
    t.SensorID,
    s.Volcano_Name,
    s.Sensor_Type,
    t.Reading_Timestamp,
    t.Value_Metric,
    t.Unit,
    -- Normalize specific thresholds per type
    CASE 
        WHEN s.Sensor_Type = 'Seismic' AND t.Value_Metric > 50 THEN 100 -- High danger
        WHEN s.Sensor_Type = 'Seismic' AND t.Value_Metric > 20 THEN 50
        WHEN s.Sensor_Type = 'Gas' AND t.Value_Metric > 1000 THEN 100
        WHEN s.Sensor_Type = 'Gas' AND t.Value_Metric > 500 THEN 50
        WHEN s.Sensor_Type = 'Tiltmeter' AND t.Value_Metric > 10 THEN 75
        ELSE 0
    END as Danger_Score
FROM Volcano_Watch.Bronze.Bronze_Telemetry t
JOIN Volcano_Watch.Bronze.Bronze_Sensors s ON t.SensorID = s.SensorID;

-- 4c. GOLD LAYER (Alert Dashboard)
CREATE OR REPLACE VIEW Volcano_Watch.Gold.Gold_Volcano_Status AS
SELECT
    Volcano_Name,
    MAX(Reading_Timestamp) as Last_Updated,
    MAX(Danger_Score) as Max_Danger_Detected,
    CASE 
        WHEN MAX(Danger_Score) >= 100 THEN 'RED: ERUPTION IMMINENT'
        WHEN MAX(Danger_Score) >= 75 THEN 'ORANGE: MAGMA MOVEMENT'
        WHEN MAX(Danger_Score) >= 50 THEN 'YELLOW: ELEVATED ACTIVITY'
        ELSE 'GREEN: NORMAL'
    END as Alert_Level,
    COUNT(*) as Readings_Last_24h
FROM Volcano_Watch.Silver.Silver_Unified_Monitoring
GROUP BY Volcano_Name;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Check 'Gold_Volcano_Status' for any volcano at 'ORANGE' or 'RED' level. 
 * If found, drill down into 'Silver_Unified_Monitoring' to see if Gas or Seismic sensors triggered the alert first."
 */
