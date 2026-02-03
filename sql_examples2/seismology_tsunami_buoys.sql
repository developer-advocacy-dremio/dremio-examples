/*
 * Dremio Seismology Tsunami Buoys Example
 * 
 * Domain: Earth Science & Disaster Warning
 * Scenario: 
 * A network of DART (Deep-ocean Assessment and Reporting of Tsunamis) buoys measures 
 * "Bottom Pressure" to detect sea level anomalies.
 * When a seismic event triggers, the system correlates "Wave Height" anomalies with 
 * earthquake epicenters to issue evacuation warnings.
 * 
 * Complexity: Medium (Sliding window anomaly detection, geospatial distance)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Tsunami_Early_Warning;
CREATE FOLDER IF NOT EXISTS Tsunami_Early_Warning.Sources;
CREATE FOLDER IF NOT EXISTS Tsunami_Early_Warning.Bronze;
CREATE FOLDER IF NOT EXISTS Tsunami_Early_Warning.Silver;
CREATE FOLDER IF NOT EXISTS Tsunami_Early_Warning.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Tsunami_Early_Warning.Sources.Buoy_Net (
    BuoyID VARCHAR,
    Location_Name VARCHAR, -- 'Aleutian Trench', 'Pacific-Center'
    Latitude DOUBLE,
    Longitude DOUBLE,
    Baseline_Depth_Meters DOUBLE
);

CREATE TABLE IF NOT EXISTS Tsunami_Early_Warning.Sources.Pressure_Readings (
    ReadingID VARCHAR,
    BuoyID VARCHAR,
    Timestamp TIMESTAMP,
    Bottom_Pressure_PSI DOUBLE,
    Water_Column_Height_Meters DOUBLE,
    Alert_Mode BOOLEAN -- True if buoy triggered internal logic
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Buoys
INSERT INTO Tsunami_Early_Warning.Sources.Buoy_Net VALUES
('DART-01', 'Aleutian Trench', 51.0, -170.0, 5000.0),
('DART-02', 'Japan Trench', 38.0, 145.0, 6000.0),
('DART-03', 'Hawaii-North', 25.0, -155.0, 4500.0),
('DART-04', 'Chile-Coast', -30.0, -75.0, 4000.0),
('DART-05', 'Cascadia-West', 45.0, -130.0, 3000.0);

-- Seed Readings (Simulating a seismic event near Aleutians)
-- DART-01 sees the wave first
INSERT INTO Tsunami_Early_Warning.Sources.Pressure_Readings VALUES
-- Peace time
('RD-001', 'DART-01', '2023-11-01 08:00:00', 7250.0, 5000.0, false),
('RD-002', 'DART-01', '2023-11-01 08:15:00', 7250.0, 5000.0, false),
('RD-003', 'DART-02', '2023-11-01 08:00:00', 8700.0, 6000.0, false),
-- Earthquake hits near Aleutians at 08:30
('RD-004', 'DART-01', '2023-11-01 08:30:00', 7250.0, 5000.0, false),
('RD-005', 'DART-01', '2023-11-01 08:35:00', 7255.0, 5003.5, true), -- Rise!
('RD-006', 'DART-01', '2023-11-01 08:40:00', 7260.0, 5010.0, true), -- 10m swell (huge)
('RD-007', 'DART-01', '2023-11-01 08:45:00', 7255.0, 5005.0, true),
('RD-008', 'DART-01', '2023-11-01 08:50:00', 7240.0, 4990.0, true), -- Trough
-- Propagation to Hawaii (DART-03) at speed of jet liner (~500mph). Dist ~2000 miles? ~4 hours.
-- Just simulation:
('RD-009', 'DART-03', '2023-11-01 12:00:00', 6525.0, 4500.0, false),
('RD-010', 'DART-03', '2023-11-01 12:15:00', 6526.0, 4501.0, true), -- Small rise
('RD-011', 'DART-03', '2023-11-01 12:30:00', 6527.0, 4502.0, true), -- 2m swell
-- Cascadia (DART-05)
('RD-012', 'DART-05', '2023-11-01 11:00:00', 4350.0, 3000.0, false),
('RD-013', 'DART-05', '2023-11-01 11:15:00', 4351.0, 3001.0, true),
-- Normal noise
('RD-014', 'DART-02', '2023-11-01 08:30:00', 8700.0, 6000.0, false),
('RD-015', 'DART-02', '2023-11-01 09:00:00', 8700.0, 6000.0, false),
-- More DART-01 granularity
('RD-016', 'DART-01', '2023-11-01 09:00:00', 7250.0, 5000.0, false), -- Settling
('RD-017', 'DART-04', '2023-11-01 08:00:00', 5800.0, 4000.0, false),
('RD-018', 'DART-04', '2023-11-01 14:00:00', 5800.0, 4000.0, false), -- Too far south yet
-- False positives?
('RD-019', 'DART-02', '2023-11-02 08:00:00', 8700.1, 6000.1, false), -- Drift
('RD-020', 'DART-02', '2023-11-02 09:00:00', 8699.9, 5999.9, false);
-- Fill to 50
INSERT INTO Tsunami_Early_Warning.Sources.Pressure_Readings VALUES
('RD-021', 'DART-01', '2023-11-03 00:00', 7250, 5000, false),
('RD-022', 'DART-01', '2023-11-03 01:00', 7250, 5000, false),
('RD-023', 'DART-01', '2023-11-03 02:00', 7250, 5000, false),
('RD-024', 'DART-01', '2023-11-03 03:00', 7250, 5000, false),
('RD-025', 'DART-01', '2023-11-03 04:00', 7250, 5000, false),
('RD-026', 'DART-02', '2023-11-03 00:00', 8700, 6000, false),
('RD-027', 'DART-02', '2023-11-03 01:00', 8700, 6000, false),
('RD-028', 'DART-02', '2023-11-03 02:00', 8700, 6000, false),
('RD-029', 'DART-02', '2023-11-03 03:00', 8700, 6000, false),
('RD-030', 'DART-02', '2023-11-03 04:00', 8700, 6000, false),
('RD-031', 'DART-03', '2023-11-03 00:00', 6525, 4500, false),
('RD-032', 'DART-03', '2023-11-03 01:00', 6525, 4500, false),
('RD-033', 'DART-03', '2023-11-03 02:00', 6525, 4500, false),
('RD-034', 'DART-03', '2023-11-03 03:00', 6525, 4500, false),
('RD-035', 'DART-03', '2023-11-03 04:00', 6525, 4500, false),
('RD-036', 'DART-04', '2023-11-03 00:00', 5800, 4000, false),
('RD-037', 'DART-04', '2023-11-03 01:00', 5800, 4000, false),
('RD-038', 'DART-04', '2023-11-03 02:00', 5800, 4000, false),
('RD-039', 'DART-04', '2023-11-03 03:00', 5800, 4000, false),
('RD-040', 'DART-04', '2023-11-03 04:00', 5800, 4000, false),
('RD-041', 'DART-05', '2023-11-03 00:00', 4350, 3000, false),
('RD-042', 'DART-05', '2023-11-03 01:00', 4350, 3000, false),
('RD-043', 'DART-05', '2023-11-03 02:00', 4350, 3000, false),
('RD-044', 'DART-05', '2023-11-03 03:00', 4350, 3000, false),
('RD-045', 'DART-05', '2023-11-03 04:00', 4350, 3000, false),
('RD-046', 'DART-01', '2023-11-04 00:00', 7250, 5000, false),
('RD-047', 'DART-01', '2023-11-04 01:00', 7250, 5000, false),
('RD-048', 'DART-01', '2023-11-04 02:00', 7250, 5000, false),
('RD-049', 'DART-01', '2023-11-04 03:00', 7250, 5000, false),
('RD-050', 'DART-01', '2023-11-04 04:00', 7250, 5000, false);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Tsunami_Early_Warning.Bronze.Bronze_Buoys AS SELECT * FROM Tsunami_Early_Warning.Sources.Buoy_Net;
CREATE OR REPLACE VIEW Tsunami_Early_Warning.Bronze.Bronze_Readings AS SELECT * FROM Tsunami_Early_Warning.Sources.Pressure_Readings;

-- 4b. SILVER LAYER (Anomaly Detection)
CREATE OR REPLACE VIEW Tsunami_Early_Warning.Silver.Silver_Wave_Anomalies AS
SELECT
    r.ReadingID,
    r.BuoyID,
    b.Location_Name,
    r.Timestamp,
    r.Water_Column_Height_Meters,
    b.Baseline_Depth_Meters,
    -- Calculate deviation
    (r.Water_Column_Height_Meters - b.Baseline_Depth_Meters) as Height_Anomaly_Meters,
    -- Classify
    CASE 
        WHEN ABS(r.Water_Column_Height_Meters - b.Baseline_Depth_Meters) > 3.0 THEN 'TSUNAMI DETECTED'
        WHEN ABS(r.Water_Column_Height_Meters - b.Baseline_Depth_Meters) > 0.5 THEN 'Watch Mode'
        ELSE 'Normal'
    END as Alert_Status
FROM Tsunami_Early_Warning.Bronze.Bronze_Readings r
JOIN Tsunami_Early_Warning.Bronze.Bronze_Buoys b ON r.BuoyID = b.BuoyID;

-- 4c. GOLD LAYER (Event Triangulation)
CREATE OR REPLACE VIEW Tsunami_Early_Warning.Gold.Gold_Active_Alerts AS
SELECT
    Location_Name,
    MAX(Height_Anomaly_Meters) as Max_Wave_Height,
    MIN(Timestamp) as First_Detection_Time,
    CASE 
        WHEN MAX(Height_Anomaly_Meters) > 5.0 THEN 'EVACUATE COASTAL AREAS'
        WHEN MAX(Height_Anomaly_Meters) > 1.0 THEN 'ISSUE ADVISORY'
        ELSE 'MONITOR'
    END as Action_Plan
FROM Tsunami_Early_Warning.Silver.Silver_Wave_Anomalies
WHERE Alert_Status != 'Normal'
GROUP BY Location_Name
ORDER BY Max_Wave_Height DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Monitor 'Silver_Wave_Anomalies' for a sequential propagation pattern from 'Aleutian Trench' to 'Hawaii-North'. 
 * Calculate the wave travel speed based on 'First_Detection_Time' diff and distance."
 */
