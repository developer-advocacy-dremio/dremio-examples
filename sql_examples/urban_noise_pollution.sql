/*
 * Dremio Urban Noise Pollution Mapping Example
 * 
 * Domain: Smart Cities & Public Health
 * Scenario: 
 * A Smart City initiative deploys decibel sensors to monitor noise pollution. 
 * The goal is to enforce noise ordinances which vary by "Zone Type" (Residential vs Industrial)
 * and "Time of Day" (Day vs Night).
 * Sensor readings are joined with Zoning definitions to flag violations.
 * 
 * Complexity: Medium (Geospatial logical join, time-window compliance)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Smart_City;
CREATE FOLDER IF NOT EXISTS Smart_City.Sources;
CREATE FOLDER IF NOT EXISTS Smart_City.Bronze;
CREATE FOLDER IF NOT EXISTS Smart_City.Silver;
CREATE FOLDER IF NOT EXISTS Smart_City.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Smart_City.Sources.City_Zoning (
    ZoneID VARCHAR,
    Zone_Type VARCHAR, -- 'Residential', 'Commercial', 'Industrial', 'Quiet Zone'
    Max_Decibels_Day INT,
    Max_Decibels_Night INT,
    Lat_Start DOUBLE,
    Lat_End DOUBLE,
    Lon_Start DOUBLE,
    Lon_End DOUBLE
);

CREATE TABLE IF NOT EXISTS Smart_City.Sources.Noise_Sensors (
    SensorID VARCHAR,
    ZoneID_Mapped VARCHAR, -- In reality, this might be derived spatially
    Timestamp TIMESTAMP,
    Decibel_Level DOUBLE,
    Frequency_Hz DOUBLE -- Dominant frequency
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Zones
INSERT INTO Smart_City.Sources.City_Zoning VALUES
('Z-001', 'Residential', 60, 45, 40.70, 40.71, -74.00, -73.99),
('Z-002', 'Commercial', 75, 60, 40.71, 40.72, -74.00, -73.99),
('Z-003', 'Industrial', 85, 75, 40.72, 40.73, -74.00, -73.99),
('Z-004', 'Quiet Zone', 50, 40, 40.73, 40.74, -74.00, -73.99), -- Hospital area
('Z-005', 'Residential', 60, 45, 40.74, 40.75, -74.00, -73.99);

-- Seed Sensor Readings (Simulating one day)
INSERT INTO Smart_City.Sources.Noise_Sensors VALUES
('S-101', 'Z-001', '2023-09-01 08:00:00', 55.0, 400),
('S-101', 'Z-001', '2023-09-01 12:00:00', 58.0, 450),
('S-101', 'Z-001', '2023-09-01 22:00:00', 44.0, 300), -- Night compliant
('S-102', 'Z-001', '2023-09-01 08:30:00', 56.0, 410),
('S-102', 'Z-001', '2023-09-01 12:30:00', 65.0, 800), -- Day Violation (Lawnmower?)
('S-102', 'Z-001', '2023-09-01 23:00:00', 43.0, 200),
('S-201', 'Z-002', '2023-09-01 09:00:00', 70.0, 600),
('S-201', 'Z-002', '2023-09-01 13:00:00', 72.0, 650),
('S-201', 'Z-002', '2023-09-01 23:00:00', 58.0, 400),
('S-202', 'Z-002', '2023-09-01 09:10:00', 74.0, 620),
('S-202', 'Z-002', '2023-09-01 13:10:00', 78.0, 900), -- Day Violation (Construction)
('S-202', 'Z-002', '2023-09-01 23:10:00', 55.0, 380),
('S-301', 'Z-003', '2023-09-01 10:00:00', 80.0, 200),
('S-301', 'Z-003', '2023-09-01 14:00:00', 82.0, 250),
('S-301', 'Z-003', '2023-09-01 02:00:00', 74.0, 150),
('S-401', 'Z-004', '2023-09-01 08:00:00', 48.0, 300), -- Quiet Zone
('S-401', 'Z-004', '2023-09-01 09:00:00', 49.0, 310),
('S-401', 'Z-004', '2023-09-01 10:00:00', 55.0, 600), -- Breach! Siren?
('S-401', 'Z-004', '2023-09-01 11:00:00', 47.0, 300),
('S-401', 'Z-004', '2023-09-01 12:00:00', 48.0, 310),
('S-401', 'Z-004', '2023-09-01 13:00:00', 49.0, 320),
('S-401', 'Z-004', '2023-09-01 14:00:00', 52.0, 400), -- Breach
('S-401', 'Z-004', '2023-09-01 15:00:00', 48.0, 300),
('S-401', 'Z-004', '2023-09-01 16:00:00', 47.0, 290),
('S-401', 'Z-004', '2023-09-01 17:00:00', 49.0, 310),
('S-401', 'Z-004', '2023-09-01 18:00:00', 55.0, 500), -- Breach
('S-401', 'Z-004', '2023-09-01 22:00:00', 35.0, 200),
('S-401', 'Z-004', '2023-09-01 23:00:00', 38.0, 210),
('S-402', 'Z-004', '2023-09-01 08:00:00', 45.0, 280),
('S-402', 'Z-004', '2023-09-01 09:00:00', 46.0, 290),
('S-402', 'Z-004', '2023-09-01 10:00:00', 60.0, 800), -- Major Breach near S-402
('S-402', 'Z-004', '2023-09-01 11:00:00', 45.0, 280),
('S-501', 'Z-005', '2023-09-01 08:00:00', 50.0, 400),
('S-501', 'Z-005', '2023-09-01 12:00:00', 52.0, 420),
('S-501', 'Z-005', '2023-09-01 18:00:00', 58.0, 480),
('S-501', 'Z-005', '2023-09-01 22:00:00', 40.0, 250),
('S-103', 'Z-001', '2023-09-01 08:15:00', 56.0, 410),
('S-103', 'Z-001', '2023-09-01 12:15:00', 59.0, 460),
('S-103', 'Z-001', '2023-09-01 22:15:00', 43.0, 300),
('S-203', 'Z-002', '2023-09-01 09:20:00', 71.0, 610),
('S-203', 'Z-002', '2023-09-01 13:20:00', 73.0, 660),
('S-203', 'Z-002', '2023-09-01 23:20:00', 59.0, 410),
('S-302', 'Z-003', '2023-09-01 10:15:00', 81.0, 210),
('S-302', 'Z-003', '2023-09-01 14:15:00', 83.0, 260),
('S-302', 'Z-003', '2023-09-01 02:15:00', 75.0, 160),
('S-403', 'Z-004', '2023-09-01 10:30:00', 57.0, 610), -- Another breach
('S-403', 'Z-004', '2023-09-01 14:30:00', 48.0, 310),
('S-502', 'Z-005', '2023-09-01 08:30:00', 51.0, 410),
('S-502', 'Z-005', '2023-09-01 12:30:00', 53.0, 430),
('S-502', 'Z-005', '2023-09-01 18:30:00', 59.0, 490),
('S-502', 'Z-005', '2023-09-01 22:30:00', 41.0, 260);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Smart_City.Bronze.Bronze_Noise_Levels AS
SELECT
    SensorID,
    ZoneID_Mapped as ZoneID,
    Timestamp,
    Decibel_Level,
    Frequency_Hz
FROM Smart_City.Sources.Noise_Sensors;

CREATE OR REPLACE VIEW Smart_City.Bronze.Bronze_Zones AS
SELECT
    ZoneID,
    Zone_Type,
    Max_Decibels_Day,
    Max_Decibels_Night
FROM Smart_City.Sources.City_Zoning;

-- 4b. SILVER LAYER (Compliance Check)
CREATE OR REPLACE VIEW Smart_City.Silver.Silver_Compliance_Events AS
SELECT
    n.SensorID,
    n.Timestamp,
    n.Decibel_Level,
    z.ZoneID,
    z.Zone_Type,
    -- Determine Day/Night (07:00 to 22:00 is Day)
    CASE 
        WHEN EXTRACT(HOUR FROM n.Timestamp) BETWEEN 7 AND 21 THEN 'Day'
        ELSE 'Night'
    END as Time_Period,
    CASE 
        WHEN EXTRACT(HOUR FROM n.Timestamp) BETWEEN 7 AND 21 THEN z.Max_Decibels_Day
        ELSE z.Max_Decibels_Night
    END as Limit_dB,
    -- Violation logic
    CASE 
        WHEN EXTRACT(HOUR FROM n.Timestamp) BETWEEN 7 AND 21 AND n.Decibel_Level > z.Max_Decibels_Day THEN 'Violation'
        WHEN NOT(EXTRACT(HOUR FROM n.Timestamp) BETWEEN 7 AND 21) AND n.Decibel_Level > z.Max_Decibels_Night THEN 'Violation'
        ELSE 'Compliant'
    END as Status
FROM Smart_City.Bronze.Bronze_Noise_Levels n
JOIN Smart_City.Bronze.Bronze_Zones z ON n.ZoneID = z.ZoneID;

-- 4c. GOLD LAYER (Heatmap)
CREATE OR REPLACE VIEW Smart_City.Gold.Gold_Noise_Heatmap AS
SELECT
    ZoneID,
    Zone_Type,
    Time_Period,
    COUNT(*) as Readings_Count,
    AVG(Decibel_Level) as Avg_dB,
    MAX(Decibel_Level) as Max_dB,
    COUNT(CASE WHEN Status = 'Violation' THEN 1 END) as Violation_Count,
    ROUND((COUNT(CASE WHEN Status = 'Violation' THEN 1 END) * 100.0 / COUNT(*)), 1) as Violation_Rate_Pct
FROM Smart_City.Silver.Silver_Compliance_Events
GROUP BY ZoneID, Zone_Type, Time_Period;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Gold_Noise_Heatmap' for 'Quiet Zone' violations. 
 * If Violation_Rate_Pct > 10% in Zone-004 (Hospital), recommend deploying traffic calming measures."
 */
