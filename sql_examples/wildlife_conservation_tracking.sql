/*
 * Dremio Wildlife Conservation Tracking Example
 * 
 * Domain: Telemetry & Conservation Biology
 * Scenario: 
 * A Wildlife Reserve needs to track the movements of tagged animals (Elephants, Rhinos, Lions)
 * to ensure their safety and monitor migration patterns. 
 * The challenge is to correlate real-time GPS pings from collars with static animal health records
 * and geospatial zone definitions to identify poaching risks or distressed animals.
 * 
 * Complexity: High (Geospatial analysis simulation, temporal tracking)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Wildlife_Tracker;
CREATE FOLDER IF NOT EXISTS Wildlife_Tracker.Sources;
CREATE FOLDER IF NOT EXISTS Wildlife_Tracker.Bronze;
CREATE FOLDER IF NOT EXISTS Wildlife_Tracker.Silver;
CREATE FOLDER IF NOT EXISTS Wildlife_Tracker.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables (Simulates Raw Telemetry & Metadata)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Wildlife_Tracker.Sources.Animal_Metadata (
    AnimalID VARCHAR,
    Species VARCHAR,
    Age_Years INT,
    CollarID VARCHAR,
    Health_Status VARCHAR,
    Tagging_Date DATE
);

CREATE TABLE IF NOT EXISTS Wildlife_Tracker.Sources.GPS_Pings (
    PingID VARCHAR,
    CollarID VARCHAR,
    Latitude DOUBLE,
    Longitude DOUBLE,
    Ping_Timestamp TIMESTAMP,
    Activity_Level VARCHAR, -- 'Resting', 'Moving', 'Sprinting'
    Battery_Level INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records per table)
-------------------------------------------------------------------------------

-- Seed Animal_Metadata (E.g., Elephants, Rhinos, Lions in a reserve)
INSERT INTO Wildlife_Tracker.Sources.Animal_Metadata VALUES
('E-001', 'African Elephant', 12, 'CLR-101', 'Healthy', '2023-01-15'),
('E-002', 'African Elephant', 25, 'CLR-102', 'Healthy', '2023-01-15'),
('E-003', 'African Elephant', 8,  'CLR-103', 'Injured', '2023-01-20'),
('E-004', 'African Elephant', 30, 'CLR-104', 'Healthy', '2023-02-10'),
('E-005', 'African Elephant', 4,  'CLR-105', 'Healthy', '2023-03-01'),
('R-001', 'Black Rhino',      10, 'CLR-201', 'Healthy', '2023-01-10'),
('R-002', 'Black Rhino',      14, 'CLR-202', 'Critical', '2023-01-12'),
('R-003', 'Black Rhino',      7,  'CLR-203', 'Healthy', '2023-01-15'),
('R-004', 'Black Rhino',      9,  'CLR-204', 'Healthy', '2023-02-01'),
('L-001', 'Lion',             5,  'CLR-301', 'Healthy', '2023-01-05'),
('L-002', 'Lion',             3,  'CLR-302', 'Healthy', '2023-01-08'),
('L-003', 'Lion',             6,  'CLR-303', 'Unknown', '2023-01-20'),
('E-006', 'African Elephant', 45, 'CLR-106', 'Healthy', '2023-03-05'),
('E-007', 'African Elephant', 2,  'CLR-107', 'Healthy', '2023-03-10'),
('E-008', 'African Elephant', 18, 'CLR-108', 'Healthy', '2023-03-15'),
('E-009', 'African Elephant', 22, 'CLR-109', 'Healthy', '2023-03-20'),
('E-010', 'African Elephant', 33, 'CLR-110', 'Healthy', '2023-03-25'),
('R-005', 'Black Rhino',      11, 'CLR-205', 'Healthy', '2023-02-15'),
('R-006', 'Black Rhino',      13, 'CLR-206', 'Healthy', '2023-02-20'),
('R-007', 'Black Rhino',      6,  'CLR-207', 'Healthy', '2023-02-25'),
('L-004', 'Lion',             4,  'CLR-304', 'Healthy', '2023-02-05'),
('L-005', 'Lion',             7,  'CLR-305', 'Healthy', '2023-02-10'),
('Z-001', 'Zebra',            4,  'CLR-401', 'Healthy', '2023-04-01'),
('Z-002', 'Zebra',            5,  'CLR-402', 'Healthy', '2023-04-02'),
('Z-003', 'Zebra',            3,  'CLR-403', 'Healthy', '2023-04-03'),
('Z-004', 'Zebra',            6,  'CLR-404', 'Healthy', '2023-04-04'),
('Z-005', 'Zebra',            2,  'CLR-405', 'Healthy', '2023-04-05'),
('G-001', 'Giraffe',          8,  'CLR-501', 'Healthy', '2023-04-01'),
('G-002', 'Giraffe',          9,  'CLR-502', 'Old Age', '2023-04-02'),
('G-003', 'Giraffe',          7,  'CLR-503', 'Healthy', '2023-04-03'),
('G-004', 'Giraffe',          10, 'CLR-504', 'Healthy', '2023-04-04'),
('G-005', 'Giraffe',          6,  'CLR-505', 'Healthy', '2023-04-05'),
('E-011', 'African Elephant', 15, 'CLR-111', 'Healthy', '2023-05-01'),
('E-012', 'African Elephant', 28, 'CLR-112', 'Healthy', '2023-05-05'),
('E-013', 'African Elephant', 35, 'CLR-113', 'Healthy', '2023-05-10'),
('E-014', 'African Elephant', 19, 'CLR-114', 'Healthy', '2023-05-15'),
('E-015', 'African Elephant', 9,  'CLR-115', 'Healthy', '2023-05-20'),
('R-008', 'Black Rhino',      15, 'CLR-208', 'Healthy', '2023-03-01'),
('R-009', 'Black Rhino',      12, 'CLR-209', 'Healthy', '2023-03-05'),
('R-010', 'Black Rhino',      8,  'CLR-210', 'Healthy', '2023-03-10'),
('L-006', 'Lion',             8,  'CLR-306', 'Healthy', '2023-02-15'),
('L-007', 'Lion',             2,  'CLR-307', 'Healthy', '2023-02-20'),
('L-008', 'Lion',             5,  'CLR-308', 'Healthy', '2023-02-25'),
('H-001', 'Hyena',            4,  'CLR-601', 'Healthy', '2023-04-10'),
('H-002', 'Hyena',            5,  'CLR-602', 'Healthy', '2023-04-11'),
('H-003', 'Hyena',            3,  'CLR-603', 'Healthy', '2023-04-12'),
('H-004', 'Hyena',            6,  'CLR-604', 'Healthy', '2023-04-13'),
('H-005', 'Hyena',            2,  'CLR-605', 'Healthy', '2023-04-14'),
('W-001', 'Wild Dog',         3,  'CLR-701', 'Healthy', '2023-04-15'),
('W-002', 'Wild Dog',         4,  'CLR-702', 'Healthy', '2023-04-16');

-- Seed GPS_Pings (Simulating movement tracks)
INSERT INTO Wildlife_Tracker.Sources.GPS_Pings VALUES
('P-1001', 'CLR-101', -1.2921, 36.8219, '2023-06-01 08:00:00', 'Moving', 95),
('P-1002', 'CLR-101', -1.2930, 36.8225, '2023-06-01 09:00:00', 'Moving', 94),
('P-1003', 'CLR-101', -1.2945, 36.8230, '2023-06-01 10:00:00', 'Resting', 93),
('P-1004', 'CLR-101', -1.2950, 36.8240, '2023-06-01 11:00:00', 'Moving', 92),
('P-1005', 'CLR-102', -2.3333, 34.8333, '2023-06-01 08:30:00', 'Moving', 88),
('P-1006', 'CLR-102', -2.3340, 34.8340, '2023-06-01 09:30:00', 'Sprinting', 87), -- Startled?
('P-1007', 'CLR-201', -1.5000, 37.0000, '2023-06-01 08:15:00', 'Resting', 99),
('P-1008', 'CLR-201', -1.5001, 37.0001, '2023-06-01 12:15:00', 'Resting', 98),
('P-1009', 'CLR-301', -3.2000, 35.5000, '2023-06-01 22:00:00', 'Hunting', 90),
('P-1010', 'CLR-301', -3.2010, 35.5010, '2023-06-01 22:30:00', 'Sprinting', 89),
('P-1011', 'CLR-103', -1.4000, 36.5000, '2023-06-02 08:00:00', 'Resting', 40),
('P-1012', 'CLR-103', -1.4000, 36.5000, '2023-06-02 12:00:00', 'Resting', 39), -- Injured static
('P-1013', 'CLR-104', -2.0000, 35.0000, '2023-06-02 09:00:00', 'Moving', 70),
('P-1014', 'CLR-105', -2.1000, 35.1000, '2023-06-02 09:05:00', 'Moving', 100),
('P-1015', 'CLR-202', -1.8000, 36.2000, '2023-06-03 06:00:00', 'Moving', 20), -- Low battery
('P-1016', 'CLR-202', -1.8050, 36.2050, '2023-06-03 07:00:00', 'Moving', 19),
('P-1017', 'CLR-302', -3.1000, 35.2000, '2023-06-03 20:00:00', 'Resting', 85),
('P-1018', 'CLR-303', -3.1500, 35.2500, '2023-06-03 20:00:00', 'Resting', 50),
('P-1019', 'CLR-401', -1.9000, 36.4000, '2023-06-04 10:00:00', 'Moving', 88),
('P-1020', 'CLR-402', -1.9010, 36.4010, '2023-06-04 10:05:00', 'Moving', 87),
('P-1021', 'CLR-403', -1.9020, 36.4020, '2023-06-04 10:10:00', 'Moving', 86),
('P-1022', 'CLR-101', -1.3000, 36.8300, '2023-06-05 08:00:00', 'Moving', 90),
('P-1023', 'CLR-102', -2.3400, 34.8400, '2023-06-05 08:00:00', 'Resting', 85),
('P-1024', 'CLR-501', -1.6000, 36.6000, '2023-06-05 09:00:00', 'Moving', 92),
('P-1025', 'CLR-502', -1.6100, 36.6100, '2023-06-05 09:10:00', 'Moving', 91),
('P-1026', 'CLR-601', -2.5000, 35.8000, '2023-06-06 23:00:00', 'Sprinting', 88), -- Pack hunting
('P-1027', 'CLR-602', -2.5010, 35.8010, '2023-06-06 23:05:00', 'Sprinting', 87),
('P-1028', 'CLR-701', -2.6000, 35.9000, '2023-06-06 14:00:00', 'Resting', 95),
('P-1029', 'CLR-101', -1.3100, 36.8400, '2023-06-07 08:00:00', 'Moving', 88),
('P-1030', 'CLR-102', -2.3500, 34.8500, '2023-06-07 08:00:00', 'Moving', 82),
('P-1031', 'CLR-201', -1.5100, 37.0100, '2023-06-07 09:00:00', 'Moving', 95),
('P-1032', 'CLR-301', -3.2100, 35.5100, '2023-06-07 20:00:00', 'Moving', 85),
('P-1033', 'CLR-106', -1.2000, 36.7000, '2023-06-08 08:00:00', 'Moving', 90),
('P-1034', 'CLR-107', -1.2100, 36.7100, '2023-06-08 08:10:00', 'Moving', 92),
('P-1035', 'CLR-108', -1.2500, 36.7500, '2023-06-09 10:00:00', 'Resting', 88),
('P-1036', 'CLR-205', -1.7000, 36.1000, '2023-06-09 11:00:00', 'Moving', 91),
('P-1037', 'CLR-206', -1.7100, 36.1100, '2023-06-09 11:10:00', 'Moving', 90),
('P-1038', 'CLR-304', -3.3000, 35.6000, '2023-06-10 18:00:00', 'Resting', 95),
('P-1039', 'CLR-305', -3.3100, 35.6100, '2023-06-10 18:10:00', 'Resting', 94),
('P-1040', 'CLR-404', -1.9100, 36.4100, '2023-06-11 12:00:00', 'Moving', 85),
('P-1041', 'CLR-405', -1.9200, 36.4200, '2023-06-11 12:10:00', 'Moving', 84),
('P-1042', 'CLR-503', -1.6200, 36.6200, '2023-06-12 14:00:00', 'Feeding', 80),
('P-1043', 'CLR-504', -1.6300, 36.6300, '2023-06-12 14:10:00', 'Feeding', 79),
('P-1044', 'CLR-109', -1.2600, 36.7600, '2023-06-13 09:00:00', 'Moving', 87),
('P-1045', 'CLR-110', -1.2700, 36.7700, '2023-06-13 09:10:00', 'Moving', 86),
('P-1046', 'CLR-207', -1.7200, 36.1200, '2023-06-14 10:30:00', 'Resting', 93),
('P-1047', 'CLR-306', -3.3200, 35.6200, '2023-06-15 19:00:00', 'Moving', 88),
('P-1048', 'CLR-505', -1.6400, 36.6400, '2023-06-16 15:00:00', 'Feeding', 85),
('P-1049', 'CLR-603', -2.5100, 35.8100, '2023-06-17 22:00:00', 'Moving', 82),
('P-1050', 'CLR-702', -2.6100, 35.9100, '2023-06-18 13:00:00', 'Resting', 90),
('P-1051', 'CLR-111', -1.1000, 36.9000, '2023-06-19 08:00:00', 'Moving', 95),
('P-1052', 'CLR-112', -1.1100, 36.9100, '2023-06-19 08:10:00', 'Moving', 94),
('P-1053', 'CLR-113', -1.1200, 36.9200, '2023-06-19 08:20:00', 'Moving', 93),
('P-1054', 'CLR-114', -1.1300, 36.9300, '2023-06-19 08:30:00', 'Moving', 92),
('P-1055', 'CLR-115', -1.1400, 36.9400, '2023-06-19 08:40:00', 'Moving', 91);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER (Raw one-to-one views, type casting if needed)
CREATE OR REPLACE VIEW Wildlife_Tracker.Bronze.Bronze_Animals AS
SELECT
    AnimalID,
    Species,
    Age_Years,
    CollarID,
    Health_Status,
    Tagging_Date
FROM Wildlife_Tracker.Sources.Animal_Metadata;

CREATE OR REPLACE VIEW Wildlife_Tracker.Bronze.Bronze_Pings AS
SELECT
    PingID,
    CollarID,
    Latitude,
    Longitude,
    Ping_Timestamp,
    Activity_Level,
    Battery_Level
FROM Wildlife_Tracker.Sources.GPS_Pings;

-- 4b. SILVER LAYER (Enriched, Joined, Data Quality Flags)
CREATE OR REPLACE VIEW Wildlife_Tracker.Silver.Silver_Enriched_Tracks AS
SELECT
    p.PingID,
    a.Species,
    a.AnimalID,
    p.Latitude,
    p.Longitude,
    p.Ping_Timestamp,
    p.Activity_Level,
    a.Health_Status,
    -- Simple geo-fencing flag logic (Simulation)
    -- Assuming a "Safe Zone" is broadly defined; anything outside is 'Risk'
    CASE 
        WHEN p.Latitude BETWEEN -2.0 AND -1.0 AND p.Longitude BETWEEN 36.0 AND 37.0 THEN 'Safe Zone'
        ELSE 'Potential Risk Zone'
    END AS Zone_Status,
    p.Battery_Level
FROM Wildlife_Tracker.Bronze.Bronze_Pings p
JOIN Wildlife_Tracker.Bronze.Bronze_Animals a ON p.CollarID = a.CollarID;

-- 4c. GOLD LAYER (Aggregated Business/Scientific Logic)
CREATE OR REPLACE VIEW Wildlife_Tracker.Gold.Gold_Migration_Corridors AS
SELECT
    Species,
    TRUNC(Ping_Timestamp, 'DAY') as Tracking_Day,
    AVG(Latitude) as Centroid_Lat,
    AVG(Longitude) as Centroid_Lon,
    COUNT(*) as Ping_Count,
    -- Identify dominant activity for the day
    (SELECT Activity_Level FROM Wildlife_Tracker.Silver.Silver_Enriched_Tracks s2 
     WHERE s2.Species = s1.Species AND TRUNC(s2.Ping_Timestamp, 'DAY') = TRUNC(s1.Ping_Timestamp, 'DAY') 
     GROUP BY Activity_Level ORDER BY COUNT(*) DESC LIMIT 1) as Primary_Activity
FROM Wildlife_Tracker.Silver.Silver_Enriched_Tracks s1
GROUP BY Species, TRUNC(Ping_Timestamp, 'DAY');

CREATE OR REPLACE VIEW Wildlife_Tracker.Gold.Gold_Poaching_Risk_Alerts AS
SELECT
    AnimalID,
    Species,
    Ping_Timestamp,
    Latitude,
    Longitude,
    Health_Status,
    Zone_Status,
    'Alert' as Alert_Type
FROM Wildlife_Tracker.Silver.Silver_Enriched_Tracks
WHERE Zone_Status = 'Potential Risk Zone'
AND (Health_Status = 'Critical' OR Activity_Level = 'Sprinting');

-------------------------------------------------------------------------------
-- 5. AI PROMPT FOR DREMIO ASSISTANT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze the 'Gold_Poaching_Risk_Alerts' view to identify clusters of sprinting animals in risk zones. 
 * correlate this with 'Gold_Migration_Corridors' to see if herds are deviating from expected safe paths."
 */
