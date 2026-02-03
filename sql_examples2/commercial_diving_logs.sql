/*
 * Dremio Commercial Diving Logs Example
 * 
 * Domain: Industrial Safety, Oil & Gas, Marine Construction
 * Scenario: 
 * Commercial divers perform "Saturation Diving" missions at extreme depths.
 * They live in a pressurized chamber for weeks.
 * The system tracks "Bottom Time", "Gas Mix" (Heliox usage), and "Decompression" schedules.
 * The goal is to monitor for Nitrogen Narcosis risks and ensure compliance with Dept of Labor (OSHA) 
 * or IMCA (International Marine Contractors Association) tables.
 * 
 * Complexity: Medium (Time interval sums, pressure/gas calculations)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Diving_Ops;
CREATE FOLDER IF NOT EXISTS Diving_Ops.Sources;
CREATE FOLDER IF NOT EXISTS Diving_Ops.Bronze;
CREATE FOLDER IF NOT EXISTS Diving_Ops.Silver;
CREATE FOLDER IF NOT EXISTS Diving_Ops.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Diving_Ops.Sources.Dive_Manifest (
    DiveID VARCHAR,
    DiverID VARCHAR,
    Support_Vessel_Name VARCHAR,
    Dive_Type VARCHAR, -- 'Saturation', 'Surface Supplied'
    Max_Depth_FSW INT, -- Feet of Sea Water
    Gas_Mix_Type VARCHAR, -- 'Air', 'Nitrox', 'Heliox'
    Start_Time TIMESTAMP,
    End_Time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Diving_Ops.Sources.Diver_Biometrics (
    LogID VARCHAR,
    DiveID VARCHAR,
    Biometric_Timestamp TIMESTAMP,
    Heart_Rate_BPM INT,
    Suit_Temp_C DOUBLE,
    Breathing_Rate_RPM INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Dives
INSERT INTO Diving_Ops.Sources.Dive_Manifest VALUES
('DIVE-001', 'DIV-01', 'DSV-DeepStar', 'Saturation', 400, 'Heliox', '2023-01-01 08:00:00', '2023-01-01 12:00:00'),
('DIVE-002', 'DIV-02', 'DSV-DeepStar', 'Saturation', 400, 'Heliox', '2023-01-01 08:00:00', '2023-01-01 12:00:00'),
('DIVE-003', 'DIV-01', 'DSV-DeepStar', 'Saturation', 380, 'Heliox', '2023-01-02 08:00:00', '2023-01-02 14:00:00'), -- Long shift
('DIVE-004', 'DIV-03', 'DSV-Coastal', 'Surface Supplied', 60, 'Air', '2023-01-05 09:00:00', '2023-01-05 09:45:00'),
('DIVE-005', 'DIV-04', 'DSV-Coastal', 'Surface Supplied', 90, 'Nitrox', '2023-01-05 10:00:00', '2023-01-05 11:00:00'),
('DIVE-006', 'DIV-01', 'DSV-DeepStar', 'Saturation', 410, 'Heliox', '2023-01-03 08:00:00', '2023-01-03 11:00:00'),
('DIVE-007', 'DIV-05', 'DSV-DeepStar', 'Saturation', 410, 'Heliox', '2023-01-03 08:00:00', '2023-01-03 11:00:00'),
('DIVE-008', 'DIV-02', 'DSV-DeepStar', 'Saturation', 400, 'Heliox', '2023-01-04 08:00:00', '2023-01-04 12:00:00'),
('DIVE-009', 'DIV-03', 'DSV-Coastal', 'Surface Supplied', 50, 'Air', '2023-01-06 09:00:00', '2023-01-06 10:00:00'),
('DIVE-010', 'DIV-04', 'DSV-Coastal', 'Surface Supplied', 110, 'Nitrox', '2023-01-06 13:00:00', '2023-01-06 13:30:00'), -- Deep for nitrox
('DIVE-011', 'DIV-06', 'DSV-Pipeline', 'Surface Supplied', 30, 'Air', '2023-02-01 08:00:00', '2023-02-01 12:00:00'),
('DIVE-012', 'DIV-06', 'DSV-Pipeline', 'Surface Supplied', 30, 'Air', '2023-02-01 13:00:00', '2023-02-01 16:00:00'),
('DIVE-013', 'DIV-07', 'DSV-Pipeline', 'Surface Supplied', 40, 'Air', '2023-02-01 08:00:00', '2023-02-01 12:00:00'),
('DIVE-014', 'DIV-01', 'DSV-DeepStar', 'Saturation', 390, 'Heliox', '2023-01-05 08:00:00', '2023-01-05 12:00:00'),
('DIVE-015', 'DIV-02', 'DSV-DeepStar', 'Saturation', 390, 'Heliox', '2023-01-05 08:00:00', '2023-01-05 12:00:00'),
('DIVE-016', 'DIV-05', 'DSV-DeepStar', 'Saturation', 390, 'Heliox', '2023-01-06 08:00:00', '2023-01-06 12:00:00'),
('DIVE-017', 'DIV-03', 'DSV-Coastal', 'Surface Supplied', 15, 'Air', '2023-01-07 09:00:00', '2023-01-07 15:00:00'), -- Long hull scrub
('DIVE-018', 'DIV-04', 'DSV-Coastal', 'Surface Supplied', 20, 'Air', '2023-01-07 09:00:00', '2023-01-07 14:00:00'),
('DIVE-019', 'DIV-08', 'DSV-Rig1', 'Surface Supplied', 130, 'Heliox', '2023-03-01 10:00:00', '2023-03-01 10:45:00'), -- Bounce dive
('DIVE-020', 'DIV-08', 'DSV-Rig1', 'Surface Supplied', 140, 'Heliox', '2023-03-02 10:00:00', '2023-03-02 10:30:00');

-- Seed Biometrics (One reading per hour/event simulation)
INSERT INTO Diving_Ops.Sources.Diver_Biometrics VALUES
('BIO-100', 'DIVE-001', '2023-01-01 08:30:00', 70, 28.0, 12),
('BIO-101', 'DIVE-001', '2023-01-01 09:30:00', 75, 27.5, 14),
('BIO-102', 'DIVE-001', '2023-01-01 10:30:00', 80, 27.0, 16), -- Getting colder
('BIO-103', 'DIVE-002', '2023-01-01 08:30:00', 65, 28.0, 11),
('BIO-104', 'DIVE-002', '2023-01-01 09:30:00', 68, 27.8, 12),
('BIO-105', 'DIVE-003', '2023-01-02 09:00:00', 90, 29.0, 18), -- Hard work
('BIO-106', 'DIVE-003', '2023-01-02 10:00:00', 95, 29.5, 20),
('BIO-107', 'DIVE-003', '2023-01-02 11:00:00', 100, 30.0, 22), -- Stress?
('BIO-108', 'DIVE-004', '2023-01-05 09:10:00', 60, 15.0, 10), -- Cold water (non-sat suit)
('BIO-109', 'DIVE-004', '2023-01-05 09:30:00', 62, 14.5, 12),
('BIO-110', 'DIVE-010', '2023-01-06 13:10:00', 80, 18.0, 14),
('BIO-111', 'DIVE-010', '2023-01-06 13:20:00', 85, 18.0, 15), -- Nitrogen effects at 110?
('BIO-112', 'DIVE-001', '2023-01-01 11:30:00', 82, 26.5, 15),
('BIO-113', 'DIVE-002', '2023-01-01 11:30:00', 70, 27.5, 12),
('BIO-114', 'DIVE-018', '2023-01-07 10:00:00', 75, 20.0, 14),
('BIO-115', 'DIVE-018', '2023-01-07 11:00:00', 78, 20.0, 14),
('BIO-116', 'DIVE-018', '2023-01-07 12:00:00', 80, 20.0, 15),
('BIO-117', 'DIVE-018', '2023-01-07 13:00:00', 75, 20.0, 13),
('BIO-118', 'DIVE-017', '2023-01-07 10:00:00', 65, 20.0, 11),
('BIO-119', 'DIVE-017', '2023-01-07 11:00:00', 68, 20.0, 12),
('BIO-120', 'DIVE-017', '2023-01-07 12:00:00', 70, 20.0, 12),
('BIO-121', 'DIVE-017', '2023-01-07 13:00:00', 72, 20.0, 13),
('BIO-122', 'DIVE-017', '2023-01-07 14:00:00', 74, 20.0, 14),
('BIO-123', 'DIVE-006', '2023-01-03 09:00:00', 70, 28.0, 12),
('BIO-124', 'DIVE-006', '2023-01-03 10:00:00', 72, 28.0, 12),
('BIO-125', 'DIVE-007', '2023-01-03 09:00:00', 68, 28.0, 11),
('BIO-126', 'DIVE-007', '2023-01-03 10:00:00', 70, 28.0, 12),
('BIO-127', 'DIVE-008', '2023-01-04 09:00:00', 75, 28.0, 14),
('BIO-128', 'DIVE-008', '2023-01-04 10:00:00', 78, 28.0, 15),
('BIO-129', 'DIVE-008', '2023-01-04 11:00:00', 80, 28.0, 16),
('BIO-130', 'DIVE-009', '2023-01-06 09:30:00', 65, 18.0, 12);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Diving_Ops.Bronze.Bronze_Manifest AS SELECT * FROM Diving_Ops.Sources.Dive_Manifest;
CREATE OR REPLACE VIEW Diving_Ops.Bronze.Bronze_Biometrics AS SELECT * FROM Diving_Ops.Sources.Diver_Biometrics;

-- 4b. SILVER LAYER (Calculated Bottom Time and Safety)
CREATE OR REPLACE VIEW Diving_Ops.Silver.Silver_Diver_Safety AS
SELECT
    m.DiveID,
    m.DiverID,
    m.Dive_Type,
    m.Gas_Mix_Type,
    m.Max_Depth_FSW,
    m.Start_Time,
    m.End_Time,
    -- Bottom Time in Minutes
    EXTRACT(EPOCH FROM (m.End_Time - m.Start_Time))/60 as Bottom_Time_Min,
    b.Heart_Rate_BPM,
    b.Suit_Temp_C,
    -- Check for Nitrogen Narcosis risk (Air diving > 100ft)
    CASE 
        WHEN m.Gas_Mix_Type = 'Air' AND m.Max_Depth_FSW > 100 THEN 'High Narcosis Risk'
        WHEN m.Gas_Mix_Type = 'Nitrox' AND m.Max_Depth_FSW > 120 THEN 'O2 Toxicity Risk'
        ELSE 'Within Limits'
    END as Safety_Alert
FROM Diving_Ops.Bronze.Bronze_Manifest m
LEFT JOIN Diving_Ops.Bronze.Bronze_Biometrics b ON m.DiveID = b.DiveID;

-- 4c. GOLD LAYER (Exposure Logs)
CREATE OR REPLACE VIEW Diving_Ops.Gold.Gold_Diver_Exposure_Log AS
SELECT
    DiverID,
    COUNT(DISTINCT DiveID) as Total_Dives,
    SUM(Bottom_Time_Min) as Total_Bottom_Time_Min,
    MAX(Max_Depth_FSW) as Max_Depth_Reached,
    SUM(CASE WHEN Safety_Alert != 'Within Limits' THEN 1 ELSE 0 END) as Safety_Violations_Count
FROM Diving_Ops.Silver.Silver_Diver_Safety
GROUP BY DiverID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Review 'Gold_Diver_Exposure_Log' for divers with 'Total_Bottom_Time_Min' > 1000 in a single week. 
 * Flag them for mandatory rotation/decompression leave."
 */
