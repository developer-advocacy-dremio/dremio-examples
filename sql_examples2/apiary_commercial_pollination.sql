/*
 * Dremio Apiary Commercial Pollination Example
 * 
 * Domain: Agriculture & Biology
 * Scenario: 
 * Commercial beekeepers transport thousands of hives to Almond orchards in California.
 * They track "Brood Frames" (colony strength), "Queen Age", and "Mite Counts" (Varroa).
 * The goal is to meet contractual "Frame Count" requirements for pollination payments.
 * 
 * Complexity: Medium (Biological growth curves, inventory forecasting)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Bee_Logistics;
CREATE FOLDER IF NOT EXISTS Bee_Logistics.Sources;
CREATE FOLDER IF NOT EXISTS Bee_Logistics.Bronze;
CREATE FOLDER IF NOT EXISTS Bee_Logistics.Silver;
CREATE FOLDER IF NOT EXISTS Bee_Logistics.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Bee_Logistics.Sources.Hive_Registry (
    HiveID VARCHAR,
    Yard_Location VARCHAR, -- 'Home Yard', 'Orchard A', 'Orchard B'
    Queen_Hatch_Date DATE,
    Breed VARCHAR, -- 'Italian', 'Carniolan', 'Saskatraz'
    Box_Config VARCHAR -- 'Double Deep', 'Single Deep'
);

CREATE TABLE IF NOT EXISTS Bee_Logistics.Sources.Inspection_Log (
    InspectionID VARCHAR,
    HiveID VARCHAR,
    Inspect_Date DATE,
    Frames_of_Bees INT, -- Contract usually requires 8+
    Frames_of_Brood INT,
    Varroa_Count_Per_100 INT, -- Mites (Parasites)
    Food_Stores_Kg DOUBLE,
    Health_Status VARCHAR -- 'Strong', 'Weak', 'Queenless'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Hives
INSERT INTO Bee_Logistics.Sources.Hive_Registry VALUES
('HIVE-001', 'Orchard A', '2022-04-01', 'Italian', 'Double Deep'),
('HIVE-002', 'Orchard A', '2023-04-15', 'Carniolan', 'Double Deep'),
('HIVE-003', 'Orchard A', '2021-08-01', 'Italian', 'Double Deep'), -- Old queen
('HIVE-004', 'Orchard B', '2023-05-01', 'Saskatraz', 'Single Deep'),
('HIVE-005', 'Home Yard', '2023-06-01', 'Italian', 'Single Deep');

-- Seed Inspections (Spring buildup for almonds)
INSERT INTO Bee_Logistics.Sources.Inspection_Log VALUES
('INS-100', 'HIVE-001', '2024-01-15', 6, 3, 1, 15.0, 'Building'), -- Too weak for contract yet
('INS-101', 'HIVE-001', '2024-02-01', 8, 5, 2, 10.0, 'Strong'), -- Ready
('INS-102', 'HIVE-001', '2024-02-15', 12, 8, 3, 5.0, 'Super Strong'), -- Needs supering?
('INS-103', 'HIVE-002', '2024-01-15', 10, 6, 1, 18.0, 'Strong'),
('INS-104', 'HIVE-002', '2024-02-01', 14, 9, 2, 12.0, 'Boiler'),
('INS-105', 'HIVE-003', '2024-01-15', 4, 1, 5, 20.0, 'Weak'), -- Failing?
('INS-106', 'HIVE-003', '2024-02-01', 3, 0, 8, 19.0, 'Queenless'), -- Failed
('INS-107', 'HIVE-004', '2024-01-15', 5, 3, 0, 10.0, 'Building'),
('INS-108', 'HIVE-004', '2024-02-01', 7, 5, 0, 8.0, 'Almost Ready'),
('INS-109', 'HIVE-005', '2024-01-15', 8, 4, 1, 15.0, 'Strong'),
-- Filling history
('INS-110', 'HIVE-001', '2023-10-01', 10, 2, 2, 30.0, 'Winter Prep'),
('INS-111', 'HIVE-001', '2023-11-01', 8, 0, 1, 28.0, 'Dormant'),
('INS-112', 'HIVE-002', '2023-10-01', 12, 3, 1, 35.0, 'Winter Prep'),
('INS-113', 'HIVE-002', '2023-11-01', 10, 0, 1, 32.0, 'Dormant'),
('INS-114', 'HIVE-003', '2023-10-01', 6, 1, 6, 25.0, 'Struggling'), -- Mites high
('INS-115', 'HIVE-003', '2023-11-01', 5, 0, 5, 24.0, 'Dormant'),
('INS-116', 'HIVE-004', '2023-10-01', 8, 4, 0, 20.0, 'Winter Prep'),
('INS-117', 'HIVE-004', '2023-11-01', 6, 0, 0, 18.0, 'Dormant'),
('INS-118', 'HIVE-005', '2023-10-01', 10, 5, 1, 25.0, 'Winter Prep'),
('INS-119', 'HIVE-005', '2023-11-01', 9, 1, 1, 22.0, 'Dormant'),
-- More hives logic (HIVE-006 to 020 implied, filling rows)
('INS-120', 'HIVE-006', '2024-02-01', 8, 4, 1, 15.0, 'Strong'),
('INS-121', 'HIVE-007', '2024-02-01', 9, 5, 2, 16.0, 'Strong'),
('INS-122', 'HIVE-008', '2024-02-01', 7, 3, 1, 14.0, 'Building'),
('INS-123', 'HIVE-009', '2024-02-01', 6, 2, 1, 13.0, 'Weak'),
('INS-124', 'HIVE-010', '2024-02-01', 10, 6, 2, 18.0, 'Strong'),
('INS-125', 'HIVE-011', '2024-02-01', 11, 7, 3, 19.0, 'Strong'),
('INS-126', 'HIVE-012', '2024-02-01', 5, 1, 5, 12.0, 'Weak'), -- Mites
('INS-127', 'HIVE-013', '2024-02-01', 8, 4, 1, 15.0, 'Strong'),
('INS-128', 'HIVE-014', '2024-02-01', 9, 5, 1, 16.0, 'Strong'),
('INS-129', 'HIVE-015', '2024-02-01', 12, 8, 2, 20.0, 'Boiler'),
('INS-130', 'HIVE-016', '2024-02-01', 4, 0, 0, 10.0, 'Queenless'),
('INS-131', 'HIVE-017', '2024-02-01', 8, 4, 1, 15.0, 'Strong'),
('INS-132', 'HIVE-018', '2024-02-01', 8, 4, 1, 15.0, 'Strong'),
('INS-133', 'HIVE-019', '2024-02-01', 8, 4, 1, 15.0, 'Strong'),
('INS-134', 'HIVE-020', '2024-02-01', 8, 4, 1, 15.0, 'Strong'),
('INS-135', 'HIVE-006', '2024-01-15', 6, 2, 1, 18.0, 'Building'),
('INS-136', 'HIVE-007', '2024-01-15', 7, 3, 2, 19.0, 'Building'),
('INS-137', 'HIVE-008', '2024-01-15', 5, 1, 1, 16.0, 'Building'),
('INS-138', 'HIVE-009', '2024-01-15', 5, 1, 1, 15.0, 'Building'),
('INS-139', 'HIVE-010', '2024-01-15', 8, 4, 2, 20.0, 'Strong'),
('INS-140', 'HIVE-011', '2024-01-15', 9, 5, 3, 21.0, 'Strong'),
('INS-141', 'HIVE-012', '2024-01-15', 6, 2, 4, 14.0, 'Weak'),
('INS-142', 'HIVE-013', '2024-01-15', 6, 2, 1, 18.0, 'Building'),
('INS-143', 'HIVE-014', '2024-01-15', 7, 3, 1, 19.0, 'Building'),
('INS-144', 'HIVE-015', '2024-01-15', 10, 6, 2, 22.0, 'Strong'),
('INS-145', 'HIVE-016', '2024-01-15', 5, 1, 0, 12.0, 'Weak'),
('INS-146', 'HIVE-017', '2024-01-15', 6, 2, 1, 18.0, 'Building'),
('INS-147', 'HIVE-018', '2024-01-15', 6, 2, 1, 18.0, 'Building'),
('INS-148', 'HIVE-019', '2024-01-15', 6, 2, 1, 18.0, 'Building'),
('INS-149', 'HIVE-020', '2024-01-15', 6, 2, 1, 18.0, 'Building'),
('INS-150', 'HIVE-001', '2024-02-15', 14, 10, 2, 4.0, 'Swarm Risk'); -- Too strong, might swarm

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Bee_Logistics.Bronze.Bronze_Hives AS SELECT * FROM Bee_Logistics.Sources.Hive_Registry;
CREATE OR REPLACE VIEW Bee_Logistics.Bronze.Bronze_Inspections AS SELECT * FROM Bee_Logistics.Sources.Inspection_Log;

-- 4b. SILVER LAYER (Contract Compliance)
CREATE OR REPLACE VIEW Bee_Logistics.Silver.Silver_Pollination_Readiness AS
SELECT
    h.HiveID,
    h.Yard_Location,
    i.Inspect_Date,
    i.Frames_of_Bees,
    i.Varroa_Count_Per_100,
    -- Almond Contract Status (Usually 8+ frames pays full price, <4 pays nothing)
    CASE 
        WHEN i.Health_Status = 'Queenless' THEN 'Deadout/Fail'
        WHEN i.Varroa_Count_Per_100 > 3 THEN 'Treat Immediately'
        WHEN i.Frames_of_Bees >= 8 THEN 'Grade A - Premium'
        WHEN i.Frames_of_Bees >= 4 THEN 'Grade B - Standard'
        ELSE 'Grade C - Substandard'
    END as Contract_Grade
FROM Bee_Logistics.Bronze.Bronze_Inspections i
JOIN Bee_Logistics.Bronze.Bronze_Hives h ON i.HiveID = h.HiveID
QUALIFY ROW_NUMBER() OVER (PARTITION BY h.HiveID ORDER BY i.Inspect_Date DESC) = 1;

-- 4c. GOLD LAYER (Revenue Forecast)
CREATE OR REPLACE VIEW Bee_Logistics.Gold.Gold_Orchard_Revenue AS
SELECT
    Yard_Location,
    COUNT(*) as Hive_Count,
    SUM(CASE WHEN Contract_Grade = 'Grade A - Premium' THEN 220 
             WHEN Contract_Grade = 'Grade B - Standard' THEN 180
             ELSE 0 END) as Projected_Revenue_USD,
    SUM(CASE WHEN Contract_Grade = 'Treat Immediately' THEN 1 ELSE 0 END) as Urgent_Health_Issues_Count
FROM Bee_Logistics.Silver.Silver_Pollination_Readiness
GROUP BY Yard_Location;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Examine 'Silver_Pollination_Readiness' for hives in 'Orchard A'. 
 * Identify any Grade A hives that have > 2 Varroa mites, as these are high-value risks that could collapse."
 */
