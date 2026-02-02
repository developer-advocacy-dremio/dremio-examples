/*
 * Dremio Nuclear Waste Storage Monitoring Example
 * 
 * Domain: Energy & Hazardous Material Safety
 * Scenario: 
 * A designated nuclear waste storage facility monitors "Casks" containing spent fuel rods.
 * Sensors track Surface Temperature, Internal Pressure, and Gamma Radiation levels 24/7.
 * Safety logic requires comparing these sensor readings against the specific decay heat 
 * profiles of the isotopes (U-235, Pu-239) inside each cask to predict "Containment Breach" risks.
 * 
 * Complexity: Medium (Threshold logic, join on metadata)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Nuclear_Site;
CREATE FOLDER IF NOT EXISTS Nuclear_Site.Sources;
CREATE FOLDER IF NOT EXISTS Nuclear_Site.Bronze;
CREATE FOLDER IF NOT EXISTS Nuclear_Site.Silver;
CREATE FOLDER IF NOT EXISTS Nuclear_Site.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Nuclear_Site.Sources.Transport_Manifests (
    ManifestID VARCHAR,
    CaskID VARCHAR,
    Origin_Facility VARCHAR,
    Storage_Block VARCHAR,
    Isotope_Type VARCHAR, -- 'U-235', 'Pu-239', 'Cs-137'
    Initial_Heat_Output_Kw DOUBLE,
    Arrival_Date DATE
);

CREATE TABLE IF NOT EXISTS Nuclear_Site.Sources.Cask_Sensors (
    ReadingID VARCHAR,
    CaskID VARCHAR,
    Timestamp TIMESTAMP,
    Surface_Temp_C DOUBLE,
    Internal_Pressure_Bar DOUBLE,
    Gamma_Radiation_Sv_h DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Manifests (Casks)
INSERT INTO Nuclear_Site.Sources.Transport_Manifests VALUES
('M-001', 'CSK-100', 'Plant-Alpha', 'Block-A', 'U-235',  5.0, '2020-01-01'),
('M-002', 'CSK-101', 'Plant-Alpha', 'Block-A', 'U-235',  5.1, '2020-01-05'),
('M-003', 'CSK-102', 'Plant-Beta',  'Block-B', 'Pu-239', 8.0, '2020-02-01'), -- Hotter
('M-004', 'CSK-103', 'Plant-Beta',  'Block-B', 'Pu-239', 8.2, '2020-02-10'),
('M-005', 'CSK-104', 'Plant-Gamma', 'Block-C', 'Cs-137', 2.0, '2020-03-01'),
('M-006', 'CSK-105', 'Plant-Alpha', 'Block-A', 'U-235',  4.9, '2020-03-05'),
('M-007', 'CSK-106', 'Plant-Alpha', 'Block-A', 'U-235',  5.0, '2020-03-10'),
('M-008', 'CSK-107', 'Plant-Beta',  'Block-B', 'Pu-239', 8.1, '2020-04-01'),
('M-009', 'CSK-108', 'Plant-Beta',  'Block-B', 'Pu-239', 8.3, '2020-04-10'),
('M-010', 'CSK-109', 'Plant-Gamma', 'Block-C', 'Cs-137', 1.9, '2020-05-01');

-- Seed Sensors (Simulate normal and critical readings)
INSERT INTO Nuclear_Site.Sources.Cask_Sensors VALUES
('R-1001', 'CSK-100', '2023-10-01 12:00:00', 80.0, 1.2, 0.05), -- Normal U-235
('R-1002', 'CSK-100', '2023-10-01 13:00:00', 81.0, 1.2, 0.05),
('R-1003', 'CSK-100', '2023-10-01 14:00:00', 80.5, 1.2, 0.05),
('R-1004', 'CSK-100', '2023-10-01 15:00:00', 80.0, 1.2, 0.05),
('R-1005', 'CSK-102', '2023-10-01 12:00:00', 140.0, 2.5, 0.10), -- Normal Pu-239 (Hotter)
('R-1006', 'CSK-102', '2023-10-01 13:00:00', 142.0, 2.6, 0.11),
('R-1007', 'CSK-102', '2023-10-01 14:00:00', 145.0, 2.7, 0.12),
('R-1008', 'CSK-102', '2023-10-01 15:00:00', 155.0, 2.9, 0.15), -- Warning Temp > 150
('R-1009', 'CSK-103', '2023-10-01 12:00:00', 135.0, 2.4, 0.09),
('R-1010', 'CSK-103', '2023-10-01 13:00:00', 136.0, 2.4, 0.09),
('R-1011', 'CSK-104', '2023-10-01 12:00:00', 40.0, 1.0, 0.02), -- Cool Cs-137
('R-1012', 'CSK-104', '2023-10-01 13:00:00', 40.5, 1.0, 0.02),
('R-1013', 'CSK-101', '2023-10-01 12:00:00', 82.0, 1.3, 0.06),
('R-1014', 'CSK-105', '2023-10-01 12:00:00', 79.0, 1.2, 0.05),
('R-1015', 'CSK-106', '2023-10-01 12:00:00', 80.0, 1.2, 0.05),
('R-1016', 'CSK-107', '2023-10-01 12:00:00', 138.0, 3.1, 0.20), -- Critical Pressure! > 3.0
('R-1017', 'CSK-107', '2023-10-01 13:00:00', 139.0, 3.2, 0.22), -- Rising
('R-1018', 'CSK-108', '2023-10-01 12:00:00', 130.0, 2.3, 0.08),
('R-1019', 'CSK-109', '2023-10-01 12:00:00', 38.0, 1.0, 0.01),
('R-1020', 'CSK-102', '2023-10-01 16:00:00', 160.0, 3.0, 0.18), -- Critical Temp/Pressure combo
-- Bulk normal readings
('R-1021', 'CSK-100', '2023-10-02 08:00:00', 79.0, 1.2, 0.05),
('R-1022', 'CSK-101', '2023-10-02 08:00:00', 81.0, 1.3, 0.06),
('R-1023', 'CSK-102', '2023-10-02 08:00:00', 138.0, 2.4, 0.10), -- Cooled down?
('R-1024', 'CSK-103', '2023-10-02 08:00:00', 134.0, 2.4, 0.09),
('R-1025', 'CSK-104', '2023-10-02 08:00:00', 39.0, 1.0, 0.02),
('R-1026', 'CSK-105', '2023-10-02 08:00:00', 78.0, 1.2, 0.05),
('R-1027', 'CSK-106', '2023-10-02 08:00:00', 79.5, 1.2, 0.05),
('R-1028', 'CSK-107', '2023-10-02 08:00:00', 135.0, 2.9, 0.18), -- Pressure dropping
('R-1029', 'CSK-108', '2023-10-02 08:00:00', 129.0, 2.3, 0.08),
('R-1030', 'CSK-109', '2023-10-02 08:00:00', 37.0, 1.0, 0.01),
('R-1031', 'CSK-100', '2023-10-02 12:00:00', 80.0, 1.2, 0.05),
('R-1032', 'CSK-101', '2023-10-02 12:00:00', 82.0, 1.3, 0.06),
('R-1033', 'CSK-102', '2023-10-02 12:00:00', 141.0, 2.5, 0.11),
('R-1034', 'CSK-103', '2023-10-02 12:00:00', 136.0, 2.4, 0.09),
('R-1035', 'CSK-104', '2023-10-02 12:00:00', 41.0, 1.0, 0.02),
('R-1036', 'CSK-105', '2023-10-02 12:00:00', 80.0, 1.2, 0.05),
('R-1037', 'CSK-106', '2023-10-02 12:00:00', 81.0, 1.2, 0.05),
('R-1038', 'CSK-107', '2023-10-02 12:00:00', 140.0, 3.0, 0.19), -- Critical again
('R-1039', 'CSK-108', '2023-10-02 12:00:00', 131.0, 2.3, 0.08),
('R-1040', 'CSK-109', '2023-10-02 12:00:00', 39.0, 1.0, 0.01),
('R-1041', 'CSK-100', '2023-10-02 16:00:00', 81.0, 1.2, 0.05),
('R-1042', 'CSK-101', '2023-10-02 16:00:00', 83.0, 1.3, 0.06),
('R-1043', 'CSK-102', '2023-10-02 16:00:00', 145.0, 2.7, 0.12),
('R-1044', 'CSK-103', '2023-10-02 16:00:00', 138.0, 2.5, 0.09),
('R-1045', 'CSK-104', '2023-10-02 16:00:00', 42.0, 1.0, 0.02),
('R-1046', 'CSK-105', '2023-10-02 16:00:00', 81.0, 1.2, 0.05),
('R-1047', 'CSK-106', '2023-10-02 16:00:00', 82.0, 1.2, 0.05),
('R-1048', 'CSK-107', '2023-10-02 16:00:00', 145.0, 3.2, 0.21), -- DANGER
('R-1049', 'CSK-108', '2023-10-02 16:00:00', 133.0, 2.4, 0.08),
('R-1050', 'CSK-109', '2023-10-02 16:00:00', 40.0, 1.0, 0.01);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Nuclear_Site.Bronze.Bronze_Manifests AS SELECT * FROM Nuclear_Site.Sources.Transport_Manifests;
CREATE OR REPLACE VIEW Nuclear_Site.Bronze.Bronze_Sensors AS SELECT * FROM Nuclear_Site.Sources.Cask_Sensors;

-- 4b. SILVER LAYER (Enriched with Isotope Metadata)
CREATE OR REPLACE VIEW Nuclear_Site.Silver.Silver_Cask_Status AS
SELECT
    s.ReadingID,
    s.CaskID,
    s.Timestamp,
    s.Surface_Temp_C,
    s.Internal_Pressure_Bar,
    s.Gamma_Radiation_Sv_h,
    m.Isotope_Type,
    m.Storage_Block,
    -- Threshold logic: Different max temps for different isotopes
    CASE 
        WHEN m.Isotope_Type = 'Pu-239' AND s.Surface_Temp_C > 150 THEN 'Warning'
        WHEN m.Isotope_Type = 'U-235' AND s.Surface_Temp_C > 100 THEN 'Warning'
        WHEN s.Internal_Pressure_Bar > 3.0 THEN 'Critical'
        ELSE 'Normal'
    END as Status_Flag
FROM Nuclear_Site.Bronze.Bronze_Sensors s
JOIN Nuclear_Site.Bronze.Bronze_Manifests m ON s.CaskID = m.CaskID;

-- 4c. GOLD LAYER (Monitoring Dashboard)
CREATE OR REPLACE VIEW Nuclear_Site.Gold.Gold_Block_Health_Dashboard AS
SELECT
    Storage_Block,
    COUNT(DISTINCT CaskID) as Cask_Count,
    MAX(Surface_Temp_C) as Max_Temp_Recorded,
    MAX(Internal_Pressure_Bar) as Max_Pressure_Recorded,
    COUNT(CASE WHEN Status_Flag = 'Critical' THEN 1 END) as Critical_Alerts
FROM Nuclear_Site.Silver.Silver_Cask_Status
GROUP BY Storage_Block;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Monitor 'Gold_Block_Health_Dashboard'. If any Storage_Block has 'Critical_Alerts' > 0, 
 * immediately list the specific CaskIDs from Silver_Cask_Status causing the alert."
 */
