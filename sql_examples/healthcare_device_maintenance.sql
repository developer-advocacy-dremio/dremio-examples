/*
    Dremio High-Volume SQL Pattern: Healthcare Medical Device Maintenance
    
    Business Scenario:
    Clinical Engineering needs to track preventive maintenance (PM) schedules for critical assets
    (MRI, Ventilators, Pumps). Missed maintenance risks patient safety and accreditation.
    
    Data Story:
    We track an Asset Inventory (Last PM Date, Frequency) and calculate the next due date.
    
    Medallion Architecture:
    - Bronze: AssetInventory, WorkOrders.
      *Volume*: 50+ records.
    - Silver: MaintenanceSchedule (Next Due Date calculation).
    - Gold: CriticalRiskReport (Overdue assets).
    
    Key Dremio Features:
    - DATE_ADD
    - TIMESTAMPDIFF for Overdue Days
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareAssetsDB;
CREATE FOLDER IF NOT EXISTS HealthcareAssetsDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareAssetsDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareAssetsDB.Gold;
USE HealthcareAssetsDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareAssetsDB.Bronze.AssetInventory (
    AssetID STRING,
    DeviceType STRING, -- MRI, CT, Vent, InfusionPump
    Manufacturer STRING,
    Model STRING,
    LastPMDate DATE,
    PMFrequencyDays INT -- e.g., 365, 180, 90
);

INSERT INTO HealthcareAssetsDB.Bronze.AssetInventory VALUES
('AST001', 'MRI', 'GE', 'Sigma', DATE '2024-01-01', 365), -- Due Jan 1 2025
('AST002', 'CT', 'Siemens', 'Somatom', DATE '2024-06-01', 180), -- Due Dec 2024 (Overdue)
('AST003', 'Vent', 'Dragher', 'Infinity', DATE '2024-10-01', 90), -- Due Jan 2025
('AST004', 'InfusionPump', 'Baxter', 'Sigma', DATE '2023-01-01', 365), -- Very Overdue
('AST005', 'XRay', 'Philips', 'Digital', DATE '2024-12-01', 180),
('AST006', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-01-15', 365),
('AST007', 'Vent', 'Dragher', 'Infinity', DATE '2024-11-01', 90),
('AST008', 'Monitor', 'Philips', 'Intellivue', DATE '2024-08-01', 180),
('AST009', 'Bed', 'HillRom', 'Progressa', DATE '2024-02-01', 365),
('AST010', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-03-01', 365),
('AST011', 'Vent', 'Dragher', 'Infinity', DATE '2024-12-01', 90),
('AST012', 'Monitor', 'Philips', 'Intellivue', DATE '2024-09-01', 180),
('AST013', 'Bed', 'HillRom', 'Progressa', DATE '2024-04-01', 365),
('AST014', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-05-01', 365),
('AST015', 'Vent', 'Dragher', 'Infinity', DATE '2025-01-01', 90),
('AST016', 'Monitor', 'Philips', 'Intellivue', DATE '2024-10-01', 180),
('AST017', 'Bed', 'HillRom', 'Progressa', DATE '2024-06-01', 365),
('AST018', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-07-01', 365),
('AST019', 'Vent', 'Dragher', 'Infinity', DATE '2025-01-10', 90),
('AST020', 'Monitor', 'Philips', 'Intellivue', DATE '2024-11-01', 180),
('AST021', 'Bed', 'HillRom', 'Progressa', DATE '2024-08-01', 365),
('AST022', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-09-01', 365),
('AST023', 'Vent', 'Dragher', 'Infinity', DATE '2025-01-15', 90),
('AST024', 'Monitor', 'Philips', 'Intellivue', DATE '2024-12-01', 180),
('AST025', 'Bed', 'HillRom', 'Progressa', DATE '2024-10-01', 365),
('AST026', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-11-01', 365),
('AST027', 'Vent', 'Dragher', 'Infinity', DATE '2025-01-20', 90),
('AST028', 'Monitor', 'Philips', 'Intellivue', DATE '2025-01-01', 180),
('AST029', 'Bed', 'HillRom', 'Progressa', DATE '2024-12-01', 365),
('AST030', 'InfusionPump', 'Baxter', 'Sigma', DATE '2025-01-01', 365),
('AST031', 'Vent', 'Dragher', 'Infinity', DATE '2024-09-01', 90), -- Overdue
('AST032', 'Monitor', 'Philips', 'Intellivue', DATE '2024-07-01', 180), -- Overdue
('AST033', 'Bed', 'HillRom', 'Progressa', DATE '2024-01-01', 365), -- Overdue
('AST034', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-02-01', 365),
('AST035', 'Vent', 'Dragher', 'Infinity', DATE '2024-10-15', 90), -- Overdue
('AST036', 'Monitor', 'Philips', 'Intellivue', DATE '2024-08-15', 180),
('AST037', 'Bed', 'HillRom', 'Progressa', DATE '2024-03-01', 365),
('AST038', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-04-01', 365),
('AST039', 'Vent', 'Dragher', 'Infinity', DATE '2024-11-15', 90),
('AST040', 'Monitor', 'Philips', 'Intellivue', DATE '2024-09-15', 180),
('AST041', 'Bed', 'HillRom', 'Progressa', DATE '2024-05-01', 365),
('AST042', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-06-01', 365),
('AST043', 'Vent', 'Dragher', 'Infinity', DATE '2024-12-15', 90),
('AST044', 'Monitor', 'Philips', 'Intellivue', DATE '2024-10-15', 180),
('AST045', 'Bed', 'HillRom', 'Progressa', DATE '2024-07-01', 365),
('AST046', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-08-01', 365),
('AST047', 'Vent', 'Dragher', 'Infinity', DATE '2025-01-05', 90),
('AST048', 'Monitor', 'Philips', 'Intellivue', DATE '2024-11-15', 180),
('AST049', 'Bed', 'HillRom', 'Progressa', DATE '2024-09-01', 365),
('AST050', 'InfusionPump', 'Baxter', 'Sigma', DATE '2024-10-01', 365);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Schedule Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareAssetsDB.Silver.MaintenanceSchedule AS
SELECT
    AssetID,
    DeviceType,
    Manufacturer,
    LastPMDate,
    PMFrequencyDays,
    DATE_ADD(LastPMDate, CAST(PMFrequencyDays AS INT)) AS NextDueDate,
    DATE '2025-02-01' AS CurrentDateSimulated,
    TIMESTAMPDIFF(DAY, DATE_ADD(LastPMDate, CAST(PMFrequencyDays AS INT)), DATE '2025-02-01') AS DaysOverdue
FROM HealthcareAssetsDB.Bronze.AssetInventory;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Critical Risk Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareAssetsDB.Gold.CriticalRiskReport AS
SELECT
    AssetID,
    DeviceType,
    NextDueDate,
    DaysOverdue,
    CASE 
        WHEN DaysOverdue > 30 THEN 'CRITICAL'
        WHEN DaysOverdue > 0 THEN 'OVERDUE'
        WHEN DaysOverdue > -30 THEN 'DUE_SOON'
        ELSE 'COMPLIANT'
    END AS Status
FROM HealthcareAssetsDB.Silver.MaintenanceSchedule
WHERE DaysOverdue > -30 -- Focus on overdue or upcoming
ORDER BY DaysOverdue DESC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Count the number of CRITICAL assets by Device Type."
    2. "List all Ventilators that are Overdue for maintenance."
    3. "Show the maintenance compliance status percentage for the whole fleet."
    4. "Which manufacturer has the most overdue devices?"
*/
