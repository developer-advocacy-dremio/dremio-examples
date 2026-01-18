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
('AST005', 'XRay', 'Philips', 'Digital', DATE '2024-12-01', 180);
-- Bulk Assets
INSERT INTO HealthcareAssetsDB.Bronze.AssetInventory
SELECT 
  'AST' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 4 = 0 THEN 'InfusionPump' WHEN rn % 4 = 1 THEN 'Vent' WHEN rn % 4 = 2 THEN 'Monitor' ELSE 'Bed' END,
  'GenericMfg',
  'ModelX',
  DATE_SUB(DATE '2025-01-01', CAST((rn % 400) AS INT)), -- Random last PM dates
  CASE WHEN rn % 2 = 0 THEN 365 ELSE 180 END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

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
