/*
    Dremio High-Volume SQL Pattern: Healthcare Blood Bank Inventory
    
    Business Scenario:
    Blood Banks must manage inventory levels of critical products (RBCs, Platelets) while
    minimizing wastage due to expiration.
    
    Data Story:
    We track Blood Units in stock and Transfusion Requests.
    
    Medallion Architecture:
    - Bronze: Inventory, Requests.
      *Volume*: 50+ records.
    - Silver: ExpiryProjections (Days until expiry).
    - Gold: ShortageAlerts (Low stock of specific types).
    
    Key Dremio Features:
    - TIMESTAMPDIFF(DAY)
    - Conditional Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareBloodDB;
CREATE FOLDER IF NOT EXISTS HealthcareBloodDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareBloodDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareBloodDB.Gold;
USE HealthcareBloodDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareBloodDB.Bronze.Inventory (
    UnitID STRING,
    BloodType STRING, -- A+, O-, etc.
    ProductType STRING, -- RBC, Platelets, Plasma
    CollectionDate DATE,
    ExpiryDate DATE,
    Status STRING -- Available, Reserved, Quarantined
);

INSERT INTO HealthcareBloodDB.Bronze.Inventory VALUES
('U101', 'O-', 'RBC', DATE '2024-12-01', DATE '2025-01-12', 'Available'), -- Expired
('U102', 'A+', 'RBC', DATE '2024-12-15', DATE '2025-01-26', 'Available'),
('U103', 'AB+', 'Plasma', DATE '2024-01-01', DATE '2025-01-01', 'Available'), -- Expired
('U104', 'O+', 'Platelets', DATE '2025-01-10', DATE '2025-01-15', 'Available'), -- Expiring Today
('U105', 'B-', 'RBC', DATE '2024-12-20', DATE '2025-01-30', 'Available'),
('U106', 'O-', 'Platelets', DATE '2025-01-12', DATE '2025-01-17', 'Available'),
('U107', 'A-', 'Plasma', DATE '2024-12-01', DATE '2025-12-01', 'Available'),
('U108', 'O+', 'RBC', DATE '2024-12-22', DATE '2025-02-02', 'Available'),
('U109', 'AB-', 'Platelets', DATE '2025-01-11', DATE '2025-01-16', 'Available'),
('U110', 'B+', 'RBC', DATE '2024-12-25', DATE '2025-02-05', 'Available'),
('U111', 'O-', 'RBC', DATE '2024-12-10', DATE '2025-01-20', 'Available'), -- Critical
('U112', 'A+', 'RBC', DATE '2024-12-12', DATE '2025-01-22', 'Available'),
('U113', 'AB+', 'Plasma', DATE '2024-11-01', DATE '2025-11-01', 'Available'),
('U114', 'O+', 'Platelets', DATE '2025-01-13', DATE '2025-01-18', 'Available'),
('U115', 'B-', 'RBC', DATE '2024-12-18', DATE '2025-01-28', 'Available'),
('U116', 'O-', 'Platelets', DATE '2025-01-14', DATE '2025-01-19', 'Available'),
('U117', 'A-', 'Plasma', DATE '2024-10-01', DATE '2025-10-01', 'Available'),
('U118', 'O+', 'RBC', DATE '2024-12-28', DATE '2025-02-08', 'Available'),
('U119', 'AB-', 'Platelets', DATE '2025-01-12', DATE '2025-01-17', 'Available'),
('U120', 'B+', 'RBC', DATE '2024-12-30', DATE '2025-02-10', 'Available'),
('U121', 'O-', 'RBC', DATE '2024-12-05', DATE '2025-01-15', 'Available'), -- Critical
('U122', 'A+', 'RBC', DATE '2024-12-06', DATE '2025-01-16', 'Available'),
('U123', 'AB+', 'Plasma', DATE '2024-09-01', DATE '2025-09-01', 'Available'),
('U124', 'O+', 'Platelets', DATE '2025-01-11', DATE '2025-01-16', 'Available'),
('U125', 'B-', 'RBC', DATE '2024-12-15', DATE '2025-01-25', 'Available'),
('U126', 'O-', 'Platelets', DATE '2025-01-13', DATE '2025-01-18', 'Available'),
('U127', 'A-', 'Plasma', DATE '2024-08-01', DATE '2025-08-01', 'Available'),
('U128', 'O+', 'RBC', DATE '2024-12-18', DATE '2025-01-28', 'Available'),
('U129', 'AB-', 'Platelets', DATE '2025-01-14', DATE '2025-01-19', 'Available'),
('U130', 'B+', 'RBC', DATE '2024-12-20', DATE '2025-01-30', 'Available'),
('U131', 'O-', 'RBC', DATE '2025-01-01', DATE '2025-02-11', 'Available'),
('U132', 'A+', 'RBC', DATE '2025-01-02', DATE '2025-02-12', 'Available'),
('U133', 'AB+', 'Plasma', DATE '2025-01-03', DATE '2026-01-03', 'Available'),
('U134', 'O+', 'Platelets', DATE '2025-01-14', DATE '2025-01-19', 'Available'),
('U135', 'B-', 'RBC', DATE '2025-01-05', DATE '2025-02-15', 'Available'),
('U136', 'O-', 'Platelets', DATE '2025-01-15', DATE '2025-01-20', 'Available'),
('U137', 'A-', 'Plasma', DATE '2025-01-07', DATE '2026-01-07', 'Available'),
('U138', 'O+', 'RBC', DATE '2025-01-08', DATE '2025-02-18', 'Available'),
('U139', 'AB-', 'Platelets', DATE '2025-01-14', DATE '2025-01-19', 'Available'),
('U140', 'B+', 'RBC', DATE '2025-01-10', DATE '2025-02-20', 'Available'),
('U141', 'O-', 'RBC', DATE '2024-12-02', DATE '2025-01-12', 'Available'), -- Expired
('U142', 'A+', 'RBC', DATE '2024-12-03', DATE '2025-01-13', 'Available'),
('U143', 'AB+', 'Plasma', DATE '2024-12-04', DATE '2025-12-04', 'Available'),
('U144', 'O+', 'Platelets', DATE '2025-01-10', DATE '2025-01-15', 'Available'),
('U145', 'B-', 'RBC', DATE '2024-12-05', DATE '2025-01-15', 'Available'),
('U146', 'O-', 'Platelets', DATE '2025-01-11', DATE '2025-01-16', 'Available'),
('U147', 'A-', 'Plasma', DATE '2024-12-07', DATE '2025-12-07', 'Available'),
('U148', 'O+', 'RBC', DATE '2024-12-08', DATE '2025-01-18', 'Available'),
('U149', 'AB-', 'Platelets', DATE '2025-01-12', DATE '2025-01-17', 'Available'),
('U150', 'B+', 'RBC', DATE '2024-12-10', DATE '2025-01-20', 'Available');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Expiry Logic
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareBloodDB.Silver.InventoryStatus AS
SELECT
    UnitID,
    BloodType,
    ProductType,
    ExpiryDate,
    DATE '2025-01-15' AS SimulatedToday,
    TIMESTAMPDIFF(DAY, DATE '2025-01-15', ExpiryDate) AS DaysUntilExpiry,
    CASE 
        WHEN ExpiryDate < DATE '2025-01-15' THEN 'EXPIRED'
        WHEN TIMESTAMPDIFF(DAY, DATE '2025-01-15', ExpiryDate) <= 3 THEN 'CRITICAL_EXPIRY'
        ELSE 'GOOD'
    END AS ExpiryStatus
FROM HealthcareBloodDB.Bronze.Inventory
WHERE Status = 'Available';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Inventory Summary & Alerts
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareBloodDB.Gold.SupplyDashboard AS
SELECT
    BloodType,
    ProductType,
    COUNT(UnitID) AS TotalUnits,
    SUM(CASE WHEN ExpiryStatus = 'GOOD' THEN 1 ELSE 0 END) AS UsableUnits,
    SUM(CASE WHEN ExpiryStatus = 'CRITICAL_EXPIRY' THEN 1 ELSE 0 END) AS ExpiringSoon,
    SUM(CASE WHEN ExpiryStatus = 'EXPIRED' THEN 1 ELSE 0 END) AS WastedUnits,
    CASE 
        WHEN BloodType = 'O-' AND SUM(CASE WHEN ExpiryStatus = 'GOOD' THEN 1 ELSE 0 END) < 10 THEN 'SHORTAGE ALERT'
        ELSE 'OK'
    END AS AlertLevel
FROM HealthcareBloodDB.Silver.InventoryStatus
GROUP BY BloodType, ProductType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me all O-Negative RBC units expiring in the next 3 days."
    2. "Which products have a Shortage Alert?"
    3. "Calculate the wastage rate (Expired / Total) by Blood Type."
    4. "List inventory count by Product Type."
*/
