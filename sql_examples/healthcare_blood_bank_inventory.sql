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

-- Insert 50+ records
INSERT INTO HealthcareBloodDB.Bronze.Inventory VALUES
('U001', 'O-', 'RBC', DATE '2024-12-01', DATE '2025-01-12', 'Available'), -- Expiring soon
('U002', 'A+', 'RBC', DATE '2024-12-15', DATE '2025-01-26', 'Available'),
('U003', 'AB+', 'Plasma', DATE '2024-01-01', DATE '2025-01-01', 'Available'),
('U004', 'O+', 'Platelets', DATE '2025-01-10', DATE '2025-01-15', 'Available'); -- Platelets short shelf life
-- Bulk Inventory
INSERT INTO HealthcareBloodDB.Bronze.Inventory
SELECT 
  'U' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 4 = 0 THEN 'O-' WHEN rn % 4 = 1 THEN 'A+' WHEN rn % 4 = 2 THEN 'B-' ELSE 'AB+' END,
  CASE WHEN rn % 3 = 0 THEN 'RBC' WHEN rn % 3 = 1 THEN 'Platelets' ELSE 'Plasma' END,
  DATE '2024-12-01',
  DATE_ADD(DATE '2025-01-15', CAST((rn % 20) - 10 AS INT)), -- Spread expiries around today
  'Available'
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

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
