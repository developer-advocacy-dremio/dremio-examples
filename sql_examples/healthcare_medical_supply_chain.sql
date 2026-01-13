/*
 * Medical Supply Chain Demo
 * 
 * Scenario:
 * Tracking inventory levels of critical supplies (PPE, Surgical Kits) to prevent stockouts.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.SupplyChainHC;
CREATE FOLDER IF NOT EXISTS RetailDB.SupplyChainHC.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.SupplyChainHC.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.SupplyChainHC.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.SupplyChainHC.Bronze.Inventory (
    ItemID INT,
    ItemName VARCHAR,
    Category VARCHAR,
    UnitsOnHand INT,
    ReorderThresh INT,
    Supplier VARCHAR
);

INSERT INTO RetailDB.SupplyChainHC.Bronze.Inventory VALUES
(1, 'N95 Mask', 'PPE', 15000, 5000, 'MedSafe'),
(2, 'Surgical Gloves (M)', 'PPE', 8000, 10000, 'MedSafe'), -- Low Stock
(3, 'IV Kit', 'Clinical', 200, 500, 'CareSupply'), -- Critical
(4, 'Surgical Gown', 'PPE', 2000, 1500, 'MedSafe'),
(5, 'Syringe 5ml', 'Clinical', 50000, 20000, 'PharmaPack'),
(6, 'Ventilator Tube', 'Equipment', 150, 200, 'RespiraCo'), -- Low
(7, 'Face Shield', 'PPE', 4000, 2000, 'MedSafe'),
(8, 'Antibiotic Ivy Bag', 'Pharma', 300, 400, 'PharmaPack'), -- Low
(9, 'Bandage Roll', 'Clinical', 1000, 500, 'CareSupply'),
(10, 'Oxygen Tank', 'Equipment', 50, 40, 'RespiraCo'),
(11, 'Defibrillator Pad', 'Equipment', 100, 50, 'RespiraCo'),
(12, 'Scalpel #10', 'Clinical', 600, 200, 'CareSupply');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.SupplyChainHC.Silver.StockStatus AS
SELECT 
    ItemID,
    ItemName,
    Category,
    UnitsOnHand,
    ReorderThresh,
    CASE 
        WHEN UnitsOnHand <= (ReorderThresh * 0.5) THEN 'CRITICAL'
        WHEN UnitsOnHand <= ReorderThresh THEN 'LOW'
        ELSE 'OK'
    END AS Status
FROM RetailDB.SupplyChainHC.Bronze.Inventory;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.SupplyChainHC.Gold.OrderRequests AS
SELECT 
    Category,
    COUNT(*) AS ItemsInNeed,
    LISTAGG(ItemName, ', ') AS ItemList 
FROM RetailDB.SupplyChainHC.Silver.StockStatus
WHERE Status IN ('LOW', 'CRITICAL')
GROUP BY Category;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Generate a list of items from RetailDB.SupplyChainHC.Silver.StockStatus where Status is 'CRITICAL'."
*/
