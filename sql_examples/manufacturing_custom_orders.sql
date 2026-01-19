/*
    Dremio High-Volume SQL Pattern: Manufacturing Custom Orders (MTO)
    
    Business Scenario:
    A furniture maker allows "Made to Order" (MTO) customization (Fabric, Wood Stain).
    MTO orders have a longer SLA (4 weeks) vs Stock items (2 days).
    We need to track if MTO production is on schedule.
    
    Data Story:
    - Bronze: OrderDetails, ProductionStatus.
    - Silver: OrderSLA_Analysis (Expc. Ship Date vs Current Status).
    - Gold: AtRiskOrders (MTO orders lagging in production).
    
    Medallion Architecture:
    - Bronze: OrderDetails, ProductionStatus.
      *Volume*: 50+ records.
    - Silver: OrderSLA_Analysis.
    - Gold: AtRiskOrders.
    
    Key Dremio Features:
    - Standard SQL Join
    - SLA Date Math
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Gold;
USE ManufacturingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ManufacturingDB.Bronze.OrderDetails (
    OrderID STRING,
    OrderDate DATE,
    ItemType STRING, -- MTO, STOCK
    Description STRING
);

INSERT INTO ManufacturingDB.Bronze.OrderDetails VALUES
('O-1001', DATE '2025-01-01', 'MTO', 'Custom Sofa - Navy Velvet'),
('O-1002', DATE '2025-01-01', 'STOCK', 'Side Table - Oak'),
('O-1003', DATE '2025-01-02', 'MTO', 'Dining Table - Walnut Custom'),
('O-1004', DATE '2025-01-02', 'MTO', 'Armchair - Leather'),
('O-1005', DATE '2025-01-03', 'STOCK', 'Lamp - Brass'),
('O-1006', DATE '2025-01-04', 'MTO', 'Bed Frame - King Custom'),
('O-1007', DATE '2025-01-05', 'STOCK', 'Rug - Wool'),
('O-1008', DATE '2025-01-06', 'MTO', 'Bookshelf - Custom Height'),
('O-1009', DATE '2025-01-07', 'STOCK', 'Mirror - Round'),
('O-1010', DATE '2025-01-08', 'MTO', 'Desk - Standing Custom'), -- 10
('O-1011', DATE '2025-01-08', 'MTO', 'Sofa - Grey Linen'),
('O-1012', DATE '2025-01-09', 'STOCK', 'Chair - Stackable'),
('O-1013', DATE '2025-01-09', 'MTO', 'Ottoman - Custom Fabric'),
('O-1014', DATE '2025-01-10', 'STOCK', 'Plant Pot - Ceramic'),
('O-1015', DATE '2025-01-10', 'MTO', 'Curtains - Custom Length'),
('O-1016', DATE '2025-01-11', 'STOCK', 'Throw Pillow'),
('O-1017', DATE '2025-01-11', 'MTO', 'Bench - Entryway Custom'),
('O-1018', DATE '2025-01-12', 'STOCK', 'Vase - Glass'),
('O-1019', DATE '2025-01-12', 'MTO', 'Headboard - Tufted'),
('O-1020', DATE '2025-01-13', 'STOCK', 'Clock - Wall'), -- 20
('O-1021', DATE '2025-01-14', 'MTO', 'Dining Chair - Custom Upholstery'),
('O-1022', DATE '2025-01-14', 'STOCK', 'Stool - Bar'),
('O-1023', DATE '2025-01-15', 'MTO', 'Coffee Table - Marble Custom'),
('O-1024', DATE '2025-01-15', 'STOCK', 'Coaster Set'),
('O-1025', DATE '2025-01-16', 'MTO', 'Cabinet - Built-in Spec'),
('O-1026', DATE '2025-01-16', 'STOCK', 'Picture Frame'),
('O-1027', DATE '2025-01-17', 'MTO', 'Credenza - Teak Custom'),
('O-1028', DATE '2025-01-17', 'STOCK', 'Candle Holder'),
('O-1029', DATE '2025-01-18', 'MTO', 'Wardrobe - Walk-in'),
('O-1030', DATE '2025-01-18', 'STOCK', 'Basket - Woven'), -- 30
('O-1031', DATE '2025-01-19', 'MTO', 'Recliner - Electric Custom'),
('O-1032', DATE '2025-01-19', 'STOCK', 'Bookend'),
('O-1033', DATE '2025-01-20', 'MTO', 'Patio Set - Custom Cushions'),
('O-1034', DATE '2025-01-20', 'STOCK', 'Lantern'),
('O-1035', DATE '2025-01-21', 'MTO', 'Nightstand - Floating Custom'),
('O-1036', DATE '2025-01-21', 'STOCK', 'Hook - Wall'),
('O-1037', DATE '2025-01-22', 'MTO', 'Dresser - 6 Drawer Custom'),
('O-1038', DATE '2025-01-22', 'STOCK', 'Tray - Serving'),
('O-1039', DATE '2025-01-23', 'MTO', 'Bar Cart - Gold Leaf'),
('O-1040', DATE '2025-01-23', 'STOCK', 'Napkin Ring'), -- 40
('O-1041', DATE '2025-01-24', 'MTO', 'Console Table - Narrow'),
('O-1042', DATE '2025-01-24', 'STOCK', 'Doorstop'),
('O-1043', DATE '2025-01-25', 'MTO', 'Vanity - Bathroom Custom'),
('O-1044', DATE '2025-01-25', 'STOCK', 'Soap Dish'),
('O-1045', DATE '2025-01-26', 'MTO', 'Pantry Shelves - Custom'),
('O-1046', DATE '2025-01-26', 'STOCK', 'Jar - Mason'),
('O-1047', DATE '2025-01-27', 'MTO', 'Mudroom Bench - Boot Storage'),
('O-1048', DATE '2025-01-27', 'STOCK', 'Mat - Welcome'),
('O-1049', DATE '2025-01-28', 'MTO', 'Fireplace Mantel - Custom'),
('O-1050', DATE '2025-01-28', 'STOCK', 'Poker - Fireplace'); -- 50

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.ProductionStatus (
    OrderID STRING,
    CurrentStep STRING, -- Queued, Cutting, Assembly, Finishing, Shipping
    LastUpdated DATE
);

INSERT INTO ManufacturingDB.Bronze.ProductionStatus VALUES
('O-1001', 'Finishing', DATE '2025-01-15'), -- On track
('O-1002', 'Shipping', DATE '2025-01-03'), -- Done
('O-1003', 'Queued', DATE '2025-01-02'), -- STUCK! (Should be further along)
('O-1004', 'Assembly', DATE '2025-01-18'),
('O-1005', 'Shipping', DATE '2025-01-05'),
('O-1006', 'Cutting', DATE '2025-01-19'), -- Late
('O-1007', 'Shipping', DATE '2025-01-07'),
('O-1008', 'Assembly', DATE '2025-01-20'),
('O-1009', 'Shipping', DATE '2025-01-09'),
('O-1010', 'Queued', DATE '2025-01-08'); -- Stuck

-- Bulk inserts for the rest (simplified)
INSERT INTO ManufacturingDB.Bronze.ProductionStatus 
SELECT 
    OrderID, 
    CASE WHEN MOD(CAST(SUBSTR(OrderID, 3) AS INT), 2) = 0 THEN 'Shipping' ELSE 'Queued' END, 
    OrderDate 
FROM ManufacturingDB.Bronze.OrderDetails 
WHERE CAST(SUBSTR(OrderID, 3) AS INT) > 1010;

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Order SLA Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Silver.OrderSLA_Analysis AS
SELECT
    o.OrderID,
    o.ItemType,
    o.OrderDate,
    p.CurrentStep,
    p.LastUpdated,
    CASE 
        WHEN o.ItemType = 'MTO' THEN DATE_ADD(o.OrderDate, 28) -- 4 Weeks
        ELSE DATE_ADD(o.OrderDate, 2) -- 2 Days
    END AS ExpectedShipDate,
    -- Simulating running this on Jan 20th
    TIMESTAMPDIFF(DAY, o.OrderDate, DATE '2025-01-20') AS DaysElapsed
FROM ManufacturingDB.Bronze.OrderDetails o
JOIN ManufacturingDB.Bronze.ProductionStatus p ON o.OrderID = p.OrderID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: At Risk Orders
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Gold.AtRiskOrders AS
SELECT
    OrderID,
    Description,
    ExpectedShipDate,
    CurrentStep,
    DaysElapsed,
    'LATE_PRODUCTION' AS RiskReason
FROM ManufacturingDB.Silver.OrderSLA_Analysis
WHERE ItemType = 'MTO' 
  AND CurrentStep IN ('Queued', 'Cutting')
  AND DaysElapsed > 14; -- If still in early stages after 2 weeks, it's at risk

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me MTO orders that are at risk of missing the ship date."
    2. "Count orders by Current Step."
    3. "What is the average age of orders currently in 'Queued' status?"
*/
