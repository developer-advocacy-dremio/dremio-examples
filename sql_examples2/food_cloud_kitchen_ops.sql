/*
 * Food Services: Cloud Kitchen Operations
 * 
 * Scenario:
 * Optimizing prep times and courier handoffs across multiple virtual brands operating from a single kitchen.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS GhostKitchenDB;
CREATE FOLDER IF NOT EXISTS GhostKitchenDB.Operations;
CREATE FOLDER IF NOT EXISTS GhostKitchenDB.Operations.Bronze;
CREATE FOLDER IF NOT EXISTS GhostKitchenDB.Operations.Silver;
CREATE FOLDER IF NOT EXISTS GhostKitchenDB.Operations.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Kitchen Data
-------------------------------------------------------------------------------

-- Orders Table
CREATE TABLE IF NOT EXISTS GhostKitchenDB.Operations.Bronze.Orders (
    OrderID VARCHAR,
    BrandName VARCHAR, -- BurgerCo, PizzaEx, ThaiSpice
    OrderTime TIMESTAMP,
    StationID VARCHAR, -- GRILL-1, OVEN-2
    CourierArrivedTime TIMESTAMP,
    OrderReadyTime TIMESTAMP
);

INSERT INTO GhostKitchenDB.Operations.Bronze.Orders VALUES
('O-001', 'BurgerCo', '2025-06-01 18:00:00', 'GRILL-1', '2025-06-01 18:20:00', '2025-06-01 18:15:00'), -- Good
('O-002', 'BurgerCo', '2025-06-01 18:05:00', 'GRILL-1', '2025-06-01 18:25:00', '2025-06-01 18:30:00'), -- Late Prep
('O-003', 'PizzaEx', '2025-06-01 18:10:00', 'OVEN-1', '2025-06-01 18:35:00', '2025-06-01 18:30:00'),
('O-004', 'ThaiSpice', '2025-06-01 18:15:00', 'WOK-1', '2025-06-01 18:40:00', '2025-06-01 18:35:00'),
('O-005', 'BurgerCo', '2025-06-01 18:20:00', 'GRILL-2', '2025-06-01 18:45:00', '2025-06-01 18:40:00'),
('O-006', 'PizzaEx', '2025-06-01 18:25:00', 'OVEN-2', '2025-06-01 18:50:00', '2025-06-01 18:45:00'),
('O-007', 'ThaiSpice', '2025-06-01 18:30:00', 'WOK-1', '2025-06-01 18:55:00', '2025-06-01 19:00:00'), -- Late Prep
('O-008', 'BurgerCo', '2025-06-01 18:35:00', 'GRILL-1', '2025-06-01 19:00:00', '2025-06-01 18:55:00'),
('O-009', 'BurgerCo', '2025-06-01 18:40:00', 'GRILL-2', '2025-06-01 19:05:00', '2025-06-01 19:10:00'), -- Late
('O-010', 'PizzaEx', '2025-06-01 18:45:00', 'OVEN-1', '2025-06-01 19:10:00', '2025-06-01 19:05:00'),
('O-011', 'BurgerCo', '2025-06-01 18:50:00', 'GRILL-1', '2025-06-01 19:15:00', '2025-06-01 19:05:00'),
('O-012', 'ThaiSpice', '2025-06-01 18:55:00', 'WOK-1', '2025-06-01 19:20:00', '2025-06-01 19:15:00'),
('O-013', 'PizzaEx', '2025-06-01 19:00:00', 'OVEN-1', '2025-06-01 19:30:00', '2025-06-01 19:20:00'),
('O-014', 'BurgerCo', '2025-06-01 19:05:00', 'GRILL-2', '2025-06-01 19:35:00', '2025-06-01 19:25:00'),
('O-015', 'BurgerCo', '2025-06-01 19:10:00', 'GRILL-1', '2025-06-01 19:40:00', '2025-06-01 19:30:00'),
('O-016', 'ThaiSpice', '2025-06-01 19:15:00', 'WOK-1', '2025-06-01 19:45:00', '2025-06-01 19:50:00'), -- Late
('O-017', 'PizzaEx', '2025-06-01 19:20:00', 'OVEN-2', '2025-06-01 19:50:00', '2025-06-01 19:45:00'),
('O-018', 'BurgerCo', '2025-06-01 19:25:00', 'GRILL-1', '2025-06-01 20:00:00', '2025-06-01 19:45:00'),
('O-019', 'PizzaEx', '2025-06-01 19:30:00', 'OVEN-1', '2025-06-01 20:00:00', '2025-06-01 19:55:00'),
('O-020', 'BurgerCo', '2025-06-01 19:35:00', 'GRILL-2', '2025-06-01 20:05:00', '2025-06-01 20:10:00'), -- Late
('O-021', 'ThaiSpice', '2025-06-01 19:40:00', 'WOK-1', '2025-06-01 20:10:00', '2025-06-01 20:00:00'),
('O-022', 'PizzaEx', '2025-06-01 19:45:00', 'OVEN-2', '2025-06-01 20:15:00', '2025-06-01 20:20:00'), -- Late
('O-023', 'BurgerCo', '2025-06-01 19:50:00', 'GRILL-1', '2025-06-01 20:20:00', '2025-06-01 20:10:00'),
('O-024', 'BurgerCo', '2025-06-01 19:55:00', 'GRILL-2', '2025-06-01 20:25:00', '2025-06-01 20:15:00'),
('O-025', 'PizzaEx', '2025-06-01 20:00:00', 'OVEN-1', '2025-06-01 20:30:00', '2025-06-01 20:25:00'),
('O-026', 'ThaiSpice', '2025-06-01 20:05:00', 'WOK-1', '2025-06-01 20:35:00', '2025-06-01 20:30:00'),
('O-027', 'BurgerCo', '2025-06-01 20:10:00', 'GRILL-1', '2025-06-01 20:40:00', '2025-06-01 20:30:00'),
('O-028', 'PizzaEx', '2025-06-01 20:15:00', 'OVEN-2', '2025-06-01 20:45:00', '2025-06-01 20:40:00'),
('O-029', 'BurgerCo', '2025-06-01 20:20:00', 'GRILL-2', '2025-06-01 20:50:00', '2025-06-01 20:55:00'), -- Late
('O-030', 'ThaiSpice', '2025-06-01 20:25:00', 'WOK-1', '2025-06-01 20:55:00', '2025-06-01 20:50:00'),
('O-031', 'PizzaEx', '2025-06-01 20:30:00', 'OVEN-1', '2025-06-01 21:00:00', '2025-06-01 20:55:00'),
('O-032', 'BurgerCo', '2025-06-01 20:35:00', 'GRILL-1', '2025-06-01 21:05:00', '2025-06-01 21:10:00'), -- Late
('O-033', 'BurgerCo', '2025-06-01 20:40:00', 'GRILL-2', '2025-06-01 21:10:00', '2025-06-01 21:00:00'),
('O-034', 'ThaiSpice', '2025-06-01 20:45:00', 'WOK-1', '2025-06-01 21:15:00', '2025-06-01 21:10:00'),
('O-035', 'PizzaEx', '2025-06-01 20:50:00', 'OVEN-2', '2025-06-01 21:20:00', '2025-06-01 21:15:00'),
('O-036', 'BurgerCo', '2025-06-01 20:55:00', 'GRILL-1', '2025-06-01 21:25:00', '2025-06-01 21:15:00'),
('O-037', 'ThaiSpice', '2025-06-01 21:00:00', 'WOK-1', '2025-06-01 21:30:00', '2025-06-01 21:35:00'), -- Late
('O-038', 'BurgerCo', '2025-06-01 21:05:00', 'GRILL-2', '2025-06-01 21:35:00', '2025-06-01 21:25:00'),
('O-039', 'PizzaEx', '2025-06-01 21:10:00', 'OVEN-1', '2025-06-01 21:40:00', '2025-06-01 21:35:00'),
('O-040', 'BurgerCo', '2025-06-01 21:15:00', 'GRILL-1', '2025-06-01 21:45:00', '2025-06-01 21:40:00'),
('O-041', 'ThaiSpice', '2025-06-01 21:20:00', 'WOK-1', '2025-06-01 21:50:00', '2025-06-01 21:55:00'), -- Late
('O-042', 'PizzaEx', '2025-06-01 21:25:00', 'OVEN-2', '2025-06-01 21:55:00', '2025-06-01 21:50:00'),
('O-043', 'BurgerCo', '2025-06-01 21:30:00', 'GRILL-2', '2025-06-01 22:00:00', '2025-06-01 21:50:00'),
('O-044', 'BurgerCo', '2025-06-01 21:35:00', 'GRILL-1', '2025-06-01 22:05:00', '2025-06-01 22:00:00'),
('O-045', 'PizzaEx', '2025-06-01 21:40:00', 'OVEN-1', '2025-06-01 22:10:00', '2025-06-01 22:05:00'),
('O-046', 'ThaiSpice', '2025-06-01 21:45:00', 'WOK-1', '2025-06-01 22:15:00', '2025-06-01 22:10:00'),
('O-047', 'BurgerCo', '2025-06-01 21:50:00', 'GRILL-2', '2025-06-01 22:20:00', '2025-06-01 22:10:00'),
('O-048', 'PizzaEx', '2025-06-01 21:55:00', 'OVEN-2', '2025-06-01 22:25:00', '2025-06-01 22:20:00'),
('O-049', 'BurgerCo', '2025-06-01 22:00:00', 'GRILL-1', '2025-06-01 22:30:00', '2025-06-01 22:25:00'),
('O-050', 'ThaiSpice', '2025-06-01 22:05:00', 'WOK-1', '2025-06-01 22:35:00', '2025-06-01 22:30:00');

-- KitchenStations Table
CREATE TABLE IF NOT EXISTS GhostKitchenDB.Operations.Bronze.KitchenStations (
    StationID VARCHAR,
    StationType VARCHAR,
    StaffCount INT
);

INSERT INTO GhostKitchenDB.Operations.Bronze.KitchenStations VALUES
('GRILL-1', 'Grill', 3),
('GRILL-2', 'Grill', 2),
('OVEN-1', 'Pizza Oven', 2),
('OVEN-2', 'Pizza Oven', 2),
('WOK-1', 'Asian Wok', 2);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Prep Time Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW GhostKitchenDB.Operations.Silver.PrepPerformance AS
SELECT 
    o.OrderID,
    o.BrandName,
    o.StationID,
    -- Dremio DATEDIFF syntax: DATE_DIFF(DAY, start, end) returns int. We want minutes.
    -- Better to use subtraction which returns interval, then extract.
    -- Normalized: (ReadyTime - OrderTime) is Prep Duration
    -- (ReadyTime - CourierArrivedTime) > 0 means Courier Waited
    o.OrderReadyTime,
    o.CourierArrivedTime,
    CASE 
        WHEN o.OrderReadyTime > o.CourierArrivedTime THEN 'Courier Waited'
        ELSE 'On Time'
    END AS HandoffStatus
FROM GhostKitchenDB.Operations.Bronze.Orders o;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Bottleneck Report
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW GhostKitchenDB.Operations.Gold.StationBottlenecks AS
SELECT 
    StationID,
    BrandName,
    COUNT(OrderID) AS TotalOrders,
    SUM(CASE WHEN HandoffStatus = 'Courier Waited' THEN 1 ELSE 0 END) AS LateHandoffs,
    (CAST(SUM(CASE WHEN HandoffStatus = 'Courier Waited' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(OrderID)) * 100 AS LateRatePct
FROM GhostKitchenDB.Operations.Silver.PrepPerformance
GROUP BY StationID, BrandName;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify which StationID has the highest Late Handoff Rate from the GhostKitchenDB.Operations.Gold.StationBottlenecks view."

PROMPT 2:
"List all orders where the 'Courier Waited' from the Silver layer, sorted by BrandName."

PROMPT 3:
"Count ticket volume per BrandName for the 'GRILL-1' station."
*/
