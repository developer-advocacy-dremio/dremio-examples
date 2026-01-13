/*
 * Food Delivery Logistics Demo
 * 
 * Scenario:
 * Optimizing rider routes, delivery times, and order batching.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Minimize delivery timestamps and maximize orders per hour.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS FoodDeliveryDB;
CREATE FOLDER IF NOT EXISTS FoodDeliveryDB.Bronze;
CREATE FOLDER IF NOT EXISTS FoodDeliveryDB.Silver;
CREATE FOLDER IF NOT EXISTS FoodDeliveryDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FoodDeliveryDB.Bronze.Orders (
    OrderID INT,
    ResturantID INT,
    CustomerLocation VARCHAR,
    OrderTime TIMESTAMP,
    ReadyTime TIMESTAMP,
    PickupTime TIMESTAMP,
    DeliveryTime TIMESTAMP,
    RiderID INT
);

INSERT INTO FoodDeliveryDB.Bronze.Orders VALUES
(1, 101, 'ZoneA', '2025-01-01 18:00:00', '2025-01-01 18:15:00', '2025-01-01 18:20:00', '2025-01-01 18:35:00', 50),
(2, 101, 'ZoneB', '2025-01-01 18:05:00', '2025-01-01 18:20:00', '2025-01-01 18:25:00', '2025-01-01 18:50:00', 51),
(3, 102, 'ZoneA', '2025-01-01 18:10:00', '2025-01-01 18:25:00', '2025-01-01 18:35:00', '2025-01-01 18:45:00', 50); -- Batched order?

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FoodDeliveryDB.Silver.DeliveryPerformance AS
SELECT 
    OrderID,
    RiderID,
    ResturantID,
    TIMESTAMPDIFF(MINUTE, OrderTime, DeliveryTime) AS TotalTimeMin,
    TIMESTAMPDIFF(MINUTE, ReadyTime, PickupTime) AS PickupWaitMin,
    TIMESTAMPDIFF(MINUTE, PickupTime, DeliveryTime) AS TransitTimeMin
FROM FoodDeliveryDB.Bronze.Orders;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FoodDeliveryDB.Gold.RiderEfficiency AS
SELECT 
    RiderID,
    COUNT(OrderID) AS OrdersDelivered,
    AVG(TotalTimeMin) AS AvgDeliveryTime,
    AVG(PickupWaitMin) AS AvgWaitAtStore
FROM FoodDeliveryDB.Silver.DeliveryPerformance
GROUP BY RiderID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which RiderID has the lowest AvgDeliveryTime in FoodDeliveryDB.Gold.RiderEfficiency?"
*/
