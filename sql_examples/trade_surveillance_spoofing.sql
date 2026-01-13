/*
 * Trade Surveillance (Spoofing) Demo
 * 
 * Scenario:
 * Detecting "Spoofing" - placing large orders to manipulate price and canceling them 
 * before execution.
 * 
 * Data Context:
 * - OrderBookLog: All Limit Orders sent to exchange.
 * - Cancels: Orders canceled within < 1 second.
 * 
 * Analytical Goal:
 * Identify traders with a high Ratio of Cancelled Value to Executed Value (Order-to-Trade Ratio).
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SurveillanceDB;
CREATE FOLDER IF NOT EXISTS SurveillanceDB.Bronze;
CREATE FOLDER IF NOT EXISTS SurveillanceDB.Silver;
CREATE FOLDER IF NOT EXISTS SurveillanceDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SurveillanceDB.Bronze.RawOrders (
    OrderID VARCHAR,
    TraderID VARCHAR,
    Symbol VARCHAR,
    Qty INT,
    Price DOUBLE,
    Action VARCHAR, -- 'New', 'Cancel', 'Fill'
    "Timestamp" TIMESTAMP
);

INSERT INTO SurveillanceDB.Bronze.RawOrders VALUES
('O-1', 'Trader_Bad', 'XYZ', 10000, 10.05, 'New', '2025-01-01 10:00:00.000'),
('O-1', 'Trader_Bad', 'XYZ', 0, 0, 'Cancel', '2025-01-01 10:00:00.500'), -- Cancel < 1s
('O-2', 'Trader_Bad', 'XYZ', 10000, 10.05, 'New', '2025-01-01 10:00:01.000'),
('O-2', 'Trader_Bad', 'XYZ', 0, 0, 'Cancel', '2025-01-01 10:00:01.500'),
('O-3', 'Trader_Bad', 'XYZ', 10000, 10.05, 'New', '2025-01-01 10:00:02.000'),
('O-3', 'Trader_Bad', 'XYZ', 0, 0, 'Cancel', '2025-01-01 10:00:02.500'),
('O-4', 'Trader_Good', 'XYZ', 100, 10.05, 'New', '2025-01-01 10:05:00.000'),
('O-4', 'Trader_Good', 'XYZ', 100, 10.05, 'Fill', '2025-01-01 10:05:05.000'), -- Filled
('O-5', 'Trader_Bad', 'XYZ', 50, 10.04, 'New', '2025-01-01 10:05:10.000'), -- Small fill
('O-5', 'Trader_Bad', 'XYZ', 50, 10.04, 'Fill', '2025-01-01 10:05:11.000'),
('O-6', 'Trader_New', 'ABC', 1000, 50.00, 'New', '2025-01-01 11:00:00'),
('O-6', 'Trader_New', 'ABC', 0, 0, 'Cancel', '2025-01-01 11:30:00'); -- Long dur cancel

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SurveillanceDB.Silver.OrderLifecycle AS
SELECT 
    n.OrderID,
    n.TraderID,
    n.Symbol,
    n.Qty AS InitialQty,
    c."Timestamp" AS CancelTime,
    f."Timestamp" AS FillTime,
    CASE 
        WHEN c.OrderID IS NOT NULL AND 
             (EXTRACT(SECOND FROM c."Timestamp") - EXTRACT(SECOND FROM n."Timestamp")) < 1.0 
        THEN 'FlashCancel'
        WHEN f.OrderID IS NOT NULL THEN 'Filled'
        ELSE 'Open/Other'
    END AS Outcome
FROM SurveillanceDB.Bronze.RawOrders n
LEFT JOIN SurveillanceDB.Bronze.RawOrders c 
    ON n.OrderID = c.OrderID AND c.Action = 'Cancel'
LEFT JOIN SurveillanceDB.Bronze.RawOrders f 
    ON n.OrderID = f.OrderID AND f.Action = 'Fill'
WHERE n.Action = 'New';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SurveillanceDB.Gold.SuspectAlerts AS
SELECT 
    TraderID,
    COUNT(*) AS TotalOrders,
    SUM(CASE WHEN Outcome = 'FlashCancel' THEN 1 ELSE 0 END) AS FlashCancelCount,
    SUM(CASE WHEN Outcome = 'Filled' THEN 1 ELSE 0 END) AS FillCount,
    (CAST(SUM(CASE WHEN Outcome = 'FlashCancel' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100 AS SpooferScore
FROM SurveillanceDB.Silver.OrderLifecycle
GROUP BY TraderID
HAVING (CAST(SUM(CASE WHEN Outcome = 'FlashCancel' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) > 0.5; -- >50% Flash Cancel

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Who has the highest SpooferScore in SurveillanceDB.Gold.SuspectAlerts?"
*/
