/*
    Dremio High-Volume SQL Pattern: Services Kitchen Display System (KDS)
    
    Business Scenario:
    Restaurants use KDS to time orders. "Ticket Time" is the key metric.
    We track time from "Order Taken" to "Bumped" (Completed) to find slow stations.
    
    Data Story:
    - Bronze: TicketLogs (Order timestamps), StationConfig.
    - Silver: StationPerformance (Avg Prep Time).
    - Gold: KitchenBottlenecks.
    
    Medallion Architecture:
    - Bronze: TicketLogs, StationConfig.
      *Volume*: 50+ records.
    - Silver: StationPerformance.
    - Gold: KitchenBottlenecks.
    
    Key Dremio Features:
    - TIMESTAMPDIFF(Second)
    - AVG / MAX
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ServicesDB;
CREATE FOLDER IF NOT EXISTS ServicesDB.Bronze;
CREATE FOLDER IF NOT EXISTS ServicesDB.Silver;
CREATE FOLDER IF NOT EXISTS ServicesDB.Gold;
USE ServicesDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ServicesDB.Bronze.StationConfig (
    StationID STRING,
    StationName STRING, -- Grill, Fry, Salad, Expo
    TargetPrepSeconds INT
);

INSERT INTO ServicesDB.Bronze.StationConfig VALUES
('ST01', 'Grill', 300), -- 5 mins main
('ST02', 'Fry', 180),   -- 3 mins sides
('ST03', 'Salad', 120), -- 2 mins cold
('ST04', 'Expo', 60);   -- 1 min assembly

CREATE OR REPLACE TABLE ServicesDB.Bronze.TicketLogs (
    TicketID INT,
    StationID STRING,
    OrderTime TIMESTAMP,
    BumpTime TIMESTAMP 
);

INSERT INTO ServicesDB.Bronze.TicketLogs VALUES
-- Ticket 101 - Fast
(101, 'ST01', TIMESTAMP '2025-01-20 18:00:00', TIMESTAMP '2025-01-20 18:04:00'), -- 240s
(101, 'ST02', TIMESTAMP '2025-01-20 18:00:00', TIMESTAMP '2025-01-20 18:02:40'), -- 160s
(101, 'ST04', TIMESTAMP '2025-01-20 18:04:00', TIMESTAMP '2025-01-20 18:04:50'), -- 50s

-- Ticket 102 - Grill Slow
(102, 'ST01', TIMESTAMP '2025-01-20 18:05:00', TIMESTAMP '2025-01-20 18:11:00'), -- 360s (Slow)
(102, 'ST02', TIMESTAMP '2025-01-20 18:05:00', TIMESTAMP '2025-01-20 18:07:00'), -- 120s
(102, 'ST04', TIMESTAMP '2025-01-20 18:11:00', TIMESTAMP '2025-01-20 18:11:45'),

-- Ticket 103 - Salad Fast
(103, 'ST03', TIMESTAMP '2025-01-20 18:10:00', TIMESTAMP '2025-01-20 18:11:30'), -- 90s
(103, 'ST04', TIMESTAMP '2025-01-20 18:11:30', TIMESTAMP '2025-01-20 18:12:00'),

-- Ticket 104 - Grill Very Slow (Burned Burger)
(104, 'ST01', TIMESTAMP '2025-01-20 18:15:00', TIMESTAMP '2025-01-20 18:25:00'), -- 600s!!
(104, 'ST02', TIMESTAMP '2025-01-20 18:15:00', TIMESTAMP '2025-01-20 18:17:00'), 
(104, 'ST04', TIMESTAMP '2025-01-20 18:25:00', TIMESTAMP '2025-01-20 18:26:00'),

-- Bulk Tickets
(105, 'ST01', TIMESTAMP '2025-01-20 18:30:00', TIMESTAMP '2025-01-20 18:34:00'),
(105, 'ST02', TIMESTAMP '2025-01-20 18:30:00', TIMESTAMP '2025-01-20 18:32:00'),
(106, 'ST01', TIMESTAMP '2025-01-20 18:35:00', TIMESTAMP '2025-01-20 18:39:00'),
(106, 'ST02', TIMESTAMP '2025-01-20 18:35:00', TIMESTAMP '2025-01-20 18:37:00'),
(107, 'ST03', TIMESTAMP '2025-01-20 18:40:00', TIMESTAMP '2025-01-20 18:41:00'),
(108, 'ST01', TIMESTAMP '2025-01-20 18:45:00', TIMESTAMP '2025-01-20 18:50:00'), -- 300s
(109, 'ST01', TIMESTAMP '2025-01-20 18:50:00', TIMESTAMP '2025-01-20 18:56:00'), -- 360s
(110, 'ST02', TIMESTAMP '2025-01-20 19:00:00', TIMESTAMP '2025-01-20 19:03:00'),
(111, 'ST01', TIMESTAMP '2025-01-20 19:05:00', TIMESTAMP '2025-01-20 19:09:00'),
(112, 'ST01', TIMESTAMP '2025-01-20 19:10:00', TIMESTAMP '2025-01-20 19:14:00'),
(113, 'ST01', TIMESTAMP '2025-01-20 19:15:00', TIMESTAMP '2025-01-20 19:20:00'), -- 300s
(114, 'ST02', TIMESTAMP '2025-01-20 19:20:00', TIMESTAMP '2025-01-20 19:22:00'),
(115, 'ST03', TIMESTAMP '2025-01-20 19:25:00', TIMESTAMP '2025-01-20 19:27:00'),
(116, 'ST01', TIMESTAMP '2025-01-20 19:30:00', TIMESTAMP '2025-01-20 19:33:00'),
(117, 'ST01', TIMESTAMP '2025-01-20 19:35:00', TIMESTAMP '2025-01-20 19:40:00'), -- 300s
(118, 'ST01', TIMESTAMP '2025-01-20 19:40:00', TIMESTAMP '2025-01-20 19:44:00'),
(119, 'ST01', TIMESTAMP '2025-01-20 19:45:00', TIMESTAMP '2025-01-20 19:50:00'),
(120, 'ST02', TIMESTAMP '2025-01-20 19:50:00', TIMESTAMP '2025-01-20 19:53:00'),
(121, 'ST02', TIMESTAMP '2025-01-20 19:55:00', TIMESTAMP '2025-01-20 19:58:00'),
(122, 'ST02', TIMESTAMP '2025-01-20 20:00:00', TIMESTAMP '2025-01-20 20:02:00'),
(123, 'ST03', TIMESTAMP '2025-01-20 20:05:00', TIMESTAMP '2025-01-20 20:06:00'),
(124, 'ST03', TIMESTAMP '2025-01-20 20:10:00', TIMESTAMP '2025-01-20 20:11:00'),
(125, 'ST04', TIMESTAMP '2025-01-20 20:15:00', TIMESTAMP '2025-01-20 20:16:00'),
(126, 'ST04', TIMESTAMP '2025-01-20 20:20:00', TIMESTAMP '2025-01-20 20:21:00'),
(127, 'ST04', TIMESTAMP '2025-01-20 20:25:00', TIMESTAMP '2025-01-20 20:26:00'),
(128, 'ST04', TIMESTAMP '2025-01-20 20:30:00', TIMESTAMP '2025-01-20 20:31:00'),
(129, 'ST01', TIMESTAMP '2025-01-20 20:35:00', TIMESTAMP '2025-01-20 20:42:00'), -- 420s (Slow)
(130, 'ST01', TIMESTAMP '2025-01-20 20:40:00', TIMESTAMP '2025-01-20 20:45:00'),
(131, 'ST02', TIMESTAMP '2025-01-20 20:45:00', TIMESTAMP '2025-01-20 20:47:00'),
(132, 'ST02', TIMESTAMP '2025-01-20 20:50:00', TIMESTAMP '2025-01-20 20:52:00'),
(133, 'ST02', TIMESTAMP '2025-01-20 20:55:00', TIMESTAMP '2025-01-20 20:57:00'),
(134, 'ST03', TIMESTAMP '2025-01-20 21:00:00', TIMESTAMP '2025-01-20 21:01:00'),
(135, 'ST03', TIMESTAMP '2025-01-20 21:05:00', TIMESTAMP '2025-01-20 21:06:00'),
(136, 'ST01', TIMESTAMP '2025-01-20 21:10:00', TIMESTAMP '2025-01-20 21:14:00'),
(137, 'ST01', TIMESTAMP '2025-01-20 21:15:00', TIMESTAMP '2025-01-20 21:19:00'),
(138, 'ST02', TIMESTAMP '2025-01-20 21:20:00', TIMESTAMP '2025-01-20 21:22:00'),
(139, 'ST02', TIMESTAMP '2025-01-20 21:25:00', TIMESTAMP '2025-01-20 21:27:00'),
(140, 'ST02', TIMESTAMP '2025-01-20 21:30:00', TIMESTAMP '2025-01-20 21:32:00'),
(141, 'ST04', TIMESTAMP '2025-01-20 21:35:00', TIMESTAMP '2025-01-20 21:36:00'),
(142, 'ST04', TIMESTAMP '2025-01-20 21:40:00', TIMESTAMP '2025-01-20 21:41:00'),
(143, 'ST04', TIMESTAMP '2025-01-20 21:45:00', TIMESTAMP '2025-01-20 21:46:00'),
(144, 'ST04', TIMESTAMP '2025-01-20 21:50:00', TIMESTAMP '2025-01-20 21:51:00'),
(145, 'ST04', TIMESTAMP '2025-01-20 21:55:00', TIMESTAMP '2025-01-20 21:56:00'),
(146, 'ST01', TIMESTAMP '2025-01-20 22:00:00', TIMESTAMP '2025-01-20 22:05:00'),
(147, 'ST01', TIMESTAMP '2025-01-20 22:05:00', TIMESTAMP '2025-01-20 22:09:00'),
(148, 'ST02', TIMESTAMP '2025-01-20 22:10:00', TIMESTAMP '2025-01-20 22:12:00'),
(149, 'ST03', TIMESTAMP '2025-01-20 22:15:00', TIMESTAMP '2025-01-20 22:16:00'),
(150, 'ST04', TIMESTAMP '2025-01-20 22:20:00', TIMESTAMP '2025-01-20 22:21:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Station Performance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.StationPerformance AS
SELECT
    t.TicketID,
    c.StationName,
    c.TargetPrepSeconds,
    TIMESTAMPDIFF(SECOND, t.OrderTime, t.BumpTime) AS ActualPrepSeconds,
    (TIMESTAMPDIFF(SECOND, t.OrderTime, t.BumpTime) - c.TargetPrepSeconds) AS Variance
FROM ServicesDB.Bronze.TicketLogs t
JOIN ServicesDB.Bronze.StationConfig c ON t.StationID = c.StationID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Kitchen Bottlenecks
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.KitchenBottlenecks AS
SELECT
    StationName,
    COUNT(*) AS TotalOrders,
    AVG(ActualPrepSeconds) AS AvgPrepTime,
    SUM(CASE WHEN Variance > 0 THEN 1 ELSE 0 END) AS SlowOrders,
    MAX(ActualPrepSeconds) AS MaxPrepTime
FROM ServicesDB.Silver.StationPerformance
GROUP BY StationName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which kitchen station is consistently slower than target?"
    2. "List all orders where Grill took more than 5 minutes."
    3. "Calculate average prep time by Station."
*/
