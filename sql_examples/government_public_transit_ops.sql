/*
 * Government Public Transit Ops Demo
 * 
 * Scenario:
 * Monitoring vehicle on-time performance and ridership to optimize routes.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Transit;
CREATE FOLDER IF NOT EXISTS RetailDB.Transit.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Transit.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Transit.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Transit.Bronze.TripLogs (
    TripID INT,
    RouteID VARCHAR,
    VehicleID INT,
    ScheduledArrival TIMESTAMP,
    ActualArrival TIMESTAMP,
    PassengerCount INT
);

INSERT INTO RetailDB.Transit.Bronze.TripLogs VALUES
(1, 'R101', 501, '2025-01-01 08:00:00', '2025-01-01 08:00:00', 45), -- On Time
(2, 'R101', 502, '2025-01-01 08:15:00', '2025-01-01 08:18:00', 50), -- Late 3m
(3, 'R102', 601, '2025-01-01 08:00:00', '2025-01-01 08:05:00', 30), -- Late 5m
(4, 'R102', 602, '2025-01-01 08:30:00', '2025-01-01 08:30:00', 35),
(5, 'R103', 701, '2025-01-01 09:00:00', '2025-01-01 09:01:00', 60),
(6, 'R101', 501, '2025-01-01 09:00:00', '2025-01-01 09:10:00', 55), -- Late 10m
(7, 'R103', 702, '2025-01-01 09:15:00', '2025-01-01 09:15:00', 20),
(8, 'R102', 601, '2025-01-01 09:00:00', '2025-01-01 09:02:00', 25),
(9, 'R101', 503, '2025-01-01 08:30:00', '2025-01-01 08:29:00', 40), -- Early
(10, 'R104', 801, '2025-01-01 08:00:00', '2025-01-01 08:00:00', 10),
(11, 'R104', 802, '2025-01-01 08:30:00', '2025-01-01 08:35:00', 12);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Transit.Silver.RoutePerformance AS
SELECT 
    TripID,
    RouteID,
    ScheduledArrival,
    ActualArrival,
    DATE_DIFF(CAST(ActualArrival AS TIMESTAMP), CAST(ScheduledArrival AS TIMESTAMP)) AS DelaySeconds,
    PassengerCount,
    -- Delay > 2 minutes (120s) considered "Late"
    CASE 
        WHEN DATE_DIFF(CAST(ActualArrival AS TIMESTAMP), CAST(ScheduledArrival AS TIMESTAMP)) > 120 THEN 'Late'
        ELSE 'OnTime'
    END AS PerformanceStatus
FROM RetailDB.Transit.Bronze.TripLogs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Transit.Gold.RouteStats AS
SELECT 
    RouteID,
    COUNT(*) AS TotalTrips,
    AVG(DelaySeconds) AS AvgDelaySeconds,
    SUM(CASE WHEN PerformanceStatus = 'Late' THEN 1 ELSE 0 END) AS LateTrips,
    SUM(PassengerCount) AS TotalPassengers
FROM RetailDB.Transit.Silver.RoutePerformance
GROUP BY RouteID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Identify the route with the highest number of LateTrips in RetailDB.Transit.Gold.RouteStats."
*/
