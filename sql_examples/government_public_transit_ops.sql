/*
    Dremio High-Volume SQL Pattern: Government Public Transit Ops
    
    Business Scenario:
    Monitoring vehicle on-time performance and ridership to optimize routes.
    Tracking "Late" arrivals (> 2 mins) to improve scheduling.
    
    Data Story:
    We track Trip Logs and Scheduled vs Actual Arrival.
    
    Medallion Architecture:
    - Bronze: TripLogs.
      *Volume*: 50+ records.
    - Silver: RoutePerformance (Delay Calculation).
    - Gold: RouteStats (Late Trip % by Route).
    
    Key Dremio Features:
    - Date Diff
    - Conditional Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentTransitDB;
CREATE FOLDER IF NOT EXISTS GovernmentTransitDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentTransitDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentTransitDB.Gold;
USE GovernmentTransitDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentTransitDB.Bronze.TripLogs (
    TripID STRING,
    RouteID STRING,
    VehicleID STRING,
    ScheduledArrival TIMESTAMP,
    ActualArrival TIMESTAMP,
    PassengerCount INT
);

INSERT INTO GovernmentTransitDB.Bronze.TripLogs VALUES
('TP1', 'R101', 'V501', TIMESTAMP '2025-01-01 08:00:00', TIMESTAMP '2025-01-01 08:00:00', 45), -- On Time
('TP2', 'R101', 'V502', TIMESTAMP '2025-01-01 08:15:00', TIMESTAMP '2025-01-01 08:18:00', 50), -- Late 3m
('TP3', 'R102', 'V601', TIMESTAMP '2025-01-01 08:00:00', TIMESTAMP '2025-01-01 08:05:00', 30), -- Late 5m
('TP4', 'R102', 'V602', TIMESTAMP '2025-01-01 08:30:00', TIMESTAMP '2025-01-01 08:30:00', 35),
('TP5', 'R103', 'V701', TIMESTAMP '2025-01-01 09:00:00', TIMESTAMP '2025-01-01 09:01:00', 60),
('TP6', 'R101', 'V501', TIMESTAMP '2025-01-01 09:00:00', TIMESTAMP '2025-01-01 09:10:00', 55), -- Late 10m
('TP7', 'R103', 'V702', TIMESTAMP '2025-01-01 09:15:00', TIMESTAMP '2025-01-01 09:15:00', 20),
('TP8', 'R102', 'V601', TIMESTAMP '2025-01-01 09:00:00', TIMESTAMP '2025-01-01 09:02:00', 25),
('TP9', 'R101', 'V503', TIMESTAMP '2025-01-01 08:30:00', TIMESTAMP '2025-01-01 08:29:00', 40), -- Early
('TP10', 'R104', 'V801', TIMESTAMP '2025-01-01 08:00:00', TIMESTAMP '2025-01-01 08:00:00', 10),
('TP11', 'R104', 'V802', TIMESTAMP '2025-01-01 08:30:00', TIMESTAMP '2025-01-01 08:35:00', 12),
('TP12', 'R101', 'V501', TIMESTAMP '2025-01-01 10:00:00', TIMESTAMP '2025-01-01 10:01:00', 42),
('TP13', 'R101', 'V502', TIMESTAMP '2025-01-01 10:15:00', TIMESTAMP '2025-01-01 10:15:00', 48),
('TP14', 'R102', 'V601', TIMESTAMP '2025-01-01 10:00:00', TIMESTAMP '2025-01-01 10:03:00', 28),
('TP15', 'R102', 'V602', TIMESTAMP '2025-01-01 10:30:00', TIMESTAMP '2025-01-01 10:30:00', 32),
('TP16', 'R103', 'V701', TIMESTAMP '2025-01-01 11:00:00', TIMESTAMP '2025-01-01 11:00:00', 58),
('TP17', 'R103', 'V702', TIMESTAMP '2025-01-01 11:15:00', TIMESTAMP '2025-01-01 11:18:00', 22),
('TP18', 'R104', 'V801', TIMESTAMP '2025-01-01 10:00:00', TIMESTAMP '2025-01-01 10:05:00', 15),
('TP19', 'R104', 'V802', TIMESTAMP '2025-01-01 10:30:00', TIMESTAMP '2025-01-01 10:30:00', 18),
('TP20', 'R101', 'V501', TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-01 12:00:00', 40),
('TP21', 'R101', 'V502', TIMESTAMP '2025-01-01 12:15:00', TIMESTAMP '2025-01-01 12:20:00', 45), -- Late 5m
('TP22', 'R102', 'V601', TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-01 12:00:00', 30),
('TP23', 'R102', 'V602', TIMESTAMP '2025-01-01 12:30:00', TIMESTAMP '2025-01-01 12:35:00', 35), -- Late 5m
('TP24', 'R103', 'V701', TIMESTAMP '2025-01-01 13:00:00', TIMESTAMP '2025-01-01 13:02:00', 55),
('TP25', 'R104', 'V801', TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-01 12:00:00', 20),
('TP26', 'R101', 'V501', TIMESTAMP '2025-01-01 14:00:00', TIMESTAMP '2025-01-01 14:00:00', 38),
('TP27', 'R101', 'V502', TIMESTAMP '2025-01-01 14:15:00', TIMESTAMP '2025-01-01 14:15:00', 42),
('TP28', 'R102', 'V601', TIMESTAMP '2025-01-01 14:00:00', TIMESTAMP '2025-01-01 14:04:00', 25),
('TP29', 'R102', 'V602', TIMESTAMP '2025-01-01 14:30:00', TIMESTAMP '2025-01-01 14:30:00', 30),
('TP30', 'R103', 'V701', TIMESTAMP '2025-01-01 15:00:00', TIMESTAMP '2025-01-01 15:05:00', 65), -- Late 5m
('TP31', 'R101', 'V501', TIMESTAMP '2025-01-01 16:00:00', TIMESTAMP '2025-01-01 16:10:00', 60), -- Late 10m
('TP32', 'R101', 'V502', TIMESTAMP '2025-01-01 16:15:00', TIMESTAMP '2025-01-01 16:20:00', 55), -- Late 5m
('TP33', 'R102', 'V601', TIMESTAMP '2025-01-01 16:00:00', TIMESTAMP '2025-01-01 16:05:00', 35),
('TP34', 'R102', 'V602', TIMESTAMP '2025-01-01 16:30:00', TIMESTAMP '2025-01-01 16:35:00', 40),
('TP35', 'R103', 'V701', TIMESTAMP '2025-01-01 17:00:00', TIMESTAMP '2025-01-01 17:00:00', 70),
('TP36', 'R104', 'V801', TIMESTAMP '2025-01-01 16:00:00', TIMESTAMP '2025-01-01 16:00:00', 25),
('TP37', 'R101', 'V501', TIMESTAMP '2025-01-01 18:00:00', TIMESTAMP '2025-01-01 18:02:00', 45),
('TP38', 'R101', 'V502', TIMESTAMP '2025-01-01 18:15:00', TIMESTAMP '2025-01-01 18:15:00', 50),
('TP39', 'R102', 'V601', TIMESTAMP '2025-01-01 18:00:00', TIMESTAMP '2025-01-01 18:00:00', 30),
('TP40', 'R102', 'V602', TIMESTAMP '2025-01-01 18:30:00', TIMESTAMP '2025-01-01 18:30:00', 35),
('TP41', 'R103', 'V701', TIMESTAMP '2025-01-01 19:00:00', TIMESTAMP '2025-01-01 19:03:00', 50),
('TP42', 'R104', 'V801', TIMESTAMP '2025-01-01 18:00:00', TIMESTAMP '2025-01-01 18:00:00', 15),
('TP43', 'R101', 'V501', TIMESTAMP '2025-01-01 20:00:00', TIMESTAMP '2025-01-01 20:00:00', 30),
('TP44', 'R101', 'V502', TIMESTAMP '2025-01-01 20:15:00', TIMESTAMP '2025-01-01 20:15:00', 35),
('TP45', 'R102', 'V601', TIMESTAMP '2025-01-01 20:00:00', TIMESTAMP '2025-01-01 20:00:00', 20),
('TP46', 'R102', 'V602', TIMESTAMP '2025-01-01 20:30:00', TIMESTAMP '2025-01-01 20:30:00', 25),
('TP47', 'R103', 'V701', TIMESTAMP '2025-01-01 21:00:00', TIMESTAMP '2025-01-01 21:00:00', 40),
('TP48', 'R104', 'V801', TIMESTAMP '2025-01-01 20:00:00', TIMESTAMP '2025-01-01 20:00:00', 10),
('TP49', 'R101', 'V501', TIMESTAMP '2025-01-01 22:00:00', TIMESTAMP '2025-01-01 22:00:00', 20),
('TP50', 'R101', 'V502', TIMESTAMP '2025-01-01 22:15:00', TIMESTAMP '2025-01-01 22:15:00', 25);


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Route Performance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentTransitDB.Silver.RoutePerformance AS
SELECT 
    TripID,
    RouteID,
    ScheduledArrival,
    ActualArrival,
    TIMESTAMPDIFF(SECOND, ScheduledArrival, ActualArrival) AS DelaySeconds,
    PassengerCount,
    CASE 
        WHEN TIMESTAMPDIFF(SECOND, ScheduledArrival, ActualArrival) > 120 THEN 'Late'
        ELSE 'OnTime'
    END AS PerformanceStatus
FROM GovernmentTransitDB.Bronze.TripLogs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Route Statistics
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentTransitDB.Gold.RouteStats AS
SELECT 
    RouteID,
    COUNT(*) AS TotalTrips,
    AVG(DelaySeconds) AS AvgDelaySeconds,
    SUM(CASE WHEN PerformanceStatus = 'Late' THEN 1 ELSE 0 END) AS LateTrips,
    SUM(PassengerCount) AS TotalPassengers
FROM GovernmentTransitDB.Silver.RoutePerformance
GROUP BY RouteID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Identify the route with the highest number of LateTrips."
    2. "Show total passengers by Route."
    3. "Which route has the highest average delay?"
*/
