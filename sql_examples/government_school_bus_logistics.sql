/*
    Dremio High-Volume SQL Pattern: Government School Bus Logistics
    
    Business Scenario:
    School Districts manage fleets of buses. "On-Time Performance" is crucial for student safety
    and maintaining parent satisfaction. Tracking fuel costs adds budget control.
    
    Data Story:
    We track Bus Routes and GPS Trip Logs.
    
    Medallion Architecture:
    - Bronze: Routes, TripLogs.
      *Volume*: 50+ records.
    - Silver: DelayMetrics (Actual vs Scheduled Arrival).
    - Gold: FleetScorecard (On-Time % per Route).
    
    Key Dremio Features:
    - Time Delta
    - Boolean Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentBusDB;
CREATE FOLDER IF NOT EXISTS GovernmentBusDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentBusDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentBusDB.Gold;
USE GovernmentBusDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentBusDB.Bronze.Routes (
    RouteID STRING,
    DriverName STRING,
    ScheduledArrivalTime TIME
);

INSERT INTO GovernmentBusDB.Bronze.Routes VALUES
('R101', 'Ms. Frizzle', TIME '08:00:00'),
('R102', 'Otto Mann', TIME '08:15:00'),
('R103', 'Speedy Gonzalez', TIME '07:45:00'),
('R104', 'Ken Kesey', TIME '08:00:00'),
('R105', 'Cameron Frye', TIME '08:30:00');

CREATE OR REPLACE TABLE GovernmentBusDB.Bronze.TripLogs (
    TripID STRING,
    RouteID STRING,
    Date DATE,
    ActualArrivalTime TIME,
    FuelConsumedGal DOUBLE
);

INSERT INTO GovernmentBusDB.Bronze.TripLogs VALUES
('T1', 'R101', DATE '2025-01-01', TIME '08:00:00', 5.0),
('T2', 'R102', DATE '2025-01-01', TIME '08:20:00', 6.0), -- Late 5m
('T3', 'R103', DATE '2025-01-01', TIME '07:45:00', 4.5),
('T4', 'R104', DATE '2025-01-01', TIME '08:00:00', 5.5),
('T5', 'R105', DATE '2025-01-01', TIME '08:35:00', 6.5), -- Late 5m
('T6', 'R101', DATE '2025-01-02', TIME '08:05:00', 5.2), -- Late 5m
('T7', 'R102', DATE '2025-01-02', TIME '08:15:00', 5.8),
('T8', 'R103', DATE '2025-01-02', TIME '07:40:00', 4.8), -- Early
('T9', 'R104', DATE '2025-01-02', TIME '08:10:00', 5.7), -- Late 10m
('T10', 'R105', DATE '2025-01-02', TIME '08:30:00', 6.2),
('T11', 'R101', DATE '2025-01-03', TIME '08:00:00', 5.0),
('T12', 'R102', DATE '2025-01-03', TIME '08:16:00', 6.1), -- Late 1m (On Time)
('T13', 'R103', DATE '2025-01-03', TIME '07:45:00', 4.6),
('T14', 'R104', DATE '2025-01-03', TIME '08:00:00', 5.4),
('T15', 'R105', DATE '2025-01-03', TIME '08:32:00', 6.4),
('T16', 'R101', DATE '2025-01-04', TIME '07:55:00', 5.1),
('T17', 'R102', DATE '2025-01-04', TIME '08:18:00', 5.9), -- Late 3m
('T18', 'R103', DATE '2025-01-04', TIME '07:50:00', 4.7), -- Late 5m
('T19', 'R104', DATE '2025-01-04', TIME '08:02:00', 5.6),
('T20', 'R105', DATE '2025-01-04', TIME '08:28:00', 6.3),
('T21', 'R101', DATE '2025-01-05', TIME '08:10:00', 5.3), -- Late 10m
('T22', 'R102', DATE '2025-01-05', TIME '08:15:00', 6.0),
('T23', 'R103', DATE '2025-01-05', TIME '07:45:00', 4.9),
('T24', 'R104', DATE '2025-01-05', TIME '07:58:00', 5.5),
('T25', 'R105', DATE '2025-01-05', TIME '08:40:00', 6.6), -- Late 10m
('T26', 'R101', DATE '2025-01-08', TIME '08:00:00', 5.0),
('T27', 'R102', DATE '2025-01-08', TIME '08:15:00', 5.8),
('T28', 'R103', DATE '2025-01-08', TIME '07:45:00', 4.5),
('T29', 'R104', DATE '2025-01-08', TIME '08:00:00', 5.4),
('T30', 'R105', DATE '2025-01-08', TIME '08:30:00', 6.2),
('T31', 'R101', DATE '2025-01-09', TIME '08:01:00', 5.1),
('T32', 'R102', DATE '2025-01-09', TIME '08:25:00', 6.2), -- Late 10m
('T33', 'R103', DATE '2025-01-09', TIME '07:44:00', 4.6),
('T34', 'R104', DATE '2025-01-09', TIME '07:59:00', 5.5),
('T35', 'R105', DATE '2025-01-09', TIME '08:31:00', 6.3),
('T36', 'R101', DATE '2025-01-10', TIME '07:58:00', 5.0),
('T37', 'R102', DATE '2025-01-10', TIME '08:14:00', 5.9),
('T38', 'R103', DATE '2025-01-10', TIME '07:55:00', 4.8), -- Late 10m
('T39', 'R104', DATE '2025-01-10', TIME '08:15:00', 5.8), -- Late 15m
('T40', 'R105', DATE '2025-01-10', TIME '08:29:00', 6.1),
('T41', 'R101', DATE '2025-01-11', TIME '08:00:00', 5.0),
('T42', 'R102', DATE '2025-01-11', TIME '08:15:00', 5.8),
('T43', 'R103', DATE '2025-01-11', TIME '07:45:00', 4.5),
('T44', 'R104', DATE '2025-01-11', TIME '08:00:00', 5.4),
('T45', 'R105', DATE '2025-01-11', TIME '08:30:00', 6.2),
('T46', 'R101', DATE '2025-01-12', TIME '08:02:00', 5.1),
('T47', 'R102', DATE '2025-01-12', TIME '08:17:00', 6.0), -- Late 2m
('T48', 'R103', DATE '2025-01-12', TIME '07:45:00', 4.6),
('T49', 'R104', DATE '2025-01-12', TIME '08:05:00', 5.6), -- Late 5m
('T50', 'R105', DATE '2025-01-12', TIME '08:35:00', 6.5); -- Late 5m


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Delay Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentBusDB.Silver.TripPerformance AS
SELECT
    t.TripID,
    t.RouteID,
    r.DriverName,
    t.Date,
    t.ActualArrivalTime,
    r.ScheduledArrivalTime,
    -- Simple check: if Actual > Scheduled + 5 mins buffer (300 seconds)
    CASE 
        WHEN t.ActualArrivalTime > DATE_ADD(r.ScheduledArrivalTime, INTERVAL '5' MINUTE) THEN 'Late'
        ELSE 'On Time'
    END AS Status
FROM GovernmentBusDB.Bronze.TripLogs t
JOIN GovernmentBusDB.Bronze.Routes r ON t.RouteID = r.RouteID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Fleet Scorecard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentBusDB.Gold.RouteReliability AS
SELECT
    RouteID,
    DriverName,
    COUNT(*) AS TotalTrips,
    SUM(CASE WHEN Status = 'Late' THEN 1 ELSE 0 END) AS LateTrips,
    (CAST(SUM(CASE WHEN Status = 'On Time' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100 AS OnTimePct
FROM GovernmentBusDB.Silver.TripPerformance
GROUP BY RouteID, DriverName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which driver has the highest on-time percentage?"
    2. "List all Late trips for Route R101."
    3. "Show total trips by Route."
*/
