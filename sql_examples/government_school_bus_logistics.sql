/*
    Dremio High-Volume SQL Pattern: Government School Bus Logistics
    
    Business Scenario:
    School Districts manage fleets of buses. "On-Time Performance" is crucial for student safety
    and manufacturing parent satisfaction. Tracking fuel costs adds budget control.
    
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
('R103', 'Speedy Gonzalez', TIME '07:45:00');

CREATE OR REPLACE TABLE GovernmentBusDB.Bronze.TripLogs (
    TripID STRING,
    RouteID STRING,
    Date DATE,
    ActualArrivalTime TIME,
    FuelConsumedGal DOUBLE
);

-- Bulk Trips
INSERT INTO GovernmentBusDB.Bronze.TripLogs
SELECT 
  'TRIP' || CAST(rn + 1000 AS STRING),
  CASE WHEN rn % 3 = 0 THEN 'R101' WHEN rn % 3 = 1 THEN 'R102' ELSE 'R103' END,
  DATE_SUB(DATE '2025-01-20', CAST((rn % 20) AS INT)),
  -- Generate times around 8am +/- 10 mins
  CASE 
    WHEN rn % 5 = 0 THEN TIME '08:10:00' -- Late
    ELSE TIME '07:55:00' -- Early/OnTime
  END,
  (rn % 10) + 5.0
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

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
    -- Simple check: if Actual > Scheduled + 5 mins buffer
    CASE 
        WHEN t.ActualArrivalTime > r.ScheduledArrivalTime THEN 'Late'
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
